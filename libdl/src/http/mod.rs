use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use futures_util::StreamExt;
use reqwest::{
    header::{ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, ETAG, LAST_MODIFIED, RANGE},
    Client, Response, StatusCode,
};
use tokio::{
    fs::{self, OpenOptions},
    io::{AsyncSeekExt, AsyncWriteExt, BufWriter, SeekFrom},
    sync::Mutex,
    task::JoinSet,
};

use crate::{
    error::{DlError, Result},
    state::{clear_inline_state, read_inline_state, write_inline_state},
    types::{
        build_segments, chunk_count, DownloadKind, DownloadOptions, DownloadPhase,
        DownloadProgress, DownloadSummary, InlineDownloadState, ProgressSender, Segment, segment_len,
    },
};

/// Write-buffer capacity used when streaming a response body to disk. 1 MiB keeps
/// the number of write syscalls low on fast (gigabit) links without using much memory
/// per worker.
const WRITE_BUFFER_CAPACITY: usize = 1024 * 1024;

#[derive(Debug, Clone)]
struct HttpProbe {
    total_size: Option<u64>,
    ranges_supported: bool,
    etag: Option<String>,
    last_modified: Option<String>,
}

#[derive(Clone)]
struct WorkerContext {
    client: Client,
    url: String,
    output_path: PathBuf,
    queue: Arc<Mutex<VecDeque<Segment>>>,
    completed: Arc<Mutex<Vec<bool>>>,
    metadata_lock: Arc<Mutex<()>>,
    last_flush: Arc<Mutex<Instant>>,
    downloaded: Arc<AtomicU64>,
    completed_count: Arc<AtomicUsize>,
    active_workers: Arc<AtomicUsize>,
    total_size: u64,
    chunk_size: u64,
    total_chunks: usize,
    flush_interval: std::time::Duration,
    progress: Option<ProgressSender>,
    etag: Option<String>,
    last_modified: Option<String>,
}

pub async fn download_http(
    url: impl Into<String>,
    output: impl AsRef<Path>,
    options: DownloadOptions,
) -> Result<DownloadSummary> {
    let url = url.into();
    let (target_path, download_path) = crate::types::determine_download_paths(output.as_ref(), options.overwrite);
    let mut options = options.normalized();

    let client = Client::builder()
        .user_agent(options.user_agent.clone())
        .pool_max_idle_per_host(options.connections)
        .connect_timeout(Duration::from_secs(10))
        .read_timeout(Duration::from_secs(30))
        .tcp_nodelay(true)
        .http1_only()
        .build()?;

    emit_progress(
        &options.progress,
        DownloadPhase::Probing,
        &url,
        &download_path,
        0,
        None,
        0,
        None,
        None,
    );

    let probe = probe_server(&client, &url).await?;

    if options.resume {
        if let Ok(Some(state)) = read_inline_state(&download_path).await {
            if let Some(total_size) = probe.total_size {
                if state.version == 1
                    && state.kind == DownloadKind::Http
                    && crate::types::urls_are_compatible(&state.source, &url)
                    && state.total_size == total_size
                    && crate::types::weak_validator_matches(state.etag.as_deref(), probe.etag.as_deref())
                    && crate::types::weak_validator_matches(state.last_modified.as_deref(), probe.last_modified.as_deref())
                {
                    options.chunk_size = state.chunk_size;
                    tracing::info!(chunk_size = options.chunk_size, "Adopting chunk size from existing download state");
                }
            }
        }
    }

    let mut use_parallel = probe.ranges_supported && probe.total_size.is_some() && options.connections > 1;

    if use_parallel && options.resume {
        if let Ok(metadata) = fs::metadata(&download_path).await {
            if metadata.len() > 0 {
                // If the file exists and has size > 0, check if we have parallel state.
                // If not, it means the download was running as single-stream, so we should
                // continue as single-stream to avoid truncating existing progress.
                if let Ok(None) = read_inline_state(&download_path).await {
                    use_parallel = false;
                    tracing::info!("Found single-stream download in progress; continuing with single-stream");
                }
            }
        }
    }

    if use_parallel {
        if let Some(total_size) = probe.total_size {
            if options.chunk_size == crate::types::DEFAULT_CHUNK_SIZE {
                options.chunk_size = calculate_dynamic_chunk_size(total_size, options.connections);
                tracing::debug!(
                    total_size,
                    connections = options.connections,
                    chunk_size = options.chunk_size,
                    "Dynamically scaled chunk size for parallel download"
                );
            }
        }

        tracing::debug!("Starting parallel download");
        match download_parallel(url.clone(), download_path.clone(), options.clone(), client.clone(), probe.clone()).await {
            Ok(mut summary) => {
                fs::rename(&download_path, &target_path).await?;
                summary.output_path = target_path;
                return Ok(summary);
            }
            Err(error) => {
                if is_error_retryable(&error) {
                    tracing::warn!(error = %error, "Parallel download failed with retryable error; falling back to single stream");
                    if matches!(error, DlError::RateLimited { .. }) {
                        tracing::warn!("Waiting 3 seconds after rate limiting before single stream fallback...");
                        tokio::time::sleep(Duration::from_secs(3)).await;
                    }
                } else {
                    return Err(error);
                }
            }
        }
    }

    // Single-stream download (either as primary, or as fallback)
    let total_size = probe.total_size;
    let mut start_offset = 0;

    if options.resume {
        // 1. Try to read inline parallel state from the file
        if let Ok(Some(state)) = read_inline_state(&download_path).await {
            if let Some(total) = total_size {
                if state.is_compatible_with(
                    DownloadKind::Http,
                    &url,
                    total,
                    options.chunk_size,
                    probe.etag.as_deref(),
                    probe.last_modified.as_deref(),
                ) {
                    start_offset = contiguous_completed_bytes(&state.completed_chunks, total, options.chunk_size);
                    tracing::info!(start_offset, "Resuming single-stream download from parallel inline state");
                }
            }
        }
        // 2. If no parallel state, check if the file exists and ranges are supported
        if start_offset == 0 && probe.ranges_supported {
            if let Ok(metadata) = fs::metadata(&download_path).await {
                let file_len = metadata.len();
                if let Some(total) = total_size {
                    if file_len < total {
                        start_offset = file_len;
                        tracing::info!(start_offset, "Resuming single-stream download from existing file length");
                    }
                }
            }
        }
    }

    // Truncate the file to start_offset (removes any inline metadata or partial blocks at the end)
    if start_offset > 0 {
        if let Ok(file) = OpenOptions::new().write(true).open(&download_path).await {
            let _ = file.set_len(start_offset).await;
        }
    }

    let mut summary = download_single_stream(url, download_path.clone(), options, client, probe, start_offset).await?;
    fs::rename(&download_path, &target_path).await?;
    summary.output_path = target_path;
    Ok(summary)
}

async fn download_parallel(
    url: String,
    output_path: PathBuf,
    options: DownloadOptions,
    client: Client,
    probe: HttpProbe,
) -> Result<DownloadSummary> {
    let total_size = probe
        .total_size
        .ok_or_else(|| DlError::InvalidResponse("missing content length".to_string()))?;
    let total_chunks = chunk_count(total_size, options.chunk_size);

    let existing_state = if options.resume {
        read_inline_state(&output_path).await?
    } else {
        None
    };

    let mut resumed = false;
    let mut completed_chunks = vec![false; total_chunks];

    match existing_state {
        Some(state)
            if state.is_compatible_with(
                DownloadKind::Http,
                &url,
                total_size,
                options.chunk_size,
                probe.etag.as_deref(),
                probe.last_modified.as_deref(),
            ) && state.completed_chunks.len() == total_chunks =>
        {
            resumed = true;
            completed_chunks = state.completed_chunks;
        }
         _ => {}
    }

    let already_downloaded = completed_chunks
        .iter()
        .enumerate()
        .filter(|(_, completed)| **completed)
        .map(|(index, _)| crate::types::segment_len(index, total_size, options.chunk_size))
        .sum::<u64>();

    if !resumed {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&output_path)
            .await?;
        file.set_len(total_size).await?;
        file.sync_data().await?;
    }

    let missing_segments = build_segments(total_size, options.chunk_size, &completed_chunks);
    let queue = Arc::new(Mutex::new(VecDeque::from(missing_segments)));
    let completed = Arc::new(Mutex::new(completed_chunks));
    let downloaded = Arc::new(AtomicU64::new(already_downloaded));
    let completed_count = Arc::new(AtomicUsize::new(
        completed
            .lock()
            .await
            .iter()
            .filter(|completed| **completed)
            .count(),
    ));
    let active_workers = Arc::new(AtomicUsize::new(0));

    emit_progress(
        &options.progress,
        DownloadPhase::Downloading,
        &url,
        &output_path,
        downloaded.load(Ordering::Relaxed),
        Some(total_size),
        0,
        Some(completed_count.load(Ordering::Relaxed)),
        Some(total_chunks),
    );

    if completed_count.load(Ordering::Relaxed) == total_chunks {
        clear_inline_state(&output_path, total_size).await?;
        return Ok(DownloadSummary {
            kind: DownloadKind::Http,
            source: url,
            output_path,
            total_bytes: total_size,
            downloaded_bytes: total_size,
            resumed,
        });
    }

    let worker_count = options.connections.min(total_chunks).max(1);
    let context = WorkerContext {
        client,
        url: url.clone(),
        output_path: output_path.clone(),
        queue,
        completed,
        metadata_lock: Arc::new(Mutex::new(())),
        last_flush: Arc::new(Mutex::new(Instant::now())),
        downloaded,
        completed_count,
        active_workers,
        total_size,
        chunk_size: options.chunk_size,
        total_chunks,
        flush_interval: options.metadata_flush_interval,
        progress: options.progress.clone(),
        etag: probe.etag,
        last_modified: probe.last_modified,
    };

    let mut workers = JoinSet::new();
    for worker_id in 0..worker_count {
        let worker_context = context.clone();
        workers.spawn(async move { run_worker(worker_id, worker_context).await });
    }

    while let Some(result) = workers.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                workers.abort_all();
                return Err(error);
            }
            Err(error) => {
                workers.abort_all();
                return Err(error.into());
            }
        }
    }

    emit_progress(
        &options.progress,
        DownloadPhase::Finalizing,
        &url,
        &output_path,
        total_size,
        Some(total_size),
        0,
        Some(total_chunks),
        Some(total_chunks),
    );

    clear_inline_state(&output_path, total_size).await?;

    emit_progress(
        &options.progress,
        DownloadPhase::Complete,
        &url,
        &output_path,
        total_size,
        Some(total_size),
        0,
        Some(total_chunks),
        Some(total_chunks),
    );

    Ok(DownloadSummary {
        kind: DownloadKind::Http,
        source: url,
        output_path,
        total_bytes: total_size,
        downloaded_bytes: total_size,
        resumed,
    })
}

async fn run_worker(worker_id: usize, context: WorkerContext) -> Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .open(&context.output_path)
        .await?;

    loop {
        let Some(segment) = pop_segment(&context).await else {
            return Ok(());
        };

        context.active_workers.fetch_add(1, Ordering::Relaxed);
        let result = download_segment(worker_id, &context, &mut file, &segment).await;
        context.active_workers.fetch_sub(1, Ordering::Relaxed);
        result?;

        {
            let mut completed = context.completed.lock().await;
            completed[segment.index] = true;
        }

        let completed_count = context.completed_count.fetch_add(1, Ordering::Relaxed) + 1;
        let is_final = completed_count == context.total_chunks;
        let should_flush = {
            let mut last_flush = context.last_flush.lock().await;
            if last_flush.elapsed() >= context.flush_interval || is_final {
                *last_flush = Instant::now();
                true
            } else {
                false
            }
        };

        if should_flush {
            // Only force an fsync on the final checkpoint; intermediate checkpoints stay
            // off the hot path (see write_inline_state).
            persist_worker_state(&context, is_final).await?;
        }

        emit_progress(
            &context.progress,
            DownloadPhase::Downloading,
            &context.url,
            &context.output_path,
            context.downloaded.load(Ordering::Relaxed),
            Some(context.total_size),
            context.active_workers.load(Ordering::Relaxed),
            Some(completed_count),
            Some(context.total_chunks),
        );
    }
}

async fn pop_segment(context: &WorkerContext) -> Option<Segment> {
    let mut queue = context.queue.lock().await;
    queue.pop_front()
}

async fn download_segment(
    worker_id: usize,
    context: &WorkerContext,
    file: &mut fs::File,
    segment: &Segment,
) -> Result<()> {
    const MAX_ATTEMPTS: usize = 6;

    for attempt in 1..=MAX_ATTEMPTS {
        let outcome = match send_range_request(&context.client, &context.url, segment).await {
            Ok(response) => stream_response_to_file(context, file, segment, response).await,
            Err(error) => Err(error),
        };

        match outcome {
            Ok(()) => return Ok(()),
            Err(error) if attempt < MAX_ATTEMPTS && is_error_retryable(&error) => {
                let delay = match &error {
                    DlError::RateLimited { retry_after, .. } => {
                        retry_after.unwrap_or_else(|| {
                            Duration::from_secs(2 + attempt as u64)
                        })
                    }
                    _ => {
                        let base = 1000;
                        let ms = base * 2_u64.pow(attempt as u32 - 1);
                        Duration::from_millis(ms + get_jitter_ms())
                    }
                };
                tracing::warn!(
                    worker_id,
                    segment = segment.index,
                    attempt,
                    delay_ms = delay.as_millis(),
                    error = %error,
                    "retrying failed segment after delay"
                );
                tokio::time::sleep(delay).await;
            }
            Err(error) => return Err(error),
        }
    }

    unreachable!("attempt loop must return")
}

/// Sends a range request for `segment` and validates the response status. Returns the
/// response with headers received (body not yet read), or a mapped error. This is the
/// part that is prefetched/pipelined while a previous segment is still streaming.
async fn send_range_request(client: &Client, url: &str, segment: &Segment) -> Result<Response> {
    let range = format!("bytes={}-{}", segment.start, segment.end);
    let response = client.get(url).header(RANGE, range).send().await?;

    if response.status() != StatusCode::PARTIAL_CONTENT {
        let status = response.status();
        let headers = response.headers().clone();
        let desc = describe_status(
            response,
            &format!("range request for segment {}", segment.index),
        )
        .await;

        if status == StatusCode::TOO_MANY_REQUESTS {
            let retry_after = headers.get(reqwest::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())
                .map(Duration::from_secs);
            return Err(DlError::RateLimited {
                message: desc,
                retry_after,
            });
        } else if status.is_server_error() || status == StatusCode::REQUEST_TIMEOUT {
            return Err(DlError::ServerError(desc));
        } else {
            return Err(DlError::InvalidResponse(desc));
        }
    }

    Ok(response)
}

async fn stream_response_to_file(
    context: &WorkerContext,
    file: &mut fs::File,
    segment: &Segment,
    response: Response,
) -> Result<()> {
    file.seek(SeekFrom::Start(segment.start)).await?;
    let mut writer = BufWriter::with_capacity(WRITE_BUFFER_CAPACITY, file);

    let mut written = 0_u64;
    let mut stream = response.bytes_stream();
    let mut last_emitted_time = Instant::now();
    let mut chunk_counter = 0_u32;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        written += chunk.len() as u64;
        if written > segment.len() {
            return Err(DlError::InvalidResponse(format!(
                "segment {} exceeded expected length {}",
                segment.index,
                segment.len()
            )));
        }

        writer.write_all(&chunk).await?;
        let current_downloaded = context
            .downloaded
            .fetch_add(chunk.len() as u64, Ordering::Relaxed) + chunk.len() as u64;

        chunk_counter += 1;
        if chunk_counter % 32 == 0 {
            let now = Instant::now();
            if now.duration_since(last_emitted_time) >= Duration::from_millis(100) {
                last_emitted_time = now;
                emit_progress(
                    &context.progress,
                    DownloadPhase::Downloading,
                    &context.url,
                    &context.output_path,
                    current_downloaded,
                    Some(context.total_size),
                    context.active_workers.load(Ordering::Relaxed),
                    Some(context.completed_count.load(Ordering::Relaxed)),
                    Some(context.total_chunks),
                );
            }
        }
    }

    if written != segment.len() {
        return Err(DlError::InvalidResponse(format!(
            "segment {} wrote {written} bytes, expected {}",
            segment.index,
            segment.len()
        )));
    }

    writer.flush().await?;
    Ok(())
}

async fn persist_worker_state(context: &WorkerContext, sync_to_disk: bool) -> Result<()> {
    let _guard = context.metadata_lock.lock().await;
    let completed_chunks = context.completed.lock().await.clone();
    let mut state = InlineDownloadState::new(
        DownloadKind::Http,
        context.url.clone(),
        context.total_size,
        context.chunk_size,
        completed_chunks,
        context.etag.clone(),
        context.last_modified.clone(),
    );
    state.updated_at_unix_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    emit_progress(
        &context.progress,
        DownloadPhase::PersistingState,
        &context.url,
        &context.output_path,
        context.downloaded.load(Ordering::Relaxed),
        Some(context.total_size),
        context.active_workers.load(Ordering::Relaxed),
        Some(context.completed_count.load(Ordering::Relaxed)),
        Some(context.total_chunks),
    );

    write_inline_state(&context.output_path, context.total_size, &state, sync_to_disk).await
}

async fn download_single_stream(
    url: String,
    output_path: PathBuf,
    options: DownloadOptions,
    client: Client,
    probe: HttpProbe,
    start_offset: u64,
) -> Result<DownloadSummary> {

    let mut response = None;
    const MAX_SINGLE_STREAM_ATTEMPTS: usize = 5;
    for attempt in 1..=MAX_SINGLE_STREAM_ATTEMPTS {
        let req = if start_offset > 0 && probe.ranges_supported {
            client.get(&url).header(RANGE, format!("bytes={}-", start_offset))
        } else {
            client.get(&url)
        };
        let res = req.send().await;
        match res {
            Ok(resp) => {
                let status = resp.status();
                let expected_status = if start_offset > 0 && probe.ranges_supported {
                    StatusCode::PARTIAL_CONTENT
                } else {
                    StatusCode::OK
                };

                if status == expected_status || (start_offset == 0 && status.is_success()) || status == StatusCode::OK {
                    response = Some(resp);
                    break;
                } else {
                    let is_rate_limited = status == StatusCode::TOO_MANY_REQUESTS;
                    let retry_after = if is_rate_limited {
                        resp.headers().get(reqwest::header::RETRY_AFTER)
                            .and_then(|v| v.to_str().ok())
                            .and_then(|s| s.parse::<u64>().ok())
                            .map(Duration::from_secs)
                    } else {
                        None
                    };
                    let desc = describe_status(resp, "GET").await;
                    let error = if is_rate_limited {
                        DlError::RateLimited {
                            message: desc,
                            retry_after,
                        }
                    } else if status.is_server_error() || status == StatusCode::REQUEST_TIMEOUT {
                        DlError::ServerError(desc)
                    } else {
                        DlError::InvalidResponse(desc)
                    };

                    if attempt < MAX_SINGLE_STREAM_ATTEMPTS && is_error_retryable(&error) {
                        let delay = match &error {
                            DlError::RateLimited { retry_after, .. } => {
                                retry_after.unwrap_or_else(|| {
                                    let base = 2000;
                                    let ms = base * 2_u64.pow(attempt as u32 - 1);
                                    Duration::from_millis(ms + get_jitter_ms())
                                }).min(Duration::from_secs(30))
                            }
                            _ => {
                                let base = 1000;
                                let ms = base * 2_u64.pow(attempt as u32 - 1);
                                Duration::from_millis(ms + get_jitter_ms())
                            }
                        };
                        tracing::warn!(
                            attempt,
                            delay_ms = delay.as_millis(),
                            error = %error,
                            "GET request failed, retrying after delay"
                        );
                        tokio::time::sleep(delay).await;
                    } else {
                        return Err(error);
                    }
                }
            }
            Err(err) => {
                let error = DlError::Http(err);
                if attempt < MAX_SINGLE_STREAM_ATTEMPTS {
                    let base = 1000;
                    let ms = base * 2_u64.pow(attempt as u32 - 1);
                    let delay = Duration::from_millis(ms + get_jitter_ms());
                    tracing::warn!(
                        attempt,
                        delay_ms = delay.as_millis(),
                        error = %error,
                        "GET request connection failed, retrying after delay"
                    );
                    tokio::time::sleep(delay).await;
                } else {
                    return Err(error);
                }
            }
        }
    }

    let response = response.expect("response must be some on success");
    let actual_start_offset = if response.status() == StatusCode::PARTIAL_CONTENT {
        start_offset
    } else {
        0
    };

    let mut inner_file = OpenOptions::new()
        .create(true)
        .write(true)
        .open(&output_path)
        .await?;

    if actual_start_offset > 0 {
        inner_file.seek(SeekFrom::Start(actual_start_offset)).await?;
    } else {
        inner_file.set_len(0).await?;
    }

    let mut file = BufWriter::with_capacity(WRITE_BUFFER_CAPACITY, inner_file);

    let mut downloaded = actual_start_offset;
    let total_size = probe.total_size.or_else(|| {
        response.content_length().map(|len| len + actual_start_offset)
    });
    let mut stream = response.bytes_stream();
    let mut last_emitted_time = Instant::now();
    let mut chunk_counter = 0_u32;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        file.write_all(&chunk).await?;
        downloaded += chunk.len() as u64;

        chunk_counter += 1;
        if chunk_counter % 32 == 0 {
            let now = Instant::now();
            if now.duration_since(last_emitted_time) >= Duration::from_millis(100) {
                last_emitted_time = now;
                emit_progress(
                    &options.progress,
                    DownloadPhase::Downloading,
                    &url,
                    &output_path,
                    downloaded,
                    total_size,
                    1,
                    None,
                    None,
                );
            }
        }
    }

    file.flush().await?;
    file.get_ref().sync_data().await?;

    emit_progress(
        &options.progress,
        DownloadPhase::Complete,
        &url,
        &output_path,
        downloaded,
        total_size,
        0,
        None,
        None,
    );

    Ok(DownloadSummary {
        kind: DownloadKind::Http,
        source: url,
        output_path,
        total_bytes: total_size.unwrap_or(downloaded),
        downloaded_bytes: downloaded,
        resumed: actual_start_offset > 0,
    })
}

async fn probe_server(client: &Client, url: &str) -> Result<HttpProbe> {
    let mut probe = HttpProbe {
        total_size: None,
        ranges_supported: false,
        etag: None,
        last_modified: None,
    };

    let range_response = client.get(url).header(RANGE, "bytes=0-0").send().await?;
    if range_response.status() == StatusCode::PARTIAL_CONTENT {
        probe.ranges_supported = true;
        probe.total_size = probe
            .total_size
            .or_else(|| parse_content_range_total(range_response.headers().get(CONTENT_RANGE)));
        probe.etag = header_string(range_response.headers().get(ETAG));
        probe.last_modified = header_string(range_response.headers().get(LAST_MODIFIED));
        return Ok(probe);
    } else if range_response.status().is_success() {
        probe.total_size = probe
            .total_size
            .or_else(|| header_u64(range_response.headers().get(CONTENT_LENGTH)));
        probe.etag = header_string(range_response.headers().get(ETAG));
        probe.last_modified = header_string(range_response.headers().get(LAST_MODIFIED));
        return Ok(probe);
    } else {
        tracing::debug!(
            status = %range_response.status(),
            "range probe failed; trying metadata fallbacks"
        );
    }

    if let Ok(response) = client.head(url).send().await {
        if response.status().is_success() {
            probe.total_size = header_u64(response.headers().get(CONTENT_LENGTH));
            probe.ranges_supported = response
                .headers()
                .get(ACCEPT_RANGES)
                .and_then(|value| value.to_str().ok())
                .map(|value| value.to_ascii_lowercase().contains("bytes"))
                .unwrap_or(false);
            probe.etag = header_string(response.headers().get(ETAG));
            probe.last_modified = header_string(response.headers().get(LAST_MODIFIED));
            return Ok(probe);
        }
    }

    let response = client.get(url).send().await?;
    if response.status().is_success() {
        probe.total_size = header_u64(response.headers().get(CONTENT_LENGTH));
        probe.ranges_supported = response
            .headers()
            .get(ACCEPT_RANGES)
            .and_then(|value| value.to_str().ok())
            .map(|value| value.to_ascii_lowercase().contains("bytes"))
            .unwrap_or(false);
        probe.etag = header_string(response.headers().get(ETAG));
        probe.last_modified = header_string(response.headers().get(LAST_MODIFIED));
        return Ok(probe);
    }

    Ok(probe)
}

async fn describe_status(response: Response, context: &str) -> String {
    let status = response.status();
    let server = header_string(response.headers().get("server"));
    let mitigation = header_string(response.headers().get("cf-mitigated"));
    let body = response.text().await.ok();

    let mut message = format!("{context} returned {status}");

    if let Some(server) = server {
        message.push_str(&format!(" (server: {server})"));
    }

    if let Some(mitigation) = mitigation {
        message.push_str(&format!("; Cloudflare mitigation: {mitigation}"));
    }

    if let Some(body_error) = body.as_deref().and_then(extract_response_error) {
        message.push_str(&format!("; response error: {body_error}"));
    }

    message
}

fn extract_response_error(body: &str) -> Option<String> {
    let code = xml_tag(body, "Code")?;
    let message = xml_tag(body, "Message");
    let canonical = xml_tag(body, "CanonicalRequest");
    let string_to_sign = xml_tag(body, "StringToSign");

    let mut err = match message {
        Some(message) => format!("{code}: {message}"),
        None => code.to_string(),
    };

    if let Some(canonical) = canonical {
        err.push_str(&format!(
            "\nCanonicalRequest calculated by S3:\n{canonical}"
        ));
    }
    if let Some(string_to_sign) = string_to_sign {
        err.push_str(&format!(
            "\nStringToSign calculated by S3:\n{string_to_sign}"
        ));
    }

    Some(err)
}

fn xml_tag<'a>(body: &'a str, tag: &str) -> Option<&'a str> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = body.find(&open)? + open.len();
    let end = body[start..].find(&close)? + start;
    Some(body[start..end].trim())
}

fn emit_progress(
    progress: &Option<ProgressSender>,
    phase: DownloadPhase,
    source: &str,
    output_path: &Path,
    downloaded_bytes: u64,
    total_bytes: Option<u64>,
    active_workers: usize,
    completed_chunks: Option<usize>,
    total_chunks: Option<usize>,
) {
    if let Some(progress) = progress {
        let _ = progress.send(DownloadProgress {
            kind: DownloadKind::Http,
            phase,
            source: source.to_string(),
            output_path: output_path.to_path_buf(),
            downloaded_bytes,
            total_bytes,
            active_workers,
            completed_chunks,
            total_chunks,
        });
    }
}

fn header_u64(value: Option<&reqwest::header::HeaderValue>) -> Option<u64> {
    value?.to_str().ok()?.parse().ok()
}

fn header_string(value: Option<&reqwest::header::HeaderValue>) -> Option<String> {
    value?.to_str().ok().map(ToOwned::to_owned)
}

fn parse_content_range_total(value: Option<&reqwest::header::HeaderValue>) -> Option<u64> {
    let value = value?.to_str().ok()?;
    let (_, total) = value.rsplit_once('/')?;
    if total == "*" {
        return None;
    }

    total.parse().ok()
}

fn get_jitter_ms() -> u64 {
    use std::time::SystemTime;
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let seed = now as u64;
    let next = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
    next % 500 // up to 500ms of jitter
}

fn is_error_retryable(error: &DlError) -> bool {
    match error {
        DlError::RateLimited { .. } => true,
        DlError::ServerError(_) => true,
        DlError::Http(_) => true,
        DlError::InvalidResponse(msg) => {
            if msg.contains("returned 4") {
                msg.contains("returned 408") || msg.contains("returned 429")
            } else {
                true
            }
        }
        _ => false,
    }
}

fn calculate_dynamic_chunk_size(total_size: u64, connections: usize) -> u64 {
    let connections = connections.max(1);

    // Choose chunk boundaries based on total file size to fit modern gigabit/5G networks.
    // For smaller files, we keep chunk sizes small to allow faster start and fine-grained updates.
    // For large files, we use very large chunk sizes to minimize connection/HTTP overhead and maintain maximum TCP speed.
    let (min_chunk_size, max_chunk_size) = if total_size < 10 * 1024 * 1024 {
        (1 * 1024 * 1024, 5 * 1024 * 1024) // < 10MB -> 1MB to 5MB chunks
    } else if total_size < 100 * 1024 * 1024 {
        (4 * 1024 * 1024, 25 * 1024 * 1024) // 10MB - 100MB -> 4MB to 25MB chunks
    } else if total_size < 1 * 1024 * 1024 * 1024 {
        (16 * 1024 * 1024, 128 * 1024 * 1024) // 100MB - 1GB -> 16MB to 128MB chunks
    } else if total_size < 10 * 1024 * 1024 * 1024 {
        (64 * 1024 * 1024, 512 * 1024 * 1024) // 1GB - 10GB -> 64MB to 512MB chunks
    } else {
        (256 * 1024 * 1024, 1 * 1024 * 1024 * 1024) // >= 10GB -> 256MB to 1GB chunks
    };

    // Target 2 chunks per connection. This ensures very long-lived TCP streams
    // that can fully utilize TCP congestion control (Cubic/BBR), while still leaving
    // 2 chunks per worker so faster connections can "steal" a second chunk of work
    // if a worker stalls or starts late.
    let target_chunks = (connections as u64).saturating_mul(2);
    let calculated = if target_chunks > 0 {
        total_size / target_chunks
    } else {
        total_size
    };

    calculated.max(min_chunk_size).min(max_chunk_size)
}

fn contiguous_completed_bytes(completed_chunks: &[bool], total_size: u64, chunk_size: u64) -> u64 {
    let mut contiguous_chunks = 0;
    for &completed in completed_chunks {
        if completed {
            contiguous_chunks += 1;
        } else {
            break;
        }
    }

    let mut completed_bytes = 0;
    for i in 0..contiguous_chunks {
        completed_bytes += segment_len(i, total_size, chunk_size);
    }
    completed_bytes
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::tempdir;
    use tokio::fs;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        sync::oneshot,
    };

    use crate::{state::read_inline_state, types::DownloadOptions};

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn parallel_download_writes_file_and_clears_inline_state() {
        let data: Vec<u8> = (0..128_u32)
            .flat_map(|value| value.to_be_bytes())
            .cycle()
            .take(512 * 1024)
            .collect();
        let (base_url, shutdown) = spawn_range_server(data.clone()).await;
        let dir = tempdir().unwrap();
        let output = dir.path().join("payload.bin");

        let summary = download_http(
            format!("{base_url}/payload.bin"),
            &output,
            DownloadOptions {
                connections: 4,
                chunk_size: 32 * 1024,
                overwrite: true,
                ..DownloadOptions::default()
            },
        )
        .await
        .unwrap();

        assert_eq!(summary.total_bytes, data.len() as u64);
        assert_eq!(fs::read(&output).await.unwrap(), data);
        assert!(read_inline_state(&output).await.unwrap().is_none());
        let _ = shutdown.send(());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn fallback_to_single_stream_on_rate_limit() {
        let data: Vec<u8> = (0..128_u32)
            .flat_map(|value| value.to_be_bytes())
            .cycle()
            .take(128 * 1024)
            .collect();
        let (base_url, shutdown) = spawn_range_server(data.clone()).await;
        let dir = tempdir().unwrap();
        let output = dir.path().join("rate-limited.bin");

        let summary = download_http(
            format!("{base_url}/rate-limited.bin"),
            &output,
            DownloadOptions {
                connections: 4,
                chunk_size: 32 * 1024,
                overwrite: true,
                ..DownloadOptions::default()
            },
        )
        .await
        .unwrap();

        assert_eq!(summary.total_bytes, data.len() as u64);
        assert_eq!(fs::read(&output).await.unwrap(), data);
        assert!(read_inline_state(&output).await.unwrap().is_none());
        let _ = shutdown.send(());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn resumes_from_existing_dl_file() {
        let data: Vec<u8> = (0..128_u32)
            .flat_map(|value| value.to_be_bytes())
            .cycle()
            .take(256 * 1024)
            .collect();
        let (base_url, shutdown) = spawn_range_server(data.clone()).await;
        let dir = tempdir().unwrap();
        let output = dir.path().join("resume_test.bin");
        let output_dl = dir.path().join("resume_test.bin.dl");

        let chunk_size = 128 * 1024;
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&output_dl)
            .await
            .unwrap();
        file.write_all(&data[..chunk_size]).await.unwrap();
        file.set_len(data.len() as u64).await.unwrap();
        file.flush().await.unwrap();
        drop(file);

        let completed_chunks = vec![true, false];
        let state = crate::InlineDownloadState::new(
            crate::DownloadKind::Http,
            format!("{base_url}/resume_test.bin"),
            data.len() as u64,
            chunk_size as u64,
            completed_chunks,
            Some("\"test\"".to_string()),
            None,
        );
        crate::state::write_inline_state(&output_dl, data.len() as u64, &state, true).await.unwrap();

        let summary = download_http(
            format!("{base_url}/resume_test.bin"),
            &output,
            DownloadOptions {
                connections: 4,
                chunk_size: chunk_size as u64,
                overwrite: true,
                resume: true,
                ..DownloadOptions::default()
            },
        )
        .await
        .unwrap();

        assert_eq!(summary.total_bytes, data.len() as u64);
        assert!(summary.resumed);
        assert_eq!(fs::read(&output).await.unwrap(), data);
        assert!(!output_dl.exists());
        assert!(read_inline_state(&output).await.unwrap().is_none());
        let _ = shutdown.send(());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn resumes_from_existing_single_stream_file() {
        let data: Vec<u8> = (0..128_u32)
            .flat_map(|value| value.to_be_bytes())
            .cycle()
            .take(256 * 1024)
            .collect();
        let (base_url, shutdown) = spawn_range_server(data.clone()).await;
        let dir = tempdir().unwrap();
        let output = dir.path().join("resume_single_test.bin");
        let output_dl = dir.path().join("resume_single_test.bin.dl");

        // Create an incomplete file without any inline parallel state
        let chunk_size = 128 * 1024;
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&output_dl)
            .await
            .unwrap();
        file.write_all(&data[..chunk_size]).await.unwrap();
        file.flush().await.unwrap();
        drop(file);

        let summary = download_http(
            format!("{base_url}/resume_single_test.bin"),
            &output,
            DownloadOptions {
                connections: 4,
                overwrite: true,
                resume: true,
                ..DownloadOptions::default()
            },
        )
        .await
        .unwrap();

        assert_eq!(summary.total_bytes, data.len() as u64);
        assert!(summary.resumed);
        assert_eq!(fs::read(&output).await.unwrap(), data);
        assert!(!output_dl.exists());
        let _ = shutdown.send(());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn resumes_from_existing_dl_file_adopts_chunk_size() {
        let data: Vec<u8> = (0..128_u32)
            .flat_map(|value| value.to_be_bytes())
            .cycle()
            .take(256 * 1024)
            .collect();
        let (base_url, shutdown) = spawn_range_server(data.clone()).await;
        let dir = tempdir().unwrap();
        let output = dir.path().join("resume_adopt_test.bin");
        let output_dl = dir.path().join("resume_adopt_test.bin.dl");

        let chunk_size = 128 * 1024;
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&output_dl)
            .await
            .unwrap();
        file.write_all(&data[..chunk_size]).await.unwrap();
        file.set_len(data.len() as u64).await.unwrap();
        file.flush().await.unwrap();
        drop(file);

        let completed_chunks = vec![true, false];
        let state = crate::InlineDownloadState::new(
            crate::DownloadKind::Http,
            format!("{base_url}/resume_adopt_test.bin"),
            data.len() as u64,
            chunk_size as u64,
            completed_chunks,
            Some("\"test\"".to_string()),
            None,
        );
        crate::state::write_inline_state(&output_dl, data.len() as u64, &state, true).await.unwrap();

        // Download without specifying chunk_size (so it defaults to DEFAULT_CHUNK_SIZE which is 2M).
        // It should adopt the chunk_size (128K) from the existing state!
        let summary = download_http(
            format!("{base_url}/resume_adopt_test.bin"),
            &output,
            DownloadOptions {
                connections: 4,
                overwrite: true,
                resume: true,
                ..DownloadOptions::default()
            },
        )
        .await
        .unwrap();

        assert_eq!(summary.total_bytes, data.len() as u64);
        assert!(summary.resumed);
        assert_eq!(fs::read(&output).await.unwrap(), data);
        assert!(!output_dl.exists());
        assert!(read_inline_state(&output).await.unwrap().is_none());
        let _ = shutdown.send(());
    }

    async fn spawn_range_server(data: Vec<u8>) -> (String, oneshot::Sender<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        let data = Arc::new(data);

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => break,
                    accepted = listener.accept() => {
                        let Ok((stream, _)) = accepted else {
                            break;
                        };
                        let data = Arc::clone(&data);
                        tokio::spawn(async move {
                            let _ = handle_connection(stream, data).await;
                        });
                    }
                }
            }
        });

        (format!("http://{address}"), shutdown_tx)
    }

    async fn handle_connection(mut stream: TcpStream, data: Arc<Vec<u8>>) -> std::io::Result<()> {
        let mut buffer = vec![0_u8; 8192];
        let mut read = 0;
        loop {
            let bytes = stream.read(&mut buffer[read..]).await?;
            if bytes == 0 {
                return Ok(());
            }
            read += bytes;
            if read >= 4
                && buffer[..read]
                    .windows(4)
                    .any(|window| window == b"\r\n\r\n")
            {
                break;
            }
        }

        let request = String::from_utf8_lossy(&buffer[..read]);
        if request.starts_with("HEAD ") {
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nETag: \"test\"\r\n\r\n",
                data.len()
            );
            stream.write_all(response.as_bytes()).await?;
            return Ok(());
        }

        let range = parse_range_header(&request);

        if request.contains("/rate-limited.bin") {
            if let Some((start, end_opt)) = range {
                let end = end_opt.unwrap_or(0);
                if start == 0 && end == 0 {
                    let body = &data[0..=0];
                    let response = format!(
                        "HTTP/1.1 206 Partial Content\r\nContent-Length: 1\r\nContent-Range: bytes 0-0/{}\r\nAccept-Ranges: bytes\r\nETag: \"test\"\r\n\r\n",
                        data.len()
                    );
                    stream.write_all(response.as_bytes()).await?;
                    stream.write_all(body).await?;
                } else {
                    let response = "HTTP/1.1 429 Too Many Requests\r\nContent-Length: 0\r\n\r\n";
                    stream.write_all(response.as_bytes()).await?;
                }
            } else {
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nETag: \"test\"\r\n\r\n",
                    data.len()
                );
                stream.write_all(response.as_bytes()).await?;
                stream.write_all(&data).await?;
            }
            return Ok(());
        }

        match range {
            Some((start, Some(end))) => {
                let end = end.min(data.len() - 1);
                let body = &data[start..=end];
                let response = format!(
                    "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {}-{}/{}\r\nAccept-Ranges: bytes\r\nETag: \"test\"\r\n\r\n",
                    body.len(),
                    start,
                    end,
                    data.len()
                );
                stream.write_all(response.as_bytes()).await?;
                stream.write_all(body).await?;
            }
            Some((start, None)) => {
                let end = data.len() - 1;
                let body = &data[start..=end];
                let response = format!(
                    "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {}-{}/{}\r\nAccept-Ranges: bytes\r\nETag: \"test\"\r\n\r\n",
                    body.len(),
                    start,
                    end,
                    data.len()
                );
                stream.write_all(response.as_bytes()).await?;
                stream.write_all(body).await?;
            }
            None => {
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nETag: \"test\"\r\n\r\n",
                    data.len()
                );
                stream.write_all(response.as_bytes()).await?;
                stream.write_all(&data).await?;
            }
        }

        Ok(())
    }

    fn parse_range_header(request: &str) -> Option<(usize, Option<usize>)> {
        let range_line = request
            .lines()
            .find(|line| line.to_ascii_lowercase().starts_with("range: bytes="))?;
        let (_, range) = range_line.split_once('=')?;
        let (start, end) = range.trim().split_once('-')?;
        let start_val = start.parse().ok()?;
        let end_val = if end.is_empty() {
            None
        } else {
            Some(end.parse().ok()?)
        };
        Some((start_val, end_val))
    }

    #[test]
    fn test_calculate_dynamic_chunk_size() {
        // Test very small files (< 10MB)
        assert_eq!(calculate_dynamic_chunk_size(4 * 1024 * 1024, 8), 1 * 1024 * 1024); // 4MB / 16 = 256KB -> capped at min 1MB
        assert_eq!(calculate_dynamic_chunk_size(4 * 1024 * 1024, 1), 2 * 1024 * 1024); // 4MB / 2 = 2MB

        // Test small-to-medium files (10MB - 100MB)
        assert_eq!(calculate_dynamic_chunk_size(40 * 1024 * 1024, 8), 4 * 1024 * 1024); // 40MB / 16 = 2.5MB -> capped at min 4MB
        assert_eq!(calculate_dynamic_chunk_size(80 * 1024 * 1024, 4), 10 * 1024 * 1024); // 80MB / 8 = 10MB

        // Test medium files (100MB - 1GB)
        assert_eq!(calculate_dynamic_chunk_size(500 * 1024 * 1024, 8), 32000 * 1024); // 500MB / 16 = 31.25MB (which is 32000 KiB)

        // Test large files (1GB - 10GB)
        assert_eq!(calculate_dynamic_chunk_size(4 * 1024 * 1024 * 1024, 8), 256 * 1024 * 1024); // 4GB / 16 = 256MB

        // Test very large files (>= 10GB)
        assert_eq!(calculate_dynamic_chunk_size(10 * 1024 * 1024 * 1024, 8), 640 * 1024 * 1024); // 10GB / 16 = 640MB
        assert_eq!(calculate_dynamic_chunk_size(20 * 1024 * 1024 * 1024, 8), 1 * 1024 * 1024 * 1024); // 20GB / 16 = 1.25GB -> capped at max 1GB
    }
}
