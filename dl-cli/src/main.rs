use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use clap::Parser;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use libdl::{
    DlClient, DownloadKind, DownloadOptions, DownloadPhase, DownloadProgress, DownloadSource,
    TorrentInput,
};
use tokio::sync::mpsc;

#[derive(Debug, Parser)]
#[command(
    name = "dl",
    version,
    about = "A lightweight HTTP and torrent download accelerator"
)]
struct Cli {
    /// URL, magnet link, or .torrent file to download.
    source: String,

    /// Output file for HTTP downloads, or output directory for torrents.
    #[arg(short, long)]
    output: Option<PathBuf>,

    /// Number of concurrent HTTP range workers.
    #[arg(short = 'j', long, default_value_t = 8)]
    connections: usize,

    /// HTTP chunk size. Supports plain bytes, K, M, and G suffixes.
    #[arg(long, default_value = "2M", value_parser = parse_size)]
    chunk_size: u64,

    /// Disable resumable inline metadata.
    #[arg(long)]
    no_resume: bool,

    /// Replace an existing output path.
    #[arg(long)]
    overwrite: bool,

    /// Treat an HTTP(S) source as a torrent URL instead of a plain HTTP file.
    #[arg(long)]
    torrent: bool,
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

async fn run() -> libdl::Result<()> {
    let cli = Cli::parse();
    let source = classify_source(&cli);
    let output = cli
        .output
        .clone()
        .unwrap_or_else(|| default_output_path(&source, &cli.source));

    let mut selected_files = None;

    if let DownloadSource::Torrent(ref torrent_input) = source {
        println!("Resolving torrent metadata...");
        let files = libdl::list_torrent_files(torrent_input.clone(), &output).await?;
        if files.is_empty() {
            println!("No files found in torrent.");
        } else if files.len() > 1 {
            selected_files = select_torrent_files_interactive(&files)?;
        }
    }

    let (progress_tx, progress_rx) = mpsc::unbounded_channel();
    let render_task = tokio::spawn(render_progress(progress_rx));

    let options = DownloadOptions {
        connections: cli.connections,
        chunk_size: cli.chunk_size,
        resume: !cli.no_resume,
        overwrite: cli.overwrite,
        only_files: selected_files,
        ..DownloadOptions::default()
    }
    .with_progress(progress_tx);

    let client = DlClient::new(options);
    let result = client.download(source, &output).await;
    drop(client);

    let _ = render_task.await;
    let summary = result?;
    println!(
        "downloaded {} bytes to {}",
        summary.downloaded_bytes,
        summary.output_path.display()
    );
    Ok(())
}

fn select_torrent_files_interactive(files: &[(usize, String, u64)]) -> libdl::Result<Option<Vec<usize>>> {
    use dialoguer::{theme::ColorfulTheme, Confirm, MultiSelect};

    let theme = ColorfulTheme::default();

    let choose = Confirm::with_theme(&theme)
        .with_prompt("This torrent contains multiple files. Do you want to choose which files to download?")
        .default(false)
        .interact()
        .map_err(|err| libdl::DlError::Torrent(format!("interactive prompt failed: {err}")))?;

    if !choose {
        return Ok(None);
    }

    let items: Vec<String> = files
        .iter()
        .map(|(_, name, size)| format!("{} ({})", name, format_bytes(*size)))
        .collect();

    let defaults = vec![true; files.len()];

    loop {
        let selections = MultiSelect::with_theme(&theme)
            .with_prompt("Select files to download (use Space key to toggle, Enter to confirm)")
            .items(&items)
            .defaults(&defaults)
            .interact()
            .map_err(|err| libdl::DlError::Torrent(format!("interactive selection failed: {err}")))?;

        if selections.is_empty() {
            let cancel = Confirm::with_theme(&theme)
                .with_prompt("No files selected. Do you want to cancel the download?")
                .default(true)
                .interact()
                .map_err(|err| libdl::DlError::Torrent(format!("interactive cancel prompt failed: {err}")))?;

            if cancel {
                return Err(libdl::DlError::Torrent("download cancelled by user".to_string()));
            } else {
                continue;
            }
        }

        let mut indices: Vec<usize> = selections.into_iter().map(|idx| files[idx].0).collect();
        indices.sort_unstable();
        indices.dedup();
        return Ok(Some(indices));
    }
}

fn classify_source(cli: &Cli) -> DownloadSource {
    if cli.torrent
        || cli.source.starts_with("magnet:")
        || (!cli.source.starts_with("http://")
            && !cli.source.starts_with("https://")
            && cli.source.ends_with(".torrent"))
    {
        DownloadSource::Torrent(TorrentInput::from_source(cli.source.clone()))
    } else {
        DownloadSource::Http(cli.source.clone())
    }
}

fn default_output_path(source: &DownloadSource, raw_source: &str) -> PathBuf {
    match source {
        DownloadSource::Torrent(_) => PathBuf::from("."),
        DownloadSource::Http(_) => url::Url::parse(raw_source)
            .ok()
            .and_then(|url| {
                url.path_segments()
                    .and_then(|mut segments| segments.next_back())
                    .filter(|segment| !segment.is_empty())
                    .map(PathBuf::from)
            })
            .unwrap_or_else(|| PathBuf::from("download.bin")),
    }
}

struct SpeedTracker {
    history: VecDeque<(Instant, u64)>,
    window_duration: Duration,
}

impl SpeedTracker {
    fn new(window_duration: Duration) -> Self {
        Self {
            history: VecDeque::new(),
            window_duration,
        }
    }

    fn add_sample(&mut self, bytes: u64) {
        let now = Instant::now();
        self.history.push_back((now, bytes));
        self.prune(now);
    }

    fn prune(&mut self, now: Instant) {
        let threshold = now.checked_sub(self.window_duration).unwrap_or(now);
        while self.history.len() > 1 && self.history[0].0 < threshold {
            self.history.pop_front();
        }
    }

    fn current_speed(&mut self) -> f64 {
        let now = Instant::now();
        self.prune(now);

        if self.history.len() < 2 {
            return 0.0;
        }

        let oldest = self.history.front().unwrap();
        let youngest = self.history.back().unwrap();

        // If the last update was too long ago, we've stalled/stopped
        if now.duration_since(youngest.0) > Duration::from_secs(2) {
            return 0.0;
        }

        let duration = youngest.0.duration_since(oldest.0);
        if duration.as_secs_f64() < 0.01 {
            return 0.0;
        }

        let bytes = youngest.1.saturating_sub(oldest.1);
        bytes as f64 / duration.as_secs_f64()
    }
}

async fn render_progress(mut progress_rx: libdl::ProgressReceiver) {
    let multi = MultiProgress::new();
    let overall = multi.add(ProgressBar::new_spinner());
    let status = multi.add(ProgressBar::new_spinner());

    let initial_bytes = Arc::new(AtomicU64::new(u64::MAX));

    // Sliding window speed tracker (3 seconds window)
    let tracker = Arc::new(Mutex::new(SpeedTracker::new(Duration::from_secs(3))));
    let tracker_for_rx = Arc::clone(&tracker);
    let tracker_for_speed = Arc::clone(&tracker);
    let tracker_for_eta = Arc::clone(&tracker);

    overall.set_style(
        ProgressStyle::with_template(
            "{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} {speed_resumed} {eta_resumed}",
        )
        .unwrap()
        .with_key("speed_resumed", move |_state: &indicatif::ProgressState, w: &mut dyn std::fmt::Write| {
            let mut t = tracker_for_speed.lock().unwrap();
            let speed = t.current_speed();
            let _ = write!(w, "{}/s", format_bytes(speed as u64));
        })
        .with_key("eta_resumed", move |state: &indicatif::ProgressState, w: &mut dyn std::fmt::Write| {
            let mut t = tracker_for_eta.lock().unwrap();
            let speed = t.current_speed();
            if speed < 1.0 {
                let _ = write!(w, "--:--:--");
            } else {
                let len = state.len().unwrap_or(state.pos());
                if len <= state.pos() {
                    let _ = write!(w, "0s");
                } else {
                    let remaining = len - state.pos();
                    let eta_secs = remaining as f64 / speed;
                    let _ = write!(w, "{}", format_duration(Duration::from_secs_f64(eta_secs)));
                }
            }
        })
        .progress_chars("=>-"),
    );
    status.set_style(ProgressStyle::with_template("{msg}").unwrap());

    while let Some(progress) = progress_rx.recv().await {
        if progress.phase == DownloadPhase::Downloading {
            let initial = progress.downloaded_bytes;
            let _ = initial_bytes.compare_exchange(u64::MAX, initial, Ordering::SeqCst, Ordering::SeqCst);
            
            let mut t = tracker_for_rx.lock().unwrap();
            t.add_sample(progress.downloaded_bytes);
        }

        update_overall(&overall, &progress);
        status.set_message(status_message(&progress));

        if progress.phase == DownloadPhase::Complete {
            overall.finish();
            status.finish_and_clear();
        }
    }

    overall.finish_and_clear();
    status.finish_and_clear();
}

fn format_duration(duration: Duration) -> String {
    let secs = duration.as_secs();
    if secs == 0 {
        return "0s".to_string();
    }
    let hours = secs / 3600;
    let minutes = (secs % 3600) / 60;
    let seconds = secs % 60;

    if hours > 0 {
        format!("{}h {}m", hours, minutes)
    } else if minutes > 0 {
        format!("{}m {}s", minutes, seconds)
    } else {
        format!("{}s", seconds)
    }
}

fn update_overall(overall: &ProgressBar, progress: &DownloadProgress) {
    if let Some(total) = progress.total_bytes {
        overall.set_length(total);
    }
    overall.set_position(progress.downloaded_bytes);

    if progress.total_bytes.is_none() {
        overall.set_message(format_bytes(progress.downloaded_bytes));
    }
}

fn status_message(progress: &DownloadProgress) -> String {
    let kind = match progress.kind {
        DownloadKind::Http => "http",
        DownloadKind::Torrent => "torrent",
    };
    let phase = match progress.phase {
        DownloadPhase::Probing => "probing",
        DownloadPhase::Downloading => "downloading",
        DownloadPhase::PersistingState => "saving state",
        DownloadPhase::Finalizing => "finalizing",
        DownloadPhase::Complete => "complete",
    };

    match (progress.completed_chunks, progress.total_chunks) {
        (Some(completed), Some(total)) => format!(
            "{kind}: {phase} | workers={} | chunks={completed}/{total} | {}",
            progress.active_workers,
            progress.output_path.display()
        ),
        _ => format!(
            "{kind}: {phase} | workers={} | {}",
            progress.active_workers,
            progress.output_path.display()
        ),
    }
}

fn parse_size(input: &str) -> Result<u64, String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err("size must not be empty".to_string());
    }

    let (number, multiplier) = match trimmed.as_bytes().last().copied() {
        Some(b'k') | Some(b'K') => (&trimmed[..trimmed.len() - 1], 1024),
        Some(b'm') | Some(b'M') => (&trimmed[..trimmed.len() - 1], 1024 * 1024),
        Some(b'g') | Some(b'G') => (&trimmed[..trimmed.len() - 1], 1024 * 1024 * 1024),
        _ => (trimmed, 1),
    };

    number
        .parse::<u64>()
        .map(|value| value.saturating_mul(multiplier))
        .map_err(|error| format!("invalid size `{input}`: {error}"))
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB"];
    let mut size = bytes as f64;
    let mut unit = 0;
    while size >= 1024.0 && unit < UNITS.len() - 1 {
        size /= 1024.0;
        unit += 1;
    }

    format!("{size:.1} {}", UNITS[unit])
}
