# libdl

`libdl` is a robust, high-performance downloader library for Rust, designed to support parallel segmented HTTP downloads and BitTorrent downloads with seamless resume support and real-time progress tracking.

It is built on top of `reqwest` and `tokio` for HTTP downloads, and `librqbit` for optional BitTorrent support.

---

## Features

- **Parallel Segmented Downloads**: Downloads files in multiple parallel segments (chunks) over HTTP to maximize bandwidth usage.
- **Dynamic Connection Scaling**: Automatically and adaptively scales the number of parallel HTTP connections based on network conditions and speed improvements to achieve optimal throughput without congestion.
- **Dynamic Chunk Sizing**: Automatically selects optimal chunk sizes based on overall file size to minimize HTTP overhead on modern high-speed networks.
- **Resumable Downloads**: Features a robust, crash-resilient resuming mechanism. State is persisted inline directly at the end of the temporary `.dl` file, eliminating disk pollution with extra tracking files.
- **BitTorrent Support (Optional)**: Full support for downloading magnets, `.torrent` files, and torrent URLs, including listing multi-file torrents and downloading specific files.
- **Real-Time Progress Tracking**: Emits fine-grained progress updates through a non-blocking `tokio` channel, including current phase, downloaded bytes, speed indicators, active workers, and more.

---

## Installation

Add `libdl` to your `Cargo.toml` dependencies by pointing directly to the GitHub repository:

```toml
[dependencies]
libdl = { git = "https://github.com/gkpln3/dl", package = "libdl" }
```

### Feature Flags

- `torrent` (enabled by default): Enables BitTorrent downloading and metadata querying capabilities. If disabled, torrent functions will return an error, and BitTorrent dependencies (`librqbit`) will be omitted.

To disable torrent support and minimize dependencies:
```toml
[dependencies]
libdl = { git = "https://github.com/gkpln3/dl", package = "libdl", default-features = false }
```

---

## Quick Start

### 1. Simple HTTP Download

The easiest way to download a file is using `DlClient` with default options:

```rust
use libdl::{DlClient, DownloadSource, Result};

#[tokio::main]
async fn main() -> Result<()> {
    // Create a default client
    let client = DlClient::default();

    // Start downloading
    let source = DownloadSource::Http("https://example.com/large-file.zip".to_string());
    let summary = client.download(source, "large-file.zip").await?;

    println!("Download complete!");
    println!("File size: {} bytes", summary.total_bytes);
    println!("Resumed: {}", summary.resumed);

    Ok(())
}
```

### 2. HTTP Download with Progress Reporting & Dynamic Scaling

By leaving `connections` as `None` (or configuring it with `with_progress`), `libdl` will scale the worker pool dynamically up to 32 connections depending on speed feedback.

```rust
use std::time::Duration;
use libdl::{DlClient, DownloadOptions, DownloadSource, Result};
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> Result<()> {
    // Create an unbounded channel for progress reporting
    let (tx, mut rx) = mpsc::unbounded_channel();

    // Configure options
    let options = DownloadOptions::default()
        .with_progress(tx)
        // Leaving connections as None enables Dynamic Connection Scaling!
        // It starts with 4 workers and scales up/down adaptively.
        ;

    let client = DlClient::new(options);

    // Spawn a background task to process progress updates
    tokio::spawn(async move {
        while let Some(progress) = rx.recv().await {
            if let Some(percent) = progress.percent() {
                println!(
                    "[{:?}] {:.2}% ({}/{} bytes) - Workers: {}",
                    progress.phase,
                    percent,
                    progress.downloaded_bytes,
                    progress.total_bytes.unwrap_or(0),
                    progress.active_workers
                );
            } else {
                println!(
                    "[{:?}] Downloaded {} bytes (unknown total) - Workers: {}",
                    progress.phase,
                    progress.downloaded_bytes,
                    progress.active_workers
                );
            }
        }
    });

    let source = DownloadSource::Http("https://example.com/large-file.zip".to_string());
    client.download(source, "large-file.zip").await?;

    Ok(())
}
```

### 3. Torrent Downloads (Single File or Directory)

You can download torrents via magnet links, local torrent files, or HTTP torrent URLs.

```rust
use libdl::{DlClient, DownloadSource, TorrentInput, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let client = DlClient::default();

    // From magnet link
    let magnet = "magnet:?xt=urn:btih:3fa85f6249b068224d03e5461e7127e2b69...".to_string();
    let source = DownloadSource::Torrent(TorrentInput::from_source(magnet));

    // Torrent output paths specify the target output directory
    let summary = client.download(source, "./downloads").await?;

    println!("Torrent downloaded to: {:?}", summary.output_path);
    Ok(())
}
```

### 4. Listing and Downloading Specific Torrent Files

If you are dealing with a large multi-file torrent (e.g. dataset), you can inspect its files and download only the ones you need.

```rust
use libdl::{list_torrent_files, download_torrent, DownloadOptions, TorrentInput, TorrentOptions, Result};
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> Result<()> {
    let input = TorrentInput::from_source("http://example.com/dataset.torrent");
    let output_dir = "./downloads";

    // 1. List files inside torrent (index, file name, file size)
    let files = list_torrent_files(input.clone(), output_dir).await?;
    for (index, name, size) in &files {
        println!("File [{}]: {} ({} bytes)", index, name, size);
    }

    // 2. Select specific file indices (e.g., download only file indices 0 and 2)
    let (tx, _rx) = mpsc::unbounded_channel();
    let options = TorrentOptions {
        progress: Some(tx),
        only_files: Some(vec![0, 2]),
    };

    download_torrent(input, output_dir, options).await?;
    println!("Selected files downloaded successfully!");

    Ok(())
}
```

---

## Core API & Configurations

### `DownloadOptions`

Configuration options passed when initializing a `DlClient` or when invoking individual download functions.

| Option | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `connections` | `Option<usize>` | `None` | The number of parallel TCP connections for HTTP download. If `None`, enables **Dynamic Connection Scaling** (starts with 4, adaptively scales up/down). |
| `chunk_size` | `u64` | `2 * 1024 * 1024` (2 MiB) | Chunk size for split parallel downloads. If left at default, the library automatically scales the chunk size based on overall file size for high-speed transfer. |
| `resume` | `bool` | `true` | Allows resuming incomplete downloads if they are interrupted. |
| `overwrite` | `bool` | `false` | If `true`, overwrites any existing file. If `false`, automatically finds a unique name (e.g., `file.zip.1`) to avoid conflicts. |
| `user_agent` | `String` | `"dl/[version]"` | HTTP user-agent header. |
| `metadata_flush_interval` | `Duration` | `5s` | How frequently to persist intermediate chunk states to disk during HTTP downloads. |
| `progress` | `Option<ProgressSender>` | `None` | An unbounded sender (`tokio::sync::mpsc::UnboundedSender<DownloadProgress>`) to stream real-time updates. |
| `only_files` | `Option<Vec<usize>>` | `None` | Set of file indices to select files in a torrent. |

---

## The Download Lifecycle (`DownloadPhase`)

Progress updates contain a `phase` representing the state of the download:

1. **`Probing`**: Initial connection to the HTTP server to determine if split downloading is supported, get content length, and validate cache headers (`ETag`/`Last-Modified`).
2. **`Downloading`**: Active transfer of data.
3. **`PersistingState`**: Periodically flushing the list of completed chunks inline to the file.
4. **`Finalizing`**: Reassembling chunks, clearing the inline metadata footer, and preparing the output file.
5. **`Complete`**: The download has completed successfully, state metadata is removed, and the file is in its final form.

---

## Error Handling

All API calls return a `Result<T, DlError>`. The `DlError` enum is represented by the following variants:

- `Io(std::io::Error)`: Low-level file write, read, or creation errors.
- `Http(reqwest::Error)`: Network layer errors, connection timeouts, DNS failure, etc.
- `InvalidResponse(String)`: Non-200 or unexpected headers returned by server.
- `RateLimited { message, retry_after }`: The server returned `429 Too Many Requests`. Includes optional duration to wait.
- `ServerError(String)`: Remote server errors (`5xx`).
- `RangesUnsupported`: Server doesn't support HTTP Range requests when range support is strictly required.
- `InvalidState(String)`: Local metadata download state is corrupted or mismatched.
- `Serialization(serde_json::Error)`: JSON errors when reading/writing download states.
- `Torrent(String)`: Errors coming from the BitTorrent engine (`librqbit`).
- `Join(tokio::task::JoinError)`: Task orchestration failure on tokio worker threads.

---

## Resuming Architecture (Behind the Scenes)

Instead of dumping separate configuration files (like `.aria2` or `.torrent.state`) next to your downloads, `libdl` appends an elegant binary metadata footer to the file currently being downloaded (ending in `.dl`). 

At periodic intervals (configurable via `metadata_flush_interval`), the download manager writes:
1. Active chunk completion bitmaps.
2. HTTP validator details (`ETag` and `Last-Modified`).
3. Total file and chunk size constraints.

When a download is resumed, the file footer is inspected and parsed. If the server validators still match the local ones, the client will skip already downloaded chunks and fetch the missing segments. Upon successful completion, the footer is cleanly truncated, and the `.dl` extension is removed to produce the final, unmodified file.
