# dl - A fast lightweight CLI downloader and accelerator with torrent support

[![Release and Brew Publish](https://github.com/gkpln3/dl/actions/workflows/release.yml/badge.svg)](https://github.com/gkpln3/dl/actions/workflows/release.yml)
[![Homebrew Formula](https://img.shields.io/homebrew/v/dl?color=blue&logo=homebrew)](https://formulae.brew.sh/formula/dl)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A lightweight, high-performance CLI downloader and accelerator written in Rust. `dl` speeds up downloads by utilizing multiple concurrent connections for HTTP streams and supports downloading directly from BitTorrent (including magnet links and `.torrent` files) with an interactive file selector.


## Features

- **⚡ HTTP/HTTPS Download Acceleration**: Downloads files using concurrent range requests split into multiple chunks, dramatically speeding up HTTP/HTTPS transfers.
- **📈 Dynamic & Adaptive Worker Scaling**: Automatically optimizes the number of concurrent connections when not specified. It starts with a base set of workers and progressively increases or decreases the count based on real-time speed feedback, maximizing bandwidth while avoiding congestion or rate limits.
- **🧲 BitTorrent & Magnet Link Support**: Seamlessly downloads torrents or magnet links.
- **📂 Interactive File Selection**: For multi-file torrents, `dl` interactively prompts you to choose which specific files you want to download.
- **⏯️ Resumable Streams**: Automatically saves transfer state so you can pause and resume downloads without losing progress.


## Installation

### Homebrew (Recommended for macOS and Linux)

You can install `dl` using Homebrew via a custom tap:

```bash
brew install gkpln3/tap/dl
```

Or tap the repository first and then install:

```bash
brew tap gkpln3/tap
brew install dl
```

### Prerequisites

For compiling from source, you must have [Rust and Cargo](https://rustup.rs/) installed on your machine.

### Installing from Source

If you clone this repository, you cannot run `cargo install` directly from the workspace root folder because it is a virtual cargo workspace. Instead, you need to target the CLI package specifically.

Run the following command from the project root directory:

```bash
cargo install --path dl-cli
```

This compiles `dl` in release mode and installs the executable directly to your local Cargo bin directory (usually `~/.cargo/bin`). Make sure this directory is in your shell's `PATH` to run `dl` from anywhere!


## Performance Comparison ⚡

To put `dl`'s multi-threaded acceleration into perspective, here is a relative speed comparison of downloading a large file (Ubuntu 26.04 Desktop ISO) using **wget** (single-threaded), [**pget**](https://github.com/Code-Hex/pget) (multi-threaded concurrent downloader), and **dl** (this utility):

| Utility | Connection Model | Relative Speed (Higher is Better) | Performance Gain vs Wget |
| :--- | :--- | :---: | :---: |
| **wget** | Single-threaded | **100%** (22.4 MiB/s) | Baseline |
| [**pget**](https://github.com/Code-Hex/pget) | Concurrent | **54%** (12.1 MiB/s) | -46% (Slower) |
| **dl** | **Concurrent Chunked / Range-based** | **133%** (29.8 MiB/s) | **+33% (Faster)** |

`dl`'s range-based concurrent request scheduling optimizes bandwidth utilization, letting you pull down assets substantially faster than traditional single-threaded utilities or poorly-optimized concurrent utilities.


## Usage

```bash
dl [OPTIONS] <SOURCE>
```

### Options

- `-o, --output <OUTPUT>`: Output file for HTTP downloads, or output directory for torrents.
- `-j, --connections <CONNECTIONS>`: Number of concurrent HTTP range workers (dynamic/auto-scaling by default).
- `--chunk-size <CHUNK_SIZE>`: HTTP chunk size (supports plain bytes, or suffix units like `K`, `M`, `G`) (default: `2M`).
- `--no-resume`: Disable resumable inline metadata (state saving).
- `--overwrite`: Force-replace an existing output path.
- `--torrent`: Force treat an HTTP/HTTPS source URL as a torrent file.
- `-h, --help`: Print help information.
- `-V, --version`: Print version information.

### Examples

#### 1. Standard HTTP/HTTPS Download
Fast download using the dynamic/auto-scaling connection manager:
```bash
dl https://example.com/large-file.zip
```

#### 2. Download with Fixed Connection Count
Set a fixed number of concurrent threads (e.g. 16 workers):
```bash
dl -j 16 https://example.com/large-file.zip
```

#### 3. Downloading via Torrent Magnet Link
```bash
dl "magnet:?xt=urn:btih:..."
```

#### 4. Downloading via `.torrent` File
```bash
dl ./ubuntu-desktop.torrent
```

## License

This project is licensed under MIT License. See the [LICENSE](LICENSE) file for details.
