# dl - A fast lightweight CLI downloader and accelerator with torrent support

[![Release and Brew Publish](https://github.com/gkpln3/dl/actions/workflows/release.yml/badge.svg)](https://github.com/gkpln3/dl/actions/workflows/release.yml)
[![Homebrew Formula](https://img.shields.io/homebrew/v/dl?color=blue&logo=homebrew)](https://formulae.brew.sh/formula/dl)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A lightweight, high-performance CLI downloader and accelerator written in Rust. `dl` speeds up downloads by utilizing multiple concurrent connections for HTTP streams and supports downloading directly from BitTorrent (including magnet links and `.torrent` files) with an interactive file selector.


## Features

- **⚡ HTTP/HTTPS Download Acceleration**: Downloads files using concurrent range requests split into multiple chunks, dramatically speeding up HTTP/HTTPS transfers.
- **📈 Dynamic & Adaptive Worker Scaling**: Automatically optimizes the number of concurrent connections when not specified. It starts with a base set of workers and progressively increases or decreases the count based on real-time speed feedback, maximizing bandwidth while avoiding congestion or rate limits.
- **🌐 Dynamic Protocol Negotiation & Connection Multiplexing**: Native support for **ALPN protocol negotiation** and advanced stream-level connection multiplexing via **HTTP/2** with adaptive flow control, drastically reducing handshake overhead.
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

### Benchmark Results: Ubuntu 26.04 Desktop ISO (6.5 GB)
File Size: 6.07 GiB

URL: https://releases.ubuntu.com/26.04/ubuntu-26.04-desktop-amd64.iso

| Downloader       | Connections | Downloaded | Duration | Avg Speed   | Relative Speed | Performance Gain    |
| ---------------- | :---------: | :--------: | :------: | :---------: | :------------: | :-----------------: |
| **wget**         | 1           | 974.19 MiB | 60.1s    | 16.21 MiB/s | 100.0%         | Baseline            |
| **axel**         | 8           | 1.09 GiB   | 60.0s    | 18.64 MiB/s | 115.0%         | **+15.0% (1.15x)**  |
| **axel**         | 16          | 1.46 GiB   | 60.0s    | 24.85 MiB/s | 153.3%         | **+53.3% (1.53x)**  |
| **dl**           | 8           | 1.75 GiB   | 60.1s    | 29.84 MiB/s | 184.0%         | **+84.0% (1.84x)**  |
| **dl**           | 16          | 2.69 GiB   | 60.1s    | 45.86 MiB/s | 282.9%         | **+182.9% (2.83x)** |
| **dl (dynamic)** | Auto        | 1.69 GiB   | 60.0s    | 28.83 MiB/s | 177.8%         | **+77.8% (1.78x)**  |

### Benchmark Results: iPhone 18,2 iOS 26.5 Restore IPSW (11.3 GB)
File Size: 10.53 GiB

URL: https://updates.cdn-apple.com/2026SpringFCS/fullrestores/122-56404/B6269659-BD71-4CB7-AF7C-F8D9C3CC6E2D/iPhone18,2_26.5_23F77_Restore.ipsw

| Downloader       | Connections | Downloaded | Duration | Avg Speed   | Relative Speed | Performance Gain   |
| ---------------- | :---------: | :--------: | :------: | :---------: | :------------: | :----------------: |
| **wget**         | 1           | 1.46 GiB   | 60.1s    | 24.93 MiB/s | 100.0%         | Baseline           |
| **axel**         | 8           | 2.11 GiB   | 60.0s    | 35.91 MiB/s | 144.1%         | **+44.1% (1.44x)** |
| **axel**         | 16          | 862.46 MiB | 60.0s    | 14.37 MiB/s | 57.6%          | -42.4% (0.58x)     |
| **dl**           | 8           | 2.65 GiB   | 60.1s    | 45.16 MiB/s | 181.2%         | **+81.2% (1.81x)** |
| **dl**           | 16          | 2.16 GiB   | 60.0s    | 36.86 MiB/s | 147.9%         | **+47.9% (1.48x)** |
| **dl (dynamic)** | Auto        | 2.48 GiB   | 60.0s    | 42.29 MiB/s | 169.7%         | **+69.7% (1.70x)** |

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

## Benchmarking 📊

You can benchmark `dl` against `wget` and `axel` using the provided `benchmark.py` script in the root directory. The script compiles `dl` in release mode, runs each download utility inside a pseudo-terminal (to capture active progress updates), measures their average transfer speed over a set duration.

### Prerequisites

Make sure you have `wget` and `axel` installed on your system (e.g., via Homebrew on macOS):

```bash
brew install wget axel
```

### Running the Benchmark

To run a timed benchmark (by default, 30 seconds per downloader configuration) on the default Ubuntu desktop and Apple restore IPSW files:

```bash
./benchmark.py --duration 30
```

You can customize the connection counts to test:

```bash
./benchmark.py --duration 30 --connections 8,16,32
```

To run a full download benchmark (Warning: these files are 6.5 GB and 11.3 GB, respectively!):

```bash
./benchmark.py --full
```

For more options (like benchmarking a custom URL), run:

```bash
./benchmark.py --help
```

## License

This project is licensed under MIT License. See the [LICENSE](LICENSE) file for details.
