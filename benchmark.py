#!/usr/bin/env python3
import argparse
import os
import re
import sys
import subprocess
import time
import threading
import urllib.request
from pathlib import Path

# Default files to benchmark
DEFAULT_TARGETS = [
    {
        "name": "Ubuntu 26.04 Desktop ISO (6.5 GB)",
        "url": "https://releases.ubuntu.com/26.04/ubuntu-26.04-desktop-amd64.iso",
        "temp_name": "ubuntu-desktop"
    },
    {
        "name": "iPhone 18,2 iOS 26.5 Restore IPSW (11.3 GB)",
        "url": "https://updates.cdn-apple.com/2026SpringFCS/fullrestores/122-56404/B6269659-BD71-4CB7-AF7C-F8D9C3CC6E2D/iPhone18,2_26.5_23F77_Restore.ipsw",
        "temp_name": "iphone-restore"
    }
]

def format_bytes(n):
    for unit in ['B', 'KiB', 'MiB', 'GiB', 'TiB']:
        if n < 1024:
            return f"{n:.2f} {unit}"
        n /= 1024
    return f"{n:.2f} PiB"

def format_speed(bytes_per_sec):
    return f"{format_bytes(bytes_per_sec)}/s"

def parse_bytes(value_str, unit_str):
    value = float(value_str)
    unit = unit_str.strip().lower()
    multipliers = {
        'b': 1,
        'kb': 1000,
        'mb': 1000 * 1000,
        'gb': 1000 * 1000 * 1000,
        'tb': 1000 * 1000 * 1000 * 1000,
        'pb': 1000 * 1000 * 1000 * 1000 * 1000,
        'kib': 1024,
        'mib': 1024 * 1024,
        'gib': 1024 * 1024 * 1024,
        'tib': 1024 * 1024 * 1024 * 1024,
        'pib': 1024 * 1024 * 1024 * 1024 * 1024
    }
    return int(value * multipliers.get(unit, 1))

def get_content_length(url):
    try:
        req = urllib.request.Request(url, method='HEAD')
        with urllib.request.urlopen(req, timeout=10) as resp:
            return int(resp.headers.get('Content-Length', 0))
    except Exception:
        # Fallback to GET with range 0-0
        try:
            req = urllib.request.Request(url)
            req.add_header('Range', 'bytes=0-0')
            with urllib.request.urlopen(req, timeout=10) as resp:
                content_range = resp.headers.get('Content-Range', '')
                if '/' in content_range:
                    return int(content_range.split('/')[-1])
                return int(resp.headers.get('Content-Length', 0))
        except Exception:
            return None

def build_dl():
    print("Compiling `dl` in release mode...")
    start_time = time.time()
    try:
        # Build workspace member 'dl-cli' which generates the 'dl' binary
        subprocess.run(
            ["cargo", "build", "--release", "-p", "dl"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE
        )
        elapsed = time.time() - start_time
        print(f"Successfully compiled `dl` in {elapsed:.2f}s!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error compiling `dl`: {e.stderr.decode('utf-8', errors='ignore')}", file=sys.stderr)
        return False

def read_output(stream, progress_tracker, parser_func):
    buffer = b""
    while True:
        try:
            chunk = stream.read(128)
            if not chunk:
                break
            buffer += chunk
            # Split by \r and \n to capture overwrite lines
            while b'\r' in buffer or b'\n' in buffer:
                r_idx = buffer.find(b'\r')
                n_idx = buffer.find(b'\n')
                if r_idx != -1 and (n_idx == -1 or r_idx < n_idx):
                    line, buffer = buffer[:r_idx], buffer[r_idx+1:]
                else:
                    line, buffer = buffer[:n_idx], buffer[n_idx+1:]
                
                decoded = line.decode('utf-8', errors='ignore').strip()
                if decoded:
                    parser_func(decoded, progress_tracker)
        except Exception:
            break

def parse_dl_output(line, tracker):
    # Match progress bar: e.g. "12.34 MB/6.52 GB" or "512 B/4.2 GiB"
    # Case-insensitive to handle b, kb, mb, gb, tb, pb, kib, mib, gib, tib, pib
    match = re.search(r'([\d.]+)\s*(B|KB|MB|GB|TB|PB|KiB|MiB|GiB|TiB|PiB)/', line, re.IGNORECASE)
    if match:
        val_str, unit_str = match.groups()
        tracker['downloaded'] = parse_bytes(val_str, unit_str)
    
    # Match final summary: e.g. "downloaded 123456 bytes"
    match_final = re.search(r'downloaded\s+(\d+)\s+bytes', line)
    if match_final:
        tracker['downloaded'] = int(match_final.group(1))

def parse_axel_output(line, tracker):
    # Match percentage: e.g. "[ 12%]"
    match = re.search(r'\[\s*(\d+)%\]', line)
    if match:
        pct = int(match.group(1))
        if tracker['total_size']:
            tracker['downloaded'] = int(tracker['total_size'] * (pct / 100.0))

def parse_wget_output(line, tracker):
    # Match progress percent: e.g. " 12%" followed by spaces and numbers
    # /dev/null             0%[                    ]       0  --.-KB/s
    match = re.search(r'(\d+)%\[', line)
    if match:
        pct = int(match.group(1))
        if tracker['total_size']:
            tracker['downloaded'] = int(tracker['total_size'] * (pct / 100.0))

def read_pty(master_fd, progress_tracker, parser_func):
    buffer = b""
    while True:
        try:
            chunk = os.read(master_fd, 512)
            if not chunk:
                break
            buffer += chunk
            while b'\r' in buffer or b'\n' in buffer:
                r_idx = buffer.find(b'\r')
                n_idx = buffer.find(b'\n')
                if r_idx != -1 and (n_idx == -1 or r_idx < n_idx):
                    line, buffer = buffer[:r_idx], buffer[r_idx+1:]
                else:
                    line, buffer = buffer[:n_idx], buffer[n_idx+1:]
                
                decoded = line.decode('utf-8', errors='ignore').strip()
                if decoded:
                    parser_func(decoded, progress_tracker)
        except OSError:
            break

def run_downloader(name, cmd, temp_file, tracker, duration, is_full):
    # Ensure any previous temp files are deleted
    cleanup_temp_files(temp_file)

    tracker['downloaded'] = 0
    start_time = time.time()
    
    # Determine which parser to use
    if "dl" in cmd[0]:
        parser_func = parse_dl_output
    elif "axel" in cmd[0]:
        parser_func = parse_axel_output
    else:
        parser_func = parse_wget_output

    use_pty = sys.platform != "win32"
    
    if use_pty:
        import pty
        master_fd, slave_fd = pty.openpty()
        stdout_target = slave_fd
        stderr_target = slave_fd
    else:
        stdout_target = subprocess.PIPE
        stderr_target = subprocess.PIPE

    try:
        # Run subprocess in its own session group so we can terminate it and its children cleanly
        process = subprocess.Popen(
            cmd,
            stdout=stdout_target,
            stderr=stderr_target,
            preexec_fn=os.setsid if sys.platform != "win32" else None
        )
    except FileNotFoundError:
        if use_pty:
            os.close(master_fd)
            os.close(slave_fd)
        print(f"Error: {cmd[0]} is not installed or not in PATH.")
        return None

    if use_pty:
        # Close the slave file descriptor in parent so EOF is triggered when child exits
        os.close(slave_fd)
        # Start PTY reading thread
        t_read = threading.Thread(target=read_pty, args=(master_fd, tracker, parser_func))
        t_read.daemon = True
        t_read.start()
    else:
        # Start standard pipes reading threads
        t_out = threading.Thread(target=read_output, args=(process.stdout, tracker, parser_func))
        t_err = threading.Thread(target=read_output, args=(process.stderr, tracker, parser_func))
        t_out.daemon = True
        t_err.daemon = True
        t_out.start()
        t_err.start()

    # Monitor loop
    try:
        if is_full:
            while process.poll() is None:
                if "wget" in cmd[0] and os.path.exists(temp_file):
                    tracker['downloaded'] = os.path.getsize(temp_file)
                time.sleep(0.1)
        else:
            # Timed run
            while time.time() - start_time < duration:
                if process.poll() is not None:
                    break
                if "wget" in cmd[0] and os.path.exists(temp_file):
                    tracker['downloaded'] = os.path.getsize(temp_file)
                time.sleep(0.1)

    finally:
        # Check end time
        end_time = time.time()
        elapsed = end_time - start_time

        # Clean terminate or kill process
        if process.poll() is None:
            try:
                # Terminate the whole process group
                if sys.platform != "win32":
                    os.killpg(os.getpgid(process.pid), subprocess.signal.SIGTERM)
                else:
                    process.terminate()
                process.wait(timeout=2)
            except Exception:
                try:
                    if sys.platform != "win32":
                        os.killpg(os.getpgid(process.pid), subprocess.signal.SIGKILL)
                    else:
                        process.kill()
                except Exception:
                    pass

        if use_pty:
            try:
                os.close(master_fd)
            except Exception:
                pass

        # Final check on wget file size
        if "wget" in cmd[0] and os.path.exists(temp_file):
            tracker['downloaded'] = os.path.getsize(temp_file)
        # If it was a full run and completed successfully, make sure downloaded is exactly total_size
        elif is_full and process.returncode == 0 and tracker['total_size']:
            tracker['downloaded'] = tracker['total_size']

        # Cleanup downloaded temp files
        cleanup_temp_files(temp_file)

    return elapsed

def cleanup_temp_files(temp_file):
    # Delete main temp file
    if os.path.exists(temp_file):
        try:
            os.remove(temp_file)
        except Exception:
            pass
    # Delete axel state file
    axel_st = f"{temp_file}.st"
    if os.path.exists(axel_st):
        try:
            os.remove(axel_st)
        except Exception:
            pass

def print_markdown_results(target_name, file_size, url, results):
    print("\n" + "="*80)
    print(f"### Benchmark Results: {target_name}")
    print(f"File Size: {format_bytes(file_size)} ({file_size} bytes)")
    print(f"URL: {url}")
    print("="*80 + "\n")

    # Find wget speed as baseline
    wget_speed = None
    for r in results:
        if r["downloader"] == "wget":
            wget_speed = r["speed"]
            break

    headers = [
        "Downloader", "Connections", "Downloaded", "Duration", "Avg Speed", "Relative Speed", "Performance Gain"
    ]
    
    # Format table rows
    rows = []
    for r in results:
        downloader = f"**{r['downloader']}**"
        if r['downloader'] == "dl" and r['connections'] == "Auto":
            downloader = f"**dl (dynamic)**"
            
        conns = str(r['connections'])
        downloaded = format_bytes(r['downloaded'])
        duration = f"{r['duration']:.1f}s"
        speed_str = format_speed(r['speed'])
        
        # Calculate relative speed vs wget
        if wget_speed and wget_speed > 0:
            rel = (r['speed'] / wget_speed) * 100.0
            rel_str = f"{rel:.1f}%"
            if r['downloader'] == "wget":
                gain_str = "Baseline"
            else:
                gain = ((r['speed'] - wget_speed) / wget_speed) * 100.0
                times = r['speed'] / wget_speed
                gain_str = f"**+{gain:.1f}% ({times:.2f}x)**" if gain >= 0 else f"{gain:.1f}% ({times:.2f}x)"
        else:
            rel_str = "N/A"
            gain_str = "N/A"
            
        rows.append([downloader, conns, downloaded, duration, speed_str, rel_str, gain_str])

    # Print markdown format
    col_widths = [max(len(row[i]) for row in [headers] + rows) for i in range(len(headers))]
    
    def format_row(row):
        return "| " + " | ".join(row[i].ljust(col_widths[i]) for i in range(len(row))) + " |"

    print(format_row(headers))
    # Separator
    separator = []
    for i in range(len(headers)):
        if i in (1, 2, 3, 4, 5, 6): # Center or right align
            separator.append(":" + "-"*(col_widths[i]-2) + ":")
        else:
            separator.append("-" * col_widths[i])
    print("| " + " | ".join(separator) + " |")
    
    for row in rows:
        print(format_row(row))
        
    print("\n*Copy and paste the table above into your README.md!*\n")

def main():
    parser = argparse.ArgumentParser(
        description="Benchmark `dl` against `wget` and `axel`."
    )
    parser.add_argument(
        "--duration", "-d",
        type=float,
        default=30.0,
        help="Duration of the timed benchmark in seconds (default: 30.0)"
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Run full download instead of a timed benchmark (WARNING: files are very large)"
    )
    parser.add_argument(
        "--connections", "-c",
        type=str,
        default="8,16",
        help="Comma-separated list of connection counts to test (default: '8,16')"
    )
    parser.add_argument(
        "--no-wget",
        action="store_true",
        help="Skip wget benchmark"
    )
    parser.add_argument(
        "--no-axel",
        action="store_true",
        help="Skip axel benchmark"
    )
    parser.add_argument(
        "--no-dl",
        action="store_true",
        help="Skip dl benchmark"
    )
    parser.add_argument(
        "--url",
        type=str,
        help="Custom URL to benchmark instead of default Ubuntu and Apple files"
    )
    parser.add_argument(
        "--target-name",
        type=str,
        default="Custom Target",
        help="Display name for custom URL benchmark"
    )

    args = parser.parse_args()

    # Resolve connections list
    conn_list = [int(x.strip()) for x in args.connections.split(",") if x.strip().isdigit()]

    # Locate dl executable path
    workspace_root = Path(__file__).parent.absolute()
    dl_bin = workspace_root / "target" / "release" / "dl"

    # Compile dl if needed or if requested
    if not args.no_dl:
        if not build_dl():
            print("Cannot compile `dl`. Skipping `dl` benchmarks.", file=sys.stderr)
            args.no_dl = True

    # Setup targets
    if args.url:
        targets = [{
            "name": args.target_name,
            "url": args.url,
            "temp_name": "custom-target-download"
        }]
    else:
        targets = DEFAULT_TARGETS

    for target in targets:
        print(f"\nResolving size for: {target['name']}...")
        total_size = get_content_length(target['url'])
        if not total_size:
            print(f"Could not retrieve file size for {target['url']}. Skipping target.")
            continue
        print(f"File size confirmed: {format_bytes(total_size)}")

        results = []

        # 1. Wget Benchmark (Single-threaded baseline)
        if not args.no_wget:
            print(f"Running Wget benchmark...")
            temp_file = str(workspace_root / f"bench_temp_wget_{target['temp_name']}")
            cmd = ["wget", "--progress=bar:force", "-O", temp_file, target['url']]
            tracker = {"total_size": total_size, "downloaded": 0}
            
            elapsed = run_downloader("wget", cmd, temp_file, tracker, args.duration, args.full)
            if elapsed is not None:
                speed = tracker['downloaded'] / elapsed if elapsed > 0 else 0
                results.append({
                    "downloader": "wget",
                    "connections": 1,
                    "downloaded": tracker['downloaded'],
                    "duration": elapsed,
                    "speed": speed
                })
                print(f"Wget completed: {format_bytes(tracker['downloaded'])} downloaded in {elapsed:.1f}s ({format_speed(speed)})")

        # 2. Axel Benchmarks
        if not args.no_axel:
            for conn in conn_list:
                print(f"Running Axel benchmark (connections={conn})...")
                temp_file = str(workspace_root / f"bench_temp_axel_{conn}_{target['temp_name']}")
                cmd = ["axel", "-n", str(conn), "-o", temp_file, target['url']]
                tracker = {"total_size": total_size, "downloaded": 0}
                
                elapsed = run_downloader(f"axel-{conn}", cmd, temp_file, tracker, args.duration, args.full)
                if elapsed is not None:
                    speed = tracker['downloaded'] / elapsed if elapsed > 0 else 0
                    results.append({
                        "downloader": "axel",
                        "connections": conn,
                        "downloaded": tracker['downloaded'],
                        "duration": elapsed,
                        "speed": speed
                    })
                    print(f"Axel-{conn} completed: {format_bytes(tracker['downloaded'])} downloaded in {elapsed:.1f}s ({format_speed(speed)})")

        # 3. DL Benchmarks
        if not args.no_dl:
            # Fixed connections
            for conn in conn_list:
                print(f"Running dl benchmark (connections={conn})...")
                temp_file = str(workspace_root / f"bench_temp_dl_{conn}_{target['temp_name']}")
                cmd = [str(dl_bin), "--overwrite", "-j", str(conn), "-o", temp_file, target['url']]
                tracker = {"total_size": total_size, "downloaded": 0}
                
                elapsed = run_downloader(f"dl-{conn}", cmd, temp_file, tracker, args.duration, args.full)
                if elapsed is not None:
                    speed = tracker['downloaded'] / elapsed if elapsed > 0 else 0
                    results.append({
                        "downloader": "dl",
                        "connections": conn,
                        "downloaded": tracker['downloaded'],
                        "duration": elapsed,
                        "speed": speed
                    })
                    print(f"dl-{conn} completed: {format_bytes(tracker['downloaded'])} downloaded in {elapsed:.1f}s ({format_speed(speed)})")

            # Dynamic/Auto-scaling connection benchmark
            print(f"Running dl dynamic benchmark (auto connections)...")
            temp_file = str(workspace_root / f"bench_temp_dl_auto_{target['temp_name']}")
            cmd = [str(dl_bin), "--overwrite", "-o", temp_file, target['url']]
            tracker = {"total_size": total_size, "downloaded": 0}
            
            elapsed = run_downloader("dl-auto", cmd, temp_file, tracker, args.duration, args.full)
            if elapsed is not None:
                speed = tracker['downloaded'] / elapsed if elapsed > 0 else 0
                results.append({
                    "downloader": "dl",
                    "connections": "Auto",
                    "downloaded": tracker['downloaded'],
                    "duration": elapsed,
                    "speed": speed
                })
                print(f"dl-auto completed: {format_bytes(tracker['downloaded'])} downloaded in {elapsed:.1f}s ({format_speed(speed)})")

        # Output results
        if results:
            print_markdown_results(target['name'], total_size, target['url'], results)
        else:
            print(f"No results generated for {target['name']}.")

if __name__ == "__main__":
    main()
