use std::{
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

pub const DEFAULT_CONNECTIONS: usize = 8;
pub const DEFAULT_CHUNK_SIZE: u64 = 2 * 1024 * 1024;
pub const DEFAULT_METADATA_FLUSH_INTERVAL: Duration = Duration::from_secs(5);

pub type ProgressSender = mpsc::UnboundedSender<DownloadProgress>;
pub type ProgressReceiver = mpsc::UnboundedReceiver<DownloadProgress>;

#[derive(Debug, Clone)]
pub struct DownloadOptions {
    pub connections: usize,
    pub chunk_size: u64,
    pub resume: bool,
    pub overwrite: bool,
    pub user_agent: String,
    pub metadata_flush_interval: Duration,
    pub progress: Option<ProgressSender>,
    pub only_files: Option<Vec<usize>>,
}

impl Default for DownloadOptions {
    fn default() -> Self {
        Self {
            connections: DEFAULT_CONNECTIONS,
            chunk_size: DEFAULT_CHUNK_SIZE,
            resume: true,
            overwrite: false,
            user_agent: format!("dl/{}", env!("CARGO_PKG_VERSION")),
            metadata_flush_interval: DEFAULT_METADATA_FLUSH_INTERVAL,
            progress: None,
            only_files: None,
        }
    }
}

impl DownloadOptions {
    pub fn with_progress(mut self, progress: ProgressSender) -> Self {
        self.progress = Some(progress);
        self
    }

    pub fn normalized(mut self) -> Self {
        self.connections = self.connections.max(1);
        self.chunk_size = self.chunk_size.max(64 * 1024);
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DownloadKind {
    Http,
    Torrent,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DownloadPhase {
    Probing,
    Downloading,
    PersistingState,
    Finalizing,
    Complete,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadProgress {
    pub kind: DownloadKind,
    pub phase: DownloadPhase,
    pub source: String,
    pub output_path: PathBuf,
    pub downloaded_bytes: u64,
    pub total_bytes: Option<u64>,
    pub active_workers: usize,
    pub completed_chunks: Option<usize>,
    pub total_chunks: Option<usize>,
}

impl DownloadProgress {
    pub fn percent(&self) -> Option<f64> {
        let total = self.total_bytes?;
        if total == 0 {
            return Some(100.0);
        }

        Some((self.downloaded_bytes as f64 / total as f64) * 100.0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Segment {
    pub index: usize,
    pub start: u64,
    pub end: u64,
}

impl Segment {
    pub fn len(&self) -> u64 {
        self.end - self.start + 1
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InlineDownloadState {
    pub version: u16,
    pub kind: DownloadKind,
    pub source: String,
    pub total_size: u64,
    pub chunk_size: u64,
    pub completed_chunks: Vec<bool>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub updated_at_unix_secs: u64,
}

pub(crate) fn urls_are_compatible(url1: &str, url2: &str) -> bool {
    if url1 == url2 {
        return true;
    }

    if is_http_or_keys_and_tokens_stripped(url1) && is_http_or_keys_and_tokens_stripped(url2) {
        if let (Ok(u1), Ok(u2)) = (url::Url::parse(url1), url::Url::parse(url2)) {
            return u1.scheme() == u2.scheme()
                && u1.host_str() == u2.host_str()
                && u1.port() == u2.port()
                && u1.path() == u2.path();
        }
    }

    false
}

fn is_http_or_keys_and_tokens_stripped(u: &str) -> bool {
    u.starts_with("http://") || u.starts_with("https://")
}

impl InlineDownloadState {
    pub fn new(
        kind: DownloadKind,
        source: String,
        total_size: u64,
        chunk_size: u64,
        completed_chunks: Vec<bool>,
        etag: Option<String>,
        last_modified: Option<String>,
    ) -> Self {
        Self {
            version: 1,
            kind,
            source,
            total_size,
            chunk_size,
            completed_chunks,
            etag,
            last_modified,
            updated_at_unix_secs: unix_now(),
        }
    }

    pub fn completed_bytes(&self) -> u64 {
        self.completed_chunks
            .iter()
            .enumerate()
            .filter(|(_, completed)| **completed)
            .map(|(index, _)| segment_len(index, self.total_size, self.chunk_size))
            .sum()
    }

    pub fn is_compatible_with(
        &self,
        kind: DownloadKind,
        source: &str,
        total_size: u64,
        chunk_size: u64,
        etag: Option<&str>,
        last_modified: Option<&str>,
    ) -> bool {
        self.version == 1
            && self.kind == kind
            && urls_are_compatible(&self.source, source)
            && self.total_size == total_size
            && self.chunk_size == chunk_size
            && weak_validator_matches(self.etag.as_deref(), etag)
            && weak_validator_matches(self.last_modified.as_deref(), last_modified)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadSummary {
    pub kind: DownloadKind,
    pub source: String,
    pub output_path: PathBuf,
    pub total_bytes: u64,
    pub downloaded_bytes: u64,
    pub resumed: bool,
}

pub fn chunk_count(total_size: u64, chunk_size: u64) -> usize {
    if total_size == 0 {
        return 0;
    }

    total_size.div_ceil(chunk_size) as usize
}

pub fn build_segments(total_size: u64, chunk_size: u64, completed: &[bool]) -> Vec<Segment> {
    (0..chunk_count(total_size, chunk_size))
        .filter(|index| !completed.get(*index).copied().unwrap_or(false))
        .map(|index| {
            let start = index as u64 * chunk_size;
            let end = (start + chunk_size - 1).min(total_size - 1);
            Segment { index, start, end }
        })
        .collect()
}

pub fn segment_len(index: usize, total_size: u64, chunk_size: u64) -> u64 {
    let start = index as u64 * chunk_size;
    if start >= total_size {
        return 0;
    }

    (chunk_size).min(total_size - start)
}

pub(crate) fn weak_validator_matches(stored: Option<&str>, remote: Option<&str>) -> bool {
    match (stored, remote) {
        (Some(stored), Some(remote)) => stored == remote,
        (None, _) | (_, None) => true,
    }
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Generate a unique output path by appending a numeric suffix (e.g., `.1`, `.2`) 
/// if the desired path already exists.
pub fn generate_unique_output_path(path: &Path) -> PathBuf {
    if !path.exists() {
        return path.to_path_buf();
    }

    let mut counter = 1;
    loop {
        let candidate = format!("{}.{}", path.display(), counter);
        if !Path::new(&candidate).exists() {
            return PathBuf::from(candidate);
        }
        counter += 1;
    }
}

/// Determine the target output path and the temporary download path.
/// If overwrite is disabled and a download path already exists, we resume it.
pub fn determine_download_paths(
    output_path: &Path,
    overwrite: bool,
) -> (PathBuf, PathBuf) {
    let target_path = output_path.to_path_buf();
    let download_path = get_download_path(&target_path);

    if overwrite {
        return (target_path, download_path);
    }

    // 1. If download_path (.dl) already exists, use it to resume
    if download_path.exists() {
        return (target_path, download_path);
    }

    // 2. If target_path doesn't exist, we can use it
    if !target_path.exists() {
        return (target_path, download_path);
    }

    // 3. Otherwise, search for a unique candidate
    let mut counter = 1;
    loop {
        let candidate_target = PathBuf::from(format!("{}.{}", target_path.display(), counter));
        let candidate_download = get_download_path(&candidate_target);

        if candidate_download.exists() {
            return (candidate_target, candidate_download);
        }

        if !candidate_target.exists() {
            return (candidate_target, candidate_download);
        }

        counter += 1;
    }
}

fn get_download_path(path: &Path) -> PathBuf {
    let mut download_path = path.to_path_buf();
    let mut file_name = download_path.file_name().unwrap_or_default().to_os_string();
    file_name.push(".dl");
    download_path.set_file_name(file_name);
    download_path
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_missing_segments_only() {
        let completed = vec![true, false, true, false];
        let segments = build_segments(10, 3, &completed);

        assert_eq!(
            segments,
            vec![
                Segment {
                    index: 1,
                    start: 3,
                    end: 5
                },
                Segment {
                    index: 3,
                    start: 9,
                    end: 9
                }
            ]
        );
    }
    #[test]
    fn test_generate_unique_output_path() {
        use std::fs;
        use tempfile::tempdir;

        let temp_dir = tempdir().unwrap();
        let base = temp_dir.path().join("test_dl_output.bin");

        // No existing file -> same path
        let result = generate_unique_output_path(&base);
        assert_eq!(result, base.clone());

        // File exists -> .1
        fs::write(&base, b"test").unwrap();
        let result = generate_unique_output_path(&base);
        assert_eq!(result, temp_dir.path().join("test_dl_output.bin.1"));

        // .1 also exists -> .2
        fs::write(&result, b"test1").unwrap();
        let result2 = generate_unique_output_path(&base);
        assert_eq!(result2, temp_dir.path().join("test_dl_output.bin.2"));
    }

    #[test]
    fn test_determine_download_paths() {
        use std::fs;
        use tempfile::tempdir;

        let temp_dir = tempdir().unwrap();
        let base = temp_dir.path().join("test_dl_output.bin");
        let base_dl = temp_dir.path().join("test_dl_output.bin.dl");

        // 1. None of the files exist
        let (t, d) = determine_download_paths(&base, false);
        assert_eq!(t, base);
        assert_eq!(d, base_dl);

        // 2. target_path exists, but no download_path -> should use unique target
        fs::write(&base, b"done").unwrap();
        let (t, d) = determine_download_paths(&base, false);
        assert_eq!(t, temp_dir.path().join("test_dl_output.bin.1"));
        assert_eq!(d, temp_dir.path().join("test_dl_output.bin.1.dl"));

        // 3. Target and target.1.dl exist -> should resume target.1.dl
        let t1_dl = temp_dir.path().join("test_dl_output.bin.1.dl");
        fs::write(&t1_dl, b"partial").unwrap();
        let (t, d) = determine_download_paths(&base, false);
        assert_eq!(t, temp_dir.path().join("test_dl_output.bin.1"));
        assert_eq!(d, t1_dl);

        // 4. Overwrite is true -> bypass unique checks
        let (t, d) = determine_download_paths(&base, true);
        assert_eq!(t, base);
        assert_eq!(d, base_dl);
    }
}
