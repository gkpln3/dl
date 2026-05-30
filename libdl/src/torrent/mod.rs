use std::{path::Path, time::Duration};

use serde::{Deserialize, Serialize};

use crate::{
    error::{DlError, Result},
    types::{DownloadKind, DownloadPhase, DownloadProgress, DownloadSummary, ProgressSender},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TorrentInput {
    Magnet(String),
    TorrentFile(std::path::PathBuf),
    TorrentUrl(String),
}

#[derive(Debug, Default, Clone)]
pub struct TorrentOptions {
    pub progress: Option<ProgressSender>,
    pub only_files: Option<Vec<usize>>,
}

#[cfg(feature = "torrent")]
pub async fn download_torrent(
    input: TorrentInput,
    output_dir: impl AsRef<Path>,
    options: TorrentOptions,
) -> Result<DownloadSummary> {
    use std::{borrow::Cow, sync::Arc};

    use bytes::Bytes;
    use librqbit::{AddTorrent, AddTorrentResponse, ManagedTorrent, Session};
    use tokio::{fs, time};

    let output_dir = output_dir.as_ref().to_path_buf();
    fs::create_dir_all(&output_dir).await?;

    let source = input.source_label();
    let add_torrent = match input {
        TorrentInput::Magnet(magnet) | TorrentInput::TorrentUrl(magnet) => {
            AddTorrent::Url(Cow::Owned(magnet))
        }
        TorrentInput::TorrentFile(path) => {
            let bytes = fs::read(path).await?;
            AddTorrent::TorrentFileBytes(Bytes::from(bytes))
        }
    };

    let session = Session::new(output_dir.clone())
        .await
        .map_err(|error| DlError::Torrent(error.to_string()))?;
    let response = session
        .add_torrent(
            add_torrent,
            Some(librqbit::AddTorrentOptions {
                overwrite: true,
                only_files: options.only_files.clone(),
                ..Default::default()
            }),
        )
        .await
        .map_err(|error| DlError::Torrent(error.to_string()))?;

    let torrent = managed_torrent(response)?;
    let mut ticker = time::interval(Duration::from_millis(500));

    loop {
        let stats = torrent.stats();
        emit_torrent_progress(&options.progress, &source, &output_dir, &stats);

        if let Some(error) = stats.error {
            return Err(DlError::Torrent(error));
        }

        if stats.finished {
            return Ok(DownloadSummary {
                kind: DownloadKind::Torrent,
                source,
                output_path: output_dir,
                total_bytes: stats.total_bytes,
                downloaded_bytes: stats.progress_bytes,
                resumed: true,
            });
        }

        ticker.tick().await;
    }

    fn managed_torrent(response: AddTorrentResponse) -> Result<Arc<ManagedTorrent>> {
        match response {
            AddTorrentResponse::Added(_, torrent)
            | AddTorrentResponse::AlreadyManaged(_, torrent) => Ok(torrent),
            AddTorrentResponse::ListOnly(_) => Err(DlError::Torrent(
                "torrent was added in list-only mode unexpectedly".to_string(),
            )),
        }
    }
}

#[cfg(not(feature = "torrent"))]
pub async fn download_torrent(
    _input: TorrentInput,
    _output_dir: impl AsRef<Path>,
    _options: TorrentOptions,
) -> Result<DownloadSummary> {
    Err(DlError::Torrent(
        "libdl was built without the `torrent` feature".to_string(),
    ))
}

#[cfg(feature = "torrent")]
pub async fn list_torrent_files(
    input: TorrentInput,
    output_dir: impl AsRef<Path>,
) -> Result<Vec<(usize, String, u64)>> {
    use std::borrow::Cow;
    use bytes::Bytes;
    use librqbit::{AddTorrent, AddTorrentResponse, Session};
    use tokio::fs;

    let output_dir = output_dir.as_ref().to_path_buf();
    fs::create_dir_all(&output_dir).await?;

    let add_torrent = match input {
        TorrentInput::Magnet(magnet) | TorrentInput::TorrentUrl(magnet) => {
            AddTorrent::Url(Cow::Owned(magnet))
        }
        TorrentInput::TorrentFile(path) => {
            let bytes = fs::read(path).await?;
            AddTorrent::TorrentFileBytes(Bytes::from(bytes))
        }
    };

    let session = Session::new(output_dir.clone())
        .await
        .map_err(|error| DlError::Torrent(error.to_string()))?;
    let response = session
        .add_torrent(
            add_torrent,
            Some(librqbit::AddTorrentOptions {
                list_only: true,
                overwrite: true,
                ..Default::default()
            }),
        )
        .await
        .map_err(|error| DlError::Torrent(error.to_string()))?;

    match response {
        AddTorrentResponse::ListOnly(list_only) => {
            let details = list_only.info.iter_file_details()
                .map_err(|error| DlError::Torrent(error.to_string()))?;
            let mut files = Vec::new();
            for (index, file) in details.enumerate() {
                let name = file.filename.to_string()
                    .map_err(|error| DlError::Torrent(error.to_string()))?;
                files.push((index, name, file.len));
            }
            Ok(files)
        }
        _ => Err(DlError::Torrent("expected list-only response".to_string())),
    }
}

#[cfg(not(feature = "torrent"))]
pub async fn list_torrent_files(
    _input: TorrentInput,
    _output_dir: impl AsRef<Path>,
) -> Result<Vec<(usize, String, u64)>> {
    Err(DlError::Torrent(
        "libdl was built without the `torrent` feature".to_string(),
    ))
}

impl TorrentInput {
    pub fn from_source(source: impl Into<String>) -> Self {
        let source = source.into();
        if source.starts_with("magnet:") {
            Self::Magnet(source)
        } else if source.starts_with("http://") || source.starts_with("https://") {
            Self::TorrentUrl(source)
        } else {
            Self::TorrentFile(source.into())
        }
    }

    pub fn source_label(&self) -> String {
        match self {
            Self::Magnet(source) | Self::TorrentUrl(source) => source.clone(),
            Self::TorrentFile(path) => path.display().to_string(),
        }
    }
}

#[cfg(feature = "torrent")]
fn emit_torrent_progress(
    progress: &Option<ProgressSender>,
    source: &str,
    output_path: &Path,
    stats: &librqbit::TorrentStats,
) {
    if let Some(progress) = progress {
        let phase = if stats.finished {
            DownloadPhase::Complete
        } else {
            DownloadPhase::Downloading
        };

        let _ = progress.send(DownloadProgress {
            kind: DownloadKind::Torrent,
            phase,
            source: source.to_string(),
            output_path: output_path.to_path_buf(),
            downloaded_bytes: stats.progress_bytes,
            total_bytes: Some(stats.total_bytes),
            active_workers: 0,
            completed_chunks: None,
            total_chunks: None,
        });
    }
}
