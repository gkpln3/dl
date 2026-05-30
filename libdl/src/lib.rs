pub mod error;
pub mod http;
pub mod state;
pub mod torrent;
pub mod types;

use std::path::Path;

pub use error::{DlError, Result};
pub use http::download_http;
pub use torrent::{download_torrent, list_torrent_files, TorrentInput, TorrentOptions};
pub use types::{
    DownloadKind, DownloadOptions, DownloadPhase, DownloadProgress, DownloadSummary,
    InlineDownloadState, ProgressReceiver, ProgressSender, Segment,
};

#[derive(Debug, Clone)]
pub enum DownloadSource {
    Http(String),
    Torrent(TorrentInput),
}

#[derive(Debug, Default, Clone)]
pub struct DlClient {
    options: DownloadOptions,
}

impl DlClient {
    pub fn new(options: DownloadOptions) -> Self {
        Self {
            options: options.normalized(),
        }
    }

    pub async fn download(
        &self,
        source: DownloadSource,
        output: impl AsRef<Path>,
    ) -> Result<DownloadSummary> {
        match source {
            DownloadSource::Http(url) => download_http(url, output, self.options.clone()).await,
            DownloadSource::Torrent(input) => {
                let options = TorrentOptions {
                    progress: self.options.progress.clone(),
                    only_files: self.options.only_files.clone(),
                };
                download_torrent(input, output, options).await
            }
        }
    }
}
