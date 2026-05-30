use std::io;

#[derive(Debug, thiserror::Error)]
pub enum DlError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("invalid HTTP response: {0}")]
    InvalidResponse(String),

    #[error("rate limited: {message}")]
    RateLimited {
        message: String,
        retry_after: Option<std::time::Duration>,
    },

    #[error("server error: {0}")]
    ServerError(String),

    #[error("server does not support resumable range downloads")]
    RangesUnsupported,

    #[error("download state is invalid: {0}")]
    InvalidState(String),

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("torrent error: {0}")]
    Torrent(String),

    #[error("worker task failed: {0}")]
    Join(#[from] tokio::task::JoinError),
}

pub type Result<T> = std::result::Result<T, DlError>;
