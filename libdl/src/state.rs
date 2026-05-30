use std::path::Path;

use tokio::{
    fs::{self, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom},
};

use crate::{
    error::{DlError, Result},
    types::InlineDownloadState,
};

pub const METADATA_MAGIC: &[u8; 12] = b"DL-METADATA\0";
pub const METADATA_LEN_SIZE: u64 = 8;
pub const METADATA_FOOTER_SIZE: u64 = METADATA_LEN_SIZE + METADATA_MAGIC.len() as u64;
const MAX_METADATA_SIZE: u64 = 16 * 1024 * 1024;

pub async fn read_inline_state(path: impl AsRef<Path>) -> Result<Option<InlineDownloadState>> {
    let path = path.as_ref();
    let metadata = match fs::metadata(path).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };

    let file_len = metadata.len();
    if file_len < METADATA_FOOTER_SIZE {
        return Ok(None);
    }

    let mut file = OpenOptions::new().read(true).open(path).await?;
    file.seek(SeekFrom::End(-(METADATA_MAGIC.len() as i64)))
        .await?;

    let mut magic = [0_u8; METADATA_MAGIC.len()];
    file.read_exact(&mut magic).await?;
    if &magic != METADATA_MAGIC {
        return Ok(None);
    }

    file.seek(SeekFrom::End(-(METADATA_FOOTER_SIZE as i64)))
        .await?;
    let mut len_bytes = [0_u8; 8];
    file.read_exact(&mut len_bytes).await?;
    let state_len = u64::from_be_bytes(len_bytes);

    if state_len == 0
        || state_len > MAX_METADATA_SIZE
        || state_len + METADATA_FOOTER_SIZE > file_len
    {
        return Err(DlError::InvalidState(format!(
            "inline metadata length {state_len} is not valid for file length {file_len}"
        )));
    }

    let state_start = file_len - METADATA_FOOTER_SIZE - state_len;
    file.seek(SeekFrom::Start(state_start)).await?;

    let mut state_bytes = vec![0_u8; state_len as usize];
    file.read_exact(&mut state_bytes).await?;

    Ok(Some(serde_json::from_slice(&state_bytes)?))
}

pub async fn write_inline_state(
    path: impl AsRef<Path>,
    total_size: u64,
    state: &InlineDownloadState,
    sync_to_disk: bool,
) -> Result<()> {
    let state_bytes = serde_json::to_vec(state)?;
    let state_len = state_bytes.len() as u64;
    if state_len > MAX_METADATA_SIZE {
        return Err(DlError::InvalidState(format!(
            "inline metadata exceeds maximum size: {state_len} bytes"
        )));
    }

    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(path)
        .await?;

    file.seek(SeekFrom::Start(total_size)).await?;
    file.write_all(&state_bytes).await?;
    file.write_all(&state_len.to_be_bytes()).await?;
    file.write_all(METADATA_MAGIC).await?;
    file.set_len(total_size + state_len + METADATA_FOOTER_SIZE)
        .await?;
    // `sync_data` (fsync) is expensive and stalls the writing worker. Resume metadata
    // is best-effort, so intermediate checkpoints skip the fsync and rely on the page
    // cache; only the final checkpoint forces durability.
    if sync_to_disk {
        file.sync_data().await?;
    }
    Ok(())
}

pub async fn clear_inline_state(path: impl AsRef<Path>, total_size: u64) -> Result<()> {
    let file = OpenOptions::new().write(true).open(path).await?;
    file.set_len(total_size).await?;
    file.sync_data().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;
    use tokio::io::AsyncWriteExt;

    use crate::types::DownloadKind;

    use super::*;

    #[tokio::test]
    async fn writes_reads_and_clears_inline_state() {
        let dir = tempdir().unwrap();
        let output = dir.path().join("file.bin");
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&output)
            .await
            .unwrap();
        file.write_all(b"payload").await.unwrap();
        file.flush().await.unwrap();
        drop(file);

        let state = InlineDownloadState::new(
            DownloadKind::Http,
            "https://example.com/file.bin".to_string(),
            7,
            2,
            vec![true, false, true, false],
            Some("etag".to_string()),
            None,
        );

        write_inline_state(&output, 7, &state, true).await.unwrap();
        let loaded = read_inline_state(&output).await.unwrap().unwrap();
        assert_eq!(loaded.source, state.source);
        assert_eq!(loaded.completed_chunks, state.completed_chunks);

        clear_inline_state(&output, 7).await.unwrap();
        assert_eq!(fs::metadata(&output).await.unwrap().len(), 7);
        assert!(read_inline_state(&output).await.unwrap().is_none());
    }
}
