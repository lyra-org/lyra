// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::path::{
    Path as FsPath,
    PathBuf,
};

use anyhow::{
    Context,
    Result,
    bail,
};
use axum::body::Body;
use axum::http::{
    HeaderValue,
    StatusCode,
};
use bytes::Bytes;
use tokio::io::{
    AsyncReadExt,
    AsyncSeekExt,
    SeekFrom,
};
use tokio::sync::mpsc as tokio_mpsc;
use tokio_stream::wrappers::ReceiverStream;

const FILE_STREAM_CHUNK_SIZE: usize = 64 * 1024;

enum RangeParse {
    Valid { start: u64, end: u64 },
    Invalid,
}

pub(crate) struct RangedFileBody {
    pub(crate) body: Body,
    pub(crate) content_length: HeaderValue,
    pub(crate) status: StatusCode,
    pub(crate) content_range: Option<HeaderValue>,
}

fn parse_range_header(value: &HeaderValue, size: u64) -> RangeParse {
    let range = match value.to_str() {
        Ok(range) => range,
        Err(_) => return RangeParse::Invalid,
    };
    let Some(range_spec) = range.strip_prefix("bytes=") else {
        return RangeParse::Invalid;
    };
    if range_spec.contains(',') {
        return RangeParse::Invalid;
    }
    let Some((start_str, end_str)) = range_spec.split_once('-') else {
        return RangeParse::Invalid;
    };
    if size == 0 {
        return RangeParse::Invalid;
    }
    if start_str.is_empty() {
        let Ok(suffix) = end_str.parse::<u64>() else {
            return RangeParse::Invalid;
        };
        if suffix == 0 {
            return RangeParse::Invalid;
        }
        let end = size.saturating_sub(1);
        let start = size.saturating_sub(suffix);
        return RangeParse::Valid { start, end };
    }
    let Ok(start) = start_str.parse::<u64>() else {
        return RangeParse::Invalid;
    };
    let end = if end_str.is_empty() {
        size.saturating_sub(1)
    } else {
        let Ok(end) = end_str.parse::<u64>() else {
            return RangeParse::Invalid;
        };
        end
    };
    if start > end || end >= size {
        return RangeParse::Invalid;
    }
    RangeParse::Valid { start, end }
}

async fn cleanup_temp_file(path: Option<PathBuf>) {
    let Some(path) = path else {
        return;
    };

    if let Err(err) = tokio::fs::remove_file(&path).await {
        tracing::debug!(
            path = %path.display(),
            error = %err,
            "failed to remove temp file"
        );
    }
}

pub(crate) async fn build_ranged_file_body(
    path: &FsPath,
    range_header: Option<&HeaderValue>,
    base_status: StatusCode,
    cleanup_path: Option<PathBuf>,
) -> Result<RangedFileBody> {
    let mut file = tokio::fs::File::open(path)
        .await
        .with_context(|| format!("failed to open file '{}'", path.display()))?;
    let metadata = file
        .metadata()
        .await
        .with_context(|| format!("failed to read file metadata '{}'", path.display()))?;
    if !metadata.is_file() {
        bail!("path '{}' is not a file", path.display());
    }

    let size = metadata.len();
    let mut start = 0_u64;
    let mut end = size.saturating_sub(1);
    let mut status = base_status;
    let mut content_range = None::<HeaderValue>;

    if let Some(range_header) = range_header {
        match parse_range_header(range_header, size) {
            RangeParse::Valid {
                start: range_start,
                end: range_end,
            } => {
                start = range_start;
                end = range_end;
                status = StatusCode::PARTIAL_CONTENT;
                let encoded = HeaderValue::from_str(&format!("bytes {start}-{end}/{size}"))
                    .context("failed to encode content-range header")?;
                content_range = Some(encoded);
            }
            RangeParse::Invalid => {
                cleanup_temp_file(cleanup_path).await;
                let encoded = HeaderValue::from_str(&format!("bytes */{size}"))
                    .context("failed to encode content-range header")?;
                return Ok(RangedFileBody {
                    body: Body::empty(),
                    content_length: HeaderValue::from_static("0"),
                    status: StatusCode::RANGE_NOT_SATISFIABLE,
                    content_range: Some(encoded),
                });
            }
        }
    }

    let content_length_u64 = if size == 0 { 0 } else { end - start + 1 };
    let content_length = HeaderValue::from_str(&content_length_u64.to_string())
        .context("failed to encode content-length header")?;
    let (tx, rx) = tokio_mpsc::channel::<Result<Bytes, std::io::Error>>(64);

    tokio::spawn(async move {
        let mut buffer = vec![0_u8; FILE_STREAM_CHUNK_SIZE];
        if start > 0
            && let Err(err) = file.seek(SeekFrom::Start(start)).await
        {
            let _ = tx.send(Err(err)).await;
            cleanup_temp_file(cleanup_path).await;
            return;
        }
        let mut remaining = content_length_u64;
        while remaining > 0 {
            let read_len = std::cmp::min(buffer.len() as u64, remaining) as usize;
            match file.read(&mut buffer[..read_len]).await {
                Ok(0) => break,
                Ok(read_len) => {
                    remaining = remaining.saturating_sub(read_len as u64);
                    if tx
                        .send(Ok(Bytes::copy_from_slice(&buffer[..read_len])))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                Err(err) => {
                    let _ = tx.send(Err(err)).await;
                    break;
                }
            }
        }
        cleanup_temp_file(cleanup_path).await;
    });

    Ok(RangedFileBody {
        body: Body::from_stream(ReceiverStream::new(rx)),
        content_length,
        status,
        content_range,
    })
}
