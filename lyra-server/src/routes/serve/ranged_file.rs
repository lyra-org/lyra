// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    borrow::Cow,
    path::Path as FsPath,
};

use anyhow::{
    Context,
    Result,
    bail,
};
use axum::body::Body;
use axum::http::{
    HeaderMap,
    HeaderValue,
    StatusCode,
    header::{
        ETAG,
        IF_RANGE,
        LAST_MODIFIED,
        RANGE,
    },
};
use bytes::Bytes;
use tokio::io::{
    AsyncReadExt,
    AsyncSeekExt,
    SeekFrom,
};
use tokio::sync::mpsc as tokio_mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::routes::streamed_body::streamed_body;

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
        end.min(size - 1)
    };
    if start > end {
        return RangeParse::Invalid;
    }
    RangeParse::Valid { start, end }
}

/// The range `request` asks for of a body whose ETag and Last-Modified are in `validators`. Under
/// an `If-Range` the range stands only when its entity tag strongly matches the ETag, or its date
/// is exactly the Last-Modified; otherwise it names another version, and the whole body is sent.
pub(crate) fn requested_range<'a>(
    request: &'a HeaderMap,
    validators: &HeaderMap,
) -> Option<&'a HeaderValue> {
    let range = request.get(RANGE)?;
    let Some(if_range) = request.get(IF_RANGE) else {
        return Some(range);
    };
    let validator = if if_range.as_bytes().starts_with(b"\"") {
        validators.get(ETAG)
    } else if if_range.as_bytes().starts_with(b"W/") {
        None
    } else {
        validators.get(LAST_MODIFIED)
    };
    (validator == Some(if_range)).then_some(range)
}

/// `request` with its `If-Range` settled against `validators`, for a body whose validators are
/// set only after Lyra serves it: the range stays only if it would stand, and the If-Range goes.
pub(crate) fn settle_if_range<'a>(
    request: &'a HeaderMap,
    validators: &HeaderMap,
) -> Cow<'a, HeaderMap> {
    if !request.contains_key(IF_RANGE) {
        return Cow::Borrowed(request);
    }
    let mut settled = request.clone();
    settled.remove(IF_RANGE);
    if requested_range(request, validators).is_none() {
        settled.remove(RANGE);
    }
    Cow::Owned(settled)
}

pub(crate) async fn build_ranged_file_body(
    path: &FsPath,
    range_header: Option<&HeaderValue>,
    base_status: StatusCode,
) -> Result<RangedFileBody> {
    let file = tokio::fs::File::open(path)
        .await
        .with_context(|| format!("failed to open file '{}'", path.display()))?;
    let metadata = file
        .metadata()
        .await
        .with_context(|| format!("failed to read file metadata '{}'", path.display()))?;
    if !metadata.is_file() {
        bail!("path '{}' is not a file", path.display());
    }
    build_ranged_body(file, metadata.len(), range_header, base_status)
}

/// Serves `size` bytes of an already open `file`, honoring a single byte range.
pub(crate) fn build_ranged_body(
    mut file: tokio::fs::File,
    size: u64,
    range_header: Option<&HeaderValue>,
    base_status: StatusCode,
) -> Result<RangedFileBody> {
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
    let shutdown = crate::services::shutdown::token();

    tokio::spawn(async move {
        let mut buffer = vec![0_u8; FILE_STREAM_CHUNK_SIZE];
        if start > 0
            && let Err(err) = tokio::select! {
                _ = shutdown.cancelled() => return,
                result = file.seek(SeekFrom::Start(start)) => result,
            }
        {
            let _ = tx.send(Err(err)).await;
            return;
        }
        let mut remaining = content_length_u64;
        while remaining > 0 {
            if shutdown.is_cancelled() {
                break;
            }
            let read_len = std::cmp::min(buffer.len() as u64, remaining) as usize;
            let read_result = tokio::select! {
                _ = shutdown.cancelled() => break,
                result = file.read(&mut buffer[..read_len]) => result,
            };
            match read_result {
                Ok(0) => break,
                Ok(read_len) => {
                    remaining = remaining.saturating_sub(read_len as u64);
                    let chunk = Ok(Bytes::copy_from_slice(&buffer[..read_len]));
                    tokio::select! {
                        _ = shutdown.cancelled() => break,
                        sent = tx.send(chunk) => {
                            if sent.is_err() {
                                break;
                            }
                        }
                    }
                }
                Err(err) => {
                    tokio::select! {
                        _ = shutdown.cancelled() => {}
                        _ = tx.send(Err(err)) => {}
                    }
                    break;
                }
            }
        }
    });

    Ok(RangedFileBody {
        body: streamed_body(ReceiverStream::new(rx)),
        content_length,
        status,
        content_range,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use axum::http::HeaderName;
    use std::time::{
        Duration,
        SystemTime,
        UNIX_EPOCH,
    };

    struct ShutdownResetGuard;

    impl Drop for ShutdownResetGuard {
        fn drop(&mut self) {
            crate::services::shutdown::reset();
        }
    }

    fn parse(range: &'static str, size: u64) -> Option<(u64, u64)> {
        match parse_range_header(&HeaderValue::from_static(range), size) {
            RangeParse::Valid { start, end } => Some((start, end)),
            RangeParse::Invalid => None,
        }
    }

    fn request(pairs: &[(HeaderName, &'static str)]) -> HeaderMap {
        pairs
            .iter()
            .map(|(name, value)| (name.clone(), HeaderValue::from_static(value)))
            .collect()
    }

    #[test]
    fn if_range_keeps_the_range_only_for_the_same_version() {
        const DATE: &str = "Wed, 21 Oct 2015 07:28:00 GMT";
        let validators = request(&[(ETAG, "\"v2\""), (LAST_MODIFIED, DATE)]);
        let ranged = |if_range: &'static str, validators: &HeaderMap| {
            requested_range(
                &request(&[(RANGE, "bytes=100-"), (IF_RANGE, if_range)]),
                validators,
            )
            .is_some()
        };
        assert!(ranged("\"v2\"", &validators), "a strong match");
        assert!(ranged(DATE, &validators), "the exact Last-Modified");
        assert!(!ranged("\"v1\"", &validators), "another version");
        assert!(!ranged("W/\"v2\"", &validators), "a weak tag never matches");
        assert!(
            !ranged("Wed, 21 Oct 2015 07:28:01 GMT", &validators),
            "another date"
        );
        let weak = request(&[(ETAG, "W/\"v2\"")]);
        assert!(!ranged("\"v2\"", &weak), "a weak ETag never matches");
        assert!(!ranged("\"v2\"", &HeaderMap::new()), "no validators");
        assert!(
            requested_range(&request(&[(RANGE, "bytes=100-")]), &HeaderMap::new()).is_some(),
            "a range without If-Range stands"
        );

        let asked = request(&[(RANGE, "bytes=100-"), (IF_RANGE, "\"v1\"")]);
        let settled = settle_if_range(&asked, &validators);
        assert!(!settled.contains_key(RANGE) && !settled.contains_key(IF_RANGE));
        let asked = request(&[(RANGE, "bytes=100-"), (IF_RANGE, "\"v2\"")]);
        let settled = settle_if_range(&asked, &validators);
        assert!(settled.contains_key(RANGE) && !settled.contains_key(IF_RANGE));
    }

    #[test]
    fn range_end_past_size_is_clamped() {
        assert_eq!(parse("bytes=0-99999999999", 1000), Some((0, 999)));
        assert_eq!(parse("bytes=500-1000", 1000), Some((500, 999)));
        assert_eq!(parse("bytes=999-999999", 1000), Some((999, 999)));
    }

    #[test]
    fn range_start_at_or_past_size_is_unsatisfiable() {
        assert_eq!(parse("bytes=1000-", 1000), None);
        assert_eq!(parse("bytes=1000-2000", 1000), None);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ranged_file_body_finishes_after_shutdown_cancelled() -> anyhow::Result<()> {
        let _runtime_guard = crate::services::testing::runtime_test_lock().await;
        let _guard = ShutdownResetGuard;
        let shutdown = crate::services::shutdown::reset();
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let path = std::env::temp_dir().join(format!("lyra-ranged-shutdown-test-{nanos}.bin"));
        let file_size = FILE_STREAM_CHUNK_SIZE * 96;
        tokio::fs::write(&path, vec![7_u8; file_size]).await?;

        let ranged = build_ranged_file_body(&path, None, StatusCode::OK).await?;
        shutdown.cancel();
        let bytes = tokio::time::timeout(Duration::from_secs(1), to_bytes(ranged.body, usize::MAX))
            .await??;

        assert!(bytes.len() <= file_size);
        let _ = tokio::fs::remove_file(path).await;
        Ok(())
    }
}
