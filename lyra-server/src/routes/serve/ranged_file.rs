// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    borrow::Cow,
    io,
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
use futures::{
    Stream,
    StreamExt,
};
use tokio::io::{
    AsyncReadExt,
    AsyncSeekExt,
    SeekFrom,
};
use tokio_util::io::ReaderStream;

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
    build_ranged_body(file, metadata.len(), range_header, base_status).await
}

/// Serves `size` bytes of an already open `file`, honoring a single byte range.
pub(crate) async fn build_ranged_body(
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
    if start > 0 {
        file.seek(SeekFrom::Start(start))
            .await
            .context("failed to seek to the range start")?;
    }

    Ok(RangedFileBody {
        body: streamed_body(exactly(file, content_length_u64)),
        content_length,
        status,
        content_range,
    })
}

/// The next `len` bytes of `file`, failing if it ends before them.
fn exactly(file: tokio::fs::File, len: u64) -> impl Stream<Item = io::Result<Bytes>> {
    let chunks = ReaderStream::with_capacity(file.take(len), FILE_STREAM_CHUNK_SIZE);
    futures::stream::unfold(Some((chunks, len)), |state| async move {
        let (mut chunks, remaining) = state?;
        match chunks.next().await {
            Some(Ok(chunk)) => {
                let remaining = remaining - chunk.len() as u64;
                Some((Ok(chunk), Some((chunks, remaining))))
            }
            Some(Err(err)) => Some((Err(err), None)),
            None if remaining > 0 => Some((
                Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!("the file ended {remaining} bytes short"),
                )),
                None,
            )),
            None => None,
        }
    })
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;
    use axum::http::HeaderName;

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

    /// A temporary file of `len` bytes, each its offset modulo 251.
    fn patterned_file(len: usize) -> anyhow::Result<(tempfile::NamedTempFile, Vec<u8>)> {
        let content: Vec<u8> = (0..len).map(|offset| (offset % 251) as u8).collect();
        let mut file = tempfile::NamedTempFile::new()?;
        file.write_all(&content)?;
        Ok((file, content))
    }

    /// The bytes `body` yields before it ends, and the kind of failure it ends with, if any.
    async fn drain(body: Body) -> (Vec<u8>, Option<io::ErrorKind>) {
        let mut stream = body.into_data_stream();
        let mut received = Vec::new();
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(chunk) => received.extend_from_slice(&chunk),
                Err(err) => {
                    let kind = std::iter::successors(Some(&err as &dyn std::error::Error), |err| {
                        err.source()
                    })
                    .find_map(|err| err.downcast_ref::<io::Error>())
                    .map(io::Error::kind);
                    return (received, Some(kind.expect("an io error")));
                }
            }
        }
        (received, None)
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

    #[tokio::test]
    async fn a_range_yields_exactly_its_bytes() -> anyhow::Result<()> {
        let (file, content) = patterned_file(FILE_STREAM_CHUNK_SIZE * 3)?;
        let size = content.len() as u64;
        let first = FILE_STREAM_CHUNK_SIZE - 10;
        let last = FILE_STREAM_CHUNK_SIZE * 2 + 10;
        let cases = [
            (format!("bytes={first}-{last}"), first, last),
            (
                "bytes=-100".to_owned(),
                content.len() - 100,
                content.len() - 1,
            ),
            (format!("bytes={first}-"), first, content.len() - 1),
        ];
        for (range, first, last) in cases {
            let ranged = build_ranged_file_body(
                file.path(),
                Some(&HeaderValue::from_str(&range)?),
                StatusCode::OK,
            )
            .await?;
            assert_eq!(ranged.status, StatusCode::PARTIAL_CONTENT, "{range}");
            assert_eq!(
                ranged.content_range,
                Some(HeaderValue::from_str(&format!(
                    "bytes {first}-{last}/{size}"
                ))?),
                "{range}"
            );
            assert_eq!(ranged.content_length, (last - first + 1).to_string());
            let (received, failure) = drain(ranged.body).await;
            assert!(failure.is_none(), "{range}: {failure:?}");
            assert!(received == content[first..=last], "{range}");
        }

        let whole = build_ranged_file_body(file.path(), None, StatusCode::OK).await?;
        assert_eq!(whole.status, StatusCode::OK);
        let (received, failure) = drain(whole.body).await;
        assert!(failure.is_none(), "{failure:?}");
        assert!(received == content);
        Ok(())
    }

    #[tokio::test]
    async fn a_file_cut_short_after_opening_fails_the_body() -> anyhow::Result<()> {
        let (file, content) = patterned_file(FILE_STREAM_CHUNK_SIZE * 2)?;
        let opened = tokio::fs::File::open(file.path()).await?;
        let size = opened.metadata().await?.len();
        let kept = FILE_STREAM_CHUNK_SIZE + 7;
        file.as_file().set_len(kept as u64)?;

        let ranged = build_ranged_body(opened, size, None, StatusCode::OK).await?;
        assert_eq!(ranged.content_length, size.to_string());
        let (received, failure) = drain(ranged.body).await;

        assert!(received == content[..kept]);
        assert_eq!(failure, Some(io::ErrorKind::UnexpectedEof));
        Ok(())
    }

    #[tokio::test]
    async fn a_body_is_read_to_its_end_through_shutdown() -> anyhow::Result<()> {
        let _runtime_guard = crate::services::testing::runtime_test_lock().await;
        let _guard = ShutdownResetGuard;
        let shutdown = crate::services::shutdown::reset();
        let (file, content) = patterned_file(FILE_STREAM_CHUNK_SIZE * 8)?;

        let ranged = build_ranged_file_body(file.path(), None, StatusCode::OK).await?;
        let mut stream = ranged.body.into_data_stream();
        let mut received = stream.next().await.expect("a first chunk")?.to_vec();
        shutdown.cancel();
        while let Some(chunk) = stream.next().await {
            received.extend_from_slice(&chunk?);
        }
        assert!(received == content);

        let ranged = build_ranged_file_body(file.path(), None, StatusCode::OK).await?;
        let (received, failure) = drain(ranged.body).await;
        assert!(failure.is_none(), "{failure:?}");
        assert!(received == content, "a body built after shutdown is whole");
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread")]
    async fn a_download_in_flight_at_shutdown_completes_within_the_drain() -> anyhow::Result<()> {
        use axum::routing::get;
        use rustix::fs::{
            CWD,
            Mode,
        };
        use tokio::io::{
            AsyncReadExt,
            AsyncWriteExt,
        };

        use crate::routes::listener::tests::{
            BOUND,
            connect,
            limits,
            start,
        };

        let _runtime_guard = crate::services::testing::runtime_test_lock().await;
        let _guard = ShutdownResetGuard;
        crate::services::shutdown::reset();
        // The body is served from a pipe, so it cannot be read past what the test has written.
        // Each half fits the pipe's buffer, and the writer holds the read end open too, so no
        // open or write here blocks.
        let content: Vec<u8> = (0..8 * 1024).map(|offset| (offset % 251) as u8).collect();
        let (before, after) = content.split_at(content.len() / 2);
        let dir = tempfile::tempdir()?;
        let fifo = dir.path().join("download");
        rustix::fs::mkfifoat(CWD, &fifo, Mode::RUSR | Mode::WUSR)?;
        let mut writer = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&fifo)?;
        let size = content.len() as u64;
        let app = axum::Router::new().route(
            "/download",
            get(move || {
                let fifo = fifo.clone();
                async move {
                    let file = tokio::fs::File::open(&fifo).await.unwrap();
                    let ranged = build_ranged_body(file, size, None, StatusCode::OK)
                        .await
                        .unwrap();
                    axum::response::Response::builder()
                        .header(axum::http::header::CONTENT_LENGTH, ranged.content_length)
                        .body(ranged.body)
                        .unwrap()
                }
            }),
        );
        let server = start(app, limits()).await;

        let mut stream = connect(server.addr).await;
        stream
            .write_all(b"GET /download HTTP/1.1\r\nHost: lyra.test\r\n\r\n")
            .await?;
        writer.write_all(before)?;
        let mut head = Vec::new();
        let mut received = vec![0; before.len()];
        tokio::time::timeout(BOUND, async {
            while !head.ends_with(b"\r\n\r\n") {
                head.push(stream.read_u8().await?);
            }
            stream.read_exact(&mut received).await?;
            anyhow::Ok(())
        })
        .await??;
        assert!(head.starts_with(b"HTTP/1.1 200"));

        crate::services::shutdown::cancel();
        let mut stopping = std::pin::pin!(server.stop());
        assert!(futures::poll!(stopping.as_mut()).is_pending());
        writer.write_all(after)?;
        tokio::time::timeout(BOUND, stream.read_to_end(&mut received)).await??;
        stopping.await;

        assert!(received == content);
        Ok(())
    }
}
