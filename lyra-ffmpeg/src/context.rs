// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashMap;
use std::ffi::{
    CString,
    c_void,
};
use std::marker::PhantomData;
use std::panic::{
    AssertUnwindSafe,
    catch_unwind,
};
use std::ptr::{
    NonNull,
    null,
    null_mut,
};
use std::sync::Arc;
use std::sync::atomic::{
    AtomicBool,
    Ordering,
};
use std::sync::{
    Condvar,
    Mutex,
};
use std::thread::JoinHandle;
use std::time::Duration;

use ffmpeg_sys_next::*;

use crate::audio::AudioCodec;
use crate::error::{
    Error,
    Result,
    av_error_string,
    ffmpeg_operation_error,
};
use crate::output::{
    Output,
    SeekCallback,
    SeekRequest,
    SeekResult,
    WriteCallback,
    WriteResult,
};

static FFMPEG_INITIALIZED: AtomicBool = AtomicBool::new(false);

/// Well below any real format's rate, the lowest in use being 5512 Hz. A lower declared rate lets a
/// tiny input expand into gigabytes of resampled output.
const MIN_INPUT_SAMPLE_RATE: i32 = 1_000;
const AVERROR_INPUT_CHANGED: i32 = -0x636e6701;
const AVERROR_OUTPUT_CHANGED: i32 = -0x636e6702;

fn ensure_ffmpeg_initialized() {
    if !FFMPEG_INITIALIZED.swap(true, Ordering::SeqCst) {
        unsafe {
            av_log_set_level(AV_LOG_WARNING);
        }
    }
}

fn to_cstring(value: &str, field: &'static str) -> Result<CString> {
    CString::new(value).map_err(|_| Error::InvalidCString { field })
}

pub struct FfmpegContext {
    input_path: String,
    start_ms: Option<u64>,
    end_ms: Option<u64>,
    output: Output,
}

impl FfmpegContext {
    pub fn builder() -> FfmpegContextBuilder {
        FfmpegContextBuilder::new()
    }

    pub fn start(self) -> Result<FfmpegHandle> {
        ensure_ffmpeg_initialized();
        FfmpegHandle::start(self)
    }
}

pub struct FfmpegContextBuilder {
    input: Option<String>,
    start_ms: Option<u64>,
    end_ms: Option<u64>,
    output: Option<Output>,
}

impl FfmpegContextBuilder {
    pub fn new() -> Self {
        Self {
            input: None,
            start_ms: None,
            end_ms: None,
            output: None,
        }
    }

    pub fn input(mut self, input: impl Into<String>) -> Self {
        self.input = Some(input.into());
        self
    }

    pub fn output(mut self, output: impl Into<Output>) -> Self {
        self.output = Some(output.into());
        self
    }

    pub fn start_ms(mut self, start_ms: Option<u64>) -> Self {
        self.start_ms = start_ms;
        self
    }

    pub fn end_ms(mut self, end_ms: Option<u64>) -> Self {
        self.end_ms = end_ms;
        self
    }

    pub fn build(self) -> Result<FfmpegContext> {
        let input_path = self
            .input
            .ok_or(Error::OpenInput("no input specified".into()))?;
        to_cstring(input_path.as_str(), "input path")?;
        let output = self
            .output
            .ok_or(Error::OpenOutput("no output specified".into()))?;
        output.validate()?;
        Ok(FfmpegContext {
            input_path,
            start_ms: self.start_ms,
            end_ms: self.end_ms,
            output,
        })
    }
}

impl Default for FfmpegContextBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[must_use = "dropping FfmpegHandle aborts the transcode and waits for the worker thread to exit"]
pub struct FfmpegHandle {
    thread: Option<JoinHandle<()>>,
    abort_flag: Arc<AtomicBool>,
    result: Arc<Mutex<Option<Result<()>>>>,
    done: Arc<(Mutex<bool>, Condvar)>,
}

impl FfmpegHandle {
    fn start(ctx: FfmpegContext) -> Result<Self> {
        let abort_flag = Arc::new(AtomicBool::new(false));
        let abort_clone = abort_flag.clone();
        let result: Arc<Mutex<Option<Result<()>>>> = Arc::new(Mutex::new(None));
        let result_clone = result.clone();
        let done = Arc::new((Mutex::new(false), Condvar::new()));
        let done_clone = done.clone();

        let thread = std::thread::spawn(move || {
            let r = run_transcode(ctx, abort_clone);
            *result_clone.lock().unwrap_or_else(|e| e.into_inner()) = Some(r);
            let (lock, cvar) = &*done_clone;
            *lock.lock().unwrap_or_else(|e| e.into_inner()) = true;
            cvar.notify_all();
        });

        Ok(Self {
            thread: Some(thread),
            abort_flag,
            result,
            done,
        })
    }

    pub fn wait(mut self) -> Result<()> {
        self.join_and_take_result()
    }

    /// Whether the transcode has stopped, by finishing, failing or being aborted.
    pub fn is_finished(&self) -> bool {
        *self.done.0.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn wait_for_completion(&self, timeout: Duration) -> bool {
        {
            let (lock, cvar) = &*self.done;
            let guard = lock.lock().unwrap_or_else(|e| e.into_inner());
            let (_guard, wait_result) = cvar
                .wait_timeout_while(guard, timeout, |done| !*done)
                .unwrap_or_else(|e| e.into_inner());
            !wait_result.timed_out()
        }
    }

    fn join_and_take_result(&mut self) -> Result<()> {
        if let Some(thread) = self.thread.take() {
            thread.join().map_err(|_| Error::ThreadJoin)?;
        }
        self.result
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .unwrap_or(Ok(()))
    }

    pub fn wait_completion_timeout(&mut self, timeout: Duration) -> Option<Result<()>> {
        self.wait_for_completion(timeout)
            .then(|| self.join_and_take_result())
    }

    pub fn abort_with_timeout(mut self, timeout: Duration) -> Result<()> {
        self.abort_flag.store(true, Ordering::SeqCst);
        let Some(result) = self.wait_completion_timeout(timeout) else {
            let _ = self.thread.take();
            return Err(Error::Timeout);
        };
        result
    }

    pub fn wait_timeout(&mut self, timeout: Duration) -> Result<()> {
        if !self.wait_for_completion(timeout) {
            self.abort_flag.store(true, Ordering::SeqCst);
            if let Some(thread) = self.thread.take() {
                let _ = thread.join();
            }
            return Err(Error::Timeout);
        }

        self.join_and_take_result()
    }
}

impl Drop for FfmpegHandle {
    fn drop(&mut self) {
        self.abort_flag.store(true, Ordering::SeqCst);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unfinished_test_handle() -> (FfmpegHandle, Arc<AtomicBool>) {
        let abort_flag = Arc::new(AtomicBool::new(false));
        let result = Arc::new(Mutex::new(None));
        let done = Arc::new((Mutex::new(false), Condvar::new()));
        let thread = std::thread::spawn(|| std::thread::sleep(Duration::from_millis(50)));

        (
            FfmpegHandle {
                thread: Some(thread),
                abort_flag: abort_flag.clone(),
                result,
                done,
            },
            abort_flag,
        )
    }

    #[test]
    fn abort_with_timeout_marks_abort_on_timeout() {
        let (handle, abort_flag) = unfinished_test_handle();

        let error = handle
            .abort_with_timeout(Duration::from_millis(1))
            .expect_err("unfinished worker should time out");

        assert!(matches!(error, Error::Timeout));
        assert!(abort_flag.load(Ordering::SeqCst));
    }

    #[test]
    fn copy_codec_keeps_time_ranges_on_remux_path() {
        let output = Output::new("/tmp/lyra-ffmpeg-copy.caf").codec(AudioCodec::Copy);

        assert!(!should_transcode(&output, Some(1_000), Some(2_000)));
    }

    #[test]
    fn builder_rejects_missing_or_invalid_rust_owned_specs_before_ffmpeg() {
        assert!(matches!(
            FfmpegContext::builder().build(),
            Err(Error::OpenInput(message)) if message == "no input specified"
        ));

        assert!(matches!(
            FfmpegContext::builder()
                .input("input.flac")
                .build(),
            Err(Error::OpenOutput(message)) if message == "no output specified"
        ));

        assert!(matches!(
            FfmpegContext::builder()
                .input("bad\0input.flac")
                .output(Output::new("/tmp/out.mp3"))
                .build(),
            Err(Error::InvalidCString {
                field: "input path"
            })
        ));
    }

    #[test]
    fn io_write_callback_records_rust_callback_outcomes() {
        let mut ctx = IoCallbackContext {
            write_callback: Box::new(|buf| WriteResult::Wrote(buf.len())),
            seek_callback: None,
            status: None,
        };
        let buf = [1_u8, 2, 3];

        let wrote = unsafe {
            io_write_callback(
                (&mut ctx as *mut IoCallbackContext).cast(),
                buf.as_ptr(),
                buf.len() as i32,
            )
        };

        assert_eq!(wrote, 3);
        assert!(ctx.status.is_none());

        ctx.write_callback = Box::new(|_| WriteResult::Wrote(usize::MAX));
        let clamped = unsafe {
            io_write_callback(
                (&mut ctx as *mut IoCallbackContext).cast(),
                buf.as_ptr(),
                buf.len() as i32,
            )
        };
        assert_eq!(clamped, 3);

        ctx.write_callback = Box::new(|_| WriteResult::Wrote(0));
        let zero = unsafe {
            io_write_callback(
                (&mut ctx as *mut IoCallbackContext).cast(),
                buf.as_ptr(),
                buf.len() as i32,
            )
        };
        assert_eq!(zero, AVERROR(EIO));
        assert!(matches!(
            ctx.status,
            Some(CallbackStatus::OutputError(ref message))
                if message == "write callback wrote zero bytes"
        ));

        ctx.status = None;
        ctx.write_callback = Box::new(|_| WriteResult::Finished);
        let finished = unsafe {
            io_write_callback(
                (&mut ctx as *mut IoCallbackContext).cast(),
                buf.as_ptr(),
                buf.len() as i32,
            )
        };
        assert_eq!(finished, AVERROR_EOF);
        assert!(matches!(ctx.status, Some(CallbackStatus::OutputStopped)));

        ctx.status = None;
        ctx.write_callback = Box::new(|_| WriteResult::error("sink failed"));
        let failed = unsafe {
            io_write_callback(
                (&mut ctx as *mut IoCallbackContext).cast(),
                buf.as_ptr(),
                buf.len() as i32,
            )
        };
        assert_eq!(failed, AVERROR(EIO));
        assert!(matches!(
            ctx.status,
            Some(CallbackStatus::OutputError(ref message)) if message == "sink failed"
        ));
    }

    #[test]
    fn io_write_callback_rejects_invalid_raw_inputs() {
        let mut ctx = IoCallbackContext {
            write_callback: Box::new(|buf| WriteResult::Wrote(buf.len())),
            seek_callback: None,
            status: None,
        };
        let buf = [1_u8];

        assert_eq!(
            unsafe { io_write_callback(null_mut(), buf.as_ptr(), buf.len() as i32) },
            AVERROR(EIO)
        );
        assert_eq!(
            unsafe {
                io_write_callback(
                    (&mut ctx as *mut IoCallbackContext).cast(),
                    null(),
                    buf.len() as i32,
                )
            },
            AVERROR(EIO)
        );
        assert_eq!(
            unsafe {
                io_write_callback((&mut ctx as *mut IoCallbackContext).cast(), buf.as_ptr(), 0)
            },
            AVERROR(EIO)
        );
        assert!(ctx.status.is_none());
    }

    #[test]
    fn io_write_callback_catches_panics() {
        let mut ctx = IoCallbackContext {
            write_callback: Box::new(|_| -> WriteResult {
                panic!("sink panic");
            }),
            seek_callback: None,
            status: None,
        };
        let buf = [1_u8];

        let result = unsafe {
            io_write_callback(
                (&mut ctx as *mut IoCallbackContext).cast(),
                buf.as_ptr(),
                buf.len() as i32,
            )
        };

        assert_eq!(result, AVERROR(EIO));
        assert!(matches!(ctx.status, Some(CallbackStatus::OutputPanic)));
    }

    #[test]
    fn io_seek_callback_maps_requests_and_status() {
        let mut seen = Vec::new();
        let mut ctx = IoCallbackContext {
            write_callback: Box::new(|buf| WriteResult::Wrote(buf.len())),
            seek_callback: Some(Box::new(move |request| {
                seen.push(request);
                match request {
                    SeekRequest::Start(offset) => SeekResult::Position(offset + 10),
                    SeekRequest::Current(_) => SeekResult::Unseekable,
                    SeekRequest::End(_) => SeekResult::Unsupported,
                    SeekRequest::Size => SeekResult::error("size failed"),
                }
            })),
            status: None,
        };
        let opaque = (&mut ctx as *mut IoCallbackContext).cast();

        assert_eq!(unsafe { io_seek_callback(opaque, 5, 0) }, 15);
        assert_eq!(
            unsafe { io_seek_callback(opaque, 5, 1) },
            AVERROR(ESPIPE) as i64
        );
        assert_eq!(
            unsafe { io_seek_callback(opaque, 5, 2) },
            AVERROR(ENOSYS) as i64
        );
        assert_eq!(
            unsafe { io_seek_callback(opaque, 0, AVSEEK_SIZE) },
            AVERROR(EIO) as i64
        );
        assert!(matches!(
            ctx.status,
            Some(CallbackStatus::SeekError(ref message)) if message == "size failed"
        ));
    }

    #[test]
    fn io_seek_callback_rejects_missing_or_unsupported_seek() {
        let mut ctx = IoCallbackContext {
            write_callback: Box::new(|buf| WriteResult::Wrote(buf.len())),
            seek_callback: None,
            status: None,
        };

        assert_eq!(
            unsafe { io_seek_callback(null_mut(), 0, 0) },
            AVERROR(EIO) as i64
        );
        assert_eq!(
            unsafe { io_seek_callback((&mut ctx as *mut IoCallbackContext).cast(), 0, 0) },
            AVERROR(ESPIPE) as i64
        );

        ctx.seek_callback = Some(Box::new(|_| SeekResult::Position(0)));
        assert_eq!(
            unsafe { io_seek_callback((&mut ctx as *mut IoCallbackContext).cast(), 0, 9999) },
            AVERROR(ENOSYS) as i64
        );
        assert!(ctx.status.is_none());
    }

    #[test]
    fn io_seek_callback_catches_panics() {
        let mut ctx = IoCallbackContext {
            write_callback: Box::new(|buf| WriteResult::Wrote(buf.len())),
            seek_callback: Some(Box::new(|_| -> SeekResult {
                panic!("seek panic");
            })),
            status: None,
        };

        let result = unsafe { io_seek_callback((&mut ctx as *mut IoCallbackContext).cast(), 0, 0) };

        assert_eq!(result, AVERROR(EIO) as i64);
        assert!(matches!(ctx.status, Some(CallbackStatus::SeekPanic)));
    }

    #[test]
    fn interrupt_callback_reads_abort_flag_without_ffmpeg() {
        let abort_flag = AtomicBool::new(false);

        assert_eq!(unsafe { interrupt_callback(null_mut()) }, 0);
        assert_eq!(
            unsafe { interrupt_callback((&abort_flag as *const AtomicBool).cast_mut().cast()) },
            0
        );

        abort_flag.store(true, Ordering::SeqCst);
        assert_eq!(
            unsafe { interrupt_callback((&abort_flag as *const AtomicBool).cast_mut().cast()) },
            1
        );
    }

    #[test]
    fn sample_rate_selection_never_exceeds_a_cap_nor_drops_below_the_input_uncapped() {
        let supported = [44_100, 48_000, 96_000];

        assert_eq!(select_sample_rate(48_000, None, Some(&supported)), 48_000);
        assert_eq!(select_sample_rate(47_000, None, Some(&supported)), 48_000);
        assert_eq!(select_sample_rate(45_000, None, Some(&supported)), 48_000);
        assert_eq!(select_sample_rate(192_000, None, Some(&supported)), 96_000);
        assert_eq!(
            select_sample_rate(96_000, Some(47_000), Some(&supported)),
            44_100
        );
        assert_eq!(
            select_sample_rate(96_000, Some(48_000), Some(&supported)),
            48_000
        );
        assert_eq!(
            select_sample_rate(96_000, Some(22_050), Some(&supported)),
            44_100
        );
        assert_eq!(select_sample_rate(50_000, None, None), 50_000);
        assert_eq!(select_sample_rate(50_000, Some(22_050), None), 22_050);
    }

    #[test]
    fn integer_sample_format_selection_matches_the_bit_depth() {
        let flac = [
            AVSampleFormat::AV_SAMPLE_FMT_S16,
            AVSampleFormat::AV_SAMPLE_FMT_S32,
        ];
        let alac = [
            AVSampleFormat::AV_SAMPLE_FMT_S32P,
            AVSampleFormat::AV_SAMPLE_FMT_S16P,
        ];

        assert_eq!(
            select_integer_sample_format(16, &flac),
            Some(AVSampleFormat::AV_SAMPLE_FMT_S16)
        );
        assert_eq!(
            select_integer_sample_format(24, &flac),
            Some(AVSampleFormat::AV_SAMPLE_FMT_S32)
        );
        assert_eq!(
            select_integer_sample_format(16, &alac),
            Some(AVSampleFormat::AV_SAMPLE_FMT_S16P)
        );
        assert_eq!(
            select_integer_sample_format(24, &alac),
            Some(AVSampleFormat::AV_SAMPLE_FMT_S32P)
        );
        assert_eq!(
            select_integer_sample_format(24, &[AVSampleFormat::AV_SAMPLE_FMT_FLTP]),
            None
        );
    }

    #[test]
    fn sample_format_selection_honors_supported_and_unrestricted_requests() {
        let supported = [
            AVSampleFormat::AV_SAMPLE_FMT_FLTP,
            AVSampleFormat::AV_SAMPLE_FMT_S16,
        ];

        assert_eq!(
            select_sample_format(Some(AVSampleFormat::AV_SAMPLE_FMT_S16), Some(&supported)),
            AVSampleFormat::AV_SAMPLE_FMT_S16
        );
        assert_eq!(
            select_sample_format(Some(AVSampleFormat::AV_SAMPLE_FMT_DBL), Some(&supported)),
            AVSampleFormat::AV_SAMPLE_FMT_FLTP
        );
        assert_eq!(
            select_sample_format(Some(AVSampleFormat::AV_SAMPLE_FMT_S16), None),
            AVSampleFormat::AV_SAMPLE_FMT_S16
        );
        assert_eq!(
            select_sample_format(None, Some(&[])),
            AVSampleFormat::AV_SAMPLE_FMT_FLTP
        );
    }
}

struct IoCallbackContext {
    write_callback: WriteCallback,
    seek_callback: Option<SeekCallback>,
    status: Option<CallbackStatus>,
}

#[derive(Debug, Clone)]
enum CallbackStatus {
    OutputStopped,
    OutputError(String),
    OutputPanic,
    SeekError(String),
    SeekPanic,
}

impl CallbackStatus {
    fn into_error(self) -> Error {
        match self {
            Self::OutputStopped => Error::OutputStopped,
            Self::OutputError(message) => Error::OutputCallback(message),
            Self::OutputPanic => Error::OutputCallbackPanic,
            Self::SeekError(message) => Error::SeekCallback(message),
            Self::SeekPanic => Error::SeekCallbackPanic,
        }
    }
}

unsafe extern "C" fn io_write_callback(
    opaque: *mut std::ffi::c_void,
    buf: *const u8,
    buf_size: i32,
) -> i32 {
    unsafe {
        if opaque.is_null() || buf.is_null() || buf_size <= 0 {
            return AVERROR(EIO);
        }
        let ctx = &mut *(opaque as *mut IoCallbackContext);
        let slice = std::slice::from_raw_parts(buf, buf_size as usize);
        match catch_unwind(AssertUnwindSafe(|| (ctx.write_callback)(slice))) {
            Ok(WriteResult::Wrote(0)) => {
                ctx.status = Some(CallbackStatus::OutputError(
                    "write callback wrote zero bytes".to_string(),
                ));
                AVERROR(EIO)
            }
            Ok(WriteResult::Wrote(bytes)) => bytes.min(buf_size as usize) as i32,
            Ok(WriteResult::Finished) => {
                ctx.status = Some(CallbackStatus::OutputStopped);
                AVERROR_EOF
            }
            Ok(WriteResult::Error(message)) => {
                ctx.status = Some(CallbackStatus::OutputError(message));
                AVERROR(EIO)
            }
            Err(_) => {
                ctx.status = Some(CallbackStatus::OutputPanic);
                AVERROR(EIO)
            }
        }
    }
}

unsafe extern "C" fn io_seek_callback(
    opaque: *mut std::ffi::c_void,
    offset: i64,
    whence: i32,
) -> i64 {
    unsafe {
        if opaque.is_null() {
            return AVERROR(EIO) as i64;
        }
        let ctx = &mut *(opaque as *mut IoCallbackContext);
        if let Some(ref mut seek_fn) = ctx.seek_callback {
            let Some(request) = seek_request_from_ffmpeg(offset, whence) else {
                return AVERROR(ENOSYS) as i64;
            };
            match catch_unwind(AssertUnwindSafe(|| seek_fn(request))) {
                Ok(SeekResult::Position(position)) => position,
                Ok(SeekResult::Unseekable) => AVERROR(ESPIPE) as i64,
                Ok(SeekResult::Unsupported) => AVERROR(ENOSYS) as i64,
                Ok(SeekResult::Error(message)) => {
                    ctx.status = Some(CallbackStatus::SeekError(message));
                    AVERROR(EIO) as i64
                }
                Err(_) => {
                    ctx.status = Some(CallbackStatus::SeekPanic);
                    AVERROR(EIO) as i64
                }
            }
        } else {
            AVERROR(ESPIPE) as i64
        }
    }
}

fn seek_request_from_ffmpeg(offset: i64, whence: i32) -> Option<SeekRequest> {
    match whence {
        AVSEEK_SIZE => Some(SeekRequest::Size),
        0 => Some(SeekRequest::Start(offset)),
        1 => Some(SeekRequest::Current(offset)),
        2 => Some(SeekRequest::End(offset)),
        _ => None,
    }
}

unsafe extern "C" fn interrupt_callback(opaque: *mut std::ffi::c_void) -> i32 {
    unsafe {
        if opaque.is_null() {
            return 0;
        }
        let abort_flag = &*(opaque as *const AtomicBool);
        i32::from(abort_flag.load(Ordering::SeqCst))
    }
}

struct InputContext {
    ptr: NonNull<AVFormatContext>,
}

impl InputContext {
    unsafe fn open(input_path: &CString, abort_flag: &AtomicBool) -> Result<Self> {
        unsafe {
            let mut fmt_ctx = avformat_alloc_context();
            if fmt_ctx.is_null() {
                return Err(Error::OpenInput("failed to allocate input context".into()));
            }
            (*fmt_ctx).interrupt_callback = AVIOInterruptCB {
                callback: Some(interrupt_callback),
                opaque: abort_flag as *const AtomicBool as *mut std::ffi::c_void,
            };

            let ret = avformat_open_input(&mut fmt_ctx, input_path.as_ptr(), null(), null_mut());
            if ret < 0 {
                avformat_close_input(&mut fmt_ctx);
                return Err(Error::OpenInput(av_error_string(ret)));
            }

            Ok(Self {
                ptr: NonNull::new(fmt_ctx).ok_or_else(|| {
                    Error::OpenInput("ffmpeg returned a null input context".into())
                })?,
            })
        }
    }

    fn as_mut_ptr(&self) -> *mut AVFormatContext {
        self.ptr.as_ptr()
    }

    unsafe fn audio_stream(&self) -> Result<(usize, Stream<'_>)> {
        unsafe {
            let nb_streams = (*self.as_mut_ptr()).nb_streams as usize;
            for index in 0..nb_streams {
                let stream = *(*self.as_mut_ptr()).streams.add(index);
                let Some(ptr) = NonNull::new(stream) else {
                    continue;
                };
                if (*(*stream).codecpar).codec_type == AVMediaType::AVMEDIA_TYPE_AUDIO {
                    return Ok((
                        index,
                        Stream {
                            ptr,
                            _ctx: PhantomData,
                        },
                    ));
                }
            }
            Err(Error::NoAudioStream)
        }
    }
}

impl Drop for InputContext {
    fn drop(&mut self) {
        unsafe {
            let mut ptr = self.ptr.as_ptr();
            avformat_close_input(&mut ptr);
        }
    }
}

struct Stream<'ctx> {
    ptr: NonNull<AVStream>,
    _ctx: PhantomData<&'ctx InputContext>,
}

impl Stream<'_> {
    fn as_mut_ptr(&self) -> *mut AVStream {
        self.ptr.as_ptr()
    }

    unsafe fn codec_params(&self) -> Result<CodecParams<'_>> {
        unsafe {
            Ok(CodecParams {
                ptr: NonNull::new((*self.as_mut_ptr()).codecpar)
                    .ok_or_else(|| Error::AllocCodecContext("stream codec parameters".into()))?,
                _stream: PhantomData,
            })
        }
    }
}

struct CodecParams<'stream> {
    ptr: NonNull<AVCodecParameters>,
    _stream: PhantomData<&'stream AVStream>,
}

impl CodecParams<'_> {
    fn as_ptr(&self) -> *const AVCodecParameters {
        self.ptr.as_ptr()
    }
}

struct OutputContext {
    ptr: NonNull<AVFormatContext>,
    io_ctx: Option<Box<IoCallbackContext>>,
    file_io_open: bool,
}

impl OutputContext {
    unsafe fn create(
        output: &mut Output,
        format_name: &str,
        abort_flag: &AtomicBool,
    ) -> Result<Self> {
        unsafe {
            let mut out_fmt_ctx: *mut AVFormatContext = null_mut();
            let format_c = to_cstring(format_name, "format name")?;
            let _filename_c = output
                .url
                .as_ref()
                .map(|url| to_cstring(url.as_str(), "output url"))
                .transpose()?;
            let filename_ptr = _filename_c.as_ref().map_or(null(), |c| c.as_ptr());

            let ret = avformat_alloc_output_context2(
                &mut out_fmt_ctx,
                null(),
                format_c.as_ptr(),
                filename_ptr,
            );
            if ret < 0 {
                return Err(Error::OpenOutput(format!(
                    "{} ({})",
                    format_name,
                    av_error_string(ret)
                )));
            }
            let mut out = Self {
                ptr: NonNull::new(out_fmt_ctx).ok_or(Error::AllocOutputContext)?,
                io_ctx: None,
                file_io_open: false,
            };
            (*out.as_mut_ptr()).interrupt_callback = AVIOInterruptCB {
                callback: Some(interrupt_callback),
                opaque: abort_flag as *const AtomicBool as *mut std::ffi::c_void,
            };

            if let Some(write_cb) = output.write_callback.take() {
                out.attach_callback_io(write_cb, output.seek_callback.take())?;
            }

            Ok(out)
        }
    }

    fn as_mut_ptr(&self) -> *mut AVFormatContext {
        self.ptr.as_ptr()
    }

    unsafe fn attach_callback_io(
        &mut self,
        write_callback: WriteCallback,
        seek_callback: Option<SeekCallback>,
    ) -> Result<()> {
        unsafe {
            let io_ctx = Box::new(IoCallbackContext {
                write_callback,
                seek_callback,
                status: None,
            });

            let io_ctx_ptr = Box::into_raw(io_ctx);
            let buffer_size = 32768;
            let buffer = av_malloc(buffer_size) as *mut u8;
            if buffer.is_null() {
                drop(Box::from_raw(io_ctx_ptr));
                return Err(Error::AllocAvio);
            }

            let has_seek = (*io_ctx_ptr).seek_callback.is_some();
            let avio_ctx = avio_alloc_context(
                buffer,
                buffer_size as i32,
                1,
                io_ctx_ptr as *mut std::ffi::c_void,
                None,
                Some(io_write_callback),
                if has_seek {
                    Some(io_seek_callback)
                } else {
                    None
                },
            );

            if avio_ctx.is_null() {
                av_free(buffer as *mut std::ffi::c_void);
                drop(Box::from_raw(io_ctx_ptr));
                return Err(Error::AllocAvio);
            }

            (*self.as_mut_ptr()).pb = avio_ctx;
            (*self.as_mut_ptr()).flags |= AVFMT_FLAG_CUSTOM_IO;
            self.io_ctx = Some(Box::from_raw(io_ctx_ptr));
            Ok(())
        }
    }

    unsafe fn open_file_output_if_needed(&mut self, output: &Output) -> Result<()> {
        unsafe {
            let Some(url) = output.url.as_ref() else {
                return Ok(());
            };
            if ((*(*self.as_mut_ptr()).oformat).flags & AVFMT_NOFILE) != 0
                || output.write_callback.is_some()
            {
                return Ok(());
            }
            let url_c = to_cstring(url.as_str(), "output url")?;
            let ret = avio_open(
                &mut (*self.as_mut_ptr()).pb,
                url_c.as_ptr(),
                AVIO_FLAG_WRITE,
            );
            if ret < 0 {
                return Err(Error::OpenOutput(av_error_string(ret)));
            }
            self.file_io_open = true;
            Ok(())
        }
    }

    fn callback_error(&self) -> Option<Error> {
        self.io_ctx
            .as_ref()
            .and_then(|ctx| ctx.status.clone())
            .map(CallbackStatus::into_error)
    }
}

impl Drop for OutputContext {
    fn drop(&mut self) {
        unsafe {
            if !(*self.as_mut_ptr()).pb.is_null() {
                if self.io_ctx.is_some() {
                    avio_context_free(&mut (*self.as_mut_ptr()).pb);
                } else if self.file_io_open {
                    avio_closep(&mut (*self.as_mut_ptr()).pb);
                }
            }
            self.io_ctx.take();
            avformat_free_context(self.as_mut_ptr());
        }
    }
}

struct CodecContext {
    ptr: NonNull<AVCodecContext>,
}

impl CodecContext {
    unsafe fn new(codec: *const AVCodec, description: impl Into<String>) -> Result<Self> {
        unsafe {
            let ptr = avcodec_alloc_context3(codec);
            Ok(Self {
                ptr: NonNull::new(ptr)
                    .ok_or_else(|| Error::AllocCodecContext(description.into()))?,
            })
        }
    }

    fn as_mut_ptr(&self) -> *mut AVCodecContext {
        self.ptr.as_ptr()
    }
}

impl Drop for CodecContext {
    fn drop(&mut self) {
        unsafe {
            let mut ptr = self.ptr.as_ptr();
            avcodec_free_context(&mut ptr);
        }
    }
}

struct Frame {
    ptr: NonNull<AVFrame>,
}

impl Frame {
    unsafe fn new(description: impl Into<String>) -> Result<Self> {
        unsafe {
            let ptr = av_frame_alloc();
            Ok(Self {
                ptr: NonNull::new(ptr)
                    .ok_or_else(|| Error::AllocCodecContext(description.into()))?,
            })
        }
    }

    fn as_mut_ptr(&self) -> *mut AVFrame {
        self.ptr.as_ptr()
    }
}

impl Drop for Frame {
    fn drop(&mut self) {
        unsafe {
            let mut ptr = self.ptr.as_ptr();
            av_frame_free(&mut ptr);
        }
    }
}

struct AudioFifo {
    ptr: NonNull<AVAudioFifo>,
}

impl AudioFifo {
    unsafe fn new(sample_fmt: AVSampleFormat, channels: i32, frame_size: i32) -> Result<Self> {
        unsafe {
            let ptr = av_audio_fifo_alloc(sample_fmt, channels, frame_size);
            Ok(Self {
                ptr: NonNull::new(ptr)
                    .ok_or_else(|| Error::AllocCodecContext("audio fifo".into()))?,
            })
        }
    }

    fn as_mut_ptr(&self) -> *mut AVAudioFifo {
        self.ptr.as_ptr()
    }
}

impl Drop for AudioFifo {
    fn drop(&mut self) {
        unsafe {
            av_audio_fifo_free(self.ptr.as_ptr());
        }
    }
}

struct Resampler {
    ptr: NonNull<SwrContext>,
}

impl Resampler {
    /// A resampler with `swr_opts` applied, configured from the first frame it converts. The options
    /// outlive each reconfiguration, which resets only the formats and buffered samples.
    unsafe fn new(swr_opts: &HashMap<String, String>) -> Result<Self> {
        unsafe {
            let resampler = Self {
                ptr: NonNull::new(swr_alloc())
                    .ok_or_else(|| Error::AllocCodecContext("swr context".into()))?,
            };
            for (key, value) in swr_opts {
                let key_c = to_cstring(key.as_str(), "swr option key")?;
                let value_c = to_cstring(value.as_str(), "swr option value")?;
                let ret = av_opt_set(
                    resampler.as_mut_ptr() as *mut _,
                    key_c.as_ptr(),
                    value_c.as_ptr(),
                    0,
                );
                if ret < 0 {
                    return Err(Error::AllocCodecContext(format!(
                        "swr {}: {}",
                        key,
                        av_error_string(ret)
                    )));
                }
            }
            Ok(resampler)
        }
    }

    fn as_mut_ptr(&self) -> *mut SwrContext {
        self.ptr.as_ptr()
    }

    fn is_configured(&self) -> bool {
        unsafe { swr_is_initialized(self.as_mut_ptr()) != 0 }
    }

    /// Maps `input`'s format onto `output`'s, discarding any samples still buffered.
    unsafe fn configure(&self, output: *const AVFrame, input: *const AVFrame) -> Result<()> {
        unsafe {
            if (*input).sample_rate < MIN_INPUT_SAMPLE_RATE {
                return Err(Error::UnsupportedInputSampleRate((*input).sample_rate));
            }
            let ret = swr_config_frame(self.as_mut_ptr(), output, input);
            if ret < 0 {
                return Err(ffmpeg_operation_error("configure resampler", ret));
            }
            let ret = swr_init(self.as_mut_ptr());
            if ret < 0 {
                return Err(ffmpeg_operation_error("initialize resampler", ret));
            }
            Ok(())
        }
    }
}

impl Drop for Resampler {
    fn drop(&mut self) {
        unsafe {
            let mut ptr = self.ptr.as_ptr();
            swr_free(&mut ptr);
        }
    }
}

struct Packet {
    inner: AVPacket,
}

impl Packet {
    fn new() -> Self {
        unsafe {
            Self {
                inner: std::mem::zeroed(),
            }
        }
    }

    fn unref(&mut self) {
        unsafe {
            av_packet_unref(&mut self.inner);
        }
    }
}

impl std::ops::Deref for Packet {
    type Target = AVPacket;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl std::ops::DerefMut for Packet {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Drop for Packet {
    fn drop(&mut self) {
        self.unref();
    }
}

fn run_transcode(ctx: FfmpegContext, abort_flag: Arc<AtomicBool>) -> Result<()> {
    unsafe { run_transcode_inner(ctx, abort_flag) }
}

unsafe fn run_transcode_inner(mut ctx: FfmpegContext, abort_flag: Arc<AtomicBool>) -> Result<()> {
    unsafe {
        let input_path_c = to_cstring(ctx.input_path.as_str(), "input path")?;

        let input = InputContext::open(&input_path_c, &abort_flag)?;
        let in_fmt_ctx = input.as_mut_ptr();

        let ret = avformat_find_stream_info(in_fmt_ctx, null_mut());
        if ret < 0 {
            return Err(ffmpeg_operation_error("find stream info", ret));
        }

        let (audio_stream_idx, in_stream_ref) = input.audio_stream()?;
        let in_stream = in_stream_ref.as_mut_ptr();
        let in_codecpar = in_stream_ref.codec_params()?;

        let format_name = ctx
            .output
            .format
            .clone()
            .unwrap_or_else(|| "mp3".to_string());
        let mut output = OutputContext::create(&mut ctx.output, &format_name, &abort_flag)?;
        let out_fmt_ctx = output.as_mut_ptr();

        let needs_transcode = should_transcode(&ctx.output, ctx.start_ms, ctx.end_ms);

        let out_stream = avformat_new_stream(out_fmt_ctx, null());
        if out_stream.is_null() {
            return Err(Error::AllocStream);
        }

        let enc_ctx = if needs_transcode {
            Some(setup_encoder(
                out_fmt_ctx,
                out_stream,
                in_codecpar.as_ptr(),
                &ctx.output,
            )?)
        } else {
            let ret = avcodec_parameters_copy((*out_stream).codecpar, in_codecpar.as_ptr());
            if ret < 0 {
                return Err(ffmpeg_operation_error("copy codec parameters", ret));
            }
            (*out_stream).time_base = (*in_stream).time_base;
            None
        };

        apply_format_opts(out_fmt_ctx, &ctx.output.format_opts)?;

        output.open_file_output_if_needed(&ctx.output)?;

        let ret = avformat_write_header(out_fmt_ctx, null_mut());
        if ret < 0 {
            return Err(output
                .callback_error()
                .unwrap_or_else(|| ffmpeg_operation_error("write header", ret)));
        }

        let result = if needs_transcode {
            let Some(enc_ctx) = enc_ctx.as_ref() else {
                return Err(Error::AllocCodecContext(
                    "missing encoder context for transcode".into(),
                ));
            };
            transcode_audio(
                in_fmt_ctx,
                out_fmt_ctx,
                in_stream,
                out_stream,
                audio_stream_idx,
                enc_ctx.as_mut_ptr(),
                &ctx.output.swr_opts,
                ctx.start_ms,
                ctx.end_ms,
                &abort_flag,
            )
        } else {
            remux_audio(
                in_fmt_ctx,
                out_fmt_ctx,
                in_stream,
                out_stream,
                audio_stream_idx,
                ctx.start_ms,
                ctx.end_ms,
                &abort_flag,
            )
        };

        if result.is_ok() || !abort_flag.load(Ordering::SeqCst) {
            let ret = av_write_trailer(out_fmt_ctx);
            if ret < 0 && result.is_ok() {
                return Err(output
                    .callback_error()
                    .unwrap_or_else(|| ffmpeg_operation_error("write trailer", ret)));
            }
        }

        result.map_err(|err| output.callback_error().unwrap_or(err))
    }
}

fn should_transcode(output: &Output, start_ms: Option<u64>, end_ms: Option<u64>) -> bool {
    if matches!(output.audio_codec_kind, Some(AudioCodec::Copy)) {
        return false;
    }
    if start_ms.is_some() || end_ms.is_some() {
        return true;
    }
    output.audio_codec.is_some()
}

unsafe fn timestamp_from_ms(ms: u64, time_base: AVRational) -> i64 {
    unsafe { av_rescale_q(ms as i64, AVRational { num: 1, den: 1000 }, time_base) }
}

unsafe fn seek_to_audio_start(
    in_fmt_ctx: *mut AVFormatContext,
    audio_idx: usize,
    in_stream: *mut AVStream,
    start_ms: Option<u64>,
) -> Result<Option<i64>> {
    let Some(start_ms) = start_ms else {
        return Ok(None);
    };
    if start_ms == 0 {
        return Ok(Some(0));
    }

    let start_ts = unsafe { timestamp_from_ms(start_ms, (*in_stream).time_base) };
    let ret =
        unsafe { av_seek_frame(in_fmt_ctx, audio_idx as i32, start_ts, AVSEEK_FLAG_BACKWARD) };
    if ret < 0 {
        return Err(ffmpeg_operation_error("seek audio start", ret));
    }
    unsafe { avformat_flush(in_fmt_ctx) };
    Ok(Some(start_ts))
}

fn bounded_output_sample_limit(
    start_ms: Option<u64>,
    end_ms: Option<u64>,
    sample_rate: i32,
) -> Option<i64> {
    let end_ms = end_ms?;
    let start_ms = start_ms.unwrap_or(0);
    if end_ms <= start_ms {
        return Some(0);
    }

    let duration_ms = end_ms - start_ms;
    let scaled = (duration_ms as u128 * sample_rate as u128) / 1000;
    Some(scaled.min(i64::MAX as u128) as i64)
}

unsafe fn setup_encoder(
    out_fmt_ctx: *mut AVFormatContext,
    out_stream: *mut AVStream,
    in_codecpar: *const AVCodecParameters,
    output: &Output,
) -> Result<CodecContext> {
    unsafe {
        let codec_name = output.audio_codec.as_deref().unwrap_or("libmp3lame");
        let codec_name_c = to_cstring(codec_name, "codec name")?;

        let encoder = avcodec_find_encoder_by_name(codec_name_c.as_ptr());
        if encoder.is_null() {
            return Err(Error::FindEncoder(codec_name.to_string()));
        }

        let enc_ctx = CodecContext::new(encoder, "encoder context")?;
        let enc_ctx_ptr = enc_ctx.as_mut_ptr();

        let input_sample_rate = (*in_codecpar).sample_rate.max(1);
        let output_sample_rate =
            select_encoder_sample_rate(encoder, input_sample_rate, output.audio_sample_rate)?;

        (*enc_ctx_ptr).sample_rate = output_sample_rate;
        if let Some(channels) = output.audio_channels.filter(|&c| c > 0) {
            av_channel_layout_default(&mut (*enc_ctx_ptr).ch_layout, channels);
        } else {
            (*enc_ctx_ptr).ch_layout = (*in_codecpar).ch_layout;
            if (*enc_ctx_ptr).ch_layout.nb_channels <= 0 {
                av_channel_layout_default(&mut (*enc_ctx_ptr).ch_layout, 2);
            }
        }
        (*enc_ctx_ptr).time_base = AVRational {
            num: 1,
            den: output_sample_rate,
        };

        let bit_depth = match output.audio_codec_kind {
            Some(codec @ (AudioCodec::Flac | AudioCodec::Alac)) => codec.encoded_bit_depth(
                output
                    .audio_bit_depth
                    .unwrap_or_else(|| source_bit_depth(in_codecpar)),
            ),
            _ => None,
        };
        (*enc_ctx_ptr).sample_fmt = match bit_depth {
            Some(bits) => select_encoder_integer_sample_format(enc_ctx_ptr, encoder, bits)?,
            None => select_encoder_sample_format(
                enc_ctx_ptr,
                encoder,
                output.audio_sample_fmt.map(|format| format.as_ffmpeg()),
            )?,
        };
        if let Some(bits) = bit_depth {
            (*enc_ctx_ptr).bits_per_raw_sample = bits as i32;
        }

        for (key, value) in &output.audio_codec_opts {
            let key_c = to_cstring(key.as_str(), "codec option key")?;
            let value_c = to_cstring(value.as_str(), "codec option value")?;
            let ret = av_opt_set(enc_ctx_ptr as *mut _, key_c.as_ptr(), value_c.as_ptr(), 0);
            if ret < 0 {
                return Err(ffmpeg_operation_error("set codec option", ret));
            }
        }

        if let Some(quality) = output.audio_global_quality {
            (*enc_ctx_ptr).flags |= AV_CODEC_FLAG_QSCALE as i32;
            (*enc_ctx_ptr).global_quality = quality * FF_QP2LAMBDA;
        }

        if ((*(*out_fmt_ctx).oformat).flags & AVFMT_GLOBALHEADER) != 0 {
            (*enc_ctx_ptr).flags |= AV_CODEC_FLAG_GLOBAL_HEADER as i32;
        }

        let ret = avcodec_open2(enc_ctx_ptr, encoder, null_mut());
        if ret < 0 {
            return Err(Error::OpenEncoder(av_error_string(ret)));
        }

        let ret = avcodec_parameters_from_context((*out_stream).codecpar, enc_ctx_ptr);
        if ret < 0 {
            return Err(ffmpeg_operation_error("copy encoder parameters", ret));
        }

        (*out_stream).time_base = (*enc_ctx_ptr).time_base;

        Ok(enc_ctx)
    }
}

unsafe fn query_encoder_supported_config(
    enc_ctx: *const AVCodecContext,
    encoder: *const AVCodec,
    config: AVCodecConfig,
    operation: &'static str,
) -> Result<Option<(*const c_void, usize)>> {
    unsafe {
        let mut configs = null();
        let mut config_count = 0;
        let ret = avcodec_get_supported_config(
            enc_ctx,
            encoder,
            config,
            0,
            &mut configs,
            &mut config_count,
        );
        if ret < 0 {
            return Err(ffmpeg_operation_error(operation, ret));
        }
        if configs.is_null() {
            return Ok(None);
        }
        let config_count = usize::try_from(config_count)
            .map_err(|_| Error::OpenEncoder("invalid supported configuration count".into()))?;

        Ok(Some((configs, config_count)))
    }
}

/// The sample rate `encoder` encodes audio at `input_rate`, capped at `cap`, with; see
/// [`select_sample_rate`]. The encoder's options don't change its rates, so they are queried
/// without a codec context.
unsafe fn select_encoder_sample_rate(
    encoder: *const AVCodec,
    input_rate: i32,
    cap: Option<i32>,
) -> Result<i32> {
    unsafe {
        let supported = query_encoder_supported_config(
            null(),
            encoder,
            AVCodecConfig::AV_CODEC_CONFIG_SAMPLE_RATE,
            "query encoder sample rates",
        )?;
        let supported =
            supported.map(|(configs, len)| std::slice::from_raw_parts(configs.cast::<i32>(), len));

        Ok(select_sample_rate(input_rate, cap, supported))
    }
}

/// The sample rate the encoder named `encoder_name` encodes audio at `input_rate`, capped at
/// `cap`, with.
pub(crate) fn encoder_sample_rate(
    encoder_name: &str,
    input_rate: i32,
    cap: Option<i32>,
) -> Result<i32> {
    let name = to_cstring(encoder_name, "codec name")?;
    // SAFETY: `name` is a valid C string, and the encoder FFmpeg returns is static.
    unsafe {
        let encoder = avcodec_find_encoder_by_name(name.as_ptr());
        if encoder.is_null() {
            return Err(Error::FindEncoder(encoder_name.to_string()));
        }
        select_encoder_sample_rate(encoder, input_rate, cap)
    }
}

/// The rate to encode audio at `input_rate` with, from the encoder's `supported` rates: under a
/// `cap`, the highest supported rate at or below it, so a cap is never exceeded; without one, the
/// lowest at or above the input, so the audio loses no bandwidth it needn't. When none qualifies,
/// the supported rate nearest that bound. An encoder without a list takes the cap or the input.
fn select_sample_rate(input_rate: i32, cap: Option<i32>, supported: Option<&[i32]>) -> i32 {
    let Some(rates) = supported.filter(|rates| !rates.is_empty()) else {
        return cap.unwrap_or(input_rate);
    };
    let rates = rates.iter().copied();
    match cap {
        Some(cap) => rates
            .clone()
            .filter(|&rate| rate <= cap)
            .max()
            .or_else(|| rates.min()),
        None => rates
            .clone()
            .filter(|&rate| rate >= input_rate)
            .min()
            .or_else(|| rates.max()),
    }
    .unwrap_or(input_rate)
}

unsafe fn select_encoder_sample_format(
    enc_ctx: *const AVCodecContext,
    encoder: *const AVCodec,
    requested: Option<AVSampleFormat>,
) -> Result<AVSampleFormat> {
    unsafe {
        let supported = query_encoder_supported_config(
            enc_ctx,
            encoder,
            AVCodecConfig::AV_CODEC_CONFIG_SAMPLE_FORMAT,
            "query encoder sample formats",
        )?;
        let supported = supported.map(|(configs, len)| {
            std::slice::from_raw_parts(configs.cast::<AVSampleFormat>(), len)
        });

        Ok(select_sample_format(requested, supported))
    }
}

/// The bit depth of the source's samples: as its stream records it, else as wide as the samples
/// it decodes to, with floating-point samples read as 16 bits.
unsafe fn source_bit_depth(in_codecpar: *const AVCodecParameters) -> u32 {
    unsafe {
        let recorded = [
            (*in_codecpar).bits_per_raw_sample,
            (*in_codecpar).bits_per_coded_sample,
        ];
        if let Some(bits) = recorded.into_iter().find(|bits| *bits > 0) {
            return bits as u32;
        }
        let decoded = (*in_codecpar).format;
        if decoded == AVSampleFormat::AV_SAMPLE_FMT_S32 as i32
            || decoded == AVSampleFormat::AV_SAMPLE_FMT_S32P as i32
        {
            24
        } else {
            16
        }
    }
}

/// The encoder's integer sample format wide enough for `bits`: 16-bit samples up to 16 bits,
/// 32-bit ones above.
unsafe fn select_encoder_integer_sample_format(
    enc_ctx: *const AVCodecContext,
    encoder: *const AVCodec,
    bits: u32,
) -> Result<AVSampleFormat> {
    unsafe {
        let supported = query_encoder_supported_config(
            enc_ctx,
            encoder,
            AVCodecConfig::AV_CODEC_CONFIG_SAMPLE_FORMAT,
            "query encoder sample formats",
        )?;
        let supported = supported
            .map(|(configs, len)| std::slice::from_raw_parts(configs.cast::<AVSampleFormat>(), len))
            .unwrap_or_default();
        select_integer_sample_format(bits, supported).ok_or_else(|| {
            Error::OpenEncoder(format!("the encoder stores no {bits}-bit integer samples"))
        })
    }
}

fn select_integer_sample_format(bits: u32, supported: &[AVSampleFormat]) -> Option<AVSampleFormat> {
    let wanted: &[AVSampleFormat] = if bits <= 16 {
        &[
            AVSampleFormat::AV_SAMPLE_FMT_S16,
            AVSampleFormat::AV_SAMPLE_FMT_S16P,
        ]
    } else {
        &[
            AVSampleFormat::AV_SAMPLE_FMT_S32,
            AVSampleFormat::AV_SAMPLE_FMT_S32P,
        ]
    };
    supported
        .iter()
        .copied()
        .find(|format| wanted.contains(format))
}

fn select_sample_format(
    requested: Option<AVSampleFormat>,
    supported: Option<&[AVSampleFormat]>,
) -> AVSampleFormat {
    match supported {
        None => requested.unwrap_or(AVSampleFormat::AV_SAMPLE_FMT_FLTP),
        Some(formats) => requested
            .filter(|requested| formats.contains(requested))
            .or_else(|| formats.first().copied())
            .unwrap_or(AVSampleFormat::AV_SAMPLE_FMT_FLTP),
    }
}

unsafe fn apply_format_opts(
    fmt_ctx: *mut AVFormatContext,
    opts: &HashMap<String, String>,
) -> Result<()> {
    unsafe {
        if (*fmt_ctx).priv_data.is_null() {
            return Ok(());
        }

        for (key, value) in opts {
            let key_c = to_cstring(key.as_str(), "format option key")?;
            let value_c = to_cstring(value.as_str(), "format option value")?;
            let ret = av_opt_set((*fmt_ctx).priv_data, key_c.as_ptr(), value_c.as_ptr(), 0);
            if ret < 0 {
                return Err(ffmpeg_operation_error("set format option", ret));
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
unsafe fn remux_audio(
    in_fmt_ctx: *mut AVFormatContext,
    out_fmt_ctx: *mut AVFormatContext,
    in_stream: *mut AVStream,
    out_stream: *mut AVStream,
    audio_idx: usize,
    start_ms: Option<u64>,
    end_ms: Option<u64>,
    abort_flag: &AtomicBool,
) -> Result<()> {
    unsafe {
        let start_ts = seek_to_audio_start(in_fmt_ctx, audio_idx, in_stream, start_ms)?;
        let end_ts = end_ms.map(|ms| timestamp_from_ms(ms, (*in_stream).time_base));

        let mut pkt = Packet::new();
        let mut first_packet_ts: Option<i64> = None;

        loop {
            if abort_flag.load(Ordering::SeqCst) {
                break;
            }

            let ret = av_read_frame(in_fmt_ctx, &mut *pkt);
            if ret < 0 {
                if ret == AVERROR_EOF {
                    break;
                }
                return Err(ffmpeg_operation_error("read frame", ret));
            }

            if pkt.stream_index as usize != audio_idx {
                pkt.unref();
                continue;
            }

            let packet_ts = if pkt.pts != AV_NOPTS_VALUE {
                pkt.pts
            } else if pkt.dts != AV_NOPTS_VALUE {
                pkt.dts
            } else {
                AV_NOPTS_VALUE
            };

            if let Some(start_ts) = start_ts
                && packet_ts != AV_NOPTS_VALUE
                && packet_ts < start_ts
            {
                pkt.unref();
                continue;
            }
            if let Some(end_ts) = end_ts
                && packet_ts != AV_NOPTS_VALUE
                && packet_ts >= end_ts
            {
                pkt.unref();
                break;
            }

            if first_packet_ts.is_none() && packet_ts != AV_NOPTS_VALUE {
                first_packet_ts = Some(packet_ts);
            }
            if let Some(first_packet_ts) = first_packet_ts {
                if pkt.pts != AV_NOPTS_VALUE {
                    pkt.pts = pkt.pts.saturating_sub(first_packet_ts);
                }
                if pkt.dts != AV_NOPTS_VALUE {
                    pkt.dts = pkt.dts.saturating_sub(first_packet_ts);
                }
            }

            pkt.stream_index = 0;
            av_packet_rescale_ts(&mut *pkt, (*in_stream).time_base, (*out_stream).time_base);
            pkt.pos = -1;

            let ret = av_interleaved_write_frame(out_fmt_ctx, &mut *pkt);
            pkt.unref();

            if ret < 0 {
                return Err(ffmpeg_operation_error("write remuxed frame", ret));
            }
        }

        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
unsafe fn transcode_audio(
    in_fmt_ctx: *mut AVFormatContext,
    out_fmt_ctx: *mut AVFormatContext,
    in_stream: *mut AVStream,
    out_stream: *mut AVStream,
    audio_idx: usize,
    enc_ctx: *mut AVCodecContext,
    swr_opts: &HashMap<String, String>,
    start_ms: Option<u64>,
    end_ms: Option<u64>,
    abort_flag: &AtomicBool,
) -> Result<()> {
    unsafe {
        let in_codecpar = (*in_stream).codecpar;
        let decoder = avcodec_find_decoder((*in_codecpar).codec_id);
        if decoder.is_null() {
            return Err(Error::FindEncoder("decoder".to_string()));
        }

        let dec_ctx_owner = CodecContext::new(decoder, "decoder context")?;
        let dec_ctx = dec_ctx_owner.as_mut_ptr();

        let ret = avcodec_parameters_to_context(dec_ctx, in_codecpar);
        if ret < 0 {
            return Err(ffmpeg_operation_error("copy decoder parameters", ret));
        }
        (*dec_ctx).pkt_timebase = (*in_stream).time_base;

        let ret = avcodec_open2(dec_ctx, decoder, null_mut());
        if ret < 0 {
            return Err(Error::OpenEncoder(format!(
                "decoder: {}",
                av_error_string(ret)
            )));
        }

        let resampler = Resampler::new(swr_opts)?;

        let frame_size = if (*enc_ctx).frame_size > 0 {
            (*enc_ctx).frame_size
        } else {
            1024
        };

        let fifo_owner = AudioFifo::new(
            (*enc_ctx).sample_fmt,
            (*enc_ctx).ch_layout.nb_channels,
            frame_size,
        )?;
        let fifo = fifo_owner.as_mut_ptr();

        let frame_owner = Frame::new("decode frame")?;
        let resampled_frame_owner = Frame::new("resampled frame")?;
        let output_frame_owner = Frame::new("output frame")?;
        let frame = frame_owner.as_mut_ptr();
        let resampled_frame = resampled_frame_owner.as_mut_ptr();
        let output_frame = output_frame_owner.as_mut_ptr();

        let mut pkt = Packet::new();
        let mut pts: i64 = 0;
        let start_ts = seek_to_audio_start(in_fmt_ctx, audio_idx, in_stream, start_ms)?;
        let max_output_samples =
            bounded_output_sample_limit(start_ms, end_ms, (*enc_ctx).sample_rate);
        let mut produced_samples: i64 = 0;

        loop {
            if abort_flag.load(Ordering::SeqCst) {
                break;
            }

            if let Some(max_output_samples) = max_output_samples
                && produced_samples >= max_output_samples
            {
                break;
            }

            let ret = av_read_frame(in_fmt_ctx, &mut *pkt);
            if ret < 0 {
                if ret != AVERROR_EOF {
                    return Err(ffmpeg_operation_error("read frame", ret));
                }
                drain_fifo_and_encode(
                    fifo,
                    enc_ctx,
                    output_frame,
                    out_fmt_ctx,
                    out_stream,
                    frame_size,
                    &mut pts,
                    max_output_samples,
                    &mut produced_samples,
                )?;
                flush_encoder(enc_ctx, out_fmt_ctx, out_stream)?;
                break;
            }

            if pkt.stream_index as usize != audio_idx {
                pkt.unref();
                continue;
            }

            let packet_ts = if pkt.pts != AV_NOPTS_VALUE {
                pkt.pts
            } else if pkt.dts != AV_NOPTS_VALUE {
                pkt.dts
            } else {
                AV_NOPTS_VALUE
            };
            if let Some(start_ts) = start_ts
                && packet_ts != AV_NOPTS_VALUE
                && packet_ts < start_ts
            {
                pkt.unref();
                continue;
            }

            let ret = avcodec_send_packet(dec_ctx, &*pkt);
            pkt.unref();
            if ret < 0 {
                return Err(ffmpeg_operation_error("send packet to decoder", ret));
            }

            let mut stop_after_decode = false;
            loop {
                let ret = avcodec_receive_frame(dec_ctx, frame);
                if ret == AVERROR(EAGAIN) || ret == AVERROR_EOF {
                    break;
                }
                if ret < 0 {
                    return Err(ffmpeg_operation_error("receive decoded frame", ret));
                }

                resample_into_fifo(&resampler, frame, resampled_frame, enc_ctx, fifo)?;

                while av_audio_fifo_size(fifo) >= frame_size {
                    let mut samples_to_read = frame_size;
                    if let Some(max_output_samples) = max_output_samples {
                        let remaining_samples = max_output_samples.saturating_sub(produced_samples);
                        if remaining_samples <= 0 {
                            stop_after_decode = true;
                            break;
                        }
                        samples_to_read = std::cmp::min(samples_to_read, remaining_samples as i32);
                    }

                    let read_samples =
                        read_fifo_frame(fifo, output_frame, enc_ctx, samples_to_read)?;
                    (*output_frame).pts = pts;
                    pts += read_samples as i64;
                    produced_samples += read_samples as i64;

                    encode_frame(enc_ctx, output_frame, out_fmt_ctx, out_stream)?;

                    if let Some(max_output_samples) = max_output_samples
                        && produced_samples >= max_output_samples
                    {
                        stop_after_decode = true;
                        break;
                    }
                }

                av_frame_unref(frame);
                av_frame_unref(resampled_frame);
                if stop_after_decode {
                    break;
                }
            }

            if stop_after_decode {
                drain_fifo_and_encode(
                    fifo,
                    enc_ctx,
                    output_frame,
                    out_fmt_ctx,
                    out_stream,
                    frame_size,
                    &mut pts,
                    max_output_samples,
                    &mut produced_samples,
                )?;
                flush_encoder(enc_ctx, out_fmt_ctx, out_stream)?;
                break;
            }
        }

        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
unsafe fn drain_fifo_and_encode(
    fifo: *mut AVAudioFifo,
    enc_ctx: *mut AVCodecContext,
    output_frame: *mut AVFrame,
    out_fmt_ctx: *mut AVFormatContext,
    out_stream: *mut AVStream,
    frame_size: i32,
    pts: &mut i64,
    max_output_samples: Option<i64>,
    produced_samples: &mut i64,
) -> Result<()> {
    unsafe {
        while av_audio_fifo_size(fifo) > 0 {
            let mut samples_to_read = std::cmp::min(av_audio_fifo_size(fifo), frame_size);
            if let Some(max_output_samples) = max_output_samples {
                let remaining_samples = max_output_samples.saturating_sub(*produced_samples);
                if remaining_samples <= 0 {
                    break;
                }
                samples_to_read = std::cmp::min(samples_to_read, remaining_samples as i32);
            }

            let read_samples = read_fifo_frame(fifo, output_frame, enc_ctx, samples_to_read)?;
            (*output_frame).pts = *pts;
            *pts += read_samples as i64;
            *produced_samples += read_samples as i64;

            encode_frame(enc_ctx, output_frame, out_fmt_ctx, out_stream)?;

            if let Some(max_output_samples) = max_output_samples
                && *produced_samples >= max_output_samples
            {
                break;
            }
        }
        Ok(())
    }
}

/// Resamples `frame` onto the encoder's format into `fifo`. A frame whose format differs from the
/// previous one first flushes what the resampler holds, then reconfigures it.
unsafe fn resample_into_fifo(
    resampler: &Resampler,
    frame: *mut AVFrame,
    resampled_frame: *mut AVFrame,
    enc_ctx: *mut AVCodecContext,
    fifo: *mut AVAudioFifo,
) -> Result<()> {
    unsafe {
        specify_channel_layout(&mut (*frame).ch_layout);
        prepare_resampled_frame(resampled_frame, enc_ctx)?;
        if !resampler.is_configured() {
            resampler.configure(resampled_frame, frame)?;
        }

        let mut ret = swr_convert_frame(resampler.as_mut_ptr(), resampled_frame, frame);
        if ret == AVERROR_INPUT_CHANGED || ret == AVERROR_OUTPUT_CHANGED {
            let flushed = swr_convert_frame(resampler.as_mut_ptr(), resampled_frame, null());
            if flushed < 0 {
                return Err(ffmpeg_operation_error("flush resampler", flushed));
            }
            write_fifo_frame(fifo, resampled_frame)?;

            prepare_resampled_frame(resampled_frame, enc_ctx)?;
            resampler.configure(resampled_frame, frame)?;
            ret = swr_convert_frame(resampler.as_mut_ptr(), resampled_frame, frame);
        }
        if ret < 0 {
            return Err(ffmpeg_operation_error("resample audio", ret));
        }
        write_fifo_frame(fifo, resampled_frame)
    }
}

/// An unspecified layout replaced by the default for its channel count, as the resampler stores it;
/// left unspecified, the resampler would report every frame as a format change.
fn specify_channel_layout(layout: &mut AVChannelLayout) {
    if layout.order == AVChannelOrder::AV_CHANNEL_ORDER_UNSPEC {
        unsafe { av_channel_layout_default(layout, layout.nb_channels) };
    }
}

/// Empties `frame` and gives it the encoder's format, for the resampler to size and fill.
unsafe fn prepare_resampled_frame(frame: *mut AVFrame, enc_ctx: *mut AVCodecContext) -> Result<()> {
    unsafe {
        set_encoder_format(frame, enc_ctx)?;
        specify_channel_layout(&mut (*frame).ch_layout);
        Ok(())
    }
}

unsafe fn set_encoder_format(frame: *mut AVFrame, enc_ctx: *mut AVCodecContext) -> Result<()> {
    unsafe {
        av_frame_unref(frame);
        (*frame).format = (*enc_ctx).sample_fmt as i32;
        (*frame).sample_rate = (*enc_ctx).sample_rate;
        let ret = av_channel_layout_copy(&mut (*frame).ch_layout, &(*enc_ctx).ch_layout);
        if ret < 0 {
            return Err(ffmpeg_operation_error("copy channel layout", ret));
        }
        Ok(())
    }
}

unsafe fn write_fifo_frame(fifo: *mut AVAudioFifo, frame: *const AVFrame) -> Result<()> {
    unsafe {
        let ret = av_audio_fifo_write(
            fifo,
            (*frame).extended_data as *const *mut c_void,
            (*frame).nb_samples,
        );
        if ret < 0 {
            return Err(ffmpeg_operation_error("write audio fifo", ret));
        }
        Ok(())
    }
}

/// Reads up to `samples` from `fifo` into `frame` in the encoder's format, returning how many it read.
unsafe fn read_fifo_frame(
    fifo: *mut AVAudioFifo,
    frame: *mut AVFrame,
    enc_ctx: *mut AVCodecContext,
    samples: i32,
) -> Result<i32> {
    unsafe {
        set_encoder_format(frame, enc_ctx)?;
        (*frame).nb_samples = samples;
        let ret = av_frame_get_buffer(frame, 0);
        if ret < 0 {
            return Err(ffmpeg_operation_error("allocate output frame", ret));
        }

        let read_samples =
            av_audio_fifo_read(fifo, (*frame).extended_data as *const *mut c_void, samples);
        if read_samples < 0 {
            return Err(ffmpeg_operation_error("read audio fifo", read_samples));
        }
        (*frame).nb_samples = read_samples;
        Ok(read_samples)
    }
}

unsafe fn encode_frame(
    enc_ctx: *mut AVCodecContext,
    frame: *mut AVFrame,
    out_fmt_ctx: *mut AVFormatContext,
    out_stream: *mut AVStream,
) -> Result<()> {
    unsafe {
        let ret = avcodec_send_frame(enc_ctx, frame);
        if ret < 0 {
            return Err(ffmpeg_operation_error("send frame to encoder", ret));
        }

        let mut pkt = Packet::new();

        loop {
            let ret = avcodec_receive_packet(enc_ctx, &mut *pkt);
            if ret == AVERROR(EAGAIN) || ret == AVERROR_EOF {
                break;
            }
            if ret < 0 {
                return Err(ffmpeg_operation_error("receive encoded packet", ret));
            }

            pkt.stream_index = 0;
            av_packet_rescale_ts(&mut *pkt, (*enc_ctx).time_base, (*out_stream).time_base);

            let ret = av_interleaved_write_frame(out_fmt_ctx, &mut *pkt);
            pkt.unref();

            if ret < 0 {
                return Err(ffmpeg_operation_error("write encoded packet", ret));
            }
        }

        Ok(())
    }
}

unsafe fn flush_encoder(
    enc_ctx: *mut AVCodecContext,
    out_fmt_ctx: *mut AVFormatContext,
    out_stream: *mut AVStream,
) -> Result<()> {
    unsafe {
        let ret = avcodec_send_frame(enc_ctx, null());
        if ret < 0 {
            return Err(ffmpeg_operation_error("flush encoder", ret));
        }

        let mut pkt = Packet::new();

        loop {
            let ret = avcodec_receive_packet(enc_ctx, &mut *pkt);
            if ret == AVERROR(EAGAIN) || ret == AVERROR_EOF {
                break;
            }
            if ret < 0 {
                return Err(ffmpeg_operation_error("receive flushed packet", ret));
            }

            pkt.stream_index = 0;
            av_packet_rescale_ts(&mut *pkt, (*enc_ctx).time_base, (*out_stream).time_base);

            let ret = av_interleaved_write_frame(out_fmt_ctx, &mut *pkt);
            pkt.unref();

            if ret < 0 {
                return Err(ffmpeg_operation_error("write flushed packet", ret));
            }
        }

        Ok(())
    }
}
