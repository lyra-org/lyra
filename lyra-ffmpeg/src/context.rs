// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashMap;
use std::ffi::CString;
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
        let output = Output::new("/tmp/lyra-ffmpeg-copy.caf").codec(crate::audio::AudioCodec::Copy);

        assert!(!should_transcode(&output, Some(1_000), Some(2_000)));
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
    fn from_raw(ptr: *mut SwrContext) -> Result<Self> {
        Ok(Self {
            ptr: NonNull::new(ptr).ok_or_else(|| Error::AllocCodecContext("swr context".into()))?,
        })
    }

    fn as_mut_ptr(&self) -> *mut SwrContext {
        self.ptr.as_ptr()
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
    if matches!(
        output.audio_codec_kind,
        Some(crate::audio::AudioCodec::Copy)
    ) {
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
        let output_sample_rate = match output.audio_sample_rate {
            Some(rate) => select_encoder_sample_rate(encoder, rate),
            None => select_encoder_sample_rate(encoder, input_sample_rate),
        };

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

        let mut sample_fmt = if !(*encoder).sample_fmts.is_null() {
            *(*encoder).sample_fmts
        } else {
            AVSampleFormat::AV_SAMPLE_FMT_FLTP
        };
        if let Some(requested) = output.audio_sample_fmt
            && encoder_supports_sample_fmt(encoder, requested.as_ffmpeg())
        {
            sample_fmt = requested.as_ffmpeg();
        }
        (*enc_ctx_ptr).sample_fmt = sample_fmt;

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

unsafe fn select_encoder_sample_rate(encoder: *const AVCodec, input_rate: i32) -> i32 {
    unsafe {
        if (*encoder).supported_samplerates.is_null() {
            return input_rate;
        }

        let mut best_rate = 0;
        let mut min_diff = i32::MAX;
        let mut i = 0;
        loop {
            let rate = *(*encoder).supported_samplerates.add(i);
            if rate == 0 {
                break;
            }
            if rate == input_rate {
                return input_rate;
            }
            let diff = (rate - input_rate).abs();
            if diff < min_diff {
                min_diff = diff;
                best_rate = rate;
            }
            i += 1;
        }

        if best_rate > 0 { best_rate } else { input_rate }
    }
}

unsafe fn encoder_supports_sample_fmt(encoder: *const AVCodec, sample_fmt: AVSampleFormat) -> bool {
    unsafe {
        if (*encoder).sample_fmts.is_null() {
            return false;
        }

        let mut i = 0;
        loop {
            let fmt = *(*encoder).sample_fmts.add(i);
            if fmt == AVSampleFormat::AV_SAMPLE_FMT_NONE {
                break;
            }
            if fmt == sample_fmt {
                return true;
            }
            i += 1;
        }
        false
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

        let swr_ctx_owner = create_resampler(dec_ctx, enc_ctx, swr_opts)?;
        let swr_ctx = swr_ctx_owner.as_mut_ptr();

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

                let dst_nb_samples = av_rescale_rnd(
                    swr_get_delay(swr_ctx, (*dec_ctx).sample_rate as i64)
                        + (*frame).nb_samples as i64,
                    (*enc_ctx).sample_rate as i64,
                    (*dec_ctx).sample_rate as i64,
                    AVRounding::AV_ROUND_UP,
                ) as i32;

                av_frame_unref(resampled_frame);
                (*resampled_frame).format = (*enc_ctx).sample_fmt as i32;
                (*resampled_frame).ch_layout = (*enc_ctx).ch_layout;
                (*resampled_frame).sample_rate = (*enc_ctx).sample_rate;
                (*resampled_frame).nb_samples = dst_nb_samples;

                let ret = av_frame_get_buffer(resampled_frame, 0);
                if ret < 0 {
                    av_frame_unref(frame);
                    return Err(ffmpeg_operation_error("allocate resampled frame", ret));
                }

                let converted_samples = swr_convert(
                    swr_ctx,
                    (*resampled_frame).data.as_mut_ptr(),
                    dst_nb_samples,
                    (*frame).data.as_ptr() as *mut *const u8,
                    (*frame).nb_samples,
                );
                if converted_samples < 0 {
                    av_frame_unref(frame);
                    av_frame_unref(resampled_frame);
                    return Err(ffmpeg_operation_error("resample audio", converted_samples));
                }

                (*resampled_frame).nb_samples = converted_samples;

                let ret = av_audio_fifo_write(
                    fifo,
                    (*resampled_frame).data.as_ptr() as *mut *mut std::ffi::c_void,
                    converted_samples,
                );
                if ret < 0 {
                    av_frame_unref(frame);
                    av_frame_unref(resampled_frame);
                    return Err(ffmpeg_operation_error("write audio fifo", ret));
                }

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

                    av_frame_unref(output_frame);
                    (*output_frame).format = (*enc_ctx).sample_fmt as i32;
                    (*output_frame).ch_layout = (*enc_ctx).ch_layout;
                    (*output_frame).sample_rate = (*enc_ctx).sample_rate;
                    (*output_frame).nb_samples = samples_to_read;

                    let ret = av_frame_get_buffer(output_frame, 0);
                    if ret < 0 {
                        return Err(ffmpeg_operation_error("allocate output frame", ret));
                    }

                    let read_samples = av_audio_fifo_read(
                        fifo,
                        (*output_frame).data.as_ptr() as *mut *mut std::ffi::c_void,
                        samples_to_read,
                    );
                    if read_samples < 0 {
                        return Err(ffmpeg_operation_error("read audio fifo", read_samples));
                    }

                    (*output_frame).nb_samples = read_samples;
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

            av_frame_unref(output_frame);
            (*output_frame).format = (*enc_ctx).sample_fmt as i32;
            (*output_frame).ch_layout = (*enc_ctx).ch_layout;
            (*output_frame).sample_rate = (*enc_ctx).sample_rate;
            (*output_frame).nb_samples = samples_to_read;

            let ret = av_frame_get_buffer(output_frame, 0);
            if ret < 0 {
                return Err(ffmpeg_operation_error("allocate output frame", ret));
            }

            let read_samples = av_audio_fifo_read(
                fifo,
                (*output_frame).data.as_ptr() as *mut *mut std::ffi::c_void,
                samples_to_read,
            );
            if read_samples < 0 {
                return Err(ffmpeg_operation_error("read audio fifo", read_samples));
            }

            (*output_frame).nb_samples = read_samples;
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

unsafe fn create_resampler(
    dec_ctx: *mut AVCodecContext,
    enc_ctx: *mut AVCodecContext,
    swr_opts: &HashMap<String, String>,
) -> Result<Resampler> {
    unsafe {
        let swr_ctx = swr_alloc();
        if swr_ctx.is_null() {
            return Err(Error::AllocCodecContext("swr context".into()));
        }

        if (*dec_ctx).sample_rate <= 0 || (*enc_ctx).sample_rate <= 0 {
            swr_free(&mut { swr_ctx });
            return Err(Error::AllocCodecContext(
                "swr sample rates must be greater than zero".into(),
            ));
        }

        let ret = av_opt_set_chlayout(
            swr_ctx as *mut _,
            c"in_chlayout".as_ptr(),
            &(*dec_ctx).ch_layout,
            0,
        );
        if ret < 0 {
            swr_free(&mut { swr_ctx });
            return Err(ffmpeg_operation_error("set swr input channel layout", ret));
        }
        let ret = av_opt_set_int(
            swr_ctx as *mut _,
            c"in_sample_rate".as_ptr(),
            (*dec_ctx).sample_rate as i64,
            0,
        );
        if ret < 0 {
            swr_free(&mut { swr_ctx });
            return Err(ffmpeg_operation_error("set swr input sample rate", ret));
        }
        let ret = av_opt_set_sample_fmt(
            swr_ctx as *mut _,
            c"in_sample_fmt".as_ptr(),
            (*dec_ctx).sample_fmt,
            0,
        );
        if ret < 0 {
            swr_free(&mut { swr_ctx });
            return Err(ffmpeg_operation_error("set swr input sample format", ret));
        }

        let ret = av_opt_set_chlayout(
            swr_ctx as *mut _,
            c"out_chlayout".as_ptr(),
            &(*enc_ctx).ch_layout,
            0,
        );
        if ret < 0 {
            swr_free(&mut { swr_ctx });
            return Err(ffmpeg_operation_error("set swr output channel layout", ret));
        }
        let ret = av_opt_set_int(
            swr_ctx as *mut _,
            c"out_sample_rate".as_ptr(),
            (*enc_ctx).sample_rate as i64,
            0,
        );
        if ret < 0 {
            swr_free(&mut { swr_ctx });
            return Err(ffmpeg_operation_error("set swr output sample rate", ret));
        }
        let ret = av_opt_set_sample_fmt(
            swr_ctx as *mut _,
            c"out_sample_fmt".as_ptr(),
            (*enc_ctx).sample_fmt,
            0,
        );
        if ret < 0 {
            swr_free(&mut { swr_ctx });
            return Err(ffmpeg_operation_error("set swr output sample format", ret));
        }

        for (key, value) in swr_opts {
            let key_c = to_cstring(key.as_str(), "swr option key")?;
            let value_c = to_cstring(value.as_str(), "swr option value")?;
            let ret = av_opt_set(swr_ctx as *mut _, key_c.as_ptr(), value_c.as_ptr(), 0);
            if ret < 0 {
                swr_free(&mut { swr_ctx });
                return Err(Error::AllocCodecContext(format!(
                    "swr {}: {}",
                    key,
                    av_error_string(ret)
                )));
            }
        }

        let ret = swr_init(swr_ctx);
        if ret < 0 {
            swr_free(&mut { swr_ctx });
            return Err(Error::AllocCodecContext(format!(
                "swr init: {}",
                av_error_string(ret)
            )));
        }

        Resampler::from_raw(swr_ctx)
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
