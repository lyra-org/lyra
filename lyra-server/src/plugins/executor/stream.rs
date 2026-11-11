// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Streamed response bodies: a producer function, run as its own Luau thread after the handler
//! returns, writes the body a chunk at a time through an [`ApiStreamWriter`].
//!
//! Only a producer that returns without a failed write ends the body; every other end, whatever
//! drops it, aborts the body, so a client never takes a cut-off document for a whole one.

use std::{
    cell::RefCell,
    collections::VecDeque,
    pin::Pin,
    rc::Rc,
    sync::{
        Arc,
        Mutex,
        Weak,
        atomic::{
            AtomicBool,
            AtomicU64,
            Ordering,
        },
    },
    task::{
        Context,
        Poll,
    },
    time::{
        Duration,
        Instant,
    },
};

use bytes::Bytes;
use futures::task::AtomicWaker;
use harmony_core::{
    LocalLuauTaskCompletion,
    LocalScheduler,
    ResumeLimit,
    SchedulerWake,
    UserDataBinding,
};
use harmony_luau as luau;

/// The largest chunk a producer writes at once, in bytes: what an encoder holds unflushed, so a
/// flush always fits in one write.
pub(crate) const MAX_STREAM_CHUNK_BYTES: usize = harmony_serde::MAX_BUFFERED_BYTES;
/// Streamed bodies produced at once; another waits for a place. Producers run batches on the heap
/// all plugins share, so this bounds how many working sets add up there.
const MAX_ACTIVE_STREAMS: usize = 16;
/// Chunks written ahead of the client.
const STREAM_CHANNEL_CAPACITY: usize = 4;

#[derive(Clone, Debug, PartialEq, Eq)]
enum StreamEnd {
    Finished,
    Failed(Arc<str>),
}

/// What the body, the writer and the executor share.
struct StreamShared {
    /// Raised when the client goes away or the producer is stopped; it interrupts the producer.
    closed: luau::InterruptSignal,
    /// The plugin's run is ending, as opposed to the client going away.
    stopped: AtomicBool,
    /// A write raised, so the producer's return doesn't make the body whole.
    write_failed: AtomicBool,
    end: Mutex<Option<StreamEnd>>,
    waker: AtomicWaker,
    bytes_sent: AtomicU64,
    /// Has the executor look at a stream it runs again, once the stream must stop.
    executor: Option<SchedulerWake>,
}

impl StreamShared {
    fn end(&self) -> Option<StreamEnd> {
        self.end
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }

    fn set_end(&self, end: StreamEnd) {
        {
            let mut current = self.end.lock().unwrap_or_else(|error| error.into_inner());
            if current.is_some() {
                return;
            }
            *current = Some(end.clone());
        }
        if end != StreamEnd::Finished {
            self.closed.raise();
        }
        self.waker.wake();
    }

    fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
        self.closed.raise();
        self.waker.wake();
        self.wake_executor();
    }

    fn wake_executor(&self) {
        if let Some(executor) = &self.executor {
            executor.wake();
        }
    }
}

/// The body of a streamed response, as the client reads it.
pub(crate) struct ApiResponseStream {
    rx: tokio::sync::mpsc::Receiver<Bytes>,
    shared: Arc<StreamShared>,
    done: bool,
}

impl ApiResponseStream {
    /// A body that ends at once, whole, for an answer that has none, such as one to a HEAD.
    pub(super) fn finished() -> Self {
        let (_, body, end) = stream_channel(None);
        end.finish();
        body
    }
}

impl std::fmt::Debug for ApiResponseStream {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ApiResponseStream")
            .finish_non_exhaustive()
    }
}

impl futures::Stream for ApiResponseStream {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        if let Poll::Ready(Some(chunk)) = self.rx.poll_recv(cx) {
            return Poll::Ready(Some(Ok(chunk)));
        }
        self.shared.waker.register(cx.waker());
        let Some(end) = self.shared.end() else {
            return Poll::Pending;
        };
        // A chunk sent before the end may have arrived since the poll above.
        if let Ok(chunk) = self.rx.try_recv() {
            return Poll::Ready(Some(Ok(chunk)));
        }
        self.done = true;
        match end {
            StreamEnd::Finished => Poll::Ready(None),
            StreamEnd::Failed(reason) => Poll::Ready(Some(Err(std::io::Error::other(format!(
                "the plugin's streamed response failed: {reason}"
            ))))),
        }
    }
}

impl Drop for ApiResponseStream {
    fn drop(&mut self) {
        self.shared.closed.raise();
        // An ended stream has left the executor's hands.
        if self.shared.end().is_none() {
            self.shared.wake_executor();
        }
    }
}

/// Ends the body: as finished only through [`Self::finish`], as failed however else it goes.
struct StreamEndGuard(Arc<StreamShared>);

impl StreamEndGuard {
    fn finish(self) {
        self.0.set_end(StreamEnd::Finished);
    }

    fn fail(self, reason: &str) {
        self.0.set_end(StreamEnd::Failed(Arc::from(reason)));
    }
}

impl Drop for StreamEndGuard {
    fn drop(&mut self) {
        self.0.set_end(StreamEnd::Failed(Arc::from(
            "the producer ended without finishing",
        )));
    }
}

/// The documented type of a chunk a writer takes.
pub(crate) struct StreamChunk;

impl harmony_luau::LuauTypeInfo for StreamChunk {
    fn luau_type() -> harmony_luau::LuauType {
        harmony_luau::LuauType::union(vec![
            harmony_luau::LuauType::literal("string"),
            harmony_luau::LuauType::literal("buffer"),
        ])
    }
}

#[harmony_macros::userdata(
    name = "ApiStreamWriter",
    description = "Writes a streamed response's body."
)]
#[derive(Clone)]
pub(crate) struct ApiStreamWriter {
    tx: tokio::sync::mpsc::Sender<Bytes>,
    shared: Arc<StreamShared>,
}

impl ApiStreamWriter {
    fn failed(&self, message: impl Into<String>) -> luau::Error {
        self.shared.write_failed.store(true, Ordering::Release);
        luau::Error::Runtime(message.into())
    }
}

#[harmony_macros::userdata_methods]
impl ApiStreamWriter {
    #[harmony(
        description = "Writes `chunk` to the body, waiting while the client is behind, and yields to other plugins' work. Raises past `MAX_STREAM_CHUNK_BYTES`, and once the client has gone away; after a raise the body never ends as whole.",
        args(chunk: StreamChunk)
    )]
    fn write(&self, chunk: luau::Value) -> luau::runtime::Result<luau::ScheduledFuture> {
        if self.shared.closed.is_raised() || self.shared.end().is_some() {
            return Err(self.failed("the stream is closed"));
        }
        let chunk = match chunk {
            luau::Value::String(bytes) | luau::Value::Buffer(bytes) => bytes,
            other => {
                return Err(self.failed(format!(
                    "stream chunks must be strings or buffers, got {}",
                    other.type_name()
                )));
            }
        };
        if chunk.len() > MAX_STREAM_CHUNK_BYTES {
            return Err(self.failed(format!(
                "a stream chunk of {} bytes is larger than MAX_STREAM_CHUNK_BYTES ({MAX_STREAM_CHUNK_BYTES})",
                chunk.len()
            )));
        }
        let writer = self.clone();
        // Host futures that are ready at once never park the thread, so the write waits a turn
        // of the scheduler even when the client keeps up, letting other plugins' work run.
        Ok(luau::ScheduledFuture::after(Duration::ZERO, async move {
            if chunk.is_empty() {
                return Ok(());
            }
            let len = chunk.len() as u64;
            writer
                .tx
                .send(Bytes::from(chunk))
                .await
                .map_err(|_| writer.failed("the stream is closed"))?;
            writer.shared.bytes_sent.fetch_add(len, Ordering::Relaxed);
            Ok(())
        }))
    }
}

fn stream_channel(
    executor: Option<SchedulerWake>,
) -> (ApiStreamWriter, ApiResponseStream, StreamEndGuard) {
    let (tx, rx) = tokio::sync::mpsc::channel(STREAM_CHANNEL_CAPACITY);
    let shared = Arc::new(StreamShared {
        closed: luau::InterruptSignal::new(),
        stopped: AtomicBool::new(false),
        write_failed: AtomicBool::new(false),
        end: Mutex::new(None),
        waker: AtomicWaker::new(),
        bytes_sent: AtomicU64::new(0),
        executor,
    });
    (
        ApiStreamWriter {
            tx,
            shared: shared.clone(),
        },
        ApiResponseStream {
            rx,
            shared: shared.clone(),
            done: false,
        },
        StreamEndGuard(shared),
    )
}

/// Stops the streams of a plugin whose run is ending, from whichever thread asks it to end. A
/// computing producer holds the executor, so the request to end its run would only be read once
/// it yields; its raised interrupt makes it yield now. The ending lasts until the executor starts
/// the plugin's next run, so a stream its ending run queues after the ask is stopped as it queues.
#[derive(Default)]
pub(crate) struct StreamStops(Mutex<Stops>);

#[derive(Default)]
struct Stops {
    streams: Vec<(String, Weak<StreamShared>)>,
    /// Plugins whose run is ending.
    ending: std::collections::HashSet<String>,
    /// The runtime is shutting down, ending every run.
    all_ending: bool,
}

impl StreamStops {
    fn stops(&self) -> std::sync::MutexGuard<'_, Stops> {
        self.0.lock().unwrap_or_else(|error| error.into_inner())
    }

    fn register(&self, plugin_id: &str, shared: &Arc<StreamShared>) {
        let mut stops = self.stops();
        stops
            .streams
            .retain(|(_, stream)| stream.strong_count() > 0);
        if stops.all_ending || stops.ending.contains(plugin_id) {
            shared.stop();
        }
        stops
            .streams
            .push((plugin_id.to_string(), Arc::downgrade(shared)));
    }

    /// Ends `plugin_id`'s run, or every run without one, stopping their streams.
    pub(crate) fn stop(&self, plugin_id: Option<&str>) {
        let mut stops = self.stops();
        match plugin_id {
            Some(plugin_id) => {
                stops.ending.insert(plugin_id.to_string());
            }
            None => stops.all_ending = true,
        }
        for (owner, stream) in &stops.streams {
            if plugin_id.is_none_or(|plugin_id| plugin_id == owner)
                && let Some(stream) = stream.upgrade()
            {
                stream.stop();
            }
        }
    }

    /// `plugin_id` has a new run, whose streams start unstopped.
    pub(super) fn run_started(&self, plugin_id: &str) {
        self.stops().ending.remove(plugin_id);
    }
}

/// What a failed stream's log names.
pub(crate) struct StreamOrigin {
    pub(crate) plugin_id: String,
    pub(crate) method: String,
    pub(crate) path: String,
}

struct Running {
    completion: Rc<LocalLuauTaskCompletion>,
    scheduler: Rc<LocalScheduler>,
    thread: luau::Thread,
}

struct StreamTask {
    produce: luau::Function,
    context: luau::CallContext,
    writer: ApiStreamWriter,
    end: StreamEndGuard,
    deadline: Instant,
    origin: StreamOrigin,
}

impl StreamTask {
    fn shared(&self) -> &StreamShared {
        &self.end.0
    }

    fn fail(self, error: &str) {
        let shared = self.shared();
        if shared.stopped.load(Ordering::Acquire) {
            tracing::info!(
                plugin_id = %self.origin.plugin_id,
                path = %self.origin.path,
                error = %error,
                "plugin streamed response stopped as its plugin's run ended"
            );
        } else if shared.closed.is_raised() {
            tracing::debug!(
                plugin_id = %self.origin.plugin_id,
                path = %self.origin.path,
                error = %error,
                "plugin streamed response stopped as its client went away"
            );
        } else {
            tracing::error!(
                plugin_id = %self.origin.plugin_id,
                method = %self.origin.method,
                path = %self.origin.path,
                error = %error,
                bytes_sent = shared.bytes_sent.load(Ordering::Relaxed),
                "plugin streamed response failed"
            );
        }
        self.end.fail(error);
    }

    /// Why the stream must end before it gets to run or goes on, if it must.
    fn stop_reason(&self, scheduler: &LocalScheduler) -> Option<&'static str> {
        let shared = self.shared();
        if shared.stopped.load(Ordering::Acquire) {
            Some("the plugin's run ended")
        } else if shared.closed.is_raised() {
            Some("the client went away")
        } else if Instant::now() >= self.deadline {
            Some("plugin handler deadline expired")
        } else if scheduler.is_group_cancelled(self.context.task_group) {
            Some("the plugin's run ended")
        } else {
            None
        }
    }

    fn start(&self, vm: &luau::Vm, scheduler: &Rc<LocalScheduler>) -> anyhow::Result<Running> {
        let writer =
            ApiStreamWriter::class().create_value(vm, &self.context.origin, self.writer.clone())?;
        let thread = vm.create_thread(&self.produce)?;
        let completion = Rc::new(LocalLuauTaskCompletion::default());
        scheduler.schedule_luau_thread_with_limit_and_completion(
            self.context.clone(),
            vm.clone(),
            thread.clone(),
            vec![writer],
            ResumeLimit::Until {
                deadline: self.deadline,
                signal: self.shared().closed.clone(),
            },
            completion.clone(),
        );
        Ok(Running {
            completion,
            scheduler: scheduler.clone(),
            thread,
        })
    }
}

/// The streams of a VM: those waiting for a place, and those producing.
#[derive(Default)]
pub(crate) struct StreamTasks {
    waiting: RefCell<VecDeque<StreamTask>>,
    running: RefCell<Vec<(StreamTask, Running)>>,
    stops: Arc<StreamStops>,
}

/// Whether a response with `status` to a request with `method` carries a body at all.
pub(super) fn has_body(method: &str, status: u16) -> bool {
    !method.eq_ignore_ascii_case("HEAD") && !matches!(status, 100..=199 | 204 | 304)
}

/// Queues `produce` to write a body, which the returned stream reads, before `deadline`. It runs
/// as `context`, once one of the places for producing streams is free.
pub(crate) fn queue_stream(
    vm: &luau::Vm,
    produce: luau::Function,
    context: luau::CallContext,
    deadline: Instant,
    origin: StreamOrigin,
) -> luau::runtime::Result<ApiResponseStream> {
    let tasks = vm.data().get::<StreamTasks>()?;
    let executor = vm.data().get::<LocalScheduler>()?.wake_handle();
    // Whoever queues it, even a task the executor is polling, the executor comes round to start it.
    executor.wake();
    let (writer, body, end) = stream_channel(Some(executor));
    tasks.stops.register(&origin.plugin_id, &end.0);
    tasks.waiting.borrow_mut().push_back(StreamTask {
        produce,
        context,
        writer,
        end,
        deadline,
        origin,
    });
    Ok(body)
}

/// The stops of `vm`'s streams, for the threads that ask plugin runs to end.
pub(super) fn stream_stops(vm: &luau::Vm) -> luau::runtime::Result<Arc<StreamStops>> {
    Ok(vm.data().get::<StreamTasks>()?.stops.clone())
}

/// Ends the streams whose producers finished or must stop, then starts waiting ones into the
/// places that are free. Returns the earliest deadline of those left; a stream stopped from another
/// thread wakes the executor, as does a producer's end.
pub(super) fn poll_streams(vm: &luau::Vm) -> Option<Instant> {
    let (Ok(tasks), Ok(scheduler)) = (
        vm.data().get::<StreamTasks>(),
        vm.data().get::<LocalScheduler>(),
    ) else {
        return None;
    };
    let waker = scheduler.wake_handle().waker();
    let mut cx = Context::from_waker(&waker);
    let mut running = tasks.running.borrow_mut();
    let mut index = 0;
    while index < running.len() {
        let (task, producing) = &running[index];
        let outcome = if let Some(reason) = task.stop_reason(&scheduler) {
            Some(Err(reason.to_string()))
        } else {
            match producing.completion.poll(&mut cx) {
                Poll::Ready(result) => Some(result.map(|_| ()).map_err(|error| error.message)),
                Poll::Pending => None,
            }
        };
        let Some(outcome) = outcome else {
            index += 1;
            continue;
        };
        let (task, producing) = running.swap_remove(index);
        producing.scheduler.cancel_luau_thread(&producing.thread);
        match outcome {
            // A producer that caught an interrupt or a failed write may still return.
            Ok(()) if task.shared().closed.is_raised() => {
                task.fail("the producer returned after it was stopped")
            }
            Ok(()) if task.shared().write_failed.load(Ordering::Acquire) => {
                task.fail("the producer returned after a failed write")
            }
            Ok(()) => task.end.finish(),
            Err(error) => task.fail(&error),
        }
    }

    let mut waiting = tasks.waiting.borrow_mut();
    let mut kept = VecDeque::with_capacity(waiting.len());
    while let Some(task) = waiting.pop_front() {
        if let Some(reason) = task.stop_reason(&scheduler) {
            task.fail(reason);
        } else if running.len() < MAX_ACTIVE_STREAMS {
            match task.start(vm, &scheduler) {
                Ok(producing) => {
                    // It has yet to run; the poll has its end wake the executor.
                    let _ = producing.completion.poll(&mut cx);
                    running.push((task, producing));
                }
                Err(error) => task.fail(&error.to_string()),
            }
        } else {
            // It still ends when it must, though it holds no place.
            kept.push_back(task);
        }
    }
    *waiting = kept;
    waiting
        .iter()
        .chain(running.iter().map(|(task, _)| task))
        .map(|task| task.deadline)
        .min()
}

/// Ends every stream as failed, as the runtime goes away.
pub(super) fn fail_streams(vm: &luau::Vm) {
    let Ok(tasks) = vm.data().get::<StreamTasks>() else {
        return;
    };
    let running = std::mem::take(&mut *tasks.running.borrow_mut());
    for (task, producing) in running {
        producing.scheduler.cancel_luau_thread(&producing.thread);
        task.fail("the plugin runtime shut down");
    }
    let waiting = std::mem::take(&mut *tasks.waiting.borrow_mut());
    for task in waiting {
        task.fail("the plugin runtime shut down");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::StreamExt;

    fn stream_channel() -> (ApiStreamWriter, ApiResponseStream, StreamEndGuard) {
        super::stream_channel(None)
    }

    #[tokio::test]
    async fn only_a_finish_ends_the_body_whole() {
        let (writer, mut body, end) = stream_channel();
        writer.tx.send(Bytes::from_static(b"a")).await.unwrap();
        end.finish();
        assert_eq!(body.next().await.unwrap().unwrap(), "a");
        assert!(body.next().await.is_none());

        let (writer, mut body, end) = stream_channel();
        writer.tx.send(Bytes::from_static(b"a")).await.unwrap();
        drop(end);
        assert_eq!(body.next().await.unwrap().unwrap(), "a");
        assert!(body.next().await.unwrap().is_err(), "a dropped end aborts");
        assert!(body.next().await.is_none());
        drop(writer);
    }

    #[tokio::test]
    async fn a_failure_reaches_a_waiting_reader_without_queue_room() {
        let (writer, mut body, end) = stream_channel();
        for _ in 0..STREAM_CHANNEL_CAPACITY {
            writer.tx.send(Bytes::from_static(b"x")).await.unwrap();
        }
        let reader = tokio::spawn(async move {
            let mut chunks = 0;
            while let Some(item) = body.next().await {
                match item {
                    Ok(_) => chunks += 1,
                    Err(_) => return chunks,
                }
            }
            panic!("the body ended whole");
        });
        drop(end);
        assert_eq!(reader.await.unwrap(), STREAM_CHANNEL_CAPACITY);
        assert!(writer.shared.closed.is_raised());
    }

    #[tokio::test]
    async fn a_finished_stream_ends_at_once() {
        let mut body = ApiResponseStream::finished();
        assert!(body.next().await.is_none());
    }

    #[test]
    fn dropping_the_body_closes_the_stream() {
        let (writer, body, _end) = stream_channel();
        assert!(!writer.shared.closed.is_raised());
        drop(body);
        assert!(writer.shared.closed.is_raised());
    }

    #[test]
    fn a_stream_queued_after_its_run_was_asked_to_end_is_stopped() {
        let stops = StreamStops::default();
        // The ask comes before the ending run's handler queues its stream.
        stops.stop(Some("mine"));
        let (_, _late, late) = stream_channel();
        stops.register("mine", &late.0);
        assert!(late.0.stopped.load(Ordering::Acquire) && late.0.closed.is_raised());
        let (_, _other, other) = stream_channel();
        stops.register("theirs", &other.0);
        assert!(!other.0.closed.is_raised(), "another plugin's run goes on");

        // The next run starts fresh.
        stops.run_started("mine");
        let (_, _next, next) = stream_channel();
        stops.register("mine", &next.0);
        assert!(!next.0.closed.is_raised());

        // A shutdown ends every run, for good.
        stops.stop(None);
        stops.run_started("mine");
        let (_, _after, after) = stream_channel();
        stops.register("mine", &after.0);
        assert!(after.0.closed.is_raised() && next.0.closed.is_raised());
    }

    #[test]
    fn stops_reach_only_their_plugin_s_streams() {
        let stops = StreamStops::default();
        let (_, _mine, mine) = stream_channel();
        let (_, _theirs, theirs) = stream_channel();
        stops.register("mine", &mine.0);
        stops.register("theirs", &theirs.0);
        stops.stop(Some("mine"));
        assert!(mine.0.stopped.load(Ordering::Acquire) && mine.0.closed.is_raised());
        assert!(!theirs.0.closed.is_raised());
        stops.stop(None);
        assert!(theirs.0.closed.is_raised());
    }

    #[test]
    fn heads_and_bodyless_statuses_have_no_body() {
        assert!(has_body("GET", 200));
        assert!(!has_body("HEAD", 200));
        assert!(!has_body("get", 204) && !has_body("GET", 304) && !has_body("GET", 101));
    }
}
