//! DataChunkRegistry - Fast path data stream registry
//!
//! # Dispatch semantics (issue #285 minimal closure)
//!
//! Each `stream_id` gets its own lazily-spawned worker task fed by a bounded
//! mpsc queue. This yields:
//!
//! - **Same stream = FIFO run-to-completion**: chunks of one stream are
//!   delivered to the callback strictly in arrival order, one at a time. The
//!   previous chunk's callback future fully resolves before the next starts.
//! - **Different streams = independent workers**: each stream has its own
//!   worker, so a slow callback on stream A never blocks stream B's callback
//!   *execution*. Enqueue runs in a shared per-class receive task, so under
//!   reliable overload a full stream-A queue can delay stream B's *enqueue*
//!   until A drains — but never its execution, and never RPC.
//! - **Reliable overflow = backpressure**: for `StreamReliable`, a full
//!   per-stream queue makes `dispatch` `await` the bounded send. The stall is
//!   confined to the **stream** receive path: RPC travels on a separate queue
//!   and receive task on both transports (WebRTC's split inbound queues,
//!   WebSocket's per-lane tasks), so stream backpressure can never starve RPC
//!   delivery. On WebRTC the stall cascades through the bounded lane/coordinator
//!   queues to the SCTP data channel's receive window, slowing the peer; it
//!   engages only when the app falls behind by a full queue depth. The callback
//!   still runs on its own worker task; we never inline-await it.
//! - **LatencyFirst overflow = drop**: for `StreamLatencyFirst`, a full queue
//!   drops the newest chunk and bumps `dropped_count`. The wire transport is
//!   already partially-reliable (maxRetransmits), so the app must not rely on
//!   every chunk arriving.
//! - **panic isolation**: a callback panic is caught, counted
//!   (`panic_count`), logged, and the worker continues with the next chunk. A
//!   sibling stream or the receive path is never taken down.
//! - **unregister = drain**: removing a stream drops the stored sender; the
//!   worker drains already-queued chunks then exits (already-accepted chunks are
//!   still delivered, matching reliable "accepted == delivered").
//! - **re-register = immediate swap**: the callback travels with each queued
//!   chunk (cloned at `dispatch` time), so replacing a stream's handler — even
//!   by re-registering the same `stream_id` without unregistering first — takes
//!   effect on the very next dispatched chunk. A worker never pins the callback
//!   it was spawned with, which also makes any residual/racing worker benign:
//!   whichever worker drains a chunk runs the callback the dispatcher captured
//!   for that chunk, never a stale one.
//! - **unregister + re-register = drain first**: if a stream is re-registered
//!   while its previous worker is draining already-accepted chunks, new
//!   dispatches wait for the previous worker to exit before creating a
//!   replacement worker. This preserves the single-worker invariant for the
//!   `stream_id`.
//! - **shutdown = cancel**: `shutdown()` cancels the shared token so workers
//!   drop queued chunks, let any in-flight callback finish, then exit; all
//!   worker handles are joined (with a bounded timeout, else aborted). No
//!   orphan tasks in either path.
//!
//! A worker is never resurrected after its channel closes: once unregistered or
//! shut down, a stream stays gone until explicitly re-registered.

use actr_protocol::{ActorResult, ActrId, DataChunk, PayloadType};
use futures_util::FutureExt as _;
use futures_util::future::BoxFuture;
use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio::task::{AbortHandle, JoinHandle};
use tokio_util::sync::CancellationToken;

/// Default per-stream bounded queue depth.
///
/// Compile-time constant for v1; a configurable buffer model is deferred to the
/// concurrency-model RFC. Tests use [`DataChunkRegistry::with_capacity`].
const DEFAULT_STREAM_QUEUE_DEPTH: usize = 64;

/// Total budget for joining worker tasks during [`DataChunkRegistry::shutdown`].
const SHUTDOWN_JOIN_TIMEOUT: Duration = Duration::from_secs(5);

/// Stream chunk callback type
///
/// # Design Rationale
/// Fast path is stream-based push, not RPC, so it doesn't need full Context:
/// - Only passes sender ActrId (to know where data comes from)
/// - Doesn't pass Context (avoids confusing RPC and Stream semantics)
/// - If reverse signaling needed, user should send via OutboundGate
pub(crate) type DataChunkCallback =
    Arc<dyn Fn(DataChunk, ActrId) -> BoxFuture<'static, ActorResult<()>> + Send + Sync>;

/// A chunk queued for a stream's worker, carrying the sender identity and the
/// callback captured at dispatch time.
///
/// The callback is enqueued per-chunk (rather than pinned when the worker is
/// spawned) so that re-registering a `stream_id`'s handler takes effect on the
/// next dispatched chunk instead of being shadowed by the worker's original
/// callback.
struct QueuedChunk {
    chunk: DataChunk,
    sender_id: ActrId,
    callback: DataChunkCallback,
}

/// Per-stream serial executor: a bounded queue plus the worker task draining it.
struct StreamWorker {
    /// Bounded sender into the worker's queue. `None` means the worker is
    /// draining a previous registration and must finish before a replacement
    /// worker can be spawned for the same stream.
    tx: Option<mpsc::Sender<QueuedChunk>>,
    /// Worker task handle (retained until shutdown or until the task exits).
    handle: JoinHandle<()>,
    /// Monotonic identity used so an exiting worker only cleans up its own slot.
    generation: u64,
    /// Completion signal for dispatches waiting on a draining old worker.
    done: Arc<WorkerDone>,
}

/// Ensures workers cannot detach if the async shutdown coordinator is itself
/// cancelled after taking ownership of their join handles.
struct AbortWorkersOnDrop {
    handles: Vec<AbortHandle>,
    armed: bool,
}

impl AbortWorkersOnDrop {
    fn new(handles: &[JoinHandle<()>]) -> Self {
        Self {
            handles: handles.iter().map(JoinHandle::abort_handle).collect(),
            armed: true,
        }
    }

    fn abort(&mut self) {
        for handle in &self.handles {
            handle.abort();
        }
        self.armed = false;
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for AbortWorkersOnDrop {
    fn drop(&mut self) {
        if self.armed {
            self.abort();
        }
    }
}

struct WorkerDone {
    finished: AtomicBool,
    done_tx: watch::Sender<bool>,
}

impl WorkerDone {
    fn new() -> Self {
        let (done_tx, _) = watch::channel(false);
        Self {
            finished: AtomicBool::new(false),
            done_tx,
        }
    }

    fn is_finished(&self) -> bool {
        self.finished.load(Ordering::Acquire)
    }

    fn subscribe(&self) -> watch::Receiver<bool> {
        self.done_tx.subscribe()
    }

    fn mark_finished(&self) {
        self.finished.store(true, Ordering::Release);
        let _ = self.done_tx.send(true);
    }
}

#[derive(Default)]
struct StreamEntry {
    callback: Option<DataChunkCallback>,
    worker: Option<StreamWorker>,
}

#[derive(Default)]
struct RegistryState {
    streams: HashMap<String, StreamEntry>,
    shutting_down: bool,
    next_generation: u64,
}

struct RegistryInner {
    state: Mutex<RegistryState>,
    shutdown: CancellationToken,
    queue_depth: usize,
    panic_count: Arc<AtomicU64>,
    dropped_count: Arc<AtomicU64>,
}

/// DataChunkRegistry - Stream chunk callback manager
///
/// # Responsibilities
/// - Receive DataChunk from Stream lanes (stream-format data packets)
/// - Maintain stream_id → callback mapping
/// - Serialize callbacks per stream via a bounded per-stream worker (see module docs)
///
/// # Typical Use Cases
/// - Streaming RPC (peer push streams)
/// - Real-time collaborative editing (multi-user editing sync)
/// - Game state streams (position updates, event streams)
/// - Log streams, sensor data streams, metrics streams
pub(crate) struct DataChunkRegistry {
    inner: Arc<RegistryInner>,
}

impl Default for DataChunkRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl DataChunkRegistry {
    /// Create a registry with its own root cancellation token.
    pub(crate) fn new() -> Self {
        Self::build(DEFAULT_STREAM_QUEUE_DEPTH, CancellationToken::new())
    }

    /// Create a registry whose worker lifecycle is tied to `shutdown`.
    ///
    /// The node passes a child of its own shutdown token so a node-wide
    /// shutdown drains all stream workers.
    pub(crate) fn with_shutdown(shutdown: CancellationToken) -> Self {
        Self::build(DEFAULT_STREAM_QUEUE_DEPTH, shutdown)
    }

    /// Test constructor with an explicit (typically tiny) queue depth so
    /// overflow / backpressure paths can be exercised deterministically.
    #[cfg(test)]
    pub(crate) fn with_capacity(queue_depth: usize) -> Self {
        Self::build(queue_depth.max(1), CancellationToken::new())
    }

    fn build(queue_depth: usize, shutdown: CancellationToken) -> Self {
        Self {
            inner: Arc::new(RegistryInner {
                state: Mutex::new(RegistryState::default()),
                shutdown,
                queue_depth,
                panic_count: Arc::new(AtomicU64::new(0)),
                dropped_count: Arc::new(AtomicU64::new(0)),
            }),
        }
    }

    /// Number of callback panics isolated so far (observability / metric hook).
    #[allow(dead_code)]
    pub(crate) fn panic_count(&self) -> u64 {
        self.inner.panic_count.load(Ordering::Relaxed)
    }

    /// Number of chunks dropped by a full LatencyFirst queue (observability / metric hook).
    #[allow(dead_code)]
    pub(crate) fn dropped_count(&self) -> u64 {
        self.inner.dropped_count.load(Ordering::Relaxed)
    }

    /// Test-only handle to the shared shutdown token so tests can observe
    /// cancellation deterministically without sleeping.
    #[cfg(test)]
    pub(crate) fn shutdown_token(&self) -> CancellationToken {
        self.inner.shutdown.clone()
    }

    #[cfg(test)]
    pub(crate) fn callback_len(&self) -> usize {
        self.inner
            .state
            .lock()
            .expect("data stream registry state poisoned")
            .streams
            .values()
            .filter(|entry| entry.callback.is_some())
            .count()
    }

    #[cfg(test)]
    pub(crate) fn worker_len(&self) -> usize {
        self.inner
            .state
            .lock()
            .expect("data stream registry state poisoned")
            .streams
            .values()
            .filter(|entry| {
                entry
                    .worker
                    .as_ref()
                    .and_then(|worker| worker.tx.as_ref())
                    .is_some()
            })
            .count()
    }

    /// Register stream callback
    ///
    /// # Arguments
    /// - `stream_id`: stream identifier (must be globally unique)
    /// - `callback`: data stream handler callback
    pub(crate) fn register(&self, stream_id: String, callback: DataChunkCallback) {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("data stream registry state poisoned");
        if state.shutting_down || self.inner.shutdown.is_cancelled() {
            tracing::debug!(
                stream_id = %stream_id,
                "data stream registry is shut down; ignoring register"
            );
            return;
        }

        state.streams.entry(stream_id.clone()).or_default().callback = Some(callback);
        tracing::info!("📡 Registered data stream handler: {}", stream_id);
    }

    /// Unregister stream callback (drain semantics)
    ///
    /// Removes the callback and drops the stored worker sender. The worker
    /// drains any already-queued chunks and then exits on its own; it is never
    /// resurrected.
    pub(crate) fn unregister(&self, stream_id: &str) {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("data stream registry state poisoned");

        let should_remove = if let Some(entry) = state.streams.get_mut(stream_id) {
            entry.callback = None;
            if let Some(worker) = entry.worker.as_mut() {
                // Dropping the sender closes the worker queue after any
                // dispatch clones complete. The handle stays tracked so a
                // re-register cannot create a concurrent same-stream worker.
                worker.tx.take();
            }

            entry
                .worker
                .as_ref()
                .is_none_or(|worker| worker.done.is_finished())
        } else {
            false
        };

        if should_remove {
            state.streams.remove(stream_id);
        }

        tracing::info!("🚫 Unregistered data stream handler: {}", stream_id);
    }

    /// Force-close the registry during synchronous teardown.
    ///
    /// Normal shutdown uses [`Self::shutdown`] so in-flight callbacks can
    /// finish. Drop paths cannot await, so they cancel the registry, remove all
    /// callbacks to break ownership cycles, and abort any remaining workers.
    pub(crate) fn clear(&self) {
        self.inner.shutdown.cancel();

        let (count, handles) = {
            let mut state = self.inner.state.lock().unwrap_or_else(|poisoned| {
                tracing::error!(
                    "data stream registry state poisoned during forced teardown; recovering"
                );
                poisoned.into_inner()
            });
            state.shutting_down = true;

            let count = state
                .streams
                .values()
                .filter(|entry| entry.callback.is_some())
                .count();
            let handles = state
                .streams
                .drain()
                .filter_map(|(_, mut entry)| {
                    entry.worker.take().map(|mut worker| {
                        worker.tx.take();
                        worker.handle
                    })
                })
                .collect::<Vec<_>>();
            (count, handles)
        };

        for handle in handles {
            handle.abort();
        }
        if count > 0 {
            tracing::debug!(count, "force-closed data stream callbacks");
        }
    }

    /// Dispatch a data stream chunk to its per-stream serial worker.
    ///
    /// # Arguments
    /// - `chunk`: data stream chunk
    /// - `sender_id`: sender ActrId
    /// - `payload_type`: transport class, selecting the overflow policy
    ///   (`StreamReliable` = backpressure, `StreamLatencyFirst` = drop-newest)
    ///
    /// Same-stream chunks are delivered in arrival order, run-to-completion.
    pub(crate) async fn dispatch(
        &self,
        chunk: DataChunk,
        sender_id: ActrId,
        payload_type: PayloadType,
    ) {
        let stream_id = chunk.stream_id.clone();

        let (tx, callback) = match self.worker_tx(&stream_id).await {
            Some(worker) => worker,
            None => return,
        };
        let queued = QueuedChunk {
            chunk,
            sender_id,
            callback,
        };

        match payload_type {
            PayloadType::StreamLatencyFirst => match tx.try_send(queued) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    let dropped = self.inner.dropped_count.fetch_add(1, Ordering::Relaxed) + 1;
                    tracing::warn!(
                        stream_id = %stream_id,
                        dropped_total = dropped,
                        "⚠️ LatencyFirst queue full; dropping chunk"
                    );
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    tracing::debug!(
                        stream_id = %stream_id,
                        "stream worker closed (unregistered/shutdown); dropping chunk"
                    );
                }
            },
            // StreamReliable (and any non-stream misroute defaults to reliable):
            // block on a full queue to apply stop-read backpressure.
            _ => match tx.try_send(queued) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(queued)) => {
                    tracing::warn!(
                        stream_id = %stream_id,
                        "backpressure engaged, inbound stalled"
                    );
                    if tx.send(queued).await.is_err() {
                        tracing::debug!(
                            stream_id = %stream_id,
                            "stream worker closed during backpressure; dropping chunk"
                        );
                    }
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    tracing::debug!(
                        stream_id = %stream_id,
                        "stream worker closed (unregistered/shutdown); dropping chunk"
                    );
                }
            },
        }
    }

    /// Invoke one callback inline so ownership and host-bridge behaviour can
    /// be asserted deterministically without observing a detached task.
    #[cfg(test)]
    pub(crate) async fn invoke_for_test(
        &self,
        chunk: DataChunk,
        sender_id: ActrId,
    ) -> Option<ActorResult<()>> {
        let callback = {
            let state = self
                .inner
                .state
                .lock()
                .expect("data stream registry state poisoned");
            state
                .streams
                .get(&chunk.stream_id)
                .and_then(|entry| entry.callback.clone())
        }?;
        Some(callback(chunk, sender_id).await)
    }

    /// Gracefully shut down every worker: cancel queued work, let in-flight
    /// callbacks finish, then join all worker tasks (bounded, else abort).
    pub(crate) async fn shutdown(&self) {
        self.inner.shutdown.cancel();

        let handles = {
            let mut state = self
                .inner
                .state
                .lock()
                .expect("data stream registry state poisoned");
            state.shutting_down = true;

            let mut handles = Vec::new();
            for (_, mut entry) in state.streams.drain() {
                if let Some(mut worker) = entry.worker.take() {
                    worker.tx.take();
                    handles.push(worker.handle);
                }
            }
            handles
        };

        if handles.is_empty() {
            return;
        }

        let mut abort_guard = AbortWorkersOnDrop::new(&handles);
        let joined = futures_util::future::join_all(handles);

        match tokio::time::timeout(SHUTDOWN_JOIN_TIMEOUT, joined).await {
            Ok(_) => {
                abort_guard.disarm();
                tracing::debug!("data stream workers joined on shutdown");
            }
            Err(_) => {
                abort_guard.abort();
                tracing::error!(
                    timeout_secs = SHUTDOWN_JOIN_TIMEOUT.as_secs(),
                    "data stream workers did not finish before timeout; aborted"
                );
            }
        }
    }

    /// Get or lazily create the sender for `stream_id`'s worker.
    ///
    /// The registry lock is never held across an `await` (the caller may block
    /// on `send().await`). The worker carries no callback of its own — each
    /// chunk carries the callback to run (see [`QueuedChunk`]).
    async fn worker_tx(
        &self,
        stream_id: &str,
    ) -> Option<(mpsc::Sender<QueuedChunk>, DataChunkCallback)> {
        loop {
            let wait_for = {
                let mut state = self
                    .inner
                    .state
                    .lock()
                    .expect("data stream registry state poisoned");

                if state.shutting_down || self.inner.shutdown.is_cancelled() {
                    tracing::debug!(
                        stream_id = %stream_id,
                        "data stream registry is shut down; dropping chunk"
                    );
                    return None;
                }

                let Some(callback) = state
                    .streams
                    .get(stream_id)
                    .and_then(|entry| entry.callback.clone())
                else {
                    tracing::warn!("⚠️ No callback registered for stream: {}", stream_id);
                    return None;
                };

                let mut should_spawn = false;
                let wait_for = {
                    let entry = state.streams.entry(stream_id.to_string()).or_default();
                    if let Some(worker) = entry.worker.as_mut() {
                        if worker.done.is_finished() {
                            entry.worker.take();
                            should_spawn = true;
                            None
                        } else if let Some(tx) = worker.tx.as_ref() {
                            return Some((tx.clone(), callback));
                        } else {
                            Some(worker.done.clone())
                        }
                    } else {
                        should_spawn = true;
                        None
                    }
                };

                if should_spawn {
                    let generation = state.next_generation;
                    state.next_generation += 1;
                    let (tx, worker) = self.spawn_worker(stream_id, generation);
                    state
                        .streams
                        .get_mut(stream_id)
                        .expect("stream entry disappeared while locked")
                        .worker = Some(worker);
                    return Some((tx, callback));
                }

                wait_for
            };

            if let Some(done) = wait_for {
                let mut done_rx = done.subscribe();
                if done.is_finished() {
                    continue;
                }
                tokio::select! {
                    _ = done_rx.changed() => {}
                    _ = self.inner.shutdown.cancelled() => return None,
                }
            }
        }
    }

    fn spawn_worker(
        &self,
        stream_id: &str,
        generation: u64,
    ) -> (mpsc::Sender<QueuedChunk>, StreamWorker) {
        let (tx, rx) = mpsc::channel(self.inner.queue_depth);
        let done = Arc::new(WorkerDone::new());
        let handle = tokio::spawn(Self::worker_loop(
            stream_id.to_string(),
            generation,
            rx,
            Arc::downgrade(&self.inner),
            self.inner.shutdown.clone(),
            self.inner.panic_count.clone(),
            done.clone(),
        ));
        (
            tx.clone(),
            StreamWorker {
                tx: Some(tx),
                handle,
                generation,
                done,
            },
        )
    }

    /// Per-stream worker loop: drain the queue in order, run-to-completion.
    async fn worker_loop(
        stream_id: String,
        generation: u64,
        mut rx: mpsc::Receiver<QueuedChunk>,
        registry: Weak<RegistryInner>,
        shutdown: CancellationToken,
        panic_count: Arc<AtomicU64>,
        done: Arc<WorkerDone>,
    ) {
        loop {
            tokio::select! {
                biased;
                // Shutdown wins: drop queued chunks and stop. An in-flight
                // callback below is not interrupted mid-await; cancellation is
                // only observed between chunks.
                _ = shutdown.cancelled() => {
                    tracing::debug!(
                        stream_id = %stream_id,
                        "data stream worker cancelled; dropping queued chunks"
                    );
                    break;
                }
                maybe = rx.recv() => match maybe {
                    Some(queued) => {
                        Self::run_callback(&stream_id, queued, &panic_count).await;
                    }
                    // All senders dropped (unregister): queue drained, exit.
                    None => {
                        tracing::debug!(
                            stream_id = %stream_id,
                            "data stream worker drained; exiting"
                        );
                        break;
                    }
                }
            }
        }

        done.mark_finished();
        if let Some(registry) = registry.upgrade() {
            let mut state = registry
                .state
                .lock()
                .expect("data stream registry state poisoned");
            let should_remove = state.streams.get(&stream_id).is_some_and(|entry| {
                entry.callback.is_none()
                    && entry
                        .worker
                        .as_ref()
                        .is_some_and(|worker| worker.generation == generation)
            });
            if should_remove {
                state.streams.remove(&stream_id);
            }
        }
    }

    /// Invoke a single callback with panic isolation; errors and panics are
    /// logged and counted, and the worker proceeds to the next chunk.
    async fn run_callback(stream_id: &str, queued: QueuedChunk, panic_count: &AtomicU64) {
        let QueuedChunk {
            chunk,
            sender_id,
            callback,
        } = queued;
        let sequence = chunk.sequence;
        let fut = match std::panic::catch_unwind(AssertUnwindSafe(|| callback(chunk, sender_id))) {
            Ok(fut) => fut,
            Err(panic_payload) => {
                Self::record_callback_panic(stream_id, sequence, panic_count, panic_payload);
                return;
            }
        };
        match AssertUnwindSafe(fut).catch_unwind().await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                tracing::error!(
                    stream_id = %stream_id,
                    sequence,
                    error = ?e,
                    "❌ data stream callback returned error; continuing"
                );
            }
            Err(panic_payload) => {
                Self::record_callback_panic(stream_id, sequence, panic_count, panic_payload);
            }
        }
    }

    fn record_callback_panic(
        stream_id: &str,
        sequence: u64,
        panic_count: &AtomicU64,
        panic_payload: Box<dyn std::any::Any + Send>,
    ) {
        let count = panic_count.fetch_add(1, Ordering::Relaxed) + 1;
        let info = panic_message(panic_payload);
        tracing::error!(
            stream_id = %stream_id,
            sequence,
            panic = %info,
            panic_total = count,
            "❌ data stream callback panicked; isolated, continuing"
        );
    }
}

fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic>".to_string()
    }
}

#[cfg(test)]
#[path = "data_chunk_registry_tests.rs"]
mod tests;
