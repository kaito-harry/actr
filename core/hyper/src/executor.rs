//! Per-actor serial command runner.
//!
//! Replaces the node-global `Arc<tokio::sync::Mutex<Workload>>` serialization
//! point with a bounded command channel feeding a single owning runner task.
//! The runner takes ownership of the [`Workload`] (and, for package-backed
//! attaches, the underlying wasmtime `Store` / native guest ABI it wraps) and
//! processes commands **strictly one at a time, run-to-completion**. This
//! reproduces exactly the old "one big lock held across the whole guest call"
//! semantics — concurrent submitters queue, and each command runs alone — while
//! removing the shared lock and the self-lock hazard that came with it.
//!
//! ## Equivalence with the old `Mutex<Workload>`
//!
//! * A `tokio::sync::Mutex` is a single-owner FIFO fair queue whose guard spans
//!   the entire guest call. A bounded single-consumer mpsc channel is likewise
//!   FIFO, processes one command to completion before receiving the next, and
//!   blocks the sender (`send().await`) when full — the same back-pressure the
//!   old lock imposed on waiters. Capacity only bounds queue memory.
//! * All command kinds (dispatch / data-chunk / hook / lifecycle) travel the
//!   same channel, exactly as they all took the same lock before.
//! * Every caller still `await`s the reply oneshot to completion, so completion
//!   ordering, dedup write-back timing, mailbox reply-before-ack, and the
//!   at-least-once crash window are all bit-for-bit preserved.
//!
//! ## Self-lock elimination
//!
//! The old model deadlocked if a guest, mid-dispatch (lock held), registered a
//! data stream whose callback then tried to take the same lock. Here a callback
//! only appends a `DataChunk` command to the channel; it is enqueued behind the
//! in-flight dispatch and runs after it. The runner never sends to its own
//! channel during a guest call, so no self-deadlock is structurally possible.
//!
//! ## Panic containment
//!
//! Each command body runs under `catch_unwind`, matching the existing lifecycle
//! hook isolation ([`crate::lifecycle::hooks::call_lifecycle_hook`]). The serial
//! runner preserves the existing per-command isolation: a panic becomes an
//! `ActrError` reply and the runner continues. A native interleaved runner must
//! fail closed instead. Its `&self` futures may mutate shared state through
//! interior mutability before unwinding, so the triggering call receives
//! `Internal` and the runner then terminates, failing sibling and queued work
//! rather than reusing a potentially inconsistent actor instance.
//!
//! ## M5 evolution path (delivered — do not break this contract)
//!
//! B1 keeps the runner body a plain serial loop. M5 **delivered** the wasm
//! open-concurrency runner: a `Wasm(V2)` workload in `Interleaved` mode now
//! runs [`crate::wasm::WasmWorkloadV2::run_interleaved`], a resident
//! `store.run_concurrent(async |accessor| { … })` region that `select!`s new
//! commands off `cmd_rx` and pushes dispatches into a `FuturesUnordered` (the
//! M0 spike's proven shape). The stable contract that made that swap
//! transparent — and which MUST hold for the serial `run_loop` degradation to
//! stay bit-for-bit B1 — is: [`ActorCmd`] variants carry fully-owned arguments,
//! `cmd_rx` is owned solely by the runner task (moved whole into the region
//! runner), and each reply is sent at the command's completion point. That
//! completion point is also where the ack / dedup callbacks hang. Do **not**
//! change [`ActorCmd`] / [`ActorHandle`] / `run_loop` shapes without preserving
//! these — the interleaved wasm runner reuses the *same* `cmd_rx` channel and
//! the *same* frozen `ActorCmd` variants.

use crate::context::RuntimeContext;
use crate::workload::{
    HostAbiFn, InvocationContext, LinkedWorkloadHandle, PackageHookEvent, Workload,
};
use actr_protocol::{ActorResult, ActrError, ActrId, DataChunk, RpcEnvelope};
use bytes::Bytes;
use futures_util::FutureExt as _;
use futures_util::stream::{FuturesUnordered, StreamExt as _};
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::Instrument as _;

/// Bounded command-queue depth. Bounds queue memory only; a full queue makes
/// the sender `await` — the same waiting a lock contender did before.
const RUNNER_QUEUE_CAPACITY: usize = 64;

/// Uniform error surfaced when the runner task is gone (only reachable after
/// shutdown / drop of the actor). The old lock model had no equivalent state.
fn runner_terminated() -> ActrError {
    ActrError::Unavailable("actor runner terminated".to_string())
}

/// Which lifecycle hook a [`ActorCmd::Lifecycle`] drives.
#[derive(Debug, Clone, Copy)]
#[allow(clippy::enum_variant_names)]
pub(crate) enum LifecyclePhase {
    OnStart,
    OnReady,
    OnStop,
}

impl LifecyclePhase {
    pub(crate) fn panic_label(self) -> &'static str {
        match self {
            LifecyclePhase::OnStart => "on_start",
            LifecyclePhase::OnReady => "on_ready",
            LifecyclePhase::OnStop => "on_stop",
        }
    }
}

/// One unit of serialized work handed to the runner. Every variant carries
/// fully-owned arguments plus the caller's `tracing::Span` (so the guest call
/// stays a child of the caller's span across the task boundary) and a reply
/// channel the runner completes when the work finishes.
pub(crate) enum ActorCmd {
    Dispatch {
        envelope: RpcEnvelope,
        ctx: RuntimeContext,
        invocation: InvocationContext,
        host_abi: HostAbiFn,
        span: tracing::Span,
        reply: oneshot::Sender<ActorResult<Bytes>>,
    },
    DataChunk {
        chunk: DataChunk,
        sender: ActrId,
        invocation: InvocationContext,
        host_abi: HostAbiFn,
        span: tracing::Span,
        reply: oneshot::Sender<ActorResult<()>>,
    },
    Hook {
        event: PackageHookEvent,
        invocation: InvocationContext,
        host_abi: HostAbiFn,
        span: tracing::Span,
        reply: oneshot::Sender<ActorResult<()>>,
    },
    Lifecycle {
        phase: LifecyclePhase,
        ctx: RuntimeContext,
        invocation: InvocationContext,
        host_abi: HostAbiFn,
        span: tracing::Span,
        reply: oneshot::Sender<ActorResult<()>>,
    },
    /// Deterministic teardown. The runner breaks its loop immediately; any
    /// commands still queued behind this one have their reply oneshots dropped,
    /// so their senders observe [`runner_terminated`]. Production graceful
    /// teardown sends this command after closing dispatch admission; dropping
    /// the handle is the forced fallback and aborts the owning task.
    Shutdown { done: Option<oneshot::Sender<()>> },
}

impl ActorCmd {
    /// A command whose caller disappeared before execution must not enter guest
    /// code. `Shutdown` is intentionally never skipped: it controls runner
    /// ownership rather than merely replying to a caller.
    fn reply_is_closed(&self) -> bool {
        match self {
            ActorCmd::Dispatch { reply, .. } => reply.is_closed(),
            ActorCmd::DataChunk { reply, .. } => reply.is_closed(),
            ActorCmd::Hook { reply, .. } => reply.is_closed(),
            ActorCmd::Lifecycle { reply, .. } => reply.is_closed(),
            ActorCmd::Shutdown { .. } => false,
        }
    }
}

/// Cheap, `&self` handle to the runner task. Held behind an `Arc` on the node.
/// Deferred host callbacks retain only `Weak` references so they cannot form an
/// ownership cycle; dropping the final strong handle aborts the owning task and
/// drops the `Workload` (and its `Store`).
pub(crate) struct ActorHandle {
    tx: mpsc::Sender<ActorCmd>,
    join: std::sync::Mutex<Option<JoinHandle<()>>>,
    abort: tokio::task::AbortHandle,
    /// Admission gate for ordinary actor commands. Shutdown takes the write
    /// side, which waits for every earlier send to finish and rejects every
    /// later dispatch / stream / hook command; the privileged `on_stop` barrier
    /// can then be enqueued behind the complete admitted prefix.
    intake_open: RwLock<bool>,
}

impl std::fmt::Debug for ActorHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActorHandle").finish_non_exhaustive()
    }
}

impl ActorHandle {
    async fn await_reply<T>(rx: oneshot::Receiver<ActorResult<T>>) -> ActorResult<T> {
        match rx.await {
            Ok(result) => result,
            Err(_) => Err(runner_terminated()),
        }
    }

    /// Send a privileged command without consulting the ordinary-work intake
    /// gate. This is reserved for the ordered `on_stop` barrier and `Shutdown`.
    async fn call_raw<T>(
        &self,
        make: impl FnOnce(oneshot::Sender<ActorResult<T>>) -> ActorCmd,
    ) -> ActorResult<T> {
        let (reply, rx) = oneshot::channel();
        if self.tx.send(make(reply)).await.is_err() {
            return Err(runner_terminated());
        }
        Self::await_reply(rx).await
    }

    /// Send ordinary work under ordered shutdown admission. The read guard is
    /// held through the channel send, so acquiring the write side proves that
    /// every pre-close sender has enqueued and every post-close sender rejects.
    async fn call_admitted<T>(
        &self,
        make: impl FnOnce(oneshot::Sender<ActorResult<T>>) -> ActorCmd,
    ) -> ActorResult<T> {
        let (reply, rx) = oneshot::channel();
        let intake = self.intake_open.read().await;
        if !*intake {
            return Err(runner_terminated());
        }
        if self.tx.send(make(reply)).await.is_err() {
            return Err(runner_terminated());
        }
        // Only the enqueue belongs to admission. The shutdown barrier must not
        // wait on the command reply; FIFO in the runner channel plus the
        // interleaved runner's barrier drain provides completion ordering.
        drop(intake);
        Self::await_reply(rx).await
    }

    /// Stop accepting ordinary actor work and wait until every command that won
    /// admission has been enqueued. Lifecycle shutdown can then safely append
    /// privileged `on_stop` behind that complete prefix.
    pub(crate) async fn stop_intake(&self) {
        let mut intake = self.intake_open.write().await;
        *intake = false;
    }

    /// Dispatch one inbound RPC envelope. Mirrors the former
    /// `Workload::dispatch_envelope`.
    pub(crate) async fn dispatch_envelope(
        &self,
        envelope: RpcEnvelope,
        ctx: RuntimeContext,
        invocation: InvocationContext,
        host_abi: &HostAbiFn,
    ) -> ActorResult<Bytes> {
        let host_abi = host_abi.clone();
        let span = tracing::Span::current();
        self.call_admitted(move |reply| ActorCmd::Dispatch {
            envelope,
            ctx,
            invocation,
            host_abi,
            span,
            reply,
        })
        .await
    }

    /// Deliver one inbound data-chunk chunk. Mirrors the former
    /// `Workload::dispatch_data_chunk`.
    pub(crate) async fn dispatch_data_chunk(
        &self,
        chunk: DataChunk,
        sender: ActrId,
        invocation: InvocationContext,
        host_abi: &HostAbiFn,
    ) -> ActorResult<()> {
        let host_abi = host_abi.clone();
        let span = tracing::Span::current();
        self.call_admitted(move |reply| ActorCmd::DataChunk {
            chunk,
            sender,
            invocation,
            host_abi,
            span,
            reply,
        })
        .await
    }

    /// Dispatch an observation hook into a package-backed workload. Mirrors the
    /// former `Workload::dispatch_hook_event`.
    pub(crate) async fn dispatch_hook_event(
        &self,
        event: PackageHookEvent,
        invocation: InvocationContext,
        host_abi: &HostAbiFn,
    ) -> ActorResult<()> {
        let host_abi = host_abi.clone();
        let span = tracing::Span::current();
        self.call_admitted(move |reply| ActorCmd::Hook {
            event,
            invocation,
            host_abi,
            span,
            reply,
        })
        .await
    }

    async fn lifecycle(
        &self,
        phase: LifecyclePhase,
        ctx: RuntimeContext,
        invocation: InvocationContext,
        host_abi: &HostAbiFn,
    ) -> ActorResult<()> {
        let host_abi = host_abi.clone();
        let span = tracing::Span::current();
        let make = move |reply| ActorCmd::Lifecycle {
            phase,
            ctx,
            invocation,
            host_abi,
            span,
            reply,
        };
        match phase {
            LifecyclePhase::OnStop => self.call_raw(make).await,
            LifecyclePhase::OnStart | LifecyclePhase::OnReady => self.call_admitted(make).await,
        }
    }

    /// Invoke `on_start`. Mirrors the former `Workload::on_start`.
    pub(crate) async fn on_start(
        &self,
        ctx: RuntimeContext,
        invocation: InvocationContext,
        host_abi: &HostAbiFn,
    ) -> ActorResult<()> {
        self.lifecycle(LifecyclePhase::OnStart, ctx, invocation, host_abi)
            .await
    }

    /// Invoke `on_ready`. Mirrors the former `Workload::on_ready`.
    pub(crate) async fn on_ready(
        &self,
        ctx: RuntimeContext,
        invocation: InvocationContext,
        host_abi: &HostAbiFn,
    ) -> ActorResult<()> {
        self.lifecycle(LifecyclePhase::OnReady, ctx, invocation, host_abi)
            .await
    }

    /// Invoke `on_stop`. Mirrors the former `Workload::on_stop`.
    pub(crate) async fn on_stop(
        &self,
        ctx: RuntimeContext,
        invocation: InvocationContext,
        host_abi: &HostAbiFn,
    ) -> ActorResult<()> {
        self.lifecycle(LifecyclePhase::OnStop, ctx, invocation, host_abi)
            .await
    }

    /// Ask the runner to stop after the in-flight command, then wait for its
    /// task to finish. Used for deterministic teardown in tests.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn shutdown(&self) {
        self.stop_intake().await;
        let (done, wait) = oneshot::channel();
        if self
            .tx
            .send(ActorCmd::Shutdown { done: Some(done) })
            .await
            .is_ok()
        {
            let _ = wait.await;
        }
        self.join().await;
    }

    /// Abort the owning runner task without waiting. Used by teardown guards so
    /// cancellation of the shutdown coordinator cannot detach a stuck runner.
    pub(crate) fn abort(&self) {
        self.abort.abort();
    }

    /// Abort the owning runner task and wait until its workload has been
    /// dropped. This is the deterministic fallback for non-cooperative actor
    /// code; normal node shutdown first runs `on_stop` and sends `Shutdown`.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn abort_and_join(&self) {
        self.abort();
        self.join().await;
    }

    /// Join the runner task if it is still owned here.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn join(&self) {
        let handle = self.join.lock().unwrap().take();
        if let Some(handle) = handle {
            let _ = handle.await;
        }
    }
}

impl Drop for ActorHandle {
    fn drop(&mut self) {
        // Tokio detaches a task when its JoinHandle is dropped. Always abort so
        // abrupt node teardown cannot orphan a runner that owns a workload.
        self.abort.abort();
    }
}

/// Execution discipline for the runner task.
///
/// `Serial` is the B1 contract: one command at a time, run-to-completion. It is
/// mandatory for `Wasm(V1)` / `DynClib` workloads (single `Store` / `&mut`
/// guest ABI) and is the default. `Interleaved` is the B2 concurrency point for
/// `Linked` workloads and `Wasm(V2)` async-world guests, whose dispatch APIs can
/// safely multiplex distinct-key invocations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RunnerMode {
    Serial,
    Interleaved,
}

/// Spawn the serial runner task that owns `workload` and return its handle.
///
/// A thin `RunnerMode::Serial` convenience used by test-support harnesses; the
/// production node path calls [`spawn_runner_with_mode`] directly so it can pick
/// the interleaved runner when the dispatch gate is on.
#[cfg(any(
    test,
    all(
        feature = "test-utils",
        any(feature = "wasm-engine", feature = "dynclib-engine")
    )
))]
pub(crate) fn spawn_runner(workload: Workload) -> ActorHandle {
    spawn_runner_with_mode(workload, RunnerMode::Serial, None)
}

/// Spawn a runner in the requested [`RunnerMode`].
///
/// `Interleaved` opens same-instance concurrency for the two workloads whose
/// dispatch is safe to multiplex:
///
/// * `Workload::Linked` — `dispatch` takes `&self`, so distinct-key dispatches
///   run concurrently in a `FuturesUnordered` (B2 native concurrency).
///   `dispatch_timeout` arms a per-dispatch deadline on each in-flight native
///   future. Because an arbitrary native future is not cancellation-safe, a
///   timeout poisons and terminates the whole runner after replying `TimedOut`;
///   sibling and queued commands fail rather than touching partially-mutated
///   actor state. A caught guest panic follows the same fail-closed path after
///   replying `Internal`.
/// * `Workload::Wasm(V2)` — the 0.2.0 async world drives a **resident**
///   `Store::run_concurrent` region ([`crate::wasm::WasmWorkloadV2::run_interleaved`]),
///   interleaving distinct-key dispatches at their host-import `.await` points
///   (M5). External per-dispatch deadlines supervise that region and discard
///   the whole Store before any timeout reply releases a scheduler key.
///
/// Every other packaged workload (`Wasm(V1)` 0.1.0 sync world, `DynClib`) falls
/// back to the serial `run_loop` even when `Interleaved` is requested, so the
/// single-`Store` / `&mut` guest-ABI contract is never violated — the conflict
/// key is a no-op routing hint there. Combined with the node's strategy-A gate
/// (`Interleaved` is only ever requested for a *keyed* actor), this keeps every
/// keyless or serial-only workload a bit-for-bit B1 degradation even though the
/// dispatch gate now defaults on.
pub(crate) fn spawn_runner_with_mode(
    workload: Workload,
    mode: RunnerMode,
    dispatch_timeout: Option<std::time::Duration>,
) -> ActorHandle {
    let (tx, rx) = mpsc::channel(RUNNER_QUEUE_CAPACITY);
    let join = match (mode, workload) {
        (RunnerMode::Interleaved, Workload::Linked(handle)) => {
            tokio::spawn(run_loop_interleaved(handle, rx, dispatch_timeout))
        }
        #[cfg(feature = "wasm-engine")]
        (RunnerMode::Interleaved, Workload::Wasm(kernel)) if kernel.is_v2() => match kernel {
            crate::wasm::WasmKernel::V2(v2) => {
                tokio::spawn(v2.run_interleaved(rx, dispatch_timeout))
            }
            // Unreachable given the `is_v2()` guard, but keeps the match total.
            other => tokio::spawn(run_loop(Workload::Wasm(other), rx)),
        },
        (_, workload) => tokio::spawn(run_loop(workload, rx)),
    };
    let abort = join.abort_handle();
    ActorHandle {
        tx,
        join: std::sync::Mutex::new(Some(join)),
        abort,
        intake_open: RwLock::new(true),
    }
}

/// Strictly serial command loop: receive one command, run it to completion,
/// send its reply, then receive the next. Exits when the channel closes (all
/// handles dropped) or on an explicit `Shutdown`.
async fn run_loop(mut workload: Workload, mut rx: mpsc::Receiver<ActorCmd>) {
    while let Some(cmd) = rx.recv().await {
        if cmd.reply_is_closed() {
            continue;
        }
        match cmd {
            ActorCmd::Dispatch {
                envelope,
                ctx,
                invocation,
                host_abi,
                span,
                mut reply,
            } => {
                let Some(result) = run_until_reply_closed(
                    guarded(
                        "dispatch",
                        workload.dispatch_envelope(envelope, ctx, invocation, &host_abi),
                    )
                    .instrument(span),
                    &mut reply,
                )
                .await
                else {
                    break;
                };
                let _ = reply.send(result);
            }
            ActorCmd::DataChunk {
                chunk,
                sender,
                invocation,
                host_abi,
                span,
                mut reply,
            } => {
                let Some(result) = run_until_reply_closed(
                    guarded(
                        "data_chunk",
                        workload.dispatch_data_chunk(chunk, sender, invocation, &host_abi),
                    )
                    .instrument(span),
                    &mut reply,
                )
                .await
                else {
                    break;
                };
                let _ = reply.send(result);
            }
            ActorCmd::Hook {
                event,
                invocation,
                host_abi,
                span,
                mut reply,
            } => {
                let Some(result) = run_until_reply_closed(
                    guarded(
                        "hook",
                        workload.dispatch_hook_event(event, invocation, &host_abi),
                    )
                    .instrument(span),
                    &mut reply,
                )
                .await
                else {
                    break;
                };
                let _ = reply.send(result);
            }
            ActorCmd::Lifecycle {
                phase,
                ctx,
                invocation,
                host_abi,
                span,
                mut reply,
            } => {
                let fut = match phase {
                    LifecyclePhase::OnStart => workload.on_start(ctx, invocation, &host_abi),
                    LifecyclePhase::OnReady => workload.on_ready(ctx, invocation, &host_abi),
                    LifecyclePhase::OnStop => workload.on_stop(ctx, invocation, &host_abi),
                };
                let Some(result) = run_until_reply_closed(
                    guarded(phase.panic_label(), fut).instrument(span),
                    &mut reply,
                )
                .await
                else {
                    break;
                };
                let _ = reply.send(result);
            }
            ActorCmd::Shutdown { done } => {
                if let Some(done) = done {
                    let _ = done.send(());
                }
                break;
            }
        }
    }
}

/// Interleaved command loop for a `Linked` workload (B2 native concurrency).
///
/// `Dispatch` commands are cloned onto owned futures (the handle is behind an
/// `Arc`, and `LinkedWorkloadHandle::dispatch` takes `&self`) and pushed into a
/// `FuturesUnordered`, so several distinct-key dispatches run concurrently. The
/// scheduler upstream bounds how many are ever in flight (budget `C`).
///
/// Every *non-Dispatch* command (`Lifecycle` / `Hook` / `DataChunk` /
/// `Shutdown`) is a **barrier**: the loop stops accepting new work, drains all
/// in-flight dispatches, runs the barrier command alone, then resumes. This
/// keeps lifecycle ordering and the single-runner guarantee intact.
///
/// `dispatch_timeout`, when set, arms a per-dispatch deadline on every in-flight
/// dispatch — the native mirror of the WASM `run_interleaved` deadline. On
/// expiry the caller's reply resolves [`ActrError::TimedOut`] and the runner is
/// terminated immediately. A caught guest panic likewise returns `Internal`
/// before terminating. Dropping an arbitrary Rust future or unwinding through
/// actor code can leave shared state partially mutated (or detached work still
/// running), so the actor instance is not reused: all sibling/queued commands
/// lose their reply channel and subsequent sends fail `Unavailable`.
///
/// The shape (`select!` over `cmd_rx` + a `FuturesUnordered`) deliberately
/// mirrors the M5 `store.run_concurrent` evolution path documented above so M5
/// swaps only the execution kernel, not this skeleton.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InterleavedDispatchCompletion {
    Completed,
    Panicked,
    TimedOut,
    CallerCanceled,
}

impl InterleavedDispatchCompletion {
    fn poisons_runner(self) -> bool {
        !matches!(self, InterleavedDispatchCompletion::Completed)
    }
}

async fn run_loop_interleaved(
    handle: Arc<dyn LinkedWorkloadHandle>,
    mut rx: mpsc::Receiver<ActorCmd>,
    dispatch_timeout: Option<std::time::Duration>,
) {
    let mut inflight: FuturesUnordered<
        Pin<Box<dyn std::future::Future<Output = InterleavedDispatchCompletion> + Send>>,
    > = FuturesUnordered::new();
    loop {
        tokio::select! {
            biased;
            Some(completion) = inflight.next(), if !inflight.is_empty() => {
                // Observe poison before admitting more commands. In
                // particular, a ready panic/timeout/cancellation must close
                // the runner even while the command channel remains busy.
                if completion.poisons_runner() {
                    break;
                }
            }
            maybe_cmd = rx.recv() => {
                match maybe_cmd {
                    Some(ActorCmd::Dispatch { envelope, ctx, invocation, host_abi, span, mut reply }) => {
                        if reply.is_closed() {
                            continue;
                        }
                        let _ = (&invocation, &host_abi);
                        let handle = handle.clone();
                        inflight.push(Box::pin(async move {
                            let fut = handle.dispatch(envelope, Arc::new(ctx));
                            // Instrument the guest call so it stays a child of the
                            // caller's span, then (optionally) arm the deadline
                            // around the instrumented future.
                            let guarded_fut = guarded_with_panic_status("dispatch", fut).instrument(span);
                            let execution = async {
                                match dispatch_timeout {
                                    // A native future has no general cancellation-
                                    // safety contract. Mark the runner poisoned on
                                    // expiry so no sibling can reuse this handle.
                                    Some(d) => match tokio::time::timeout(d, guarded_fut).await {
                                        Ok((result, panicked)) => (
                                            result,
                                            if panicked {
                                                InterleavedDispatchCompletion::Panicked
                                            } else {
                                                InterleavedDispatchCompletion::Completed
                                            },
                                        ),
                                        Err(_elapsed) => (
                                            Err(ActrError::TimedOut),
                                            InterleavedDispatchCompletion::TimedOut,
                                        ),
                                    },
                                    None => {
                                        let (result, panicked) = guarded_fut.await;
                                        (
                                            result,
                                            if panicked {
                                                InterleavedDispatchCompletion::Panicked
                                            } else {
                                                InterleavedDispatchCompletion::Completed
                                            },
                                        )
                                    }
                                }
                            };
                            let (result, completion) = tokio::select! {
                                biased;
                                result = execution => result,
                                _ = reply.closed() => {
                                    return InterleavedDispatchCompletion::CallerCanceled;
                                }
                            };
                            let _ = reply.send(result);
                            completion
                        }));
                    }
                    Some(barrier) => {
                        if barrier.reply_is_closed() {
                            continue;
                        }
                        // Drain in-flight dispatches, then run the barrier alone.
                        let mut poisoned = false;
                        while let Some(completion) = inflight.next().await {
                            if completion.poisons_runner() {
                                poisoned = true;
                                break;
                            }
                        }
                        if poisoned {
                            break;
                        }
                        match run_barrier(&handle, barrier).await {
                            BarrierOutcome::Continue => {}
                            BarrierOutcome::Shutdown
                            | BarrierOutcome::Panicked
                            | BarrierOutcome::CallerCanceled => break,
                        }
                    }
                    None => {
                        // All handles dropped: drain remaining dispatches and exit.
                        while let Some(completion) = inflight.next().await {
                            if completion.poisons_runner() {
                                break;
                            }
                        }
                        break;
                    }
                }
            }
        }
    }
}

enum BarrierOutcome {
    Continue,
    Shutdown,
    Panicked,
    CallerCanceled,
}

/// Run one barrier command against the linked handle. A caller disappearing
/// during a lifecycle hook poisons the runner just like cancellation of an
/// in-flight dispatch; queued commands with an already-closed reply are skipped
/// before reaching this function.
async fn run_barrier(handle: &Arc<dyn LinkedWorkloadHandle>, cmd: ActorCmd) -> BarrierOutcome {
    match cmd {
        ActorCmd::Lifecycle {
            phase,
            ctx,
            invocation,
            host_abi,
            span,
            mut reply,
        } => {
            let _ = (&invocation, &host_abi);
            let fut = async {
                match phase {
                    LifecyclePhase::OnStart => handle.on_start(&ctx).await,
                    LifecyclePhase::OnReady => handle.on_ready(&ctx).await,
                    LifecyclePhase::OnStop => handle.on_stop(&ctx).await,
                }
            };
            let Some((result, panicked)) = run_until_reply_closed(
                guarded_with_panic_status(phase.panic_label(), fut).instrument(span),
                &mut reply,
            )
            .await
            else {
                return BarrierOutcome::CallerCanceled;
            };
            let _ = reply.send(result);
            if panicked {
                BarrierOutcome::Panicked
            } else {
                BarrierOutcome::Continue
            }
        }
        // Linked workloads receive hooks via the observer path; the ABI hook
        // command is a no-op for them (mirrors `Workload::dispatch_hook_event`).
        ActorCmd::Hook { reply, .. } => {
            let _ = reply.send(Ok(()));
            BarrierOutcome::Continue
        }
        // Linked stream callbacks register directly on RuntimeContext; the ABI
        // stream command is not applicable (mirrors `dispatch_data_chunk`).
        ActorCmd::DataChunk { reply, .. } => {
            let _ = reply.send(Err(ActrError::NotImplemented(
                "linked workload stream callbacks are registered directly on RuntimeContext"
                    .to_string(),
            )));
            BarrierOutcome::Continue
        }
        ActorCmd::Shutdown { done } => {
            if let Some(done) = done {
                let _ = done.send(());
            }
            BarrierOutcome::Shutdown
        }
        ActorCmd::Dispatch { .. } => unreachable!("dispatch is handled before the barrier path"),
    }
}

/// Poll one command body until it completes or its caller drops the reply
/// receiver. Cancellation while guest code is running returns `None`; both
/// owning runner loops treat that as poison and drop the workload rather than
/// continuing from potentially partial state.
async fn run_until_reply_closed<T, R>(
    fut: impl std::future::Future<Output = T>,
    reply: &mut oneshot::Sender<ActorResult<R>>,
) -> Option<T> {
    tokio::select! {
        biased;
        result = fut => Some(result),
        _ = reply.closed() => None,
    }
}

/// Run a serial command body with panic isolation, converting a panic into an
/// `ActrError::Internal("{label} panicked: {info}")` — the same shape the
/// lifecycle hook path already produces — while preserving the serial runner's
/// existing survive-and-continue behaviour.
async fn guarded<T>(
    label: &'static str,
    fut: impl std::future::Future<Output = ActorResult<T>>,
) -> ActorResult<T> {
    guarded_with_panic_status(label, fut).await.0
}

/// Catch a guest panic while retaining whether unwinding occurred. Native
/// interleaved execution uses the flag to send the triggering `Internal` reply
/// before terminating the runner and dropping every co-resident future.
async fn guarded_with_panic_status<T>(
    label: &'static str,
    fut: impl std::future::Future<Output = ActorResult<T>>,
) -> (ActorResult<T>, bool) {
    match AssertUnwindSafe(fut).catch_unwind().await {
        Ok(result) => (result, false),
        Err(panic_payload) => {
            let info = crate::lifecycle::hooks::extract_panic_info(panic_payload);
            (
                Err(ActrError::Internal(format!("{label} panicked: {info}"))),
                true,
            )
        }
    }
}

#[cfg(test)]
#[path = "executor_tests.rs"]
mod tests;
