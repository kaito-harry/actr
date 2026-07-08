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
//! * All command kinds (dispatch / data-stream / hook / lifecycle) travel the
//!   same channel, exactly as they all took the same lock before.
//! * Every caller still `await`s the reply oneshot to completion, so completion
//!   ordering, dedup write-back timing, mailbox reply-before-ack, and the
//!   at-least-once crash window are all bit-for-bit preserved.
//!
//! ## Self-lock elimination
//!
//! The old model deadlocked if a guest, mid-dispatch (lock held), registered a
//! data stream whose callback then tried to take the same lock. Here a callback
//! only appends a `DataStream` command to the channel; it is enqueued behind the
//! in-flight dispatch and runs after it. The runner never sends to its own
//! channel during a guest call, so no self-deadlock is structurally possible.
//!
//! ## Panic isolation
//!
//! Each command body runs under `catch_unwind`, matching the existing lifecycle
//! hook isolation ([`crate::lifecycle::hooks::call_lifecycle_hook`]). A panic in
//! one command is converted to an `ActrError` reply and the runner survives to
//! serve the next command — a single bad message can never orphan the actor.
//!
//! ## M5 evolution path (do not break this contract)
//!
//! B1 keeps the runner body a plain serial loop. In M5 the wasm runner will
//! swap this loop for a resident `store.run_concurrent(async |accessor| { … })`
//! region that `select!`s new commands off `cmd_rx` and pushes them into a
//! `FuturesUnordered` (the M0 spike proved this shape works). The stable
//! contract that makes that swap transparent is: commands carry fully-owned
//! arguments, `cmd_rx` is owned solely by the runner task (movable into the
//! region closure), and each reply is sent at the command's completion point.
//! That completion point is also where B2 will hang the ack / dedup callbacks.
//! Do not change [`ActorCmd`] / [`ActorHandle`] shapes without preserving these.

use crate::context::RuntimeContext;
use crate::workload::{HostAbiFn, InvocationContext, PackageHookEvent, Workload};
use actr_protocol::{ActorResult, ActrError, ActrId, DataStream, RpcEnvelope};
use bytes::Bytes;
use futures_util::FutureExt as _;
use std::panic::AssertUnwindSafe;
use tokio::sync::{mpsc, oneshot};
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
    fn panic_label(self) -> &'static str {
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
    DataStream {
        chunk: DataStream,
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
    /// so their senders observe [`runner_terminated`]. Production teardown
    /// happens implicitly when the last [`ActorHandle`] drops (channel closes →
    /// `recv()` returns `None`); `Shutdown` exists for tests and explicit,
    /// ordered disposal.
    Shutdown { done: Option<oneshot::Sender<()>> },
}

/// Cheap, `&self` handle to the runner task. Held behind an `Arc` on the node
/// and cloned into deferred callbacks; when every clone drops the channel
/// closes and the runner task exits, dropping the `Workload` (and its `Store`).
pub(crate) struct ActorHandle {
    tx: mpsc::Sender<ActorCmd>,
    join: std::sync::Mutex<Option<JoinHandle<()>>>,
}

impl std::fmt::Debug for ActorHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActorHandle").finish_non_exhaustive()
    }
}

impl ActorHandle {
    /// Send a command and await its reply, mapping any channel failure (only
    /// reachable once the runner is gone) to [`runner_terminated`].
    async fn call<T>(
        &self,
        make: impl FnOnce(oneshot::Sender<ActorResult<T>>) -> ActorCmd,
    ) -> ActorResult<T> {
        let (reply, rx) = oneshot::channel();
        if self.tx.send(make(reply)).await.is_err() {
            return Err(runner_terminated());
        }
        match rx.await {
            Ok(result) => result,
            Err(_) => Err(runner_terminated()),
        }
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
        self.call(move |reply| ActorCmd::Dispatch {
            envelope,
            ctx,
            invocation,
            host_abi,
            span,
            reply,
        })
        .await
    }

    /// Deliver one inbound data-stream chunk. Mirrors the former
    /// `Workload::dispatch_data_stream`.
    pub(crate) async fn dispatch_data_stream(
        &self,
        chunk: DataStream,
        sender: ActrId,
        invocation: InvocationContext,
        host_abi: &HostAbiFn,
    ) -> ActorResult<()> {
        let host_abi = host_abi.clone();
        let span = tracing::Span::current();
        self.call(move |reply| ActorCmd::DataStream {
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
        self.call(move |reply| ActorCmd::Hook {
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
        self.call(move |reply| ActorCmd::Lifecycle {
            phase,
            ctx,
            invocation,
            host_abi,
            span,
            reply,
        })
        .await
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

    /// Join the runner task if it is still owned here.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn join(&self) {
        let handle = self.join.lock().unwrap().take();
        if let Some(handle) = handle {
            let _ = handle.await;
        }
    }
}

/// Spawn the serial runner task that owns `workload` and return its handle.
pub(crate) fn spawn_runner(workload: Workload) -> ActorHandle {
    let (tx, rx) = mpsc::channel(RUNNER_QUEUE_CAPACITY);
    let join = tokio::spawn(run_loop(workload, rx));
    ActorHandle {
        tx,
        join: std::sync::Mutex::new(Some(join)),
    }
}

/// Strictly serial command loop: receive one command, run it to completion,
/// send its reply, then receive the next. Exits when the channel closes (all
/// handles dropped) or on an explicit `Shutdown`.
async fn run_loop(mut workload: Workload, mut rx: mpsc::Receiver<ActorCmd>) {
    while let Some(cmd) = rx.recv().await {
        match cmd {
            ActorCmd::Dispatch {
                envelope,
                ctx,
                invocation,
                host_abi,
                span,
                reply,
            } => {
                let result = guarded(
                    "dispatch",
                    workload.dispatch_envelope(envelope, ctx, invocation, &host_abi),
                )
                .instrument(span)
                .await;
                let _ = reply.send(result);
            }
            ActorCmd::DataStream {
                chunk,
                sender,
                invocation,
                host_abi,
                span,
                reply,
            } => {
                let result = guarded(
                    "data_stream",
                    workload.dispatch_data_stream(chunk, sender, invocation, &host_abi),
                )
                .instrument(span)
                .await;
                let _ = reply.send(result);
            }
            ActorCmd::Hook {
                event,
                invocation,
                host_abi,
                span,
                reply,
            } => {
                let result = guarded(
                    "hook",
                    workload.dispatch_hook_event(event, invocation, &host_abi),
                )
                .instrument(span)
                .await;
                let _ = reply.send(result);
            }
            ActorCmd::Lifecycle {
                phase,
                ctx,
                invocation,
                host_abi,
                span,
                reply,
            } => {
                let fut = match phase {
                    LifecyclePhase::OnStart => workload.on_start(ctx, invocation, &host_abi),
                    LifecyclePhase::OnReady => workload.on_ready(ctx, invocation, &host_abi),
                    LifecyclePhase::OnStop => workload.on_stop(ctx, invocation, &host_abi),
                };
                let result = guarded(phase.panic_label(), fut).instrument(span).await;
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

/// Run a command body with panic isolation, converting a panic into an
/// `ActrError::Internal("{label} panicked: {info}")` — the same shape the
/// lifecycle hook path already produces — so the runner survives.
async fn guarded<T>(
    label: &'static str,
    fut: impl std::future::Future<Output = ActorResult<T>>,
) -> ActorResult<T> {
    match AssertUnwindSafe(fut).catch_unwind().await {
        Ok(result) => result,
        Err(panic_payload) => {
            let info = crate::lifecycle::hooks::extract_panic_info(panic_payload);
            Err(ActrError::Internal(format!("{label} panicked: {info}")))
        }
    }
}

#[cfg(test)]
#[path = "executor_tests.rs"]
mod tests;
