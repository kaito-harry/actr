//! Tokio runtime owned by one loaded dynclib workload image.
//!
//! Runtime-driving entrypoints are serialized here as well as by Hyper's host
//! FFI gate. Shutdown gives managed tasks five seconds to observe cancellation;
//! on timeout it returns an error and leaves the runtime in a terminal leaked
//! state so the host can keep the library mapped instead of blocking forever.

use std::future::Future;
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use actr_protocol::{ActorResult, ActrError};
use tokio::runtime::{Handle, Runtime};
use tokio::task::JoinHandle;

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const SHUTDOWN_POLL_INTERVAL: Duration = Duration::from_millis(10);

tokio::task_local! {
    static ACTIVE_BRIDGE_TOKEN: u64;
}

struct ManagedTasks {
    accepting: bool,
    handles: Vec<JoinHandle<()>>,
}

enum RuntimeState {
    Uninitialized,
    Running {
        handle: Handle,
        owner: Runtime,
        tasks: ManagedTasks,
    },
    ShuttingDown,
    ShutdownTimedOut,
}

struct DynclibRuntime {
    /// Serializes guest entrypoints that drive or tear down the runtime.
    ///
    /// Hyper also serializes `actr_handle` and `actr_shutdown` with its
    /// `ffi_gate`, but keeping the invariant here closes the check-then-act
    /// window for callers that invoke this public guest API directly.
    entry_gate: Mutex<()>,
    state: Mutex<RuntimeState>,
}

impl DynclibRuntime {
    const fn new() -> Self {
        Self {
            entry_gate: Mutex::new(()),
            state: Mutex::new(RuntimeState::Uninitialized),
        }
    }
}

// Per shared-library image. The state returns to `Uninitialized` only after a
// successful shutdown, allowing a fresh runtime to be built even when the
// loader keeps this image mapped across `dlclose` / `dlopen` cycles.
static RUNTIME: DynclibRuntime = DynclibRuntime::new();

fn lock_entry() -> ActorResult<MutexGuard<'static, ()>> {
    RUNTIME
        .entry_gate
        .lock()
        .map_err(|_| ActrError::Internal("dynclib runtime entry lock poisoned".into()))
}

fn lock_state() -> ActorResult<MutexGuard<'static, RuntimeState>> {
    RUNTIME
        .state
        .lock()
        .map_err(|_| ActrError::Internal("dynclib runtime state lock poisoned".into()))
}

/// Initialize the runtime for this shared-library image.
pub fn initialize() -> ActorResult<()> {
    let _entry_guard = lock_entry()?;
    {
        let state = lock_state()?;
        match &*state {
            RuntimeState::Uninitialized => {}
            RuntimeState::Running { .. } => {
                return Err(ActrError::Internal(
                    "dynclib Tokio runtime is already initialized".into(),
                ));
            }
            RuntimeState::ShuttingDown => {
                return Err(ActrError::Unavailable(
                    "dynclib Tokio runtime is shutting down".into(),
                ));
            }
            RuntimeState::ShutdownTimedOut => {
                return Err(ActrError::Internal(
                    "dynclib Tokio runtime shutdown previously timed out".into(),
                ));
            }
        }
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .thread_name("actr-dynclib")
        .build()
        .map_err(|error| {
            ActrError::Internal(format!(
                "failed to initialize dynclib Tokio runtime: {error}"
            ))
        })?;
    let mut state = lock_state()?;
    if !matches!(*state, RuntimeState::Uninitialized) {
        return Err(ActrError::Internal(
            "dynclib Tokio runtime state changed during initialization".into(),
        ));
    }
    *state = RuntimeState::Running {
        handle: runtime.handle().clone(),
        owner: runtime,
        tasks: ManagedTasks {
            accepting: true,
            handles: Vec::new(),
        },
    };
    Ok(())
}

/// Drive a guest future to completion with the invocation's bridge token.
pub fn block_on<F>(bridge_token: u64, future: F) -> ActorResult<F::Output>
where
    F: Future,
{
    let _entry_guard = lock_entry()?;
    let handle = {
        let state = lock_state()?;
        match &*state {
            RuntimeState::Running { handle, .. } => handle.clone(),
            RuntimeState::Uninitialized => {
                return Err(ActrError::Internal(
                    "dynclib Tokio runtime is not initialized".into(),
                ));
            }
            RuntimeState::ShuttingDown => {
                return Err(ActrError::Unavailable(
                    "dynclib Tokio runtime is shutting down".into(),
                ));
            }
            RuntimeState::ShutdownTimedOut => {
                return Err(ActrError::Internal(
                    "dynclib Tokio runtime shutdown previously timed out".into(),
                ));
            }
        }
    };

    Ok(handle.block_on(ACTIVE_BRIDGE_TOKEN.scope(bridge_token, future)))
}

/// Spawn a background task owned by the current dynclib workload.
///
/// Managed tasks are aborted and given a bounded interval to finish before the
/// shared library is unloaded. A task that does not yield causes shutdown to
/// fail, and a conforming host keeps the library mapped for process lifetime.
/// Clone any [`crate::Context`] used by the task before passing it here; the
/// cloned context retains its host bridge until the task exits.
pub fn spawn<F>(future: F) -> ActorResult<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let mut state = lock_state()?;
    match &mut *state {
        RuntimeState::Running { handle, tasks, .. } => {
            if !tasks.accepting {
                return Err(ActrError::Unavailable(
                    "dynclib workload is shutting down".into(),
                ));
            }

            tasks.handles.retain(|handle| !handle.is_finished());
            tasks.handles.push(handle.spawn(future));
            Ok(())
        }
        RuntimeState::Uninitialized => Err(ActrError::Internal(
            "dynclib Tokio runtime is not initialized".into(),
        )),
        RuntimeState::ShuttingDown => Err(ActrError::Unavailable(
            "dynclib workload is shutting down".into(),
        )),
        RuntimeState::ShutdownTimedOut => Err(ActrError::Internal(
            "dynclib Tokio runtime shutdown previously timed out".into(),
        )),
    }
}

pub(crate) fn active_bridge_token() -> Option<u64> {
    ACTIVE_BRIDGE_TOKEN.try_with(|token| *token).ok()
}

fn abort_and_wait(handles: Vec<JoinHandle<()>>, timeout: Duration) -> Result<(), usize> {
    for handle in &handles {
        handle.abort();
    }

    let deadline = Instant::now() + timeout;
    loop {
        let unfinished = handles
            .iter()
            .filter(|handle| !handle.is_finished())
            .count();
        if unfinished == 0 {
            return Ok(());
        }

        let now = Instant::now();
        if now >= deadline {
            return Err(unfinished);
        }
        std::thread::sleep(SHUTDOWN_POLL_INTERVAL.min(deadline.saturating_duration_since(now)));
    }
}

/// Stop managed tasks and shut down the runtime before `dlclose`.
///
/// A non-cooperative task is detached after [`SHUTDOWN_TIMEOUT`], reported as
/// an error, and deliberately leaves the runtime unusable. The host must treat
/// that error as "do not unload this library".
pub fn shutdown() -> ActorResult<()> {
    let _entry_guard = lock_entry()?;
    let (runtime_owner, handles) = {
        let mut state = lock_state()?;
        let previous = std::mem::replace(&mut *state, RuntimeState::ShuttingDown);
        match previous {
            RuntimeState::Running {
                owner, mut tasks, ..
            } => {
                tasks.accepting = false;
                (owner, std::mem::take(&mut tasks.handles))
            }
            RuntimeState::Uninitialized => {
                *state = RuntimeState::Uninitialized;
                return Ok(());
            }
            RuntimeState::ShuttingDown => {
                *state = RuntimeState::ShuttingDown;
                return Err(ActrError::Unavailable(
                    "dynclib Tokio runtime is already shutting down".into(),
                ));
            }
            RuntimeState::ShutdownTimedOut => {
                *state = RuntimeState::ShutdownTimedOut;
                return Err(ActrError::Internal(
                    "dynclib Tokio runtime shutdown previously timed out".into(),
                ));
            }
        }
    };

    match abort_and_wait(handles, SHUTDOWN_TIMEOUT) {
        Ok(()) => {
            drop(runtime_owner);
            let mut state = lock_state()?;
            if !matches!(*state, RuntimeState::ShuttingDown) {
                return Err(ActrError::Internal(
                    "dynclib Tokio runtime state changed during shutdown".into(),
                ));
            }
            *state = RuntimeState::Uninitialized;
            Ok(())
        }
        Err(unfinished) => {
            {
                let mut state = lock_state()?;
                if !matches!(*state, RuntimeState::ShuttingDown) {
                    return Err(ActrError::Internal(
                        "dynclib Tokio runtime state changed during shutdown".into(),
                    ));
                }
                *state = RuntimeState::ShutdownTimedOut;
            }

            tracing::error!(
                unfinished_tasks = unfinished,
                timeout_ms = SHUTDOWN_TIMEOUT.as_millis(),
                "dynclib shutdown timed out; leaking the guest runtime and requiring the host to retain the library"
            );
            runtime_owner.shutdown_background();
            Err(ActrError::Internal(format!(
                "dynclib shutdown timed out after {} ms with {unfinished} managed task(s) still running",
                SHUTDOWN_TIMEOUT.as_millis()
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    static RUNTIME_SERIAL: Mutex<()> = Mutex::new(());

    #[test]
    fn shutdown_clears_state_for_reinitialize() {
        let _guard = RUNTIME_SERIAL.lock().expect("lock runtime test");
        shutdown().expect("reset runtime before test");

        initialize().expect("first initialize");
        assert!(
            initialize().is_err(),
            "initialize should fail while a runtime is active"
        );
        shutdown().expect("first shutdown");

        initialize().expect("reinitialize after shutdown");
        shutdown().expect("second shutdown");
    }

    #[test]
    fn shutdown_without_runtime_is_noop() {
        let _guard = RUNTIME_SERIAL.lock().expect("lock runtime test");
        shutdown().expect("first shutdown without runtime");
        shutdown().expect("second shutdown without runtime");
    }

    #[test]
    fn abort_and_wait_drains_cancellable_task() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("build test runtime");
        let task = runtime.spawn(std::future::pending::<()>());

        assert_eq!(abort_and_wait(vec![task], Duration::from_secs(1)), Ok(()));
    }

    #[test]
    fn abort_and_wait_times_out_non_yielding_task() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("build test runtime");
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(1);
        let task = runtime.spawn(async move {
            started_tx.send(()).expect("signal task start");
            std::thread::sleep(Duration::from_millis(100));
        });
        started_rx.recv().expect("wait for task start");

        assert_eq!(
            abort_and_wait(vec![task], Duration::from_millis(10)),
            Err(1)
        );
        runtime.shutdown_timeout(Duration::from_secs(1));
    }
}
