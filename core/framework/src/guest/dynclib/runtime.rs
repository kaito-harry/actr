//! Tokio runtime owned by one loaded dynclib workload image.

use std::future::Future;
use std::sync::{Mutex, OnceLock};

use actr_protocol::{ActorResult, ActrError};
use tokio::runtime::{Handle, Runtime};
use tokio::task::JoinHandle;

tokio::task_local! {
    static ACTIVE_BRIDGE_TOKEN: u64;
}

struct ManagedTasks {
    accepting: bool,
    handles: Vec<JoinHandle<()>>,
}

struct DynclibRuntime {
    handle: Handle,
    owner: Mutex<Option<Runtime>>,
    tasks: Mutex<ManagedTasks>,
}

static RUNTIME: OnceLock<DynclibRuntime> = OnceLock::new();

fn runtime() -> ActorResult<&'static DynclibRuntime> {
    RUNTIME
        .get()
        .ok_or_else(|| ActrError::Internal("dynclib Tokio runtime is not initialized".into()))
}

/// Initialize the runtime for this shared-library image.
pub fn initialize() -> ActorResult<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("actr-dynclib")
        .build()
        .map_err(|error| {
            ActrError::Internal(format!(
                "failed to initialize dynclib Tokio runtime: {error}"
            ))
        })?;
    let state = DynclibRuntime {
        handle: runtime.handle().clone(),
        owner: Mutex::new(Some(runtime)),
        tasks: Mutex::new(ManagedTasks {
            accepting: true,
            handles: Vec::new(),
        }),
    };

    RUNTIME
        .set(state)
        .map_err(|_| ActrError::Internal("dynclib Tokio runtime is already initialized".into()))
}

/// Drive a guest future to completion with the invocation's bridge token.
pub fn block_on<F>(bridge_token: u64, future: F) -> ActorResult<F::Output>
where
    F: Future,
{
    let runtime = runtime()?;
    let is_running = runtime
        .owner
        .lock()
        .map_err(|_| ActrError::Internal("dynclib runtime owner lock poisoned".into()))?
        .is_some();
    if !is_running {
        return Err(ActrError::Internal(
            "dynclib Tokio runtime is shut down".into(),
        ));
    }

    Ok(runtime
        .handle
        .block_on(ACTIVE_BRIDGE_TOKEN.scope(bridge_token, future)))
}

/// Spawn a background task owned by the current dynclib workload.
///
/// Managed tasks are aborted and joined before the shared library is unloaded.
/// Clone any [`crate::Context`] used by the task before passing it here; the
/// cloned context retains its host bridge until the task exits.
pub fn spawn<F>(future: F) -> ActorResult<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let runtime = runtime()?;
    let mut tasks = runtime
        .tasks
        .lock()
        .map_err(|_| ActrError::Internal("dynclib task registry lock poisoned".into()))?;
    if !tasks.accepting {
        return Err(ActrError::Unavailable(
            "dynclib workload is shutting down".into(),
        ));
    }

    tasks.handles.retain(|handle| !handle.is_finished());
    tasks.handles.push(runtime.handle.spawn(future));
    Ok(())
}

pub(crate) fn active_bridge_token() -> Option<u64> {
    ACTIVE_BRIDGE_TOKEN.try_with(|token| *token).ok()
}

/// Stop managed tasks and shut down the runtime before `dlclose`.
pub fn shutdown() -> ActorResult<()> {
    let runtime = runtime()?;
    let is_running = runtime
        .owner
        .lock()
        .map_err(|_| ActrError::Internal("dynclib runtime owner lock poisoned".into()))?
        .is_some();
    if !is_running {
        return Ok(());
    }

    let handles = {
        let mut tasks = runtime
            .tasks
            .lock()
            .map_err(|_| ActrError::Internal("dynclib task registry lock poisoned".into()))?;
        tasks.accepting = false;
        std::mem::take(&mut tasks.handles)
    };

    runtime.handle.block_on(async move {
        for handle in &handles {
            handle.abort();
        }
        for handle in handles {
            let _ = handle.await;
        }
    });

    let owner = runtime
        .owner
        .lock()
        .map_err(|_| ActrError::Internal("dynclib runtime owner lock poisoned".into()))?
        .take();
    drop(owner);
    Ok(())
}
