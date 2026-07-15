use thiserror::Error;

pub type WasmResult<T> = Result<T, WasmError>;

#[derive(Debug, Error)]
pub enum WasmError {
    #[error("WASM package verification failed: {0}")]
    VerificationFailed(#[from] crate::error::HyperError),

    #[error("WASM module load failed: {0}")]
    LoadFailed(String),

    #[error("WASM actor initialization failed: {0}")]
    InitFailed(String),

    #[error("WASM actor execution failed: {0}")]
    ExecutionFailed(String),

    #[error("WASM instance trapped (store poisoned; rebuilt on next call): {0}")]
    InstanceTrapped(String),

    #[error("WASM invocation exceeded fuel budget")]
    OutOfFuel,
    #[error("WASM invocation interrupted by epoch deadline")]
    EpochInterrupted,
    #[error("WASM resource limit exceeded: {0}")]
    ResourceLimitExceeded(&'static str),
    #[error("WASM invocation timed out after {0:?}")]
    InvocationTimeout(std::time::Duration),
}

const MAX_TRAP_MESSAGE_BYTES: usize = 512;

pub(crate) fn classify_trap(entry: &str, trap: wasmtime::Error) -> WasmError {
    if let Some(trap_code) = trap.downcast_ref::<wasmtime::Trap>() {
        match trap_code {
            wasmtime::Trap::OutOfFuel => {
                super::runtime_limits::record_out_of_fuel();
                return WasmError::OutOfFuel;
            }
            wasmtime::Trap::Interrupt => {
                super::runtime_limits::record_epoch_trap();
                return WasmError::EpochInterrupted;
            }
            _ => {}
        }
    }

    if let Some(limit) = trap.downcast_ref::<super::runtime_limits::StoreResourceLimit>() {
        return WasmError::ResourceLimitExceeded(limit.label());
    }

    let message = format!("{trap:#}");
    if message.contains("resource limit exceeded")
        || message.contains("maximum memory size")
        || message.contains("maximum table size")
        || message.contains("forcing trap when growing memory")
        || message.contains("forcing trap when growing table")
        || message.contains("forcing a memory growth failure to be a trap")
        || message.contains("forcing a table growth failure to be a trap")
    {
        super::runtime_limits::record_resource_denial();
        return WasmError::ResourceLimitExceeded("per-store Wasmtime resource limit");
    }

    WasmError::InstanceTrapped(format!(
        "{entry} trap: {}",
        truncate_utf8(&message, MAX_TRAP_MESSAGE_BYTES)
    ))
}

fn truncate_utf8(value: &str, max_bytes: usize) -> &str {
    if value.len() <= max_bytes {
        return value;
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}
