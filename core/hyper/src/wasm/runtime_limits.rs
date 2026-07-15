use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use thiserror::Error;
use wasmtime::{Engine, ResourceLimiter};

use crate::config::WasmRuntimeLimits;

use super::error::{WasmError, WasmResult};

#[derive(Debug, Error)]
#[error("WASM resource limit exceeded: {label}")]
pub(crate) struct StoreResourceLimit {
    label: &'static str,
}

impl StoreResourceLimit {
    fn new(label: &'static str) -> Self {
        Self { label }
    }

    pub(crate) fn label(&self) -> &'static str {
        self.label
    }
}

/// Store-local limiter whose memory/table limits are aggregate across all
/// resources in the Store. Wasmtime's `StoreLimitsBuilder::memory_size` and
/// `table_elements` limits apply to each memory/table independently, which is
/// not the contract exposed by `WasmRuntimeLimits`.
#[derive(Debug)]
pub(crate) struct StoreResourceLimiter {
    max_linear_memory: usize,
    max_table_elements: usize,
    max_memories: usize,
    max_tables: usize,
    max_instances: usize,
    trap_on_grow_failure: bool,
    linear_memory: usize,
    table_elements: usize,
}

impl StoreResourceLimiter {
    pub(crate) fn new(limits: &WasmRuntimeLimits) -> Self {
        Self {
            max_linear_memory: limits.max_linear_memory,
            max_table_elements: limits.max_table_elements as usize,
            max_memories: limits.max_memories as usize,
            max_tables: limits.max_tables as usize,
            max_instances: limits.max_instances as usize,
            trap_on_grow_failure: limits.trap_on_grow_failure,
            linear_memory: 0,
            table_elements: 0,
        }
    }

    fn deny(&self, label: &'static str) -> wasmtime::Result<bool> {
        record_resource_denial();
        if self.trap_on_grow_failure {
            Err(StoreResourceLimit::new(label).into())
        } else {
            Ok(false)
        }
    }

    fn reserve_growth(
        total: &mut usize,
        current: usize,
        desired: usize,
        limit: usize,
    ) -> Option<bool> {
        let delta = desired.checked_sub(current)?;
        let next = total.checked_add(delta)?;
        if next > limit {
            return Some(false);
        }
        *total = next;
        Some(true)
    }
}

impl ResourceLimiter for StoreResourceLimiter {
    fn memory_growing(
        &mut self,
        current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        match Self::reserve_growth(
            &mut self.linear_memory,
            current,
            desired,
            self.max_linear_memory,
        ) {
            Some(true) => Ok(true),
            Some(false) | None => self.deny("aggregate linear memory per Store"),
        }
    }

    fn memory_grow_failed(&mut self, error: wasmtime::Error) -> wasmtime::Result<()> {
        // ResourceLimiter has no successful-growth callback or resource ID,
        // and Wasmtime may report a failure that occurred before calling
        // `memory_growing`. Retain the conservative reservation: rolling back
        // here could subtract a prior successful memory's bytes and let a
        // multi-memory guest exceed the aggregate Store limit.
        if self.trap_on_grow_failure {
            Err(error.context("forcing a memory growth failure to be a trap"))
        } else {
            Ok(())
        }
    }

    fn table_growing(
        &mut self,
        current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        match Self::reserve_growth(
            &mut self.table_elements,
            current,
            desired,
            self.max_table_elements,
        ) {
            Some(true) => Ok(true),
            Some(false) | None => self.deny("aggregate table elements per Store"),
        }
    }

    fn table_grow_failed(&mut self, error: wasmtime::Error) -> wasmtime::Result<()> {
        // See `memory_grow_failed`: fail closed rather than risk undercounting
        // a different table when Wasmtime reports a pre-limiter failure.
        if self.trap_on_grow_failure {
            Err(error.context("forcing a table growth failure to be a trap"))
        } else {
            Ok(())
        }
    }

    fn instances(&self) -> usize {
        self.max_instances
    }

    fn tables(&self) -> usize {
        self.max_tables
    }

    fn memories(&self) -> usize {
        self.max_memories
    }
}

static ACTIVE_COMPILES: AtomicUsize = AtomicUsize::new(0);
static ACTIVE_INSTANTIATES: AtomicUsize = AtomicUsize::new(0);
static ACTIVE_STORES: AtomicUsize = AtomicUsize::new(0);
static RESERVED_LINEAR_MEMORY: AtomicUsize = AtomicUsize::new(0);
static OUTSTANDING_INVOCATIONS: AtomicUsize = AtomicUsize::new(0);

static DENIED_COMPILES: AtomicU64 = AtomicU64::new(0);
static DENIED_INSTANTIATES: AtomicU64 = AtomicU64::new(0);
static DENIED_STORES: AtomicU64 = AtomicU64::new(0);
static DENIED_INVOCATIONS: AtomicU64 = AtomicU64::new(0);
static OUT_OF_FUEL_TRAPS: AtomicU64 = AtomicU64::new(0);
static EPOCH_TRAPS: AtomicU64 = AtomicU64::new(0);
static INVOCATION_TIMEOUTS: AtomicU64 = AtomicU64::new(0);
static RESOURCE_DENIALS: AtomicU64 = AtomicU64::new(0);
static COMPILE_FAILURES: AtomicU64 = AtomicU64::new(0);
static INSTANTIATE_FAILURES: AtomicU64 = AtomicU64::new(0);

/// Process-wide WASM resource counters suitable for metrics exporters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WasmRuntimeStats {
    pub active_compiles: usize,
    pub active_instantiates: usize,
    pub active_stores: usize,
    pub reserved_linear_memory: usize,
    pub outstanding_invocations: usize,
    pub denied_compiles: u64,
    pub denied_instantiates: u64,
    pub denied_stores: u64,
    pub denied_invocations: u64,
    pub out_of_fuel_traps: u64,
    pub epoch_traps: u64,
    pub invocation_timeouts: u64,
    pub resource_denials: u64,
    pub compile_failures: u64,
    pub instantiate_failures: u64,
}

pub fn wasm_runtime_stats() -> WasmRuntimeStats {
    WasmRuntimeStats {
        active_compiles: ACTIVE_COMPILES.load(Ordering::Relaxed),
        active_instantiates: ACTIVE_INSTANTIATES.load(Ordering::Relaxed),
        active_stores: ACTIVE_STORES.load(Ordering::Relaxed),
        reserved_linear_memory: RESERVED_LINEAR_MEMORY.load(Ordering::Relaxed),
        outstanding_invocations: OUTSTANDING_INVOCATIONS.load(Ordering::Relaxed),
        denied_compiles: DENIED_COMPILES.load(Ordering::Relaxed),
        denied_instantiates: DENIED_INSTANTIATES.load(Ordering::Relaxed),
        denied_stores: DENIED_STORES.load(Ordering::Relaxed),
        denied_invocations: DENIED_INVOCATIONS.load(Ordering::Relaxed),
        out_of_fuel_traps: OUT_OF_FUEL_TRAPS.load(Ordering::Relaxed),
        epoch_traps: EPOCH_TRAPS.load(Ordering::Relaxed),
        invocation_timeouts: INVOCATION_TIMEOUTS.load(Ordering::Relaxed),
        resource_denials: RESOURCE_DENIALS.load(Ordering::Relaxed),
        compile_failures: COMPILE_FAILURES.load(Ordering::Relaxed),
        instantiate_failures: INSTANTIATE_FAILURES.load(Ordering::Relaxed),
    }
}

#[derive(Debug, Clone, Copy)]
enum CounterKind {
    Compile,
    Instantiate,
    Store,
    Memory,
    Invocation,
}

#[derive(Debug)]
pub(crate) struct QuotaPermit {
    kind: CounterKind,
    amount: usize,
}

impl Drop for QuotaPermit {
    fn drop(&mut self) {
        counter(self.kind).fetch_sub(self.amount, Ordering::AcqRel);
    }
}

#[derive(Debug)]
pub(crate) struct StorePermit {
    _store: QuotaPermit,
    _memory: QuotaPermit,
}

fn counter(kind: CounterKind) -> &'static AtomicUsize {
    match kind {
        CounterKind::Compile => &ACTIVE_COMPILES,
        CounterKind::Instantiate => &ACTIVE_INSTANTIATES,
        CounterKind::Store => &ACTIVE_STORES,
        CounterKind::Memory => &RESERVED_LINEAR_MEMORY,
        CounterKind::Invocation => &OUTSTANDING_INVOCATIONS,
    }
}

fn acquire(
    kind: CounterKind,
    amount: usize,
    limit: usize,
    denied: &AtomicU64,
    label: &'static str,
) -> WasmResult<QuotaPermit> {
    let result = counter(kind).fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
        current.checked_add(amount).filter(|next| *next <= limit)
    });
    match result {
        Ok(_) => Ok(QuotaPermit { kind, amount }),
        Err(current) => {
            denied.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                resource = label,
                current,
                limit,
                "WASM process quota denied"
            );
            Err(WasmError::ResourceLimitExceeded(label))
        }
    }
}

pub(crate) fn acquire_compile(limits: &WasmRuntimeLimits) -> WasmResult<QuotaPermit> {
    acquire(
        CounterKind::Compile,
        1,
        limits.max_concurrent_compiles,
        &DENIED_COMPILES,
        "concurrent component compilations",
    )
}

pub(crate) fn acquire_instantiate(limits: &WasmRuntimeLimits) -> WasmResult<QuotaPermit> {
    acquire(
        CounterKind::Instantiate,
        1,
        limits.max_concurrent_instantiates,
        &DENIED_INSTANTIATES,
        "concurrent component instantiations",
    )
}

pub(crate) fn acquire_store(limits: &WasmRuntimeLimits) -> WasmResult<StorePermit> {
    let store = acquire(
        CounterKind::Store,
        1,
        limits.max_active_stores,
        &DENIED_STORES,
        "active WASM stores",
    )?;
    let memory = acquire(
        CounterKind::Memory,
        limits.max_linear_memory,
        limits.max_total_linear_memory,
        &DENIED_STORES,
        "aggregate configured linear memory",
    )?;
    Ok(StorePermit {
        _store: store,
        _memory: memory,
    })
}

pub(crate) fn acquire_invocation(limits: &WasmRuntimeLimits) -> WasmResult<QuotaPermit> {
    acquire(
        CounterKind::Invocation,
        1,
        limits.max_outstanding_invocations,
        &DENIED_INVOCATIONS,
        "outstanding WASM invocations",
    )
}

pub(crate) fn record_out_of_fuel() {
    OUT_OF_FUEL_TRAPS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_epoch_trap() {
    EPOCH_TRAPS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_timeout() {
    INVOCATION_TIMEOUTS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_resource_denial() {
    RESOURCE_DENIALS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_compile_failure() {
    COMPILE_FAILURES.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_instantiate_failure() {
    INSTANTIATE_FAILURES.fetch_add(1, Ordering::Relaxed);
}

/// Dedicated epoch ticker for one Engine. The native thread guarantees epoch
/// progress even when non-yielding guest code monopolizes a Tokio worker.
#[derive(Debug)]
pub(crate) struct EpochTicker {
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl EpochTicker {
    pub(crate) fn spawn(engine: &Engine, tick: Duration) -> WasmResult<Arc<Self>> {
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let thread_engine = engine.clone();
        let handle = thread::Builder::new()
            .name("actr-wasm-epoch".to_string())
            .spawn(move || {
                while !thread_stop.load(Ordering::Acquire) {
                    thread::park_timeout(tick);
                    if !thread_stop.load(Ordering::Acquire) {
                        thread_engine.increment_epoch();
                    }
                }
            })
            .map_err(|e| WasmError::LoadFailed(format!("spawn WASM epoch ticker: {e}")))?;
        Ok(Arc::new(Self {
            stop,
            handle: Some(handle),
        }))
    }
}

impl Drop for EpochTicker {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(handle) = self.handle.take() {
            handle.thread().unpark();
            if handle.join().is_err() {
                tracing::error!("WASM epoch ticker thread panicked");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn non_trapping_limits(memory: usize, tables: u32) -> WasmRuntimeLimits {
        WasmRuntimeLimits {
            max_linear_memory: memory,
            max_table_elements: tables,
            trap_on_grow_failure: false,
            ..WasmRuntimeLimits::default()
        }
    }

    #[test]
    fn aggregate_memory_limit_spans_multiple_memories() {
        let mut limiter = StoreResourceLimiter::new(&non_trapping_limits(64, 64));

        assert!(limiter.memory_growing(0, 40, None).unwrap());
        assert!(!limiter.memory_growing(0, 40, None).unwrap());
        assert!(limiter.memory_growing(0, 24, None).unwrap());
    }

    #[test]
    fn failed_memory_growth_retains_conservative_reservation() {
        let mut limiter = StoreResourceLimiter::new(&non_trapping_limits(64, 64));

        assert!(limiter.memory_growing(0, 48, None).unwrap());
        limiter
            .memory_grow_failed(wasmtime::Error::msg("allocation failed"))
            .unwrap();
        assert!(!limiter.memory_growing(0, 64, None).unwrap());
        assert!(limiter.memory_growing(0, 16, None).unwrap());
    }

    #[test]
    fn aggregate_table_limit_spans_multiple_tables() {
        let mut limiter = StoreResourceLimiter::new(&non_trapping_limits(64, 10));

        assert!(limiter.table_growing(0, 6, None).unwrap());
        assert!(!limiter.table_growing(0, 5, None).unwrap());
        assert!(limiter.table_growing(0, 4, None).unwrap());
    }

    #[test]
    fn trapping_denial_has_typed_cause() {
        let mut limiter = StoreResourceLimiter::new(&WasmRuntimeLimits {
            max_linear_memory: 64,
            ..WasmRuntimeLimits::default()
        });

        let error = limiter.memory_growing(0, 65, None).unwrap_err();
        let limit = error.downcast_ref::<StoreResourceLimit>().unwrap();
        assert_eq!(limit.label(), "aggregate linear memory per Store");
    }
}
