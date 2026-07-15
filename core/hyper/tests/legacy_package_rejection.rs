//! RA3 compatibility-matrix guard: wasmtime 46 must reject legacy async-lift
//! `.actr` packages with an actionable rebuild diagnostic.
//!
//! Background: pre-M3 SDKs (wit-bindgen <= 0.57 with `async: true`) emit
//! async-lift custom sections even though the `actr:workload@0.1.0` WIT world
//! is entirely synchronous `func`. wasmtime 46's embedded wasmparser enforces
//! `check_asyncness`: the `async` canonical option is only valid on WIT
//! `async func` types, so those binaries are rejected unconditionally at
//! `Component::from_binary` — there is no Config/feature escape hatch. The old
//! "load any 0.1.0 package unchanged" invariant is therefore physically
//! unsatisfiable upstream; M3 degrades it to "reject with a clear rebuild
//! hint" (see `WasmHost::compile`).
//!
//! `fixtures/legacy_asynclift_guest.wasm` is a real artifact built by the
//! wit-bindgen 0.57.1 async-lift pipeline (the 43-era ABI), checked in as a
//! frozen regression sample. Rebuilding it from source would require pinning
//! the old toolchain forever; a stored real artifact is both simpler and a
//! higher-fidelity reproduction of the exact bytes a shipped 0.1.0 package
//! carried.

#![cfg(feature = "wasm-engine")]

use actr_hyper::wasm::{WasmError, WasmHost};

/// A genuine wit-bindgen 0.57.1 async-lift Component (43-era `.actr` payload).
const LEGACY_ASYNCLIFT_GUEST: &[u8] = include_bytes!("fixtures/legacy_asynclift_guest.wasm");

#[test]
fn legacy_asynclift_package_is_rejected_with_rebuild_hint() {
    let result = WasmHost::compile(LEGACY_ASYNCLIFT_GUEST);

    let err = match result {
        Ok(_) => panic!(
            "wasmtime 46 unexpectedly loaded a legacy async-lift component; \
             the async canonical option should be rejected on the synchronous \
             actr:workload@0.1.0 world"
        ),
        Err(err) => err,
    };

    let WasmError::LoadFailed(message) = &err else {
        panic!("expected WasmError::LoadFailed, got: {err:?}");
    };

    // The diagnostic must (a) point at the async-lift cause and (b) tell the
    // operator the concrete remediation (rebuild with the current SDK).
    assert!(
        message.contains("old SDK") && message.contains("Rebuild"),
        "rejection message should carry actionable rebuild guidance, got: {message}"
    );
    assert!(
        message.contains("async` canonical option requires an async function type"),
        "rejection message should preserve the underlying wasmtime cause, got: {message}"
    );
}
