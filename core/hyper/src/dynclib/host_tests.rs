use super::*;

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

#[cfg(unix)]
fn current_process_library() -> Arc<Library> {
    Arc::new(libloading::os::unix::Library::this().into())
}

#[cfg(windows)]
fn current_process_library() -> Arc<Library> {
    Arc::new(
        libloading::os::windows::Library::this()
            .expect("open current process")
            .into(),
    )
}

unsafe extern "C" fn unused_handle(
    _req: *const u8,
    _req_len: usize,
    _resp_out: *mut *mut u8,
    _resp_len_out: *mut usize,
) -> i32 {
    guest_abi::code::GENERIC_ERROR
}

unsafe extern "C" fn unused_free_response(_ptr: *mut u8, _len: usize) {}

fn test_instance(shutdown_fn: ShutdownFn, library_guard: Arc<Library>) -> DynclibInstance {
    DynclibInstance {
        handle_fn: unused_handle,
        free_response_fn: unused_free_response,
        shutdown_fn,
        ffi_gate: Arc::new(tokio::sync::Mutex::new(())),
        library_guard,
        shutdown_state: ShutdownState::Active,
    }
}

#[test]
fn load_missing_file_errors() {
    // A path that does not exist must surface LoadFailed, not panic.
    let err = DynclibHost::load("/nonexistent/actor.dylib").unwrap_err();
    assert!(matches!(err, DynclibError::LoadFailed(_)));
}

#[test]
fn load_non_library_file_errors() {
    // A real file that is not a shared library must also fail to load.
    let tmp = std::env::temp_dir().join("actr-not-a-lib.txt");
    std::fs::write(&tmp, b"not a shared library").unwrap();
    let err = DynclibHost::load(&tmp).unwrap_err();
    // Either LoadFailed (dlopen) or MissingSymbol (if the platform somehow
    // opens it). Both are expected non-panic failures.
    let _ = err;
    let _ = std::fs::remove_file(&tmp);
}

static FALLBACK_RELEASE: AtomicBool = AtomicBool::new(false);
static FALLBACK_FINISHED: AtomicBool = AtomicBool::new(false);

unsafe extern "C" fn slow_successful_shutdown() -> i32 {
    while !FALLBACK_RELEASE.load(Ordering::Acquire) {
        std::thread::sleep(Duration::from_millis(10));
    }
    FALLBACK_FINISHED.store(true, Ordering::Release);
    guest_abi::code::SUCCESS
}

#[test]
fn drop_schedules_shutdown_without_blocking() {
    FALLBACK_RELEASE.store(false, Ordering::Release);
    FALLBACK_FINISHED.store(false, Ordering::Release);
    let library = current_process_library();
    let instance = test_instance(slow_successful_shutdown, Arc::clone(&library));
    let (drop_done_tx, drop_done_rx) = std::sync::mpsc::sync_channel(1);

    let drop_thread = std::thread::spawn(move || {
        drop(instance);
        let _ = drop_done_tx.send(());
    });
    let drop_result = drop_done_rx.recv_timeout(Duration::from_secs(2));
    let guard_count_while_shutdown_blocked = Arc::strong_count(&library);
    FALLBACK_RELEASE.store(true, Ordering::Release);
    drop_thread.join().expect("join Drop caller");

    assert!(
        drop_result.is_ok(),
        "Drop waited for fallback actr_shutdown to return"
    );
    assert_eq!(
        guard_count_while_shutdown_blocked, 2,
        "fallback thread must keep the library mapped"
    );

    let deadline = Instant::now() + Duration::from_secs(2);
    while !FALLBACK_FINISHED.load(Ordering::Acquire) && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        FALLBACK_FINISHED.load(Ordering::Acquire),
        "fallback shutdown did not finish"
    );
    while Arc::strong_count(&library) != 1 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        Arc::strong_count(&library),
        1,
        "successful fallback must release its library guard"
    );
}

unsafe extern "C" fn failed_shutdown() -> i32 {
    guest_abi::code::GENERIC_ERROR
}

#[tokio::test]
async fn failed_explicit_shutdown_retains_library() {
    let library = current_process_library();
    let mut instance = test_instance(failed_shutdown, Arc::clone(&library));

    let error = instance.shutdown().await.expect_err("shutdown must fail");

    assert!(
        error
            .to_string()
            .contains("dynamic library will remain loaded")
    );
    assert_eq!(instance.shutdown_state, ShutdownState::Failed);
    drop(instance);
    assert_eq!(
        Arc::strong_count(&library),
        2,
        "failed shutdown must retain a process-lifetime library guard"
    );
}
