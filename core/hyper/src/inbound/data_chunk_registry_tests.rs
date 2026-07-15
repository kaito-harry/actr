use super::*;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::{mpsc as tokio_mpsc, oneshot};

const RELIABLE: PayloadType = PayloadType::StreamReliable;
const LATENCY: PayloadType = PayloadType::StreamLatencyFirst;

fn chunk_seq(stream_id: &str, sequence: u64) -> DataChunk {
    DataChunk {
        stream_id: stream_id.to_string(),
        sequence,
        payload: Default::default(),
        metadata: vec![],
        timestamp_ms: None,
    }
}

fn chunk(stream_id: &str) -> DataChunk {
    chunk_seq(stream_id, 1)
}

fn counting_callback() -> (DataChunkCallback, Arc<Mutex<u32>>) {
    let count = Arc::new(Mutex::new(0u32));
    let c = count.clone();
    let cb: DataChunkCallback = Arc::new(move |_chunk, _sender| {
        let c = c.clone();
        Box::pin(async move {
            *c.lock().unwrap() += 1;
            Ok(())
        })
    });
    (cb, count)
}

/// A test harness whose callback records start/completion order and can gate
/// individual invocations on a per-sequence oneshot, all without sleeping.
struct Harness {
    gates: Arc<Mutex<HashMap<u64, oneshot::Receiver<()>>>>,
    panics: Arc<Mutex<Vec<u64>>>,
}

struct HarnessProbe {
    completions: Arc<Mutex<Vec<u64>>>,
    started_rx: tokio_mpsc::UnboundedReceiver<u64>,
    done_rx: tokio_mpsc::UnboundedReceiver<u64>,
}

impl Harness {
    fn new() -> (DataChunkCallback, Self, HarnessProbe) {
        let completions = Arc::new(Mutex::new(Vec::new()));
        let gates: Arc<Mutex<HashMap<u64, oneshot::Receiver<()>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let panics = Arc::new(Mutex::new(Vec::new()));
        let (started_tx, started_rx) = tokio_mpsc::unbounded_channel();
        let (done_tx, done_rx) = tokio_mpsc::unbounded_channel();

        let h = Harness {
            gates: gates.clone(),
            panics: panics.clone(),
        };

        let cb_completions = completions.clone();
        let cb: DataChunkCallback = Arc::new(move |chunk: DataChunk, _sender| {
            let completions = cb_completions.clone();
            let gates = gates.clone();
            let started_tx = started_tx.clone();
            let done_tx = done_tx.clone();
            let panics = panics.clone();
            Box::pin(async move {
                let seq = chunk.sequence;
                let _ = started_tx.send(seq);
                // Take this invocation's gate (if any) and await it. The guard
                // is dropped before the await.
                let gate = gates.lock().unwrap().remove(&seq);
                if let Some(rx) = gate {
                    let _ = rx.await;
                }
                if panics.lock().unwrap().contains(&seq) {
                    let _ = done_tx.send(seq);
                    panic!("intentional test panic on seq {seq}");
                }
                completions.lock().unwrap().push(seq);
                let _ = done_tx.send(seq);
                Ok(())
            })
        });

        let probe = HarnessProbe {
            completions,
            started_rx,
            done_rx,
        };
        (cb, h, probe)
    }

    /// Install a gate for `seq`, returning its release sender.
    fn gate(&self, seq: u64) -> oneshot::Sender<()> {
        let (tx, rx) = oneshot::channel();
        self.gates.lock().unwrap().insert(seq, rx);
        tx
    }

    /// Mark `seq` so its callback panics (after its gate releases).
    fn make_panic(&self, seq: u64) {
        self.panics.lock().unwrap().push(seq);
    }
}

impl HarnessProbe {
    fn completions(&self) -> Vec<u64> {
        self.completions.lock().unwrap().clone()
    }

    async fn wait_started(&mut self) -> u64 {
        tokio::time::timeout(Duration::from_secs(2), self.started_rx.recv())
            .await
            .expect("callback did not start in time")
            .expect("started channel closed")
    }

    async fn wait_done(&mut self) -> u64 {
        tokio::time::timeout(Duration::from_secs(2), self.done_rx.recv())
            .await
            .expect("callback did not finish in time")
            .expect("done channel closed")
    }
}

#[test]
fn register_and_default() {
    let reg = DataChunkRegistry::default();
    assert_eq!(reg.callback_len(), 0);
    let (cb, _) = counting_callback();
    reg.register("s1".into(), cb);
    assert_eq!(reg.callback_len(), 1);
}

#[test]
fn unregister_removes_stream() {
    let reg = DataChunkRegistry::new();
    let (cb, _) = counting_callback();
    reg.register("s1".into(), cb);
    reg.unregister("s1");
    assert_eq!(reg.callback_len(), 0);
    // Unknown id is a no-op.
    reg.unregister("never");
    assert_eq!(reg.callback_len(), 0);
}

#[test]
fn clear_removes_all_streams_and_closes_registry() {
    let reg = DataChunkRegistry::new();
    let (first, _) = counting_callback();
    let (second, _) = counting_callback();
    reg.register("s1".into(), first);
    reg.register("s2".into(), second);

    reg.clear();
    assert_eq!(reg.callback_len(), 0);
    assert!(reg.shutdown_token().is_cancelled());

    let (replacement, _) = counting_callback();
    reg.register("s3".into(), replacement);
    assert_eq!(reg.callback_len(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dispatch_invokes_registered_callback() {
    let reg = DataChunkRegistry::new();
    let (cb, _h, mut probe) = Harness::new();
    reg.register("s1".into(), cb);

    reg.dispatch(chunk("s1"), ActrId::default(), RELIABLE).await;
    assert_eq!(probe.wait_done().await, 1);
    assert_eq!(probe.completions(), vec![1]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dispatch_unknown_stream_is_noop() {
    let reg = DataChunkRegistry::new();
    reg.dispatch(chunk("missing"), ActrId::default(), RELIABLE)
        .await;
    assert_eq!(reg.callback_len(), 0);
    assert_eq!(reg.worker_len(), 0);
}

/// (1) Same stream, reliable: a slow first chunk cannot be overtaken; the
/// completion order equals arrival order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn same_stream_reliable_preserves_order() {
    let reg = DataChunkRegistry::new();
    let (cb, h, mut probe) = Harness::new();
    reg.register("s1".into(), cb);

    // Gate seq 1 so its callback blocks until we release it.
    let release1 = h.gate(1);

    reg.dispatch(chunk_seq("s1", 1), ActrId::default(), RELIABLE)
        .await;
    // Ensure the worker has actually begun seq 1 before enqueuing seq 2.
    assert_eq!(probe.wait_started().await, 1);
    reg.dispatch(chunk_seq("s1", 2), ActrId::default(), RELIABLE)
        .await;

    // seq 2 must not run while seq 1 is blocked (serial worker).
    assert_eq!(probe.completions(), Vec::<u64>::new());

    release1.send(()).unwrap();
    assert_eq!(probe.wait_done().await, 1);
    assert_eq!(probe.wait_done().await, 2);
    assert_eq!(probe.completions(), vec![1, 2]);
}

/// (2) Different streams are independent: a blocked slow stream does not stop a
/// fast stream from completing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distinct_streams_are_independent() {
    let reg = DataChunkRegistry::new();
    let (cb, h, mut probe) = Harness::new();
    reg.register("slow".into(), cb.clone());
    reg.register("fast".into(), cb);

    // Block the slow stream indefinitely (hold the sender).
    let _hold = h.gate(10);
    reg.dispatch(chunk_seq("slow", 10), ActrId::default(), RELIABLE)
        .await;
    assert_eq!(probe.wait_started().await, 10);

    // The fast stream completes regardless.
    reg.dispatch(chunk_seq("fast", 20), ActrId::default(), RELIABLE)
        .await;
    assert_eq!(probe.wait_done().await, 20);
    assert!(probe.completions().contains(&20));
    assert!(!probe.completions().contains(&10));
}

/// (3) Panic isolation: a panicking callback is counted and the worker keeps
/// processing subsequent chunks on the same stream.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn callback_panic_is_isolated() {
    let reg = DataChunkRegistry::new();
    let (cb, h, mut probe) = Harness::new();
    reg.register("s1".into(), cb);
    h.make_panic(1);

    reg.dispatch(chunk_seq("s1", 1), ActrId::default(), RELIABLE)
        .await;
    // seq 1 signals done just before panicking.
    assert_eq!(probe.wait_done().await, 1);

    reg.dispatch(chunk_seq("s1", 2), ActrId::default(), RELIABLE)
        .await;
    assert_eq!(probe.wait_done().await, 2);

    assert_eq!(probe.completions(), vec![2]);
    assert_eq!(reg.panic_count(), 1);
    // Worker still alive for this stream.
    assert_eq!(reg.worker_len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn synchronous_callback_panic_is_isolated() {
    let reg = DataChunkRegistry::new();
    let (done_tx, mut done_rx) = tokio_mpsc::unbounded_channel::<u64>();

    let cb: DataChunkCallback = Arc::new(move |chunk: DataChunk, _sender| {
        let seq = chunk.sequence;
        if seq == 1 {
            panic!("intentional synchronous test panic on seq {seq}");
        }

        let done_tx = done_tx.clone();
        Box::pin(async move {
            let _ = done_tx.send(seq);
            Ok(())
        })
    });
    reg.register("s1".into(), cb);

    reg.dispatch(chunk_seq("s1", 1), ActrId::default(), RELIABLE)
        .await;
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if reg.panic_count() == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("synchronous callback panic was not counted");

    reg.dispatch(chunk_seq("s1", 2), ActrId::default(), RELIABLE)
        .await;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), done_rx.recv())
            .await
            .expect("worker did not continue after synchronous panic")
            .expect("done channel closed"),
        2
    );
}

/// (4) unregister = drain: chunks queued behind a gate still all run after the
/// stream is unregistered and released.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unregister_drains_queued_chunks() {
    let reg = DataChunkRegistry::with_capacity(8);
    let (cb, h, mut probe) = Harness::new();
    reg.register("s1".into(), cb);

    let release1 = h.gate(1);
    // Enqueue 4 chunks; the first blocks the worker, the rest queue up.
    for seq in 1..=4 {
        reg.dispatch(chunk_seq("s1", seq), ActrId::default(), RELIABLE)
            .await;
    }
    assert_eq!(probe.wait_started().await, 1);

    // Unregister: drops the stored sender -> worker will drain then exit.
    reg.unregister("s1");
    assert_eq!(reg.callback_len(), 0);

    release1.send(()).unwrap();
    let mut seen = Vec::new();
    for _ in 1..=4 {
        seen.push(probe.wait_done().await);
    }
    seen.sort_unstable();
    assert_eq!(seen, vec![1, 2, 3, 4]);
    assert_eq!(probe.completions(), vec![1, 2, 3, 4]);
}

/// (5) shutdown = cancel: the in-flight callback runs to completion, queued
/// chunks are dropped (never dequeued), and shutdown() joins without hanging.
///
/// Determinism: the in-flight seq-1 callback completes *only* when the shared
/// shutdown token is cancelled. So cancellation is guaranteed to be observed
/// before the worker could ever dequeue seq 2 / seq 3.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_cancels_queue_and_joins() {
    let reg = DataChunkRegistry::with_capacity(8);

    let token = reg.shutdown_token();
    let completions = Arc::new(Mutex::new(Vec::<u64>::new()));
    let (started_tx, mut started_rx) = tokio_mpsc::unbounded_channel::<u64>();

    let cb_completions = completions.clone();
    let cb: DataChunkCallback = Arc::new(move |chunk: DataChunk, _sender| {
        let completions = cb_completions.clone();
        let started_tx = started_tx.clone();
        let token = token.clone();
        Box::pin(async move {
            let seq = chunk.sequence;
            let _ = started_tx.send(seq);
            // The in-flight (first) chunk blocks until shutdown is cancelled.
            if seq == 1 {
                token.cancelled().await;
            }
            completions.lock().unwrap().push(seq);
            Ok(())
        })
    });
    reg.register("s1".into(), cb);

    for seq in 1..=3 {
        reg.dispatch(chunk_seq("s1", seq), ActrId::default(), RELIABLE)
            .await;
    }
    // Worker is now in-flight on seq 1, blocked on the shutdown token.
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), started_rx.recv())
            .await
            .expect("seq 1 never started")
            .unwrap(),
        1
    );

    tokio::time::timeout(Duration::from_secs(5), reg.shutdown())
        .await
        .expect("shutdown hung");

    // The in-flight seq 1 completed; queued seq 2/3 were cancelled, not run.
    assert_eq!(*completions.lock().unwrap(), vec![1]);
    assert_eq!(reg.worker_len(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_is_terminal_for_late_dispatch() {
    let reg = DataChunkRegistry::new();
    let (cb, _h, mut probe) = Harness::new();
    reg.register("s1".into(), cb);

    reg.shutdown().await;
    reg.dispatch(chunk_seq("s1", 1), ActrId::default(), RELIABLE)
        .await;

    if let Ok(Some(started)) =
        tokio::time::timeout(Duration::from_millis(200), probe.started_rx.recv()).await
    {
        panic!("callback ran after registry shutdown: {started}");
    }
    assert_eq!(reg.worker_len(), 0, "late dispatch resurrected a worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelling_shutdown_aborts_taken_workers() {
    struct DropSignal(Option<oneshot::Sender<()>>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            if let Some(tx) = self.0.take() {
                let _ = tx.send(());
            }
        }
    }

    let reg = Arc::new(DataChunkRegistry::new());
    let (started_tx, started_rx) = oneshot::channel();
    let started_tx = Arc::new(Mutex::new(Some(started_tx)));
    let (dropped_tx, dropped_rx) = oneshot::channel();
    let dropped_tx = Arc::new(Mutex::new(Some(dropped_tx)));

    let callback: DataChunkCallback = Arc::new(move |_chunk, _sender| {
        let started_tx = started_tx.clone();
        let dropped_tx = dropped_tx.clone();
        Box::pin(async move {
            let _drop_signal = DropSignal(dropped_tx.lock().unwrap().take());
            if let Some(tx) = started_tx.lock().unwrap().take() {
                let _ = tx.send(());
            }
            std::future::pending::<()>().await;
            #[allow(unreachable_code)]
            Ok(())
        })
    });
    reg.register("s1".into(), callback);
    reg.dispatch(chunk("s1"), ActrId::default(), RELIABLE).await;
    tokio::time::timeout(Duration::from_secs(2), started_rx)
        .await
        .expect("callback did not start")
        .expect("callback start signal dropped");

    let shutdown_registry = reg.clone();
    let shutdown = tokio::spawn(async move {
        shutdown_registry.shutdown().await;
    });

    tokio::time::timeout(Duration::from_secs(2), async {
        while reg.callback_len() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("shutdown did not take worker handles");
    shutdown.abort();
    let _ = shutdown.await;

    tokio::time::timeout(Duration::from_secs(2), dropped_rx)
        .await
        .expect("worker callback was detached instead of aborted")
        .expect("callback drop signal sender disappeared");
}

/// (6) LatencyFirst overflow: with capacity 1 and a blocked callback,
/// subsequent dispatches return immediately and are dropped + counted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn latency_first_drops_on_overflow() {
    let reg = DataChunkRegistry::with_capacity(1);
    let (cb, h, mut probe) = Harness::new();
    reg.register("s1".into(), cb);

    let release1 = h.gate(1);
    // seq 1 occupies the worker (in-flight, blocked on gate).
    reg.dispatch(chunk_seq("s1", 1), ActrId::default(), LATENCY)
        .await;
    assert_eq!(probe.wait_started().await, 1);
    // seq 2 fills the single queue slot.
    reg.dispatch(chunk_seq("s1", 2), ActrId::default(), LATENCY)
        .await;

    // seq 3 and 4 must be dropped without blocking (timeout proves non-blocking).
    for seq in 3..=4 {
        tokio::time::timeout(
            Duration::from_millis(200),
            reg.dispatch(chunk_seq("s1", seq), ActrId::default(), LATENCY),
        )
        .await
        .expect("LatencyFirst dispatch blocked on full queue");
    }
    assert!(reg.dropped_count() >= 2);

    release1.send(()).unwrap();
    assert_eq!(probe.wait_done().await, 1);
    assert_eq!(probe.wait_done().await, 2);
}

/// (7) Reliable backpressure: with capacity 1 and a blocked callback, the
/// dispatch that would overflow blocks until the queue drains; no chunk is lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reliable_backpressure_blocks_then_delivers() {
    let reg = Arc::new(DataChunkRegistry::with_capacity(1));
    let (cb, h, mut probe) = Harness::new();
    reg.register("s1".into(), cb);

    let release1 = h.gate(1);
    // seq 1 in-flight (blocked), seq 2 fills the one queue slot.
    reg.dispatch(chunk_seq("s1", 1), ActrId::default(), RELIABLE)
        .await;
    assert_eq!(probe.wait_started().await, 1);
    reg.dispatch(chunk_seq("s1", 2), ActrId::default(), RELIABLE)
        .await;

    // seq 3 must block: the queue is full and the worker is stuck on seq 1.
    let reg_bg = reg.clone();
    let mut dispatch3 = tokio::spawn(async move {
        reg_bg
            .dispatch(chunk_seq("s1", 3), ActrId::default(), RELIABLE)
            .await;
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(200), &mut dispatch3)
            .await
            .is_err(),
        "reliable dispatch should block while queue is full"
    );

    // Release the worker; everything drains in order, nothing is lost.
    release1.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(2), dispatch3)
        .await
        .expect("blocked dispatch never completed")
        .expect("dispatch task panicked");

    for _ in 1..=3 {
        probe.wait_done().await;
    }
    assert_eq!(probe.completions(), vec![1, 2, 3]);
}

/// (9) Re-register replaces the handler in place: re-registering the same
/// `stream_id` (without unregistering first) makes the *new* callback handle the
/// next chunk, while chunks already in flight/queued keep FIFO order. This is
/// the regression guard for the "worker pins its spawn-time callback" bug — on
/// the buggy path seq 2 would be handled by the old handler `A`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reregister_swaps_handler_and_preserves_order() {
    // Shared log of (handler_label, seq) in completion order.
    let record: Arc<Mutex<Vec<(char, u64)>>> = Arc::new(Mutex::new(Vec::new()));
    let gates: Arc<Mutex<HashMap<u64, oneshot::Receiver<()>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let (started_tx, mut started_rx) = tokio_mpsc::unbounded_channel::<(char, u64)>();
    let (done_tx, mut done_rx) = tokio_mpsc::unbounded_channel::<(char, u64)>();

    let make_cb = |label: char| -> DataChunkCallback {
        let record = record.clone();
        let gates = gates.clone();
        let started_tx = started_tx.clone();
        let done_tx = done_tx.clone();
        Arc::new(move |chunk: DataChunk, _sender| {
            let record = record.clone();
            let gates = gates.clone();
            let started_tx = started_tx.clone();
            let done_tx = done_tx.clone();
            Box::pin(async move {
                let seq = chunk.sequence;
                let _ = started_tx.send((label, seq));
                // Take this invocation's gate (if any); guard dropped before await.
                let gate = gates.lock().unwrap().remove(&seq);
                if let Some(rx) = gate {
                    let _ = rx.await;
                }
                record.lock().unwrap().push((label, seq));
                let _ = done_tx.send((label, seq));
                Ok(())
            })
        })
    };

    let reg = DataChunkRegistry::with_capacity(8);
    reg.register("s1".into(), make_cb('A'));

    // Gate seq 1 so handler A stays in flight until we release it.
    let (release1_tx, release1_rx) = oneshot::channel();
    gates.lock().unwrap().insert(1, release1_rx);

    reg.dispatch(chunk_seq("s1", 1), ActrId::default(), RELIABLE)
        .await;
    // A has begun seq 1 (blocked on its gate).
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), started_rx.recv())
            .await
            .expect("seq 1 never started")
            .unwrap(),
        ('A', 1)
    );

    // Replace the handler in place (no unregister first).
    reg.register("s1".into(), make_cb('B'));

    // seq 2 is dispatched after the swap; it must run under B, after seq 1.
    reg.dispatch(chunk_seq("s1", 2), ActrId::default(), RELIABLE)
        .await;
    // Nothing completed yet: seq 2 is queued behind the blocked seq 1.
    assert!(record.lock().unwrap().is_empty());

    release1_tx.send(()).unwrap();

    let first = tokio::time::timeout(Duration::from_secs(2), done_rx.recv())
        .await
        .expect("seq 1 never finished")
        .unwrap();
    let second = tokio::time::timeout(Duration::from_secs(2), done_rx.recv())
        .await
        .expect("seq 2 never finished")
        .unwrap();

    assert_eq!(first, ('A', 1), "seq 1 was in flight under the old handler");
    assert_eq!(
        second,
        ('B', 2),
        "seq 2 must run under the re-registered handler, in order"
    );
    assert_eq!(*record.lock().unwrap(), vec![('A', 1), ('B', 2)]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unregister_then_reregister_waits_for_draining_worker() {
    let reg = Arc::new(DataChunkRegistry::with_capacity(8));
    let record: Arc<Mutex<Vec<(char, u64)>>> = Arc::new(Mutex::new(Vec::new()));
    let gates: Arc<Mutex<HashMap<u64, oneshot::Receiver<()>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let (started_tx, mut started_rx) = tokio_mpsc::unbounded_channel::<(char, u64)>();
    let (done_tx, mut done_rx) = tokio_mpsc::unbounded_channel::<(char, u64)>();

    let make_cb = |label: char| -> DataChunkCallback {
        let record = record.clone();
        let gates = gates.clone();
        let started_tx = started_tx.clone();
        let done_tx = done_tx.clone();
        Arc::new(move |chunk: DataChunk, _sender| {
            let record = record.clone();
            let gates = gates.clone();
            let started_tx = started_tx.clone();
            let done_tx = done_tx.clone();
            Box::pin(async move {
                let seq = chunk.sequence;
                let _ = started_tx.send((label, seq));
                let gate = gates.lock().unwrap().remove(&seq);
                if let Some(rx) = gate {
                    let _ = rx.await;
                }
                record.lock().unwrap().push((label, seq));
                let _ = done_tx.send((label, seq));
                Ok(())
            })
        })
    };

    reg.register("s1".into(), make_cb('A'));
    let (release1_tx, release1_rx) = oneshot::channel();
    gates.lock().unwrap().insert(1, release1_rx);

    reg.dispatch(chunk_seq("s1", 1), ActrId::default(), RELIABLE)
        .await;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), started_rx.recv())
            .await
            .expect("seq 1 never started")
            .unwrap(),
        ('A', 1)
    );

    reg.unregister("s1");
    reg.register("s1".into(), make_cb('B'));

    let reg_dispatch = reg.clone();
    let dispatch2 = tokio::spawn(async move {
        reg_dispatch
            .dispatch(chunk_seq("s1", 2), ActrId::default(), RELIABLE)
            .await;
    });

    assert!(
        tokio::time::timeout(Duration::from_millis(200), started_rx.recv())
            .await
            .is_err(),
        "re-register created a second same-stream worker while the old worker was draining"
    );
    assert!(record.lock().unwrap().is_empty());

    release1_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(2), dispatch2)
        .await
        .expect("dispatch after re-register never completed")
        .expect("dispatch task panicked");

    let first = tokio::time::timeout(Duration::from_secs(2), done_rx.recv())
        .await
        .expect("seq 1 never finished")
        .unwrap();
    let second = tokio::time::timeout(Duration::from_secs(2), done_rx.recv())
        .await
        .expect("seq 2 never finished")
        .unwrap();

    assert_eq!(first, ('A', 1));
    assert_eq!(second, ('B', 2));
    assert_eq!(*record.lock().unwrap(), vec![('A', 1), ('B', 2)]);
}

/// (8) Dispatching to an unregistered stream only warns; it never panics.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dispatch_after_unregister_is_warn_only() {
    let reg = DataChunkRegistry::new();
    let (cb, _h, _probe) = Harness::new();
    reg.register("s1".into(), cb);
    reg.unregister("s1");

    // No callback registered -> warn + return, no panic, no worker spawned.
    reg.dispatch(chunk("s1"), ActrId::default(), RELIABLE).await;
    assert_eq!(reg.worker_len(), 0);
}
