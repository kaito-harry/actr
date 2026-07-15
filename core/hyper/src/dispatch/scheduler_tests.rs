//! Scheduler behaviour tests. All synchronization is via gate channels — never
//! sleeps — so ordering assertions are deterministic.

use super::conflict_key::ConflictKey;
use super::scheduler::{DispatchFn, SchedulerHandle};
use actr_protocol::ActorResult;
use bytes::Bytes;
use futures_util::poll;
use std::sync::Arc;
use std::task::Poll;
use tokio::sync::oneshot;

fn scoped(domain: &str, value: &[u8]) -> ConflictKey {
    ConflictKey::Scoped {
        domain: Arc::from(domain),
        value: Bytes::copy_from_slice(value),
    }
}

/// A gated job: `run` signals `started` when it begins and blocks until
/// `release` fires, then returns `Ok(value)`.
struct Gate {
    run: DispatchFn,
    started: oneshot::Receiver<()>,
    release: oneshot::Sender<()>,
}

fn gated(value: &'static [u8]) -> Gate {
    let (started_tx, started_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let run: DispatchFn = Box::new(move || {
        Box::pin(async move {
            let _ = started_tx.send(());
            let _ = release_rx.await;
            ActorResult::Ok(Bytes::from_static(value))
        })
    });
    Gate {
        run,
        started: started_rx,
        release: release_tx,
    }
}

fn not_started(rx: &mut oneshot::Receiver<()>) -> bool {
    matches!(rx.try_recv(), Err(oneshot::error::TryRecvError::Empty))
}

#[tokio::test]
async fn same_key_is_strict_fifo() {
    let sched = SchedulerHandle::spawn(8, 64);
    let key = scoped("d", b"same");

    let j1 = gated(b"1");
    let mut j2 = gated(b"2");
    let rx1 = sched.submit(key.clone(), j1.run).await;
    let rx2 = sched.submit(key.clone(), j2.run).await;

    // j1 starts; j2 must wait behind it.
    j1.started.await.unwrap();
    assert!(
        not_started(&mut j2.started),
        "j2 must not start until j1 completes"
    );

    j1.release.send(()).unwrap();
    // now j2 may start and both complete in order.
    j2.started.await.unwrap();
    j2.release.send(()).unwrap();
    assert_eq!(rx1.await.unwrap().unwrap(), Bytes::from_static(b"1"));
    assert_eq!(rx2.await.unwrap().unwrap(), Bytes::from_static(b"2"));
}

#[tokio::test]
async fn distinct_keys_interleave() {
    let sched = SchedulerHandle::spawn(8, 64);

    let a = gated(b"a");
    let b = gated(b"b");
    let rx_a = sched.submit(scoped("d", b"a"), a.run).await;
    let rx_b = sched.submit(scoped("d", b"b"), b.run).await;

    // Both run concurrently.
    a.started.await.unwrap();
    b.started.await.unwrap();

    // B completes while A is still blocked — interleaving evidence, no wall clock.
    b.release.send(()).unwrap();
    assert_eq!(rx_b.await.unwrap().unwrap(), Bytes::from_static(b"b"));

    a.release.send(()).unwrap();
    assert_eq!(rx_a.await.unwrap().unwrap(), Bytes::from_static(b"a"));
}

#[tokio::test]
async fn budget_caps_in_flight() {
    let sched = SchedulerHandle::spawn(2, 64); // C = 2
    assert_eq!(sched.budget(), 2);

    let j1 = gated(b"1");
    let j2 = gated(b"2");
    let mut j3 = gated(b"3");
    let rx1 = sched.submit(scoped("d", b"k1"), j1.run).await;
    let _rx2 = sched.submit(scoped("d", b"k2"), j2.run).await;
    let _rx3 = sched.submit(scoped("d", b"k3"), j3.run).await;

    j1.started.await.unwrap();
    j2.started.await.unwrap();
    assert!(
        not_started(&mut j3.started),
        "budget 2 must hold the third job"
    );

    j1.release.send(()).unwrap();
    rx1.await.unwrap().unwrap();
    // A slot freed → j3 starts.
    j3.started.await.unwrap();
}

#[tokio::test]
async fn serial_key_is_a_global_barrier() {
    let sched = SchedulerHandle::spawn(8, 64);

    let scoped_job = gated(b"s");
    let rx_scoped = sched.submit(scoped("d", b"x"), scoped_job.run).await;
    scoped_job.started.await.unwrap();

    // A Serial job cannot start while a scoped job is in flight.
    let mut serial_job = gated(b"root");
    let rx_serial = sched.submit(ConflictKey::Serial, serial_job.run).await;
    assert!(
        not_started(&mut serial_job.started),
        "serial must wait for the in-flight scoped job"
    );

    // While the serial job is pending, a fresh scoped job must not start either
    // (anti-starvation: serial gets priority once nothing is in flight).
    let mut scoped_after = gated(b"y");
    let _rx_after = sched.submit(scoped("d", b"y"), scoped_after.run).await;
    assert!(
        not_started(&mut scoped_after.started),
        "serial-pending blocks new scoped starts"
    );

    // Drain the in-flight scoped job → serial runs alone.
    scoped_job.release.send(()).unwrap();
    rx_scoped.await.unwrap().unwrap();
    serial_job.started.await.unwrap();
    assert!(
        not_started(&mut scoped_after.started),
        "scoped stays blocked while serial runs"
    );

    serial_job.release.send(()).unwrap();
    rx_serial.await.unwrap().unwrap();
    // After the barrier, the queued scoped job runs.
    scoped_after.started.await.unwrap();
    scoped_after.release.send(()).unwrap();
}

#[tokio::test]
async fn queue_cap_applies_back_pressure() {
    let sched = SchedulerHandle::spawn(2, 2); // C = M = 2

    let j1 = gated(b"1");
    let j2 = gated(b"2");
    let rx1 = sched.submit(scoped("d", b"k1"), j1.run).await;
    let _rx2 = sched.submit(scoped("d", b"k2"), j2.run).await;
    j1.started.await.unwrap();

    // Both permits are held; a third submit must block on the semaphore.
    let j3 = gated(b"3");
    let mut submit3 = Box::pin(sched.submit(scoped("d", b"k3"), j3.run));
    assert!(
        matches!(poll!(submit3.as_mut()), Poll::Pending),
        "submit must block when the queue is full (back-pressure)"
    );

    // Complete j1 → a permit frees → the pending submit resolves.
    j1.release.send(()).unwrap();
    rx1.await.unwrap().unwrap();
    let _rx3 = submit3.await;
}

#[tokio::test]
async fn shutdown_admits_a_preexisting_submit_blocked_on_queue_capacity() {
    let sched = SchedulerHandle::spawn(1, 1);
    let first = gated(b"first");
    let second = gated(b"second");
    let first_rx = sched.submit(scoped("d", b"first"), first.run).await;
    first.started.await.unwrap();

    // Poll submission far enough to acquire the admission read guard and block
    // on M. Graceful shutdown's write guard must wait for this submitter.
    let mut second_submit = Box::pin(sched.submit(scoped("d", b"second"), second.run));
    assert!(matches!(poll!(second_submit.as_mut()), Poll::Pending));
    let mut shutdown = Box::pin(sched.shutdown());
    assert!(
        matches!(poll!(shutdown.as_mut()), Poll::Pending),
        "shutdown must wait for the preexisting blocked submit"
    );

    first.release.send(()).unwrap();
    assert!(first_rx.await.unwrap().is_ok());
    let second_rx = second_submit.await;
    second.started.await.unwrap();
    second.release.send(()).unwrap();
    assert!(second_rx.await.unwrap().is_ok());
    shutdown.await;
}

#[tokio::test]
async fn shutdown_drains_every_admitted_job() {
    let sched = Arc::new(SchedulerHandle::spawn(8, 64));

    // A1 in flight, A2 queued behind it (same key). Same for B.
    let a1 = gated(b"a1");
    let a2 = gated(b"a2");
    let b1 = gated(b"b1");
    let b2 = gated(b"b2");
    let rx_a1 = sched.submit(scoped("d", b"A"), a1.run).await;
    let rx_a2 = sched.submit(scoped("d", b"A"), a2.run).await;
    let rx_b1 = sched.submit(scoped("d", b"B"), b1.run).await;
    let rx_b2 = sched.submit(scoped("d", b"B"), b2.run).await;

    a1.started.await.unwrap();
    b1.started.await.unwrap();

    // Shut down: closes intake, then losslessly drains queued + in-flight jobs.
    let sched2 = sched.clone();
    let shutdown = tokio::spawn(async move { sched2.shutdown().await });

    // In-flight jobs finish once released, then the already-admitted same-key
    // jobs must run rather than being discarded during graceful teardown.
    a1.release.send(()).unwrap();
    b1.release.send(()).unwrap();
    assert!(rx_a1.await.unwrap().is_ok());
    assert!(rx_b1.await.unwrap().is_ok());

    a2.started.await.unwrap();
    b2.started.await.unwrap();
    a2.release.send(()).unwrap();
    b2.release.send(()).unwrap();
    assert!(rx_a2.await.unwrap().is_ok());
    assert!(rx_b2.await.unwrap().is_ok());

    shutdown.await.unwrap();

    // The write-side admission close rejects work submitted after shutdown.
    let late = gated(b"late");
    let late_rx = sched.submit(scoped("d", b"late"), late.run).await;
    assert!(
        matches!(
            late_rx.await,
            Ok(Err(actr_protocol::ActrError::Unavailable(_)))
        ),
        "post-shutdown work must be rejected"
    );
}

#[tokio::test]
async fn dropping_scheduler_aborts_inflight_jobs() {
    let sched = SchedulerHandle::spawn(1, 8);
    let job = gated(b"stuck");
    let reply = sched.submit(scoped("d", b"stuck"), job.run).await;
    job.started.await.unwrap();

    drop(sched);

    assert!(
        reply.await.is_err(),
        "forced-drop fallback must abort the scheduler task and close replies"
    );
}
