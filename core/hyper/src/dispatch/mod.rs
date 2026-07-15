//! In-memory dispatch scheduling layer (conflict-key routing + concurrency
//! budget + back-pressure).
//!
//! ## Where this sits — and how it differs from the SQLite mailbox
//!
//! The node has two very different "queues" and they must not be conflated:
//!
//! * The **persistent mailbox** ([`actr_runtime_mailbox`]) is the *durable*,
//!   at-least-once buffer that lives on the wire boundary. It survives process
//!   restarts, drives the reply-before-ack crash window, and provides transport
//!   back-pressure by holding messages on disk. It is upstream of everything
//!   here.
//!
//! * This **dispatch scheduler** is a purely *in-memory* layer that sits between
//!   the node entry loops (which dequeue from the durable mailbox) and the
//!   per-actor serial command runner ([`crate::executor`]). It never persists
//!   anything. Its only jobs are:
//!     1. project each inbound RPC to a [`conflict_key::ConflictKey`],
//!     2. keep same-key messages strictly FIFO / one-in-flight,
//!     3. let distinct-key messages run concurrently up to a budget `C`,
//!     4. apply a bounded queue `M` whose full state produces back-pressure that
//!        propagates up to the entry loop → the durable mailbox / SCTP flow
//!        control (design doc §8.3).
//!
//! The name deliberately avoids "mailbox" so the durable and in-memory layers
//! stay legible as distinct concepts.
//!
//! ## Default-on, serial-safe (strategy A)
//!
//! The gate now defaults **on** (`HyperConfig::dispatch_concurrency`,
//! `None` → [`crate::config::DispatchConcurrency::default`] with `enabled:
//! true`), but this is safe for the common case because of two independent nets:
//!
//! 1. **keyless zero-overhead** — the node engages this scheduler *only* when
//!    the gate is on **and** at least one conflict key is declared. A keyless
//!    actor (no declared key) is kept on the serial `run_loop` with no scheduler
//!    spawned at all — bit-for-bit the B1 serial runner, at zero cost, even with
//!    the gate on.
//! 2. **undeclared = global barrier** — when a scheduler *is* running (some
//!    method declared a key), every *undeclared* method still projects to the
//!    global [`conflict_key::ConflictKey::Serial`] barrier: at most one such
//!    dispatch in flight, in arrival order.
//!
//! Concurrency therefore only appears for methods a consumer explicitly declares
//! a conflict key for; everything else stays serial regardless of the gate.
//! Backend execution capability is a separate constraint: only native `Linked`
//! workloads and 0.2.0 async-world `Wasm(V2)` guests can multiplex dispatches.
//! A 0.1.0 sync-world `Wasm(V1)` guest or `DynClib` guest remains serial, so a
//! declared key is only a routing hint and yields no throughput increase there.
//!
//! ## Scope (B2)
//!
//! The scheduler only routes RPC **Dispatch** work. The Direction=Response
//! bypass (`gate.rs` pending_requests) and the DataChunk path
//! (`data_chunk_registry`) are untouched — DataChunk's per-stream serialization
//! is the `conflict_key = stream_id` special case, left unified for a later
//! milestone.

pub(crate) mod conflict_key;
pub(crate) mod scheduler;

pub use conflict_key::{
    ConflictKeyError, ConflictKeySpec, ConflictKeySpecBuilder, KeySource, PayloadFieldKind,
};

#[cfg(test)]
#[path = "conflict_key_tests.rs"]
mod conflict_key_tests;

#[cfg(test)]
#[path = "scheduler_tests.rs"]
mod scheduler_tests;
