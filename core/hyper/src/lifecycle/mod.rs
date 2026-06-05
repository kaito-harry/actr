//! Lifecycle management layer (non-architectural layer)
//!
//! Responsible for Actor system lifecycle management:
//! - `node::Inner`: internal running-state struct used by `Node<S>` / `ActrRef`.

pub(crate) mod compat_lock;
mod connection_supervisor;
pub(crate) mod dedup;
mod heartbeat;
pub(crate) mod hooks;
mod network_event;
pub(crate) mod node;

pub use connection_supervisor::{ConnectionFact, ConnectionSupervisor};
pub use network_event::{
    AppLifecycleState, CleanupReason, DebounceConfig, DefaultNetworkEventProcessor,
    NetworkAvailability, NetworkEvent, NetworkEventHandle, NetworkEventProcessor,
    NetworkEventRequest, NetworkEventResult, NetworkRecoveryAction, NetworkSnapshot,
    NetworkTransportFlags, ReconnectReason, process_network_event_batch,
    run_network_event_reconciler, select_network_recovery_action,
};
pub use node::CredentialState;
