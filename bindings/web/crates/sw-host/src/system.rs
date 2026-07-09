//! Service Worker System Module
//!
//! Service Worker-side ActorSystem implementation.
//! Responsible for the State Path: mailbox, scheduler, and actor execution.
//!
//! # Architecture
//!
//! ```text
//! DOM side
//!   RPC request
//!     v
//! ═══════════════════════════════════════════════════════
//! SW side
//!     v
//!   HostGate.send_request()
//!     v
//!   MessageHandler (installed by System)
//!     v
//!   ┌─────────────────────────────────────────────────┐
//!   │ System decides the target:                     │
//!   │ - Local actor?  -> Mailbox -> Scheduler -> Actor │
//!   │ - Remote actor? -> Gate -> Transport -> Remote   │
//!   └─────────────────────────────────────────────────┘
//!     v
//!   Response returns
//!     v
//!   HostGate.handle_response()
//!     v
//!   DOM side receives response
//! ```

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use actr_protocol::ActrId;
use bytes::Bytes;
use futures::channel::oneshot;
use wasm_bindgen::prelude::*;
use web_sys::MessagePort;

use crate::outbound::{Gate, HostGate, PeerGate};

/// Service Worker System
///
/// Central message-processing hub connecting the DOM side and remote actors.
///
/// Note: WASM/Service Worker runs in a single-threaded environment, so this uses
/// `Rc`/`RefCell` instead of `Arc`/`Mutex`.
pub struct System {
    /// HostGate handles requests from the DOM side.
    host_gate: Arc<HostGate>,

    /// Gate used for outbound routing.
    ///
    /// Peer route through a dedicated MessagePort plus the full transport stack.
    /// PeerGate → PeerTransport → DestTransport → WirePool → DataLane::PostMessage
    outgate: Rc<RefCell<Option<Gate>>>,

    /// DOM communication port.
    dom_port: Rc<RefCell<Option<MessagePort>>>,

    /// Local actor ID in client mode.
    local_actor_id: Rc<RefCell<Option<ActrId>>>,

    /// Pending requests used for response matching.
    pending_requests: Rc<RefCell<HashMap<String, oneshot::Sender<Bytes>>>>,
}

impl System {
    /// Create a new System.
    #[allow(clippy::arc_with_non_send_sync)]
    pub fn new() -> Self {
        let host_gate = Arc::new(HostGate::new());

        Self {
            host_gate,
            outgate: Rc::new(RefCell::new(None)),
            dom_port: Rc::new(RefCell::new(None)),
            local_actor_id: Rc::new(RefCell::new(None)),
            pending_requests: Rc::new(RefCell::new(HashMap::new())),
        }
    }

    /// Return the HostGate.
    pub fn host_gate(&self) -> &Arc<HostGate> {
        &self.host_gate
    }

    /// Set the unified outbound gate.
    pub fn set_outgate(&self, gate: Gate) {
        *self.outgate.borrow_mut() = Some(gate);
    }

    /// Set PeerGate via a convenience wrapper that converts it into `Gate::Peer`.
    pub fn set_peer_gate(&self, gate: Arc<PeerGate>) {
        self.set_outgate(Gate::peer(gate));
    }

    /// Return a clone of the current gate.
    pub fn outgate(&self) -> Option<Gate> {
        self.outgate.borrow().clone()
    }

    /// Set the DOM port.
    pub fn set_dom_port(&self, port: MessagePort) {
        *self.dom_port.borrow_mut() = Some(port);
    }

    /// Set the local actor ID.
    pub fn set_local_actor_id(&self, actor_id: ActrId) {
        *self.local_actor_id.borrow_mut() = Some(actor_id);
    }

    /// Register a pending request.
    pub fn register_pending_request(&self, request_id: String, sender: oneshot::Sender<Bytes>) {
        self.pending_requests
            .borrow_mut()
            .insert(request_id, sender);
    }

    /// Initialize the message handler.
    ///
    /// Installs the HostGate message handler and routes messages to the correct target:
    /// - Local actor -> TODO (Phase 2)
    /// - Remote actor -> `Gate.relay_envelope()`
    pub fn init_message_handler(&self) {
        let local_actor_id = Rc::clone(&self.local_actor_id);
        let outgate = Rc::clone(&self.outgate);
        let host_gate_for_reject = Arc::clone(&self.host_gate);

        self.host_gate
            .set_message_handler(move |target_id, envelope| {
                log::info!(
                    "[System] MessageHandler: routing request_id={} to target={:?}",
                    envelope.request_id,
                    target_id
                );

                let local_id = local_actor_id.borrow().clone();
                let gate = outgate.borrow().clone();
                let envelope = envelope.clone();
                let host_gate = Arc::clone(&host_gate_for_reject);

                wasm_bindgen_futures::spawn_local(async move {
                    // Decide whether this is a local or remote invocation.
                    let is_local = local_id
                        .as_ref()
                        .map(|id| id == &target_id)
                        .unwrap_or(false);

                    if is_local {
                        // In-SW local actor invocations are intentionally
                        // unsupported on the web target: every actor lives in
                        // its own browser tab / Service Worker pair, and
                        // "local" addressing collapses to "same actor calling
                        // itself" — which is a programming error. Reject loud
                        // through the host gate so the caller's `await` returns
                        // an error instead of hanging.
                        log::warn!(
                            "[System] Local actor invocation rejected (unsupported on web), request_id={}",
                            envelope.request_id
                        );
                        host_gate.reject_request(&envelope.request_id);
                    } else {
                        // Remote invocation: relay the HostGate-stamped
                        // envelope through the peer gate without changing
                        // its Request/Tell direction.
                        match gate {
                            Some(ref g) => {
                                if let Err(e) = g.relay_envelope(&target_id, envelope.clone()).await {
                                    log::error!("[System] Gate relay_envelope failed: {:?}", e);
                                    // Reject the pending HostGate oneshot so
                                    // send_request() returns an error instead of
                                    // hanging forever.
                                    host_gate.reject_request(&envelope.request_id);
                                }
                            }
                            None => {
                                log::error!("[System] Gate not set, cannot route remote message");
                                host_gate.reject_request(&envelope.request_id);
                            }
                        }
                    }
                });
            });
    }

    /// Handle a response from a remote target.
    ///
    /// Routing order:
    /// 1. Gate (DomBridge/Peer pending requests for actor-initiated calls)
    /// 2. System pending_requests
    /// 3. HostGate (for DOM-initiated calls)
    pub fn handle_remote_response(&self, request_id: &str, response: Bytes) {
        // 1. Try Gate first for actor-initiated call() responses.
        if let Some(ref gate) = *self.outgate.borrow() {
            if gate.try_handle_response(request_id, response.clone()) {
                return;
            }
        }

        // 2. Try System pending_requests.
        if let Some(tx) = self.pending_requests.borrow_mut().remove(request_id) {
            // Receiver alive, consumed; otherwise receiver dropped, fall through
            if let Ok(()) = tx.send(response.clone()) {
                return;
            }
        }

        // 3. Forward to HostGate for DOM-initiated calls.
        self.host_gate.handle_response(request_id, response);
    }

    /// Send a message to the DOM side.
    pub fn send_to_dom(&self, msg: &JsValue) -> Result<(), String> {
        let dom_port = self.dom_port.borrow();
        if let Some(ref port) = *dom_port {
            port.post_message(msg)
                .map_err(|e| format!("Failed to send to DOM: {:?}", e))?;
            Ok(())
        } else {
            Err("DOM port not set".to_string())
        }
    }
}

impl Default for System {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_system_creation() {
        let _system = System::new();
    }
}
