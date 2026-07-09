//! Destination identifier for Actor communication
//!
//! Defines the `Dest` enum with three-way destination distinction:
//! - **Host**: this node's host side (the process embedding the workload)
//! - **Workload**: this node's workload side (self when sent from the workload)
//! - **Peer**: a remote node, addressed by `ActrId`
//!
//! # Design Rationale
//!
//! All three variants answer the same question — *which role receives the
//! message* — relative to the sending node:
//! - **Consistent axis**: Host / Workload / Peer are role nouns; `Peer` makes
//!   "crosses the network" visible in the name
//! - **Symmetric communication**: Host ↔ Workload bidirectional calls
//! - **Protocol consistency**: Workload self-calls use full serialization (same as remote)
//! - **Transparent optimization**: Transport layer can short-circuit Workload calls
//!
//! # Usage
//!
//! **Host side (embedding process)**:
//! ```rust,ignore
//! // Call local Workload (implies Dest::Workload)
//! running_node.call(request).await?;  // No target parameter
//! ```
//!
//! **Workload side**:
//! ```rust,ignore
//! // Call the host side
//! ctx.call(&Dest::Host, request).await?;
//!
//! // Call self (outproc short-circuit)
//! ctx.call(&Dest::Workload, request).await?;
//!
//! // Call a remote peer
//! ctx.call(&Dest::Peer(server_id), request).await?;
//! ```
//!
//! # Placement in actr-framework
//!
//! `Dest` is placed in `actr-framework` (not `actr-protocol`) because:
//! - It's an API-level abstraction, not a protocol data type
//! - It's used directly by the `Context` trait
//! - `RpcEnvelope` (in protocol layer) does not contain destination information
//! - The runtime layer implements the routing logic based on `Dest`

use actr_protocol::ActrId;

/// Destination identifier
///
/// Three-way destination for message routing.
///
/// # Semantics
///
/// - **`Dest::Host`**: Workload -> host side (inproc reverse channel)
///   - Used by the Workload to call the process embedding it
///   - Routed through `HostGate` (zero serialization)
///   - Example: Workload pushing notifications to the host application
///
/// - **`Dest::Workload`**: Target this node's Workload
///   - From the host side: routed through `HostGate` (zero serialization)
///   - From the Workload: routed through `PeerGate` (full serialization, short-circuit at transport)
///   - Example: host calling its local Workload, or Workload calling itself
///
/// - **`Dest::Peer(ActrId)`**: Remote node (full outproc)
///   - Used for cross-process Actor communication
///   - Routed through `PeerGate` (WebRTC/WebSocket)
///   - Example: ClientWorkload calling RemoteServer
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Dest {
    /// This node's host side - Workload calls the embedding process (inproc reverse channel)
    Host,

    /// This node's Workload - self when sent from the workload (from host: inproc, from Workload: outproc short-circuit)
    Workload,

    /// Remote node - cross-process communication (WebRTC/WebSocket)
    Peer(ActrId),
}

impl Dest {
    /// Create a Host destination
    #[inline]
    pub fn host() -> Self {
        Dest::Host
    }

    /// Create a Workload destination
    #[inline]
    pub fn workload() -> Self {
        Dest::Workload
    }

    /// Create a Peer destination
    #[inline]
    pub fn peer(id: ActrId) -> Self {
        Dest::Peer(id)
    }

    /// Check if this is a Host destination
    #[inline]
    pub fn is_host(&self) -> bool {
        matches!(self, Dest::Host)
    }

    /// Check if this is a Workload destination
    #[inline]
    pub fn is_workload(&self) -> bool {
        matches!(self, Dest::Workload)
    }

    /// Check if this is a Peer destination
    #[inline]
    pub fn is_peer(&self) -> bool {
        matches!(self, Dest::Peer(_))
    }

    /// Get the peer's ActrId if this is a Peer destination
    #[inline]
    pub fn as_peer_id(&self) -> Option<&ActrId> {
        match self {
            Dest::Peer(id) => Some(id),
            _ => None,
        }
    }
}

impl From<ActrId> for Dest {
    #[inline]
    fn from(id: ActrId) -> Self {
        Dest::Peer(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dest_creation() {
        let host_dest = Dest::host();
        assert!(host_dest.is_host());
        assert!(!host_dest.is_workload());
        assert!(!host_dest.is_peer());

        let workload_dest = Dest::workload();
        assert!(!workload_dest.is_host());
        assert!(workload_dest.is_workload());
        assert!(!workload_dest.is_peer());

        let id = ActrId::default();
        let peer_dest = Dest::peer(id);
        assert!(!peer_dest.is_host());
        assert!(!peer_dest.is_workload());
        assert!(peer_dest.is_peer());
    }

    #[test]
    fn test_dest_hash() {
        use std::collections::HashMap;

        let id1 = ActrId::default();
        let id2 = ActrId {
            serial_number: 1,
            ..Default::default()
        };

        let mut map = HashMap::new();
        map.insert(Dest::host(), "host");
        map.insert(Dest::workload(), "workload");
        map.insert(Dest::peer(id1), "peer1");
        map.insert(Dest::peer(id2), "peer2");

        assert_eq!(map.len(), 4);
    }

    #[test]
    fn test_dest_as_peer_id() {
        let host_dest = Dest::host();
        assert_eq!(host_dest.as_peer_id(), None);

        let workload_dest = Dest::workload();
        assert_eq!(workload_dest.as_peer_id(), None);

        let id = ActrId {
            serial_number: 7,
            ..Default::default()
        };
        let peer_dest = Dest::peer(id.clone());
        assert_eq!(peer_dest.as_peer_id(), Some(&id));
    }

    #[test]
    fn test_dest_from_actr_id() {
        let id = ActrId::default();
        let dest: Dest = id.clone().into();
        assert_eq!(dest, Dest::Peer(id));
    }
}
