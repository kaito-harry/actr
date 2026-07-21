//! DynClib-only C ABI for actr workloads.
//!
//! This module is the handwritten C ABI used by the DynClib workload variant
//! (loaded via dlopen). The WASM variant does NOT consume these types — it
//! goes through wit-bindgen-generated code against `core/framework/wit/actr-workload.wit`.
//!
//! Do NOT add wasm-path code paths here. Do NOT reference this module from
//! the wasm guest adapter. This module is kept in sync with the WIT contract
//! by `tools/wit-lint`.

use crate::Dest;
use actr_protocol::prost::Message as ProstMessage;
use actr_protocol::{ActrError, ActrId, ActrType, DataChunk, PayloadType};

/// ABI error codes.
pub mod code {
    /// Operation succeeded.
    pub const SUCCESS: i32 = 0;
    /// Generic unrecoverable error.
    pub const GENERIC_ERROR: i32 = -1;
    /// Initialization failed.
    pub const INIT_FAILED: i32 = -2;
    /// Message handling failed.
    pub const HANDLE_FAILED: i32 = -3;
    /// Memory allocation failed.
    pub const ALLOC_FAILED: i32 = -4;
    /// Protocol / codec error.
    pub const PROTOCOL_ERROR: i32 = -5;
    /// Guest-provided reply buffer is too small.
    pub const BUFFER_TOO_SMALL: i32 = -6;
    /// Unsupported runtime operation code.
    pub const UNSUPPORTED_OP: i32 = -7;
}

/// Internal ABI version numbers.
pub mod version {
    /// ABI version 1.
    pub const V1: u32 = 1;
}

/// Runtime operation codes carried inside [`AbiFrame`].
pub mod op {
    pub const HOST_CALL: u32 = 1;
    pub const HOST_TELL: u32 = 2;
    pub const HOST_CALL_RAW: u32 = 3;
    pub const HOST_DISCOVER: u32 = 4;
    pub const HOST_REGISTER_STREAM: u32 = 5;
    pub const HOST_UNREGISTER_STREAM: u32 = 6;
    pub const HOST_SEND_DATA_CHUNK: u32 = 7;
    pub const GUEST_HANDLE: u32 = 101;
    pub const GUEST_DATA_CHUNK: u32 = 102;
    pub const GUEST_LIFECYCLE: u32 = 103;
    pub const GUEST_HOOK: u32 = 104;
}

/// Lifecycle hook identifiers carried by [`GuestLifecycleV1`].
pub mod lifecycle_hook {
    pub const ON_START: u32 = 1;
    pub const ON_READY: u32 = 2;
    pub const ON_STOP: u32 = 3;
}

/// `WebRtcPeerStatus` discriminants carried by [`PeerEventV1::status`].
/// One-to-one with `actr_framework::WebRtcPeerStatus`; 0-based to mirror
/// the enum's declaration order. `optional` prost fields track presence, so
/// `Some(IDLE)` is distinct from `None`.
pub mod webrtc_peer_status {
    pub const IDLE: u32 = 0;
    pub const CONNECTING: u32 = 1;
    pub const CONNECTED: u32 = 2;
    pub const RECOVERING: u32 = 3;
}

/// Observation hook identifiers carried by [`GuestHookV1`].
pub mod runtime_hook {
    pub const ON_SIGNALING_CONNECTING: u32 = 1;
    pub const ON_SIGNALING_CONNECTED: u32 = 2;
    pub const ON_SIGNALING_DISCONNECTED: u32 = 3;
    pub const ON_WEBSOCKET_CONNECTING: u32 = 4;
    pub const ON_WEBSOCKET_CONNECTED: u32 = 5;
    pub const ON_WEBSOCKET_DISCONNECTED: u32 = 6;
    pub const ON_WEBRTC_CONNECTING: u32 = 7;
    pub const ON_WEBRTC_CONNECTED: u32 = 8;
    pub const ON_WEBRTC_DISCONNECTED: u32 = 9;
    pub const ON_CREDENTIAL_RENEWED: u32 = 10;
    pub const ON_CREDENTIAL_EXPIRING: u32 = 11;
    pub const ON_MAILBOX_BACKPRESSURE: u32 = 12;
}

/// Dedicated payload used by `actr_init`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct InitPayloadV1 {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(string, tag = "2")]
    pub actr_type: String,
    #[prost(bytes = "vec", tag = "3")]
    pub credential: Vec<u8>,
    #[prost(bytes = "vec", tag = "4")]
    pub actor_id: Vec<u8>,
    #[prost(uint32, tag = "5")]
    pub realm_id: u32,
}

/// Runtime frame used by both host->guest and guest->host invocation.
///
/// TODO: This type is temporarily `pub` because `actr_hyper` still performs
/// cross-crate host-side ABI encoding and decoding through
/// `actr_framework::guest::dynclib_abi`. Once the shared runtime ABI types
/// are moved to a better ownership boundary, narrow this visibility to the
/// intended internal-only surface.
#[derive(Clone, PartialEq, prost::Message)]
pub struct AbiFrame {
    #[prost(uint32, tag = "1")]
    pub abi_version: u32,
    #[prost(uint32, tag = "2")]
    pub op: u32,
    #[prost(bytes = "vec", tag = "3")]
    pub payload: Vec<u8>,
}

/// Runtime reply frame.
///
/// TODO: Keep visibility aligned with [`AbiFrame`]. This is public only as a
/// temporary crate-topology workaround while host-side runtime code lives in
/// `actr_hyper`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct AbiReply {
    #[prost(uint32, tag = "1")]
    pub abi_version: u32,
    #[prost(int32, tag = "2")]
    pub status: i32,
    #[prost(bytes = "vec", tag = "3")]
    pub payload: Vec<u8>,
}

/// Invocation context injected by Hyper before entering guest handle logic.
#[derive(Clone, PartialEq, prost::Message)]
pub struct InvocationContextV1 {
    #[prost(message, required, tag = "1")]
    pub self_id: ActrId,
    #[prost(message, optional, tag = "2")]
    pub caller_id: Option<ActrId>,
    #[prost(string, tag = "3")]
    pub request_id: String,
}

/// Runtime host->guest handle payload.
#[derive(Clone, PartialEq, prost::Message)]
pub struct GuestHandleV1 {
    #[prost(message, required, tag = "1")]
    pub ctx: InvocationContextV1,
    #[prost(bytes = "vec", tag = "2")]
    pub rpc_envelope: Vec<u8>,
    #[prost(uint64, tag = "3")]
    pub bridge_token: u64,
}

/// Runtime host->guest DataChunk payload.
#[derive(Clone, PartialEq, prost::Message)]
pub struct GuestDataChunkV1 {
    #[prost(message, required, tag = "1")]
    pub chunk: DataChunk,
    #[prost(message, required, tag = "2")]
    pub sender: ActrId,
    #[prost(uint64, tag = "3")]
    pub bridge_token: u64,
}

/// Runtime host->guest lifecycle hook payload.
#[derive(Clone, PartialEq, prost::Message)]
pub struct GuestLifecycleV1 {
    #[prost(message, required, tag = "1")]
    pub ctx: InvocationContextV1,
    #[prost(uint32, tag = "2")]
    pub hook: u32,
    #[prost(uint64, tag = "3")]
    pub bridge_token: u64,
}

/// Wall-clock timestamp represented as seconds + nanoseconds since Unix epoch.
#[derive(Clone, PartialEq, prost::Message)]
pub struct TimestampV1 {
    #[prost(uint64, tag = "1")]
    pub seconds: u64,
    #[prost(uint32, tag = "2")]
    pub nanoseconds: u32,
}

/// Peer-scoped event payload for WebSocket / WebRTC hooks.
#[derive(Clone, PartialEq, prost::Message)]
pub struct PeerEventV1 {
    #[prost(message, required, tag = "1")]
    pub peer: ActrId,
    #[prost(bool, optional, tag = "2")]
    pub relayed: Option<bool>,
    /// `WebRtcPeerStatus` discriminant (see [`webrtc_peer_status`]).
    /// `None` for WebSocket events.
    #[prost(uint32, optional, tag = "3")]
    pub status: Option<u32>,
}

/// Credential lifecycle event payload.
#[derive(Clone, PartialEq, prost::Message)]
pub struct CredentialEventV1 {
    #[prost(message, required, tag = "1")]
    pub new_expiry: TimestampV1,
}

/// Mailbox backpressure event payload.
#[derive(Clone, PartialEq, prost::Message)]
pub struct BackpressureEventV1 {
    #[prost(uint64, tag = "1")]
    pub queue_len: u64,
    #[prost(uint64, tag = "2")]
    pub threshold: u64,
}

/// Runtime host->guest observation hook payload.
#[derive(Clone, PartialEq, prost::Message)]
pub struct GuestHookV1 {
    #[prost(message, required, tag = "1")]
    pub ctx: InvocationContextV1,
    #[prost(uint32, tag = "2")]
    pub hook: u32,
    #[prost(message, optional, tag = "3")]
    pub peer: Option<PeerEventV1>,
    #[prost(message, optional, tag = "4")]
    pub credential: Option<CredentialEventV1>,
    #[prost(message, optional, tag = "5")]
    pub backpressure: Option<BackpressureEventV1>,
    #[prost(uint64, tag = "6")]
    pub bridge_token: u64,
}

/// ABI-level destination encoding (replaces hand-rolled 0x00/0x01/0x02 byte protocol).
#[derive(Clone, PartialEq, prost::Message)]
pub struct DestV1 {
    #[prost(oneof = "DestKind", tags = "1, 2, 3")]
    pub kind: Option<DestKind>,
}

/// Destination variants carried inside [`DestV1`].
#[derive(Clone, PartialEq, prost::Oneof)]
pub enum DestKind {
    #[prost(bool, tag = "1")]
    Host(bool),
    #[prost(bool, tag = "2")]
    Workload(bool),
    #[prost(message, tag = "3")]
    Peer(ActrId),
}

impl DestV1 {
    /// Construct a host destination.
    pub fn host() -> Self {
        Self {
            kind: Some(DestKind::Host(true)),
        }
    }

    /// Construct a workload destination.
    pub fn workload() -> Self {
        Self {
            kind: Some(DestKind::Workload(true)),
        }
    }

    /// Construct a peer destination.
    pub fn peer(id: ActrId) -> Self {
        Self {
            kind: Some(DestKind::Peer(id)),
        }
    }

    /// Convert the ABI destination into the framework destination.
    pub fn try_into_dest(self) -> Result<Dest, ActrError> {
        match self.kind {
            Some(DestKind::Host(_)) => Ok(Dest::Host),
            Some(DestKind::Workload(_)) => Ok(Dest::Workload),
            Some(DestKind::Peer(id)) => Ok(Dest::Peer(id)),
            None => Err(ActrError::DecodeFailure(
                "destination kind is missing".into(),
            )),
        }
    }
}

/// Runtime guest->host call payload.
#[derive(Clone, PartialEq, prost::Message)]
pub struct HostCallV1 {
    #[prost(string, tag = "1")]
    pub route_key: String,
    #[prost(message, required, tag = "2")]
    pub dest: DestV1,
    #[prost(bytes = "vec", tag = "3")]
    pub payload: Vec<u8>,
}

/// Runtime guest->host tell payload.
#[derive(Clone, PartialEq, prost::Message)]
pub struct HostTellV1 {
    #[prost(string, tag = "1")]
    pub route_key: String,
    #[prost(message, required, tag = "2")]
    pub dest: DestV1,
    #[prost(bytes = "vec", tag = "3")]
    pub payload: Vec<u8>,
}

/// Runtime guest->host raw call payload.
#[derive(Clone, PartialEq, prost::Message)]
pub struct HostCallRawV1 {
    #[prost(string, tag = "1")]
    pub route_key: String,
    #[prost(message, required, tag = "2")]
    pub target: ActrId,
    #[prost(bytes = "vec", tag = "3")]
    pub payload: Vec<u8>,
}

/// Runtime guest->host discovery payload.
#[derive(Clone, PartialEq, prost::Message)]
pub struct HostDiscoverV1 {
    #[prost(message, required, tag = "1")]
    pub target_type: ActrType,
}

/// Runtime guest->host DataChunk registration payload.
#[derive(Clone, PartialEq, prost::Message)]
pub struct HostRegisterStreamV1 {
    #[prost(string, tag = "1")]
    pub stream_id: String,
}

/// Runtime guest->host DataChunk unregistration payload.
#[derive(Clone, PartialEq, prost::Message)]
pub struct HostUnregisterStreamV1 {
    #[prost(string, tag = "1")]
    pub stream_id: String,
}

/// Runtime guest->host DataChunk send payload.
#[derive(Clone, PartialEq, prost::Message)]
pub struct HostSendDataChunkV1 {
    #[prost(message, required, tag = "1")]
    pub dest: DestV1,
    #[prost(message, required, tag = "2")]
    pub chunk: DataChunk,
    #[prost(enumeration = "PayloadType", tag = "3")]
    pub payload_type: i32,
}

/// Payloads that can automatically construct runtime frames.
pub trait AbiPayload: ProstMessage + Default + Sized {
    const ABI_VERSION: u32;
    const OP: u32;

    fn to_frame(&self) -> Result<AbiFrame, i32> {
        let mut payload = Vec::new();
        self.encode(&mut payload)
            .map_err(|_| code::PROTOCOL_ERROR)?;

        Ok(AbiFrame {
            abi_version: Self::ABI_VERSION,
            op: Self::OP,
            payload,
        })
    }

    fn decode_payload(bytes: &[u8]) -> Result<Self, i32> {
        Self::decode(bytes).map_err(|_| code::PROTOCOL_ERROR)
    }
}

impl AbiPayload for HostCallV1 {
    const ABI_VERSION: u32 = version::V1;
    const OP: u32 = op::HOST_CALL;
}

impl AbiPayload for HostTellV1 {
    const ABI_VERSION: u32 = version::V1;
    const OP: u32 = op::HOST_TELL;
}

impl AbiPayload for HostCallRawV1 {
    const ABI_VERSION: u32 = version::V1;
    const OP: u32 = op::HOST_CALL_RAW;
}

impl AbiPayload for HostDiscoverV1 {
    const ABI_VERSION: u32 = version::V1;
    const OP: u32 = op::HOST_DISCOVER;
}

impl AbiPayload for HostRegisterStreamV1 {
    const ABI_VERSION: u32 = version::V1;
    const OP: u32 = op::HOST_REGISTER_STREAM;
}

impl AbiPayload for HostUnregisterStreamV1 {
    const ABI_VERSION: u32 = version::V1;
    const OP: u32 = op::HOST_UNREGISTER_STREAM;
}

impl AbiPayload for HostSendDataChunkV1 {
    const ABI_VERSION: u32 = version::V1;
    const OP: u32 = op::HOST_SEND_DATA_CHUNK;
}

impl AbiPayload for GuestHandleV1 {
    const ABI_VERSION: u32 = version::V1;
    const OP: u32 = op::GUEST_HANDLE;
}

impl AbiPayload for GuestDataChunkV1 {
    const ABI_VERSION: u32 = version::V1;
    const OP: u32 = op::GUEST_DATA_CHUNK;
}

impl AbiPayload for GuestLifecycleV1 {
    const ABI_VERSION: u32 = version::V1;
    const OP: u32 = op::GUEST_LIFECYCLE;
}

impl AbiPayload for GuestHookV1 {
    const ABI_VERSION: u32 = version::V1;
    const OP: u32 = op::GUEST_HOOK;
}

/// Encode a protobuf message into bytes.
pub fn encode_message<M: ProstMessage>(message: &M) -> Result<Vec<u8>, i32> {
    let mut out = Vec::new();
    message.encode(&mut out).map_err(|_| code::PROTOCOL_ERROR)?;
    Ok(out)
}

/// Decode a protobuf message from bytes.
pub fn decode_message<M: ProstMessage + Default>(bytes: &[u8]) -> Result<M, i32> {
    M::decode(bytes).map_err(|_| code::PROTOCOL_ERROR)
}

/// Encode a successful runtime reply.
pub fn success_reply(payload: Vec<u8>) -> Result<Vec<u8>, i32> {
    encode_message(&AbiReply {
        abi_version: version::V1,
        status: code::SUCCESS,
        payload,
    })
}

/// Encode a failed runtime reply.
pub fn error_reply(status: i32, message: impl Into<Vec<u8>>) -> Result<Vec<u8>, i32> {
    encode_message(&AbiReply {
        abi_version: version::V1,
        status,
        payload: message.into(),
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// Shared guest-side helpers (used by both WASM and DynClib contexts)
// ─────────────────────────────────────────────────────────────────────────────

/// Convert a [`crate::Dest`] to the ABI-level [`DestV1`].
pub fn dest_to_v1(dest: &crate::Dest) -> DestV1 {
    match dest {
        crate::Dest::Host => DestV1::host(),
        crate::Dest::Workload => DestV1::workload(),
        crate::Dest::Peer(id) => DestV1::peer(id.clone()),
    }
}

/// Convert an ABI-level [`DestV1`] back to [`crate::Dest`].
///
/// Returns `None` if the `kind` field is absent.
pub fn dest_v1_to_dest(v1: &DestV1) -> Option<crate::Dest> {
    v1.clone().try_into_dest().ok()
}

/// Convert an ABI error code to an [`actr_protocol::ActrError`].
pub fn abi_error_to_actr(code_val: i32) -> actr_protocol::ActrError {
    use actr_protocol::ActrError;
    match code_val {
        code::GENERIC_ERROR => ActrError::Internal("host returned generic ABI error".into()),
        code::INIT_FAILED => ActrError::Internal("host initialization failed".into()),
        code::HANDLE_FAILED => ActrError::Internal("guest handle failed".into()),
        code::ALLOC_FAILED => ActrError::Internal("memory allocation failed".into()),
        code::PROTOCOL_ERROR => ActrError::DecodeFailure("ABI payload decode failed".into()),
        code::BUFFER_TOO_SMALL => {
            ActrError::Internal("reply buffer too small for host invoke".into())
        }
        code::UNSUPPORTED_OP => ActrError::NotImplemented("unsupported ABI operation".into()),
        other => ActrError::Internal(format!("unexpected ABI status code {other}")),
    }
}

/// Convert an [`AbiReply`] with a non-success status to an [`actr_protocol::ActrError`].
pub fn reply_to_actr_error(reply: AbiReply) -> actr_protocol::ActrError {
    use actr_protocol::ActrError;
    if reply.payload.is_empty() {
        return abi_error_to_actr(reply.status);
    }

    let message = String::from_utf8(reply.payload)
        .unwrap_or_else(|_| format!("guest returned status {}", reply.status));

    match reply.status {
        code::PROTOCOL_ERROR => ActrError::DecodeFailure(message),
        code::UNSUPPORTED_OP => ActrError::NotImplemented(message),
        _ => ActrError::Internal(message),
    }
}
