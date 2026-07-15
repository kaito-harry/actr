//! Conflict-key projection: the one user-visible API surface B2 adds.
//!
//! A [`ConflictKeySpec`] is *register-once* data (not a per-message closure): a
//! small table mapping a method's `route_key` ("package.Service.Method") to a
//! rule describing which few bytes of the request project to its conflict key.
//! It is consumer-local — it never serializes, never rides the wire, never
//! enters `proto`. The producer's fields are merely projection *inputs*; the
//! authority over "what conflicts with what" lives entirely on the consumer.
//!
//! Two messages with equal [`ConflictKey`] are serialized (same-key FIFO);
//! distinct keys may run concurrently (subject to the scheduler's budget).
//!
//! ## Safety defaults (design doc §4.1)
//!
//! * **Undeclared method → [`ConflictKey::Serial`]**, which is a *global*
//!   barrier: it does not interleave with anything, declared or not. A consumer
//!   only asserts interleaving between methods it explicitly declared; every
//!   other method must stay "serial relative to everything" to preserve the safe
//!   default.
//! * **Domain defaults to the method's own `route_key`** (method-private). To
//!   let two methods share a conflict domain (e.g. `Update` and `Delete` both
//!   keyed by `document_id`) the consumer must say so explicitly with
//!   [`ConflictKeySpecBuilder::method_in_domain`] — making the "knowingly shared
//!   domain" of §4.1 a visible line of code.
//! * **Extraction failure → [`ConflictKey::Serial`]** (missing field, an
//!   unsupported wire type, a repeated field). Always falls back to *more*
//!   serialization, never less.

use actr_protocol::ActrId;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Upper bound for attacker-controlled bytes retained in one scheduler key.
///
/// Conflict keys are meant to be compact identifiers. Falling back to the
/// global serial lane for larger values preserves correctness while preventing
/// a full scheduler queue from duplicating arbitrarily large request fields.
const MAX_CONFLICT_KEY_BYTES: usize = 4 * 1024;

/// Where a method's conflict key is projected from.
///
/// `#[non_exhaustive]`: future key sources (chunk headers, composite keys)
/// must not break existing matches. `stream_id` is intentionally *not* here —
/// the DataChunk path already serializes per stream and is left unified for a
/// later milestone rather than rewritten in B2.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum KeySource {
    /// Key = the transport-authenticated sender's [`ActrId`] (the caller id,
    /// taken from `MessageRecord.from` — [`actr_protocol::RpcEnvelope`] itself
    /// carries no sender field). A missing caller (in-proc shell lane) projects
    /// to a fixed empty value *within the domain*, i.e. all callerless messages
    /// on that method share one serial lane.
    Sender,
    /// Key = the schema-aware canonical value of the top-level protobuf field
    /// with this `tag` in the request payload. `kind` must match the field's
    /// declared protobuf scalar type; carrying it explicitly prevents distinct
    /// wire values that decode to the same logical value (for example `bool`
    /// values `1` and `2`, or out-of-range 32-bit varints) from escaping
    /// same-key serialization. A missing field, type mismatch, invalid scalar,
    /// unsupported wire type (groups), repeated field, or oversized value
    /// falls back to [`ConflictKey::Serial`] with a rate-limited warning.
    PayloadField { tag: u32, kind: PayloadFieldKind },
}

/// Protobuf scalar type used by a [`KeySource::PayloadField`] projection.
///
/// The type is part of the consumer's conflict declaration rather than inferred
/// from the wire type: protobuf deliberately shares one wire representation
/// between several scalar types whose decoded equality differs. Floating-point
/// and embedded-message fields are intentionally unsupported as conflict keys;
/// prefer a stable integer, string, or bytes identifier instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PayloadFieldKind {
    /// Protobuf `bool` (only canonical wire values 0 and 1 are accepted).
    Bool,
    /// Protobuf `int32`.
    Int32,
    /// Protobuf `int64`.
    Int64,
    /// Protobuf `uint32`.
    Uint32,
    /// Protobuf `uint64`.
    Uint64,
    /// Zig-zag encoded protobuf `sint32`.
    Sint32,
    /// Zig-zag encoded protobuf `sint64`.
    Sint64,
    /// Protobuf `fixed32`.
    Fixed32,
    /// Protobuf `fixed64`.
    Fixed64,
    /// Protobuf `sfixed32`.
    Sfixed32,
    /// Protobuf `sfixed64`.
    Sfixed64,
    /// Protobuf enum numeric value.
    Enum,
    /// UTF-8 protobuf `string`.
    String,
    /// Protobuf `bytes`.
    Bytes,
}

/// Per-method conflict-key rule: a source plus an optional explicit domain.
#[derive(Debug, Clone)]
struct KeyRule {
    source: KeySource,
    /// Conflict domain; keys only compare within the same domain. `None` =
    /// the method's own `route_key` (method-private domain). `Some(name)` =
    /// a shared domain across methods that name it identically.
    domain: Option<String>,
}

/// Errors from building a [`ConflictKeySpec`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConflictKeyError {
    /// A `route_key` was registered more than once — a programming error.
    #[error("duplicate conflict-key registration for route_key `{0}`")]
    DuplicateRoute(String),
}

/// Register-once conflict-key projection table, attached to a node before it
/// links / attaches its workload via `Node::<Init>::with_conflict_keys`.
///
/// # Concurrency contract (read before enabling)
///
/// Turning on dispatch concurrency and declaring a key for a method asserts
/// that **distinct-key invocations of that method may run concurrently**. For a
/// native `Linked` workload this means the handler's `&self` body for two
/// different keys can be in flight at the same time; any state it shares across
/// an `.await` must be synchronized by the handler. The framework cannot verify
/// this — it is the consumer's contract (design doc §6).
///
/// A **0.2.0 (async-world) WASM** guest is now isomorphic to the native case
/// (M5): distinct-key dispatches run inside one resident `run_concurrent`
/// region and interleave at their host-import `.await` points, sharing the one
/// instance's linear memory exactly as a native handler shares `&self`. The
/// same contract applies — any guest state touched across an `.await` must
/// tolerate a sibling distinct-key dispatch observing it mid-flight. A 0.1.0
/// (sync-world) WASM guest and a DynClib guest stay serial regardless of the
/// gate or budget (single `Store` / `&mut` ABI). For those backends, declaring
/// a key only affects scheduler routing; it does not overlap guest dispatches
/// and therefore provides no dispatch-throughput benefit.
#[derive(Debug, Clone, Default)]
pub struct ConflictKeySpec {
    rules: HashMap<String, KeyRule>,
}

impl ConflictKeySpec {
    /// Start building a spec.
    pub fn builder() -> ConflictKeySpecBuilder {
        ConflictKeySpecBuilder::default()
    }

    /// `true` when no route declares a conflict key. A keyless spec means every
    /// dispatch projects to the global [`ConflictKey::Serial`] barrier, so
    /// concurrency can never appear — the node uses this to keep a keyless actor
    /// on the serial `run_loop` with no scheduler (strategy A zero-overhead).
    pub(crate) fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    /// Project one inbound RPC to its [`ConflictKey`].
    ///
    /// `payload` is the request payload bytes (the `RpcEnvelope.payload`). An
    /// undeclared `route_key`, a missing caller for [`KeySource::Sender`], or
    /// any extraction failure for [`KeySource::PayloadField`] falls back to
    /// [`ConflictKey::Serial`].
    pub(crate) fn extract(
        &self,
        route_key: &str,
        caller_id: Option<&ActrId>,
        payload: &[u8],
    ) -> ConflictKey {
        let Some(rule) = self.rules.get(route_key) else {
            return ConflictKey::Serial;
        };
        let domain: Arc<str> = match &rule.domain {
            Some(d) => Arc::from(d.as_str()),
            None => Arc::from(route_key),
        };
        match &rule.source {
            KeySource::Sender => {
                let value = match caller_id {
                    Some(id) => Bytes::from(id.to_string_repr().into_bytes()),
                    None => Bytes::new(),
                };
                ConflictKey::Scoped { domain, value }
            }
            KeySource::PayloadField { tag, kind } => {
                match scan_top_level_field(payload, *tag, *kind) {
                    Ok(Some(value)) => ConflictKey::Scoped { domain, value },
                    _ => {
                        warn_extract_fallback(route_key, *tag, *kind);
                        ConflictKey::Serial
                    }
                }
            }
        }
    }
}

/// Builder for [`ConflictKeySpec`]. Duplicate `route_key` registration is a
/// programming error captured here and surfaced by [`Self::build`].
#[derive(Debug, Default)]
pub struct ConflictKeySpecBuilder {
    rules: HashMap<String, KeyRule>,
    error: Option<ConflictKeyError>,
}

impl ConflictKeySpecBuilder {
    /// Declare a method keyed within its own (method-private) domain.
    pub fn method(self, route_key: impl Into<String>, source: KeySource) -> Self {
        self.insert(route_key.into(), source, None)
    }

    /// Declare a method keyed within an explicit, possibly shared, domain.
    ///
    /// Methods naming the same `domain` share one conflict space, so equal
    /// projected values across those methods serialize against each other.
    /// Payload-backed methods in one shared domain should use compatible field
    /// kinds and logical meanings; the consumer owns that cross-method contract.
    pub fn method_in_domain(
        self,
        route_key: impl Into<String>,
        domain: impl Into<String>,
        source: KeySource,
    ) -> Self {
        self.insert(route_key.into(), source, Some(domain.into()))
    }

    fn insert(mut self, route_key: String, source: KeySource, domain: Option<String>) -> Self {
        if self.rules.contains_key(&route_key) {
            if self.error.is_none() {
                self.error = Some(ConflictKeyError::DuplicateRoute(route_key));
            }
            return self;
        }
        self.rules.insert(route_key, KeyRule { source, domain });
        self
    }

    /// Finalize the spec, or return the first registration error observed.
    pub fn build(self) -> Result<ConflictKeySpec, ConflictKeyError> {
        match self.error {
            Some(err) => Err(err),
            None => Ok(ConflictKeySpec { rules: self.rules }),
        }
    }
}

/// Internal projection result routed by the scheduler. Not part of the public
/// API.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum ConflictKey {
    /// Implicit root key: undeclared method, extraction fallback, or gate off.
    /// Semantics = **global barrier** (mutually exclusive with *every* in-flight
    /// task), not merely "exclusive with other Serial".
    Serial,
    /// `(domain, raw bytes)` compared for exact equality — no hashing of the
    /// value into a smaller space, so no collisions.
    Scoped { domain: Arc<str>, value: Bytes },
}

impl ConflictKey {
    pub(crate) fn is_serial(&self) -> bool {
        matches!(self, ConflictKey::Serial)
    }
}

// ── protobuf wire-format top-level field scanner ─────────────────────────────

/// Read a base-128 varint at `off`. Returns `(value, bytes_consumed)` or `Err`
/// on truncation / overflow.
fn read_varint(buf: &[u8], off: usize) -> Result<(u64, usize), ()> {
    let mut result: u64 = 0;
    let mut i = off;
    for byte_index in 0..10u32 {
        let byte = *buf.get(i).ok_or(())?;
        // A u64 varint has at most one payload bit in byte ten. Rejecting the
        // remaining bits avoids silently truncating hostile encodings.
        if byte_index == 9 && byte > 1 {
            return Err(());
        }
        result |= u64::from(byte & 0x7f) << (byte_index * 7);
        i += 1;
        if byte & 0x80 == 0 {
            return Ok((result, i - off));
        }
    }
    Err(())
}

/// Encode a decoded protobuf varint in its unique minimal representation.
/// This makes alternate overlong wire encodings of the same scalar map to the
/// same scheduler key.
fn canonical_varint(mut value: u64) -> Bytes {
    let mut out = [0u8; 10];
    let mut len = 0usize;
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        out[len] = byte;
        len += 1;
        if value == 0 {
            return Bytes::copy_from_slice(&out[..len]);
        }
    }
}

/// Canonicalize a varint according to the protobuf schema type. Values that a
/// generated decoder may coerce or truncate, but that are not valid encodings
/// for the declared scalar, fail closed to the global serial lane.
fn canonical_typed_varint(value: u64, kind: PayloadFieldKind) -> Result<Bytes, ()> {
    match kind {
        PayloadFieldKind::Bool if value <= 1 => Ok(canonical_varint(value)),
        PayloadFieldKind::Int32 | PayloadFieldKind::Enum
            if value <= i32::MAX as u64 || value >= i32::MIN as i64 as u64 =>
        {
            Ok(canonical_varint(value))
        }
        PayloadFieldKind::Uint32 | PayloadFieldKind::Sint32 if value <= u32::MAX as u64 => {
            Ok(canonical_varint(value))
        }
        PayloadFieldKind::Int64 | PayloadFieldKind::Uint64 | PayloadFieldKind::Sint64 => {
            Ok(canonical_varint(value))
        }
        _ => Err(()),
    }
}

/// Scan the *top level* of a protobuf message for the field numbered `target`
/// and return its schema-aware canonical value bytes.
///
/// * varint → validated for `kind`, then encoded as a minimal varint
/// * fixed32 / fixed64 → the 4 / 8 raw bytes
/// * string / bytes → the content bytes (length prefix stripped; strings must
///   be valid UTF-8)
///
/// Returns `Ok(None)` when the field is absent or appears more than once
/// (repeated → ambiguous → fall back), and `Err(())` on a malformed buffer or
/// an unsupported wire type (groups). Length-delimited values are accepted only
/// as declared strings or bytes; an embedded message must not be mislabeled as
/// bytes because its semantic value has multiple valid wire encodings. The
/// caller treats any error/none as a serial fallback.
pub(crate) fn scan_top_level_field(
    payload: &[u8],
    target: u32,
    kind: PayloadFieldKind,
) -> Result<Option<Bytes>, ()> {
    let mut i = 0usize;
    let mut found: Option<Bytes> = None;
    while i < payload.len() {
        let (tag, n) = read_varint(payload, i)?;
        i += n;
        let field_no = tag >> 3;
        // Protobuf field numbers are 1..=2^29-1. Reject zero and oversized
        // wire keys before narrowing so a hostile high tag cannot truncate to
        // the configured conflict field and escape conservative serialization.
        if field_no == 0 || field_no > 0x1fff_ffff {
            return Err(());
        }
        let field_no = u32::try_from(field_no).map_err(|_| ())?;
        let wire = (tag & 0x7) as u8;
        let is_target = field_no == target;
        match wire {
            0 => {
                // varint
                let (value, m) = read_varint(payload, i)?;
                i += m;
                if is_target {
                    if found.is_some() {
                        return Ok(None);
                    }
                    found = Some(canonical_typed_varint(value, kind)?);
                }
            }
            1 => {
                // fixed64
                // Checked bound: `8 > remaining` avoids the `i + 8` overflow /
                // wraparound that a hostile length could otherwise weaponize.
                if 8 > payload.len().saturating_sub(i) {
                    return Err(());
                }
                if is_target {
                    if found.is_some() {
                        return Ok(None);
                    }
                    if !matches!(kind, PayloadFieldKind::Fixed64 | PayloadFieldKind::Sfixed64) {
                        return Err(());
                    }
                    found = Some(Bytes::copy_from_slice(&payload[i..i + 8]));
                }
                i += 8;
            }
            2 => {
                // length-delimited
                let (len, m) = read_varint(payload, i)?;
                i += m;
                let len = usize::try_from(len).map_err(|_| ())?;
                // Checked bound: a hostile length prefix can be up to
                // `u64::MAX`, so `i + len` would overflow (debug panic) or wrap
                // past the length check (release out-of-bounds slice panic).
                // Compare against the remaining bytes instead — malformed or
                // oversized lengths fall back to Serial rather than panicking.
                if len > payload.len().saturating_sub(i) {
                    return Err(());
                }
                if is_target {
                    if found.is_some() {
                        return Ok(None);
                    }
                    if !matches!(kind, PayloadFieldKind::String | PayloadFieldKind::Bytes) {
                        return Err(());
                    }
                    if len > MAX_CONFLICT_KEY_BYTES {
                        return Err(());
                    }
                    if matches!(kind, PayloadFieldKind::String)
                        && std::str::from_utf8(&payload[i..i + len]).is_err()
                    {
                        return Err(());
                    }
                    found = Some(Bytes::copy_from_slice(&payload[i..i + len]));
                }
                i += len;
            }
            5 => {
                // fixed32
                // Checked bound: mirrors the fixed64 case; `4 > remaining`
                // never overflows regardless of `i`.
                if 4 > payload.len().saturating_sub(i) {
                    return Err(());
                }
                if is_target {
                    if found.is_some() {
                        return Ok(None);
                    }
                    if !matches!(kind, PayloadFieldKind::Fixed32 | PayloadFieldKind::Sfixed32) {
                        return Err(());
                    }
                    found = Some(Bytes::copy_from_slice(&payload[i..i + 4]));
                }
                i += 4;
            }
            // groups (3/4) and any unknown wire type are unsupported → fall back.
            _ => return Err(()),
        }
    }
    Ok(found)
}

/// Rate-limited (≈ 1 Hz) warning for an extraction fallback, so a hot method
/// projecting to Serial cannot flood the log.
fn warn_extract_fallback(route_key: &str, tag: u32, kind: PayloadFieldKind) {
    static LAST_WARN_MS: AtomicU64 = AtomicU64::new(0);
    static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    let start = START.get_or_init(Instant::now);
    let now_ms = start.elapsed().as_millis() as u64;
    let last = LAST_WARN_MS.load(Ordering::Relaxed);
    if now_ms.saturating_sub(last) >= 1000
        && LAST_WARN_MS
            .compare_exchange(last, now_ms, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    {
        tracing::warn!(
            route_key,
            payload_tag = tag,
            payload_kind = ?kind,
            "conflict-key extraction failed (missing/malformed/type-mismatched/unsupported/repeated/oversized field); \
             falling back to serial dispatch"
        );
    }
}
