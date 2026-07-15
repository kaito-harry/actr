//! Unit tests for conflict-key projection and the protobuf tag scanner.

use super::conflict_key::{
    ConflictKey, ConflictKeyError, ConflictKeySpec, KeySource, PayloadFieldKind,
    scan_top_level_field as scan_top_level_field_for_tests,
};
use bytes::Bytes;

/// Encode a protobuf tag (field_number, wire_type).
fn tag(field: u32, wire: u8) -> Vec<u8> {
    encode_varint(u64::from((field << 3) | u32::from(wire)))
}

fn encode_varint(mut v: u64) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let mut byte = (v & 0x7f) as u8;
        v >>= 7;
        if v != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if v == 0 {
            break;
        }
    }
    out
}

/// Build a message with a single varint field.
fn msg_varint(field: u32, value: u64) -> Vec<u8> {
    let mut m = tag(field, 0);
    m.extend(encode_varint(value));
    m
}

/// Build a message with a single length-delimited (string/bytes) field.
fn msg_len_delim(field: u32, value: &[u8]) -> Vec<u8> {
    let mut m = tag(field, 2);
    m.extend(encode_varint(value.len() as u64));
    m.extend_from_slice(value);
    m
}

#[test]
fn scan_varint_field() {
    let payload = msg_varint(1, 300);
    let got = scan_top_level_field_for_tests(&payload, 1, PayloadFieldKind::Uint64).unwrap();
    // 300 = 0xAC 0x02 as a varint.
    assert_eq!(got, Some(Bytes::from(encode_varint(300))));
}

#[test]
fn scan_overlong_varint_uses_canonical_key() {
    let canonical = msg_varint(1, 1);
    // The same scalar encoded non-canonically as 0x81 0x00.
    let overlong = [tag(1, 0).as_slice(), &[0x81, 0x00]].concat();
    assert_eq!(
        scan_top_level_field_for_tests(&canonical, 1, PayloadFieldKind::Uint64).unwrap(),
        scan_top_level_field_for_tests(&overlong, 1, PayloadFieldKind::Uint64).unwrap()
    );
}

#[test]
fn bool_wire_alias_falls_back_to_serial() {
    let spec = ConflictKeySpec::builder()
        .method(
            "flags.Flags.Set",
            KeySource::PayloadField {
                tag: 1,
                kind: PayloadFieldKind::Bool,
            },
        )
        .build()
        .unwrap();

    let canonical_true = spec.extract("flags.Flags.Set", None, &msg_varint(1, 1));
    assert!(matches!(canonical_true, ConflictKey::Scoped { .. }));
    // Protobuf decoders commonly coerce every non-zero varint to `true`. A
    // second scoped key for value 2 would therefore let the same logical bool
    // run concurrently; fail closed instead.
    assert_eq!(
        spec.extract("flags.Flags.Set", None, &msg_varint(1, 2)),
        ConflictKey::Serial
    );
}

#[test]
fn out_of_range_uint32_wire_alias_falls_back_to_serial() {
    let spec = ConflictKeySpec::builder()
        .method(
            "counter.Counter.Update",
            KeySource::PayloadField {
                tag: 1,
                kind: PayloadFieldKind::Uint32,
            },
        )
        .build()
        .unwrap();

    let canonical = spec.extract("counter.Counter.Update", None, &msg_varint(1, 1));
    assert!(matches!(canonical, ConflictKey::Scoped { .. }));
    // Generated decoders may narrow this to the same u32 value `1`; treating
    // the 64-bit wire value as a separate key would violate same-key FIFO.
    assert_eq!(
        spec.extract(
            "counter.Counter.Update",
            None,
            &msg_varint(1, u64::from(u32::MAX) + 2),
        ),
        ConflictKey::Serial
    );
}

#[test]
fn payload_kind_wire_mismatch_falls_back_to_serial() {
    let spec = ConflictKeySpec::builder()
        .method(
            "chat.Chat.Send",
            KeySource::PayloadField {
                tag: 1,
                kind: PayloadFieldKind::Uint64,
            },
        )
        .build()
        .unwrap();
    assert_eq!(
        spec.extract("chat.Chat.Send", None, &msg_len_delim(1, b"1")),
        ConflictKey::Serial
    );
}

#[test]
fn invalid_utf8_string_falls_back_to_serial() {
    let spec = ConflictKeySpec::builder()
        .method(
            "chat.Chat.Send",
            KeySource::PayloadField {
                tag: 1,
                kind: PayloadFieldKind::String,
            },
        )
        .build()
        .unwrap();
    assert_eq!(
        spec.extract("chat.Chat.Send", None, &msg_len_delim(1, &[0xff])),
        ConflictKey::Serial
    );
}

#[test]
fn scan_varint_overflow_is_error() {
    let payload = [
        0x08, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x02,
    ];
    assert!(scan_top_level_field_for_tests(&payload, 1, PayloadFieldKind::Uint64).is_err());
}

#[test]
fn scan_rejects_zero_and_oversized_field_numbers() {
    assert!(scan_top_level_field_for_tests(&[0x00, 0x01], 1, PayloadFieldKind::Uint64).is_err());

    // Field number 2^29 is one beyond protobuf's maximum. Its key is
    // (2^29 << 3) | varint, encoded minimally as 0x80 0x80 0x80 0x80 0x10.
    let oversized_tag = [0x80, 0x80, 0x80, 0x80, 0x10, 0x01];
    assert!(scan_top_level_field_for_tests(&oversized_tag, 1, PayloadFieldKind::Uint64).is_err());
}

#[test]
fn scan_string_field_returns_content() {
    let payload = msg_len_delim(2, b"room-42");
    let got = scan_top_level_field_for_tests(&payload, 2, PayloadFieldKind::String).unwrap();
    assert_eq!(got, Some(Bytes::from_static(b"room-42")));
}

#[test]
fn oversized_length_delimited_key_falls_back_to_serial() {
    let spec = ConflictKeySpec::builder()
        .method(
            "chat.Chat.Send",
            KeySource::PayloadField {
                tag: 1,
                kind: PayloadFieldKind::Bytes,
            },
        )
        .build()
        .unwrap();
    let payload = msg_len_delim(1, &vec![b'x'; 4 * 1024 + 1]);
    assert_eq!(
        spec.extract("chat.Chat.Send", None, &payload),
        ConflictKey::Serial
    );
}

#[test]
fn scan_fixed64_field() {
    let mut payload = tag(3, 1);
    payload.extend_from_slice(&7u64.to_le_bytes());
    let got = scan_top_level_field_for_tests(&payload, 3, PayloadFieldKind::Fixed64).unwrap();
    assert_eq!(got, Some(Bytes::copy_from_slice(&7u64.to_le_bytes())));
}

#[test]
fn scan_fixed32_field() {
    let mut payload = tag(4, 5);
    payload.extend_from_slice(&9u32.to_le_bytes());
    let got = scan_top_level_field_for_tests(&payload, 4, PayloadFieldKind::Fixed32).unwrap();
    assert_eq!(got, Some(Bytes::copy_from_slice(&9u32.to_le_bytes())));
}

#[test]
fn scan_skips_other_fields() {
    // field 1 varint, field 2 string — target field 2.
    let mut payload = msg_varint(1, 99);
    payload.extend(msg_len_delim(2, b"doc-1"));
    let got = scan_top_level_field_for_tests(&payload, 2, PayloadFieldKind::String).unwrap();
    assert_eq!(got, Some(Bytes::from_static(b"doc-1")));
}

#[test]
fn scan_missing_field_is_none() {
    let payload = msg_varint(1, 5);
    assert_eq!(
        scan_top_level_field_for_tests(&payload, 7, PayloadFieldKind::Bytes).unwrap(),
        None
    );
}

#[test]
fn scan_repeated_field_falls_back_to_none() {
    let mut payload = msg_len_delim(1, b"a");
    payload.extend(msg_len_delim(1, b"b"));
    // repeated target field → ambiguous → None (safe: serial fallback).
    assert_eq!(
        scan_top_level_field_for_tests(&payload, 1, PayloadFieldKind::Bytes).unwrap(),
        None
    );
}

#[test]
fn scan_group_wire_type_is_error() {
    // wire type 3 = group start (unsupported).
    let payload = tag(1, 3);
    assert!(scan_top_level_field_for_tests(&payload, 1, PayloadFieldKind::Bytes).is_err());
}

#[test]
fn scan_truncated_length_delimited_is_error() {
    let mut payload = tag(2, 2);
    payload.extend(encode_varint(10)); // claims 10 bytes
    payload.extend_from_slice(b"short"); // only 5
    assert!(scan_top_level_field_for_tests(&payload, 2, PayloadFieldKind::Bytes).is_err());
}

/// A hostile length prefix near `u64::MAX` must fall back to `Err` (→ Serial)
/// rather than panicking. Before the checked-arithmetic fix, `i + len`
/// overflowed in debug builds (`attempt to add with overflow`) and wrapped
/// past the bounds check in release builds (out-of-bounds slice panic),
/// letting an ~11-byte payload crash the whole mailbox/inproc loop.
#[test]
fn scan_oversized_length_prefix_is_error_not_panic() {
    // field 1, wire 2, followed by a 10-byte varint declaring ≈ u64::MAX bytes.
    let payload = [
        0x0A, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01,
    ];
    assert!(scan_top_level_field_for_tests(&payload, 1, PayloadFieldKind::Bytes).is_err());
}

/// A length prefix that is merely far larger than the remaining bytes (not
/// near the integer boundary) must also error out rather than slice OOB.
#[test]
fn scan_length_prefix_exceeding_remaining_is_error() {
    let mut payload = tag(1, 2);
    payload.extend(encode_varint(1_000_000)); // claims 1M bytes
    payload.extend_from_slice(b"tiny"); // provides 4
    assert!(scan_top_level_field_for_tests(&payload, 1, PayloadFieldKind::Bytes).is_err());
}

/// A malformed (never-terminating / over-long) varint length prefix must also
/// fall back to `Err` rather than panic.
#[test]
fn scan_malformed_varint_length_prefix_is_error() {
    let mut payload = tag(1, 2);
    // 10 continuation bytes with no terminator overruns the buffer → Err.
    payload.extend_from_slice(&[0xFF; 10]);
    assert!(scan_top_level_field_for_tests(&payload, 1, PayloadFieldKind::Bytes).is_err());
}

/// End-to-end: the malachite ~11-byte payload projects to `Serial` through
/// `extract` (the same synchronous admission path that is *not* wrapped in
/// `catch_unwind`) instead of panicking.
#[test]
fn oversized_length_prefix_extracts_to_serial_not_panic() {
    let spec = ConflictKeySpec::builder()
        .method(
            "chat.Chat.Send",
            KeySource::PayloadField {
                tag: 1,
                kind: PayloadFieldKind::Bytes,
            },
        )
        .build()
        .unwrap();
    let payload = [
        0x0A, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01,
    ];
    assert_eq!(
        spec.extract("chat.Chat.Send", None, &payload),
        ConflictKey::Serial
    );
}

// ── spec / extract ───────────────────────────────────────────────────────────

#[test]
fn undeclared_method_is_serial() {
    let spec = ConflictKeySpec::builder()
        .method(
            "chat.Chat.Send",
            KeySource::PayloadField {
                tag: 1,
                kind: PayloadFieldKind::Bytes,
            },
        )
        .build()
        .unwrap();
    let payload = msg_len_delim(1, b"room");
    let key = spec.extract("other.Svc.Method", None, &payload);
    assert_eq!(key, ConflictKey::Serial);
}

#[test]
fn payload_field_projects_scoped_key() {
    let spec = ConflictKeySpec::builder()
        .method(
            "chat.Chat.Send",
            KeySource::PayloadField {
                tag: 1,
                kind: PayloadFieldKind::Bytes,
            },
        )
        .build()
        .unwrap();
    let payload = msg_len_delim(1, b"room-7");
    let key = spec.extract("chat.Chat.Send", None, &payload);
    match key {
        ConflictKey::Scoped { domain, value } => {
            assert_eq!(&*domain, "chat.Chat.Send");
            assert_eq!(value, Bytes::from_static(b"room-7"));
        }
        ConflictKey::Serial => panic!("expected scoped key"),
    }
}

#[test]
fn extraction_failure_falls_back_to_serial() {
    let spec = ConflictKeySpec::builder()
        .method(
            "chat.Chat.Send",
            KeySource::PayloadField {
                tag: 9,
                kind: PayloadFieldKind::Bytes,
            },
        )
        .build()
        .unwrap();
    let payload = msg_len_delim(1, b"room");
    // field 9 absent → serial fallback.
    assert_eq!(
        spec.extract("chat.Chat.Send", None, &payload),
        ConflictKey::Serial
    );
}

#[test]
fn same_domain_distinct_methods_share_conflict_space() {
    let spec = ConflictKeySpec::builder()
        .method_in_domain(
            "doc.Docs.Update",
            "doc",
            KeySource::PayloadField {
                tag: 2,
                kind: PayloadFieldKind::Bytes,
            },
        )
        .method_in_domain(
            "doc.Docs.Delete",
            "doc",
            KeySource::PayloadField {
                tag: 2,
                kind: PayloadFieldKind::Bytes,
            },
        )
        .build()
        .unwrap();
    let payload = msg_len_delim(2, b"doc-1");
    let a = spec.extract("doc.Docs.Update", None, &payload);
    let b = spec.extract("doc.Docs.Delete", None, &payload);
    assert_eq!(
        a, b,
        "same domain + same value across methods must be equal keys"
    );
}

#[test]
fn default_domain_is_method_private() {
    let spec = ConflictKeySpec::builder()
        .method(
            "a.S.M1",
            KeySource::PayloadField {
                tag: 1,
                kind: PayloadFieldKind::Bytes,
            },
        )
        .method(
            "a.S.M2",
            KeySource::PayloadField {
                tag: 1,
                kind: PayloadFieldKind::Bytes,
            },
        )
        .build()
        .unwrap();
    let payload = msg_len_delim(1, b"same-value");
    let a = spec.extract("a.S.M1", None, &payload);
    let b = spec.extract("a.S.M2", None, &payload);
    assert_ne!(
        a, b,
        "default (method-private) domains must not collide across methods"
    );
}

#[test]
fn sender_source_uses_caller_id() {
    let spec = ConflictKeySpec::builder()
        .method("p.P.Ping", KeySource::Sender)
        .build()
        .unwrap();
    let caller =
        actr_protocol::ActrId::from_string_repr("1a2b3c@101/acme:echo-service:1.0.0").unwrap();
    let with = spec.extract("p.P.Ping", Some(&caller), &[]);
    let without = spec.extract("p.P.Ping", None, &[]);
    match (&with, &without) {
        (ConflictKey::Scoped { value: v1, .. }, ConflictKey::Scoped { value: v2, .. }) => {
            assert!(!v1.is_empty());
            assert!(
                v2.is_empty(),
                "missing caller projects to a fixed empty in-domain value"
            );
        }
        _ => panic!("sender source must always project scoped within domain"),
    }
    assert_ne!(with, without);
}

#[test]
fn duplicate_route_registration_is_error() {
    let err = ConflictKeySpec::builder()
        .method("dup.S.M", KeySource::Sender)
        .method(
            "dup.S.M",
            KeySource::PayloadField {
                tag: 1,
                kind: PayloadFieldKind::Bytes,
            },
        )
        .build()
        .unwrap_err();
    assert_eq!(err, ConflictKeyError::DuplicateRoute("dup.S.M".to_string()));
}
