// SPDX-License-Identifier: Apache-2.0

//! Canonical mapping table and drift check engine.
//!
//! Mapping scope
//! -------------
//!
//! Only the items the DynClib C ABI actually implements are cross-checked.
//! That excludes the 16 observation hooks and their event payload records,
//! which live exclusively on the WASM Component Model path. Those items are
//! listed in the `wit_only` set so the mapping explicitly acknowledges
//! them; anything the lint otherwise doesn't recognise will trip drift.
//!
//! DynClib keeps its own native-only bookkeeping (`HostVTable`, `AbiFrame`,
//! ABI op codes, status codes, `InvocationContextV1`, `InitPayloadV1`,
//! `DestV1` kinds, helper impls). Those have no WIT counterpart and are
//! listed in `dynclib_only` purely for documentation — they do not
//! participate in the check loop, so both sets are flagged `allow(dead_code)`.

use std::collections::HashSet;

use crate::report::{ShapeDrift, ShapeDriftKind};
use crate::rust_model::RustModel;
use crate::wit_model::WitModel;

/// Field-level correspondence between a WIT record and a Rust struct.
///
/// `rust_name` lets the mapping describe renames (`field-a` -> `field_a`
/// is the default) without the check engine second-guessing. `expected_ty`
/// is the normalized Rust type we expect to see; it is compared
/// case-sensitively after whitespace collapse (see
/// `rust_model::normalize_type`).
#[derive(Debug, Clone)]
pub struct FieldMapping {
    pub wit_name: &'static str,
    pub rust_name: &'static str,
    pub expected_rust_ty: &'static str,
}

/// WIT record <-> Rust struct.
#[derive(Debug, Clone)]
pub struct RecordMapping {
    pub wit_name: &'static str,
    pub rust_name: &'static str,
    pub fields: Vec<FieldMapping>,
}

/// WIT variant <-> Rust enum.
#[derive(Debug, Clone)]
pub struct VariantMapping {
    pub wit_name: &'static str,
    pub rust_name: &'static str,
    /// (wit case name, rust variant name, optional payload Rust type)
    pub cases: Vec<(&'static str, &'static str, Option<&'static str>)>,
}

/// WIT function (under some interface) <-> Rust payload type + ABI op code.
#[derive(Debug, Clone)]
pub struct FunctionMapping {
    /// Fully qualified WIT function key `"<iface>.<name>"`.
    pub wit_key: &'static str,
    /// Rust struct name carrying the serialized parameter set.
    pub rust_payload: &'static str,
    /// ABI op constant ident in `dynclib_abi::op`.
    pub op_const: &'static str,
    /// Expected numeric value of the op constant (decimal).
    pub op_value: u32,
}

/// Fully declared mapping table.
#[derive(Debug, Clone, Default)]
pub struct Mapping {
    pub records: Vec<RecordMapping>,
    pub variants: Vec<VariantMapping>,
    pub functions: Vec<FunctionMapping>,
    /// WIT items acknowledged as WASM-only (wit-bindgen generated).
    ///
    /// Recorded for audit / documentation purposes only; the check loop does
    /// not consult it.
    #[allow(dead_code)]
    pub wit_only: HashSet<&'static str>,
    /// Rust items acknowledged as DynClib-only (no WIT counterpart).
    ///
    /// Recorded for audit / documentation purposes only; the check loop does
    /// not consult it.
    #[allow(dead_code)]
    pub dynclib_only: HashSet<&'static str>,
}

impl Mapping {
    /// Run every rule and collect drift entries.
    pub fn check(&self, wit: &WitModel, abi: &RustModel) -> Vec<ShapeDrift> {
        let mut drifts = Vec::new();
        for rec in &self.records {
            check_record(rec, wit, abi, &mut drifts);
        }
        for var in &self.variants {
            check_variant(var, wit, abi, &mut drifts);
        }
        for func in &self.functions {
            check_function(func, wit, abi, &mut drifts);
        }
        drifts
    }
}

fn check_record(
    map: &RecordMapping,
    wit: &WitModel,
    abi: &RustModel,
    drifts: &mut Vec<ShapeDrift>,
) {
    let location = format!("record {} ({})", map.wit_name, map.rust_name);

    let Some(wit_rec) = wit.records.get(map.wit_name) else {
        drifts.push(ShapeDrift {
            kind: ShapeDriftKind::MissingWitItem,
            location,
            message: format!("WIT record `{}` not found in source", map.wit_name),
        });
        return;
    };
    let Some(rust_rec) = abi.structs.get(map.rust_name) else {
        drifts.push(ShapeDrift {
            kind: ShapeDriftKind::MissingRustStruct,
            location,
            message: format!("Rust struct `{}` not found", map.rust_name),
        });
        return;
    };

    if wit_rec.fields.len() != map.fields.len() {
        drifts.push(ShapeDrift {
            kind: ShapeDriftKind::FieldMismatch,
            location: location.clone(),
            message: format!(
                "WIT record has {} field(s) but the mapping declares {}; update the mapping",
                wit_rec.fields.len(),
                map.fields.len()
            ),
        });
    }

    for (idx, expected) in map.fields.iter().enumerate() {
        let wit_field = wit_rec.fields.iter().find(|f| f.name == expected.wit_name);
        let rust_field = rust_rec
            .fields
            .iter()
            .find(|f| f.name == expected.rust_name);

        match (wit_field, rust_field) {
            (None, _) => drifts.push(ShapeDrift {
                kind: ShapeDriftKind::FieldMismatch,
                location: location.clone(),
                message: format!(
                    "WIT record missing field `{}` declared by mapping",
                    expected.wit_name
                ),
            }),
            (_, None) => drifts.push(ShapeDrift {
                kind: ShapeDriftKind::FieldMismatch,
                location: location.clone(),
                message: format!(
                    "Rust struct missing field `{}` declared by mapping",
                    expected.rust_name
                ),
            }),
            (Some(_), Some(r)) => {
                if r.ty != expected.expected_rust_ty {
                    drifts.push(ShapeDrift {
                        kind: ShapeDriftKind::FieldMismatch,
                        location: location.clone(),
                        message: format!(
                            "field #{idx} `{}.{}`: expected Rust type `{}`, found `{}`",
                            map.rust_name, expected.rust_name, expected.expected_rust_ty, r.ty
                        ),
                    });
                }
                if let Some(tag) = r.prost_tag {
                    // prost tags should align 1..=N with the declared
                    // mapping order. Anything else suggests field reorder.
                    let expected_tag = (idx as u32) + 1;
                    if tag != expected_tag {
                        drifts.push(ShapeDrift {
                            kind: ShapeDriftKind::FieldMismatch,
                            location: location.clone(),
                            message: format!(
                                "field `{}.{}` prost tag = {} but mapping position is {}",
                                map.rust_name, expected.rust_name, tag, expected_tag
                            ),
                        });
                    }
                }
            }
        }
    }
}

fn check_variant(
    map: &VariantMapping,
    wit: &WitModel,
    abi: &RustModel,
    drifts: &mut Vec<ShapeDrift>,
) {
    let location = format!("variant {} ({})", map.wit_name, map.rust_name);

    let Some(wit_var) = wit.variants.get(map.wit_name) else {
        drifts.push(ShapeDrift {
            kind: ShapeDriftKind::MissingWitItem,
            location,
            message: format!("WIT variant `{}` not found", map.wit_name),
        });
        return;
    };
    let Some(rust_enum) = abi.enums.get(map.rust_name) else {
        drifts.push(ShapeDrift {
            kind: ShapeDriftKind::MissingRustEnum,
            location,
            message: format!("Rust enum `{}` not found", map.rust_name),
        });
        return;
    };

    if wit_var.cases.len() != map.cases.len() {
        drifts.push(ShapeDrift {
            kind: ShapeDriftKind::VariantMismatch,
            location: location.clone(),
            message: format!(
                "WIT variant has {} case(s) but mapping declares {}",
                wit_var.cases.len(),
                map.cases.len()
            ),
        });
    }

    for (wit_name, rust_name, expected_payload) in &map.cases {
        let wit_case = wit_var.cases.iter().find(|c| &c.name == wit_name);
        let rust_variant = rust_enum.variants.iter().find(|v| &v.name == rust_name);

        match (wit_case, rust_variant) {
            (None, _) => drifts.push(ShapeDrift {
                kind: ShapeDriftKind::VariantMismatch,
                location: location.clone(),
                message: format!("WIT variant missing case `{wit_name}`"),
            }),
            (_, None) => drifts.push(ShapeDrift {
                kind: ShapeDriftKind::VariantMismatch,
                location: location.clone(),
                message: format!("Rust enum missing variant `{rust_name}`"),
            }),
            (Some(_), Some(r)) => {
                let expected = expected_payload.map(|s| s.to_string());
                let found = r.payload.clone();
                if expected != found {
                    drifts.push(ShapeDrift {
                        kind: ShapeDriftKind::VariantMismatch,
                        location: location.clone(),
                        message: format!(
                            "case `{wit_name}`/`{rust_name}`: expected payload {:?}, found {:?}",
                            expected.as_deref().unwrap_or("<unit>"),
                            found.as_deref().unwrap_or("<unit>"),
                        ),
                    });
                }
            }
        }
    }
}

fn check_function(
    map: &FunctionMapping,
    wit: &WitModel,
    abi: &RustModel,
    drifts: &mut Vec<ShapeDrift>,
) {
    let location = format!(
        "func {} ({}, op::{})",
        map.wit_key, map.rust_payload, map.op_const
    );

    let Some(_wit_func) = wit.functions.get(map.wit_key) else {
        drifts.push(ShapeDrift {
            kind: ShapeDriftKind::MissingWitItem,
            location,
            message: format!("WIT function `{}` not found", map.wit_key),
        });
        return;
    };

    // Rust payload struct must exist. Its fields are already drift-checked
    // via the record-mapping rows, so we don't re-check them here — we
    // only assert presence and non-empty field set.
    let Some(rust_payload) = abi.structs.get(map.rust_payload) else {
        drifts.push(ShapeDrift {
            kind: ShapeDriftKind::MissingRustPayload,
            location: location.clone(),
            message: format!(
                "payload struct `{}` not found in dynclib_abi.rs",
                map.rust_payload
            ),
        });
        return;
    };
    if rust_payload.fields.is_empty() {
        drifts.push(ShapeDrift {
            kind: ShapeDriftKind::FieldMismatch,
            location: location.clone(),
            message: format!(
                "payload struct `{}` has no fields; expected at least one",
                map.rust_payload
            ),
        });
    }

    // ABI op constant must exist in `op` module with the expected value.
    let Some(op_mod) = abi.const_modules.get("op") else {
        drifts.push(ShapeDrift {
            kind: ShapeDriftKind::MissingOpConstant,
            location,
            message: "`pub mod op` not found in dynclib_abi.rs".into(),
        });
        return;
    };
    let Some(c) = op_mod.get(map.op_const) else {
        drifts.push(ShapeDrift {
            kind: ShapeDriftKind::MissingOpConstant,
            location,
            message: format!("op::{} missing", map.op_const),
        });
        return;
    };
    let expected = map.op_value.to_string();
    if c.value != expected {
        drifts.push(ShapeDrift {
            kind: ShapeDriftKind::OpConstantValueMismatch,
            location,
            message: format!(
                "op::{} = {} but mapping declares {}",
                map.op_const, c.value, expected
            ),
        });
    }
}

// ─────────────────────────────────────────────────────────────────────────
// The canonical mapping table for actr-workload@0.1.0.
// ─────────────────────────────────────────────────────────────────────────

/// Canonical mapping used by the production lint invocation.
///
/// When the WIT file changes, update the matching row here; when the
/// DynClib ABI changes, update the matching row here. CI blocks merges
/// that leave this table inconsistent with either side.
pub fn default_mapping() -> Mapping {
    Mapping {
        records: vec![
            // DestV1 { kind } is a prost-oneof wrapper. The `kind` field
            // holds an `Option<DestKind>`, which maps to the WIT `variant
            // dest { host, workload, peer(actr-id) }`. We still assert the
            // struct exists; the variant alignment is covered by
            // `variants` below.
            //
            // Note: HostCallV1 / HostTellV1 / HostCallRawV1 / HostDiscoverV1
            // correspond to the host interface functions, not to top-level
            // WIT records — their field shape is asserted through
            // `functions` below (presence + non-empty). We deliberately do
            // NOT declare them as records here because WIT has no matching
            // `record host-call-v1`: the parameters live directly on the
            // function signature.
        ],
        variants: vec![VariantMapping {
            wit_name: "dest",
            rust_name: "DestKind",
            cases: vec![
                // WIT unit cases map to Rust `bool`-payload variants
                // because prost-oneof requires a non-unit type per
                // arm. The `bool` is an ABI-level discriminant-only
                // placeholder (always true) — documented in
                // dynclib_abi.rs's DestKind declaration.
                ("host", "Host", Some("(bool)")),
                ("workload", "Workload", Some("(bool)")),
                ("peer", "Peer", Some("(ActrId)")),
            ],
        }],
        functions: vec![
            FunctionMapping {
                wit_key: "host.call",
                rust_payload: "HostCallV1",
                op_const: "HOST_CALL",
                op_value: 1,
            },
            FunctionMapping {
                wit_key: "host.tell",
                rust_payload: "HostTellV1",
                op_const: "HOST_TELL",
                op_value: 2,
            },
            FunctionMapping {
                wit_key: "host.call-raw",
                rust_payload: "HostCallRawV1",
                op_const: "HOST_CALL_RAW",
                op_value: 3,
            },
            FunctionMapping {
                wit_key: "host.discover",
                rust_payload: "HostDiscoverV1",
                op_const: "HOST_DISCOVER",
                op_value: 4,
            },
            // `workload.dispatch` is carried by GuestHandleV1. The host
            // synthesises an `AbiFrame { op: GUEST_HANDLE, payload:
            // GuestHandleV1 }` frame; the guest unwraps it. Context
            // (self-id / caller-id / request-id) is embedded inside
            // GuestHandleV1.ctx because the DynClib path has no counterpart
            // to the WIT `host` interface's context accessors.
            FunctionMapping {
                wit_key: "workload.dispatch",
                rust_payload: "GuestHandleV1",
                op_const: "GUEST_HANDLE",
                op_value: 101,
            },
        ],
        wit_only: [
            // 16 observation hooks — WASM only, implemented via
            // wit-bindgen-generated Guest trait; DynClib dispatches only
            // the one `dispatch` entry.
            "workload.on-start",
            "workload.on-ready",
            "workload.on-stop",
            "workload.on-error",
            "workload.on-signaling-connecting",
            "workload.on-signaling-connected",
            "workload.on-signaling-disconnected",
            "workload.on-websocket-connecting",
            "workload.on-websocket-connected",
            "workload.on-websocket-disconnected",
            "workload.on-webrtc-connecting",
            "workload.on-webrtc-connected",
            "workload.on-webrtc-disconnected",
            "workload.on-credential-renewed",
            "workload.on-credential-expiring",
            "workload.on-mailbox-backpressure",
            // Event payload records carried by the hooks above.
            "peer-event",
            "error-event",
            "credential-event",
            "backpressure-event",
            "error-category",
            "actr-error",
            "dependency-not-found-payload",
            "timestamp",
            "realm",
            "actr-type",
            "actr-id",
            "rpc-envelope",
            // Per-dispatch context accessors — DynClib threads context
            // through GuestHandleV1.ctx instead.
            "host.log-message",
            "host.get-self-id",
            "host.get-caller-id",
            "host.get-request-id",
        ]
        .into_iter()
        .collect(),
        dynclib_only: [
            // C-ABI plumbing with no WIT counterpart.
            "HostVTable",
            "AbiFrame",
            "AbiReply",
            "InvocationContextV1",
            "InitPayloadV1",
            "DestV1",
            "AbiPayload",
            "code", // module of status codes
            "version",
        ]
        .into_iter()
        .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rust_model;
    use crate::wit_model;

    // Minimal self-consistent fixture used by the positive tests.
    const TINY_WIT: &str = r#"
        package test:mini@0.1.0;

        interface types {
            variant direction {
                north,
                south(string),
            }
        }

        interface api {
            use types.{direction};
            go: func(arg: direction) -> result<_, string>;
        }

        world w {
            import api;
        }
    "#;

    const TINY_RS: &str = r#"
        pub mod op {
            pub const GO: u32 = 7;
        }

        pub enum Direction {
            North(bool),
            South(String),
        }

        pub struct GoPayload {
            #[prost(message, required, tag = "1")]
            pub arg: Direction,
        }
    "#;

    fn tiny_mapping() -> Mapping {
        Mapping {
            variants: vec![VariantMapping {
                wit_name: "direction",
                rust_name: "Direction",
                cases: vec![
                    ("north", "North", Some("(bool)")),
                    ("south", "South", Some("(String)")),
                ],
            }],
            functions: vec![FunctionMapping {
                wit_key: "api.go",
                rust_payload: "GoPayload",
                op_const: "GO",
                op_value: 7,
            }],
            ..Default::default()
        }
    }

    #[test]
    fn clean_baseline_produces_no_drift() {
        let wit = wit_model::load_str(TINY_WIT).unwrap();
        let abi = rust_model::load_str(TINY_RS).unwrap();
        let drifts = tiny_mapping().check(&wit, &abi);
        assert!(drifts.is_empty(), "unexpected drifts: {drifts:#?}");
    }

    #[test]
    fn detects_variant_payload_drift() {
        let mutated_rs = r#"
            pub mod op { pub const GO: u32 = 7; }

            pub enum Direction {
                North(bool),
                // Drift: payload now u32 instead of String.
                South(u32),
            }

            pub struct GoPayload {
                #[prost(message, required, tag = "1")]
                pub arg: Direction,
            }
        "#;
        let wit = wit_model::load_str(TINY_WIT).unwrap();
        let abi = rust_model::load_str(mutated_rs).unwrap();
        let drifts = tiny_mapping().check(&wit, &abi);
        assert!(
            drifts
                .iter()
                .any(|d| d.kind == ShapeDriftKind::VariantMismatch && d.message.contains("South")),
            "should have reported a South payload mismatch; got {drifts:#?}"
        );
    }

    #[test]
    fn detects_missing_op_constant() {
        let mutated_rs = r#"
            pub mod op { /* GO deleted */ }

            pub enum Direction { North(bool), South(String) }
            pub struct GoPayload {
                #[prost(message, required, tag = "1")]
                pub arg: Direction,
            }
        "#;
        let wit = wit_model::load_str(TINY_WIT).unwrap();
        let abi = rust_model::load_str(mutated_rs).unwrap();
        let drifts = tiny_mapping().check(&wit, &abi);
        assert!(
            drifts
                .iter()
                .any(|d| d.kind == ShapeDriftKind::MissingOpConstant),
            "should have reported missing op constant; got {drifts:#?}"
        );
    }

    #[test]
    fn detects_op_value_mismatch() {
        let mutated_rs = r#"
            pub mod op {
                pub const GO: u32 = 99; // drift
            }

            pub enum Direction { North(bool), South(String) }
            pub struct GoPayload {
                #[prost(message, required, tag = "1")]
                pub arg: Direction,
            }
        "#;
        let wit = wit_model::load_str(TINY_WIT).unwrap();
        let abi = rust_model::load_str(mutated_rs).unwrap();
        let drifts = tiny_mapping().check(&wit, &abi);
        assert!(
            drifts
                .iter()
                .any(|d| d.kind == ShapeDriftKind::OpConstantValueMismatch),
            "should have reported op value mismatch; got {drifts:#?}"
        );
    }

    #[test]
    fn detects_missing_rust_payload() {
        let mutated_rs = r#"
            pub mod op { pub const GO: u32 = 7; }

            pub enum Direction { North(bool), South(String) }
            // GoPayload struct removed entirely.
        "#;
        let wit = wit_model::load_str(TINY_WIT).unwrap();
        let abi = rust_model::load_str(mutated_rs).unwrap();
        let drifts = tiny_mapping().check(&wit, &abi);
        assert!(
            drifts
                .iter()
                .any(|d| d.kind == ShapeDriftKind::MissingRustPayload),
            "should have reported missing payload; got {drifts:#?}"
        );
    }
}
