//! Shared WIT ⇄ actr value-type translation helpers.
//!
//! Kept in a separate module from [`super::context`] so the adapter code
//! ([`super::adapter`]) can reuse them without having to depend on the
//! whole `Context`-trait impl surface. Every helper is a one-shot pure
//! conversion with no I/O or async side effects.

use actr_protocol::{ActrId, ActrType, DataChunk, MetadataEntry, PayloadType, Realm};

use crate::Dest;

use super::generated::actr::workload::types as wit_types;

pub(crate) fn realm_to_wit(r: &Realm) -> wit_types::Realm {
    wit_types::Realm {
        realm_id: r.realm_id,
    }
}

pub(crate) fn realm_from_wit(r: &wit_types::Realm) -> Realm {
    Realm {
        realm_id: r.realm_id,
    }
}

pub(crate) fn actr_type_to_wit(t: &ActrType) -> wit_types::ActrType {
    wit_types::ActrType {
        manufacturer: t.manufacturer.clone(),
        name: t.name.clone(),
        version: t.version.clone(),
    }
}

pub(crate) fn actr_type_from_wit(t: &wit_types::ActrType) -> ActrType {
    ActrType {
        manufacturer: t.manufacturer.clone(),
        name: t.name.clone(),
        version: t.version.clone(),
    }
}

pub(crate) fn actr_id_to_wit(id: &ActrId) -> wit_types::ActrId {
    wit_types::ActrId {
        realm: realm_to_wit(&id.realm),
        serial_number: id.serial_number,
        type_: actr_type_to_wit(&id.r#type),
    }
}

pub(crate) fn actr_id_from_wit(id: &wit_types::ActrId) -> ActrId {
    ActrId {
        realm: realm_from_wit(&id.realm),
        serial_number: id.serial_number,
        r#type: actr_type_from_wit(&id.type_),
    }
}

pub(crate) fn dest_to_wit(dest: &Dest) -> wit_types::Dest {
    match dest {
        Dest::Host => wit_types::Dest::Host,
        Dest::Workload => wit_types::Dest::Workload,
        Dest::Peer(id) => wit_types::Dest::Peer(actr_id_to_wit(id)),
    }
}

pub(crate) fn payload_type_to_wit(payload_type: PayloadType) -> wit_types::PayloadType {
    match payload_type {
        PayloadType::RpcReliable => wit_types::PayloadType::RpcReliable,
        PayloadType::RpcSignal => wit_types::PayloadType::RpcSignal,
        PayloadType::StreamReliable => wit_types::PayloadType::StreamReliable,
        PayloadType::StreamLatencyFirst => wit_types::PayloadType::StreamLatencyFirst,
        PayloadType::MediaRtp => wit_types::PayloadType::MediaRtp,
    }
}

pub(crate) fn data_chunk_to_wit(chunk: DataChunk) -> wit_types::DataChunk {
    wit_types::DataChunk {
        stream_id: chunk.stream_id,
        sequence: chunk.sequence,
        payload: chunk.payload.to_vec(),
        metadata: chunk
            .metadata
            .into_iter()
            .map(|entry| wit_types::MetadataEntry {
                key: entry.key,
                value: entry.value,
            })
            .collect(),
        timestamp_ms: chunk.timestamp_ms,
    }
}

pub(crate) fn data_chunk_from_wit(chunk: wit_types::DataChunk) -> DataChunk {
    DataChunk {
        stream_id: chunk.stream_id,
        sequence: chunk.sequence,
        payload: chunk.payload.into(),
        metadata: chunk
            .metadata
            .into_iter()
            .map(|entry| MetadataEntry {
                key: entry.key,
                value: entry.value,
            })
            .collect(),
        timestamp_ms: chunk.timestamp_ms,
    }
}
