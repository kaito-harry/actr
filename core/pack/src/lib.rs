//! actr-pack -- .actr package format
//!
//! Provides reading, writing, signing and verification of .actr ZIP STORE packages.
//!
//! ## Package structure
//!
//! ```text
//! {mfr}-{name}-{version}-{target}.actr
//! +-- manifest.toml       # manifest (TOML, signed payload)
//! +-- manifest.sig        # Ed25519 signature (64 bytes raw)
//! +-- manifest.lock.toml  # dependency lock (optional)
//! +-- bin/actor.wasm      # binary (STORE mode, uncompressed)
//! +-- proto/*.proto       # exported proto files (optional)
//! ```
//!
//! ## Signing chain
//!
//! ```text
//! binary bytes -> SHA-256 -> manifest.toml[binary.hash]
//!                                    |
//!                          manifest.toml bytes -> Ed25519 sign -> manifest.sig
//! ```

pub mod error;
pub mod load;
pub mod manifest;
pub mod pack;
pub mod verify;

mod util;

pub use error::PackError;
pub use load::{
    load_binary, load_binary_bounded, read_glue_js, read_glue_js_bounded, read_lock_file,
    read_lock_file_bounded, read_manifest, read_manifest_raw, read_proto_files,
    read_proto_files_bounded, read_signature,
};
pub use manifest::{
    BinaryEntry, BinaryKind, LockFileEntry, ManifestMetadata, PackageManifest, ProtoFileEntry,
    ResourceEntry,
};
pub use pack::{PackOptions, pack};
pub use verify::{
    DEFAULT_MAX_VERIFIED_ENTRIES, DEFAULT_MAX_VERIFIED_ENTRY_BYTES, PackageVerificationLimits,
    VerifiedPackage, verify, verify_bounded, verify_with_limits,
};

/// Compute deterministic key_id from Ed25519 public key bytes.
///
/// Algorithm: `"mfr-" + hex(sha256(public_key_bytes))[..16]`
///
/// This MUST match the server-side implementation in `actrix-mfr::crypto::compute_key_id`.
pub fn compute_key_id(public_key_bytes: &[u8]) -> String {
    let hex_str = util::sha256_hex(public_key_bytes);
    format!("mfr-{}", &hex_str[..16])
}
