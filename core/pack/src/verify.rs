use std::io::Cursor;

use ed25519_dalek::{Signature, VerifyingKey};

use crate::error::PackError;
use crate::manifest::PackageManifest;
use crate::util::{read_zip_entry_bounded, sha256_zip_entry_bounded};

const MAX_SIGNATURE_BYTES: usize = 64;
const MAX_MANIFEST_BYTES: usize = 1024 * 1024;

/// Default decompressed-size limit for each binary or auxiliary payload
/// verified by [`verify`]. Call [`verify_bounded`] to select another limit.
pub const DEFAULT_MAX_VERIFIED_ENTRY_BYTES: usize = 64 * 1024 * 1024;

/// Default maximum number of signed payload entries in one package.
pub const DEFAULT_MAX_VERIFIED_ENTRIES: usize = 1024;

/// Decompression limits applied while verifying signed package payloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PackageVerificationLimits {
    /// Maximum decompressed bytes for any one payload entry.
    pub max_entry_bytes: usize,
    /// Maximum cumulative decompressed bytes across the binary, resources,
    /// proto files, and optional lock file.
    pub max_total_bytes: usize,
    /// Maximum number of those payload entries, including the binary.
    pub max_entries: usize,
}

impl PackageVerificationLimits {
    /// Derive package verification limits from Hyper's component-byte limit.
    ///
    /// The component limit caps both each entry and the cumulative signed
    /// payload set; the fixed entry-count ceiling prevents manifests from
    /// multiplying work through thousands of tiny or duplicate entries.
    pub const fn from_max_component_bytes(max_component_bytes: usize) -> Self {
        Self {
            max_entry_bytes: max_component_bytes,
            max_total_bytes: max_component_bytes,
            max_entries: DEFAULT_MAX_VERIFIED_ENTRIES,
        }
    }
}

impl Default for PackageVerificationLimits {
    fn default() -> Self {
        Self::from_max_component_bytes(DEFAULT_MAX_VERIFIED_ENTRY_BYTES)
    }
}

/// Result of a successful package verification.
///
/// Contains the parsed manifest along with the raw bytes needed for
/// transparent forwarding to AIS for signature verification.
#[derive(Debug, Clone)]
pub struct VerifiedPackage {
    /// Parsed package manifest.
    pub manifest: PackageManifest,
    /// Raw `manifest.toml` bytes as stored in the ZIP (the signed payload).
    pub manifest_raw: Vec<u8>,
    /// Raw `manifest.sig` bytes (64-byte Ed25519 signature).
    pub sig_raw: Vec<u8>,
}

/// Verify an .actr package with a safe default payload limit.
///
/// Each binary, resource, proto, and lock-file entry is limited to
/// [`DEFAULT_MAX_VERIFIED_ENTRY_BYTES`] of decompressed data. Payloads are
/// hashed as streams instead of being buffered in memory. Use
/// [`verify_bounded`] when the embedding runtime has its own package limit.
///
/// Verification flow:
/// 1. Read manifest.sig (64 bytes raw Ed25519 signature)
/// 2. Read manifest.toml (raw bytes)
/// 3. Verify Ed25519 signature over manifest.toml bytes
/// 4. Parse manifest.toml -> PackageManifest
/// 5. Read binary, verify SHA-256 matches manifest.binary.hash
/// 6. For each resource, verify SHA-256 matches entry hash
/// 7. For each proto file, verify SHA-256 matches entry hash
/// 8. For the optional packaged lock file, verify SHA-256 matches manifest.lock_file.hash
/// 9. Return VerifiedPackage with manifest + raw bytes
pub fn verify(actr_bytes: &[u8], pubkey: &VerifyingKey) -> Result<VerifiedPackage, PackError> {
    verify_with_limits(actr_bytes, pubkey, PackageVerificationLimits::default())
}

/// Verify an .actr package using one configured component/payload budget.
///
/// `max_component_bytes` caps both each individual payload and their
/// cumulative decompressed size. At most [`DEFAULT_MAX_VERIFIED_ENTRIES`]
/// payload entries are accepted. Use [`verify_with_limits`] to tune these
/// dimensions independently.
pub fn verify_bounded(
    actr_bytes: &[u8],
    pubkey: &VerifyingKey,
    max_component_bytes: usize,
) -> Result<VerifiedPackage, PackError> {
    verify_with_limits(
        actr_bytes,
        pubkey,
        PackageVerificationLimits::from_max_component_bytes(max_component_bytes),
    )
}

/// Verify an .actr package with explicit decompression and entry-count limits.
///
/// `manifest.sig` is always capped at 64 bytes and `manifest.toml` at 1 MiB.
/// The remaining signed payloads are metadata-checked against `limits` before
/// any of them is decompressed, then hashed incrementally under the same
/// cumulative budget.
pub fn verify_with_limits(
    actr_bytes: &[u8],
    pubkey: &VerifyingKey,
    limits: PackageVerificationLimits,
) -> Result<VerifiedPackage, PackError> {
    let cursor = Cursor::new(actr_bytes);
    let mut archive = zip::ZipArchive::new(cursor)?;

    // 1. Read manifest.sig
    let sig_raw = read_zip_entry_bounded(&mut archive, "manifest.sig", MAX_SIGNATURE_BYTES)
        .map_err(|error| required_entry(error, PackError::SignatureNotFound))?;
    if sig_raw.len() != 64 {
        return Err(PackError::SignatureVerificationFailed(format!(
            "manifest.sig must be exactly 64 bytes, got {}",
            sig_raw.len()
        )));
    }
    let sig_arr: [u8; 64] = sig_raw.clone().try_into().unwrap();
    let signature = Signature::from_bytes(&sig_arr);

    // 2. Read manifest.toml
    let manifest_bytes = read_zip_entry_bounded(&mut archive, "manifest.toml", MAX_MANIFEST_BYTES)
        .map_err(|error| required_entry(error, PackError::ManifestNotFound))?;

    // 3. Verify signature over manifest.toml
    pubkey
        .verify_strict(&manifest_bytes, &signature)
        .map_err(|e| {
            PackError::SignatureVerificationFailed(format!("Ed25519 verification failed: {e}"))
        })?;

    tracing::debug!("package signature verified");

    // 4. Parse manifest
    let manifest_str = std::str::from_utf8(&manifest_bytes)
        .map_err(|e| PackError::ManifestParseError(format!("manifest is not valid UTF-8: {e}")))?;
    let manifest = PackageManifest::from_toml(manifest_str)?;
    validate_manifest_integrity_fields(&manifest)?;

    // Reject excessive declared work before decompressing any payload. Count
    // duplicate manifest paths separately because verification would otherwise
    // decompress and hash each occurrence separately.
    let mut preflight = PayloadPreflight::new(limits);
    preflight.check(
        &mut archive,
        &manifest.binary.path,
        PackError::BinaryNotFound(manifest.binary.path.clone()),
    )?;
    for resource in &manifest.resources {
        preflight.check(
            &mut archive,
            &resource.path,
            PackError::BinaryNotFound(resource.path.clone()),
        )?;
    }
    for proto in &manifest.proto_files {
        preflight.check(
            &mut archive,
            &proto.path,
            PackError::BinaryNotFound(proto.path.clone()),
        )?;
    }
    if let Some(lock_file) = &manifest.lock_file {
        preflight.check(
            &mut archive,
            &lock_file.path,
            PackError::BinaryNotFound(lock_file.path.clone()),
        )?;
    }
    let mut budget = PayloadBudget::new(limits);

    // 5. Verify binary hash
    let computed_hash = budget.hash(
        &mut archive,
        &manifest.binary.path,
        PackError::BinaryNotFound(manifest.binary.path.clone()),
    )?;
    if computed_hash != manifest.binary.hash {
        tracing::warn!(
            expected = %manifest.binary.hash,
            computed = %computed_hash,
            path = %manifest.binary.path,
            "binary hash mismatch"
        );
        return Err(PackError::BinaryHashMismatch {
            path: manifest.binary.path.clone(),
        });
    }

    // 6. Verify resource hashes
    for resource in &manifest.resources {
        let computed = budget.hash(
            &mut archive,
            &resource.path,
            PackError::BinaryNotFound(resource.path.clone()),
        )?;
        if computed != resource.hash {
            tracing::warn!(
                expected = %resource.hash,
                computed = %computed,
                path = %resource.path,
                "resource hash mismatch"
            );
            return Err(PackError::ResourceHashMismatch {
                path: resource.path.clone(),
            });
        }
    }
    // 7. Verify proto file hashes
    for proto in &manifest.proto_files {
        let computed = budget.hash(
            &mut archive,
            &proto.path,
            PackError::BinaryNotFound(proto.path.clone()),
        )?;
        if computed != proto.hash {
            tracing::warn!(
                expected = %proto.hash,
                computed = %computed,
                path = %proto.path,
                "proto file hash mismatch"
            );
            return Err(PackError::ProtoHashMismatch {
                path: proto.path.clone(),
            });
        }
    }

    // 8. Verify packaged manifest.lock.toml hash when present
    if let Some(lock_file) = &manifest.lock_file {
        let computed = budget.hash(
            &mut archive,
            &lock_file.path,
            PackError::BinaryNotFound(lock_file.path.clone()),
        )?;
        if computed != lock_file.hash {
            tracing::warn!(
                expected = %lock_file.hash,
                computed = %computed,
                path = %lock_file.path,
                "manifest lock hash mismatch"
            );
            return Err(PackError::LockFileHashMismatch {
                path: lock_file.path.clone(),
            });
        }
    }

    tracing::info!(
        actr_type = %manifest.actr_type_str(),
        "package verification passed"
    );

    Ok(VerifiedPackage {
        manifest,
        manifest_raw: manifest_bytes,
        sig_raw,
    })
}

fn validate_manifest_integrity_fields(manifest: &PackageManifest) -> Result<(), PackError> {
    if manifest.signature_algorithm != "ed25519" {
        return Err(PackError::ManifestParseError(
            "signature_algorithm must be `ed25519`".to_string(),
        ));
    }
    validate_sha256("binary.hash", &manifest.binary.hash)?;
    for resource in &manifest.resources {
        validate_sha256("resources[].hash", &resource.hash)?;
    }
    for proto in &manifest.proto_files {
        validate_sha256("proto_files[].hash", &proto.hash)?;
    }
    if let Some(lock_file) = &manifest.lock_file {
        validate_sha256("lock_file.hash", &lock_file.hash)?;
    }
    Ok(())
}

fn validate_sha256(field: &'static str, value: &str) -> Result<(), PackError> {
    if value.len() != 64 || !value.as_bytes().iter().all(u8::is_ascii_hexdigit) {
        return Err(PackError::ManifestParseError(format!(
            "{field} must be a 64-character SHA-256 hex string"
        )));
    }
    Ok(())
}

struct PayloadPreflight {
    limits: PackageVerificationLimits,
    entries: usize,
    total_bytes: usize,
}

impl PayloadPreflight {
    fn new(limits: PackageVerificationLimits) -> Self {
        Self {
            limits,
            entries: 0,
            total_bytes: 0,
        }
    }

    fn check<R: std::io::Read + std::io::Seek>(
        &mut self,
        archive: &mut zip::ZipArchive<R>,
        name: &str,
        missing: PackError,
    ) -> Result<(), PackError> {
        self.entries = self.entries.checked_add(1).ok_or_else(|| {
            PackError::InvalidPackage("signed payload entry count overflows usize".to_string())
        })?;
        if self.entries > self.limits.max_entries {
            return Err(PackError::InvalidPackage(format!(
                "signed payload has {} entries, exceeds limit {}",
                self.entries, self.limits.max_entries
            )));
        }

        let entry = archive
            .by_name(name)
            .map_err(PackError::from)
            .map_err(|error| required_entry(error, missing))?;
        let size = usize::try_from(entry.size()).map_err(|_| {
            PackError::InvalidPackage(format!(
                "ZIP entry `{name}` declares {} bytes, cannot fit in usize",
                entry.size()
            ))
        })?;
        if size > self.limits.max_entry_bytes {
            return Err(PackError::InvalidPackage(format!(
                "ZIP entry `{name}` declares {size} bytes, exceeds limit {}",
                self.limits.max_entry_bytes
            )));
        }
        self.total_bytes = self.total_bytes.checked_add(size).ok_or_else(|| {
            PackError::InvalidPackage("signed payload size overflows usize".to_string())
        })?;
        if self.total_bytes > self.limits.max_total_bytes {
            return Err(PackError::InvalidPackage(format!(
                "signed payload declares {} cumulative bytes at `{name}`, exceeds limit {}",
                self.total_bytes, self.limits.max_total_bytes
            )));
        }
        Ok(())
    }
}

struct PayloadBudget {
    limits: PackageVerificationLimits,
    consumed: usize,
}

impl PayloadBudget {
    fn new(limits: PackageVerificationLimits) -> Self {
        Self {
            limits,
            consumed: 0,
        }
    }

    fn hash<R: std::io::Read + std::io::Seek>(
        &mut self,
        archive: &mut zip::ZipArchive<R>,
        name: &str,
        missing: PackError,
    ) -> Result<String, PackError> {
        let remaining = self.limits.max_total_bytes.saturating_sub(self.consumed);
        let allowed = self.limits.max_entry_bytes.min(remaining);
        let (hash, bytes) = sha256_zip_entry_bounded(archive, name, allowed)
            .map_err(|error| required_entry(error, missing))?;
        self.consumed = self.consumed.checked_add(bytes).ok_or_else(|| {
            PackError::InvalidPackage("signed payload size overflows usize".to_string())
        })?;
        Ok(hash)
    }
}

fn required_entry(error: PackError, missing: PackError) -> PackError {
    match error {
        PackError::ZipError(zip::result::ZipError::FileNotFound) => missing,
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{BinaryEntry, ManifestMetadata, PackageManifest, ResourceEntry};
    use crate::pack::{PackOptions, pack};
    use crate::util::sha256_hex;
    use ed25519_dalek::{Signer, SigningKey};
    use rand::rngs::OsRng;
    use std::io::{Cursor, Write};

    fn test_manifest() -> PackageManifest {
        PackageManifest {
            manufacturer: "test-mfr".to_string(),
            name: "TestActor".to_string(),
            version: "1.0.0".to_string(),
            binary: BinaryEntry {
                path: "bin/actor.wasm".to_string(),
                target: "wasm32-wasip1".to_string(),
                hash: String::new(),
                size: None,
                kind: None,
            },
            signature_algorithm: "ed25519".to_string(),
            signing_key_id: None,
            resources: vec![],
            proto_files: vec![],
            lock_file: None,
            metadata: ManifestMetadata::default(),
        }
    }

    fn make_package(
        signing_key: &SigningKey,
        binary: &[u8],
        resources: Vec<(String, Vec<u8>)>,
    ) -> Vec<u8> {
        let mut manifest = test_manifest();
        manifest.resources = resources
            .iter()
            .map(|(path, _)| ResourceEntry {
                path: path.clone(),
                hash: String::new(),
            })
            .collect();
        let opts = PackOptions {
            manifest,
            binary_bytes: binary.to_vec(),
            resources,
            proto_files: vec![],
            signing_key: signing_key.clone(),
            lock_file: None,
        };
        pack(&opts).unwrap()
    }

    fn make_deflated_package(
        signing_key: &SigningKey,
        binary: &[u8],
        resources: &[(String, Vec<u8>)],
    ) -> Vec<u8> {
        let mut manifest = test_manifest();
        manifest.binary.hash = sha256_hex(binary);
        manifest.binary.size = Some(binary.len() as u64);
        manifest.resources = resources
            .iter()
            .map(|(path, bytes)| ResourceEntry {
                path: path.clone(),
                hash: sha256_hex(bytes),
            })
            .collect();

        let manifest_toml = manifest.to_toml().unwrap();
        let signature = signing_key.sign(manifest_toml.as_bytes()).to_bytes();
        let cursor = Cursor::new(Vec::new());
        let mut zip = zip::ZipWriter::new(cursor);
        let options = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated);

        zip.start_file("manifest.toml", options).unwrap();
        zip.write_all(manifest_toml.as_bytes()).unwrap();
        zip.start_file("manifest.sig", options).unwrap();
        zip.write_all(&signature).unwrap();
        zip.start_file(&manifest.binary.path, options).unwrap();
        zip.write_all(binary).unwrap();
        for (path, bytes) in resources {
            zip.start_file(path, options).unwrap();
            zip.write_all(bytes).unwrap();
        }
        zip.finish().unwrap().into_inner()
    }

    #[test]
    fn roundtrip_succeeds() {
        let key = SigningKey::generate(&mut OsRng);
        let pkg = make_package(&key, b"wasm bytes", vec![]);
        let result = verify(&pkg, &key.verifying_key()).unwrap();
        assert_eq!(result.manifest.manufacturer, "test-mfr");
        assert_eq!(result.manifest.name, "TestActor");
        assert_eq!(result.sig_raw.len(), 64);
        assert!(!result.manifest_raw.is_empty());
    }

    #[test]
    fn tampered_binary_detected() {
        let key = SigningKey::generate(&mut OsRng);
        let pkg_bytes = make_package(&key, b"original", vec![]);
        // Modify a byte deep in the file (in the binary data area)
        let mut tampered = pkg_bytes.clone();
        // Find "original" in the ZIP and change it
        if let Some(pos) = tampered.windows(8).position(|w| w == b"original") {
            tampered[pos] ^= 0xFF;
        }
        let result = verify(&tampered, &key.verifying_key());
        // Should fail with either signature or hash mismatch
        assert!(
            result.is_err(),
            "tampered package should fail: {:?}",
            result
        );
    }

    #[test]
    fn wrong_key_rejected() {
        let key1 = SigningKey::generate(&mut OsRng);
        let key2 = SigningKey::generate(&mut OsRng);
        let pkg = make_package(&key1, b"wasm", vec![]);
        let result = verify(&pkg, &key2.verifying_key());
        assert!(matches!(
            result,
            Err(PackError::SignatureVerificationFailed(_))
        ));
    }

    #[test]
    fn missing_signature_detected() {
        // Create a ZIP without manifest.sig
        let cursor = std::io::Cursor::new(Vec::new());
        let mut zip = zip::ZipWriter::new(cursor);
        let opts = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Stored);
        zip.start_file("manifest.toml", opts).unwrap();
        zip.write_all(b"[fake]").unwrap();
        let data = zip.finish().unwrap().into_inner();

        let key = SigningKey::generate(&mut OsRng);
        let result = verify(&data, &key.verifying_key());
        assert!(matches!(result, Err(PackError::SignatureNotFound)));
    }

    #[test]
    fn resource_hash_mismatch_detected() {
        let key = SigningKey::generate(&mut OsRng);
        let pkg = make_package(
            &key,
            b"wasm",
            vec![(
                "config/settings.toml".to_string(),
                b"key = \"value\"".to_vec(),
            )],
        );
        // Tamper the resource
        let mut tampered = pkg.clone();
        if let Some(pos) = tampered.windows(5).position(|w| w == b"value") {
            tampered[pos] ^= 0xFF;
        }
        let result = verify(&tampered, &key.verifying_key());
        assert!(result.is_err());
    }

    #[test]
    fn with_resources_roundtrip() {
        let key = SigningKey::generate(&mut OsRng);
        let resources = vec![
            ("config/a.toml".to_string(), b"data_a".to_vec()),
            ("config/b.toml".to_string(), b"data_b".to_vec()),
        ];
        let pkg = make_package(&key, b"wasm", resources);
        let result = verify(&pkg, &key.verifying_key()).unwrap();
        assert_eq!(result.manifest.resources.len(), 2);
    }

    #[test]
    fn bounded_verify_rejects_deflated_binary_bomb() {
        let key = SigningKey::generate(&mut OsRng);
        let binary = vec![0u8; 128 * 1024];
        let pkg = make_deflated_package(&key, &binary, &[]);
        assert!(
            pkg.len() < binary.len() / 4,
            "fixture should be highly compressed"
        );

        let error = verify_bounded(&pkg, &key.verifying_key(), 1024).unwrap_err();
        assert!(
            matches!(&error, PackError::InvalidPackage(message) if
                message.contains("bin/actor.wasm") && message.contains("exceeds limit 1024")),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn manifest_integrity_fields_are_strictly_validated() {
        let mut manifest = test_manifest();
        manifest.signature_algorithm = "rsa".to_string();
        let error = validate_manifest_integrity_fields(&manifest).unwrap_err();
        assert!(matches!(&error, PackError::ManifestParseError(message) if
            message.contains("signature_algorithm")));

        manifest.signature_algorithm = "ed25519".to_string();
        manifest.binary.hash = "00".repeat(32);
        manifest.resources = vec![ResourceEntry {
            path: "resources/a.bin".to_string(),
            hash: "not-a-sha256".to_string(),
        }];
        let error = validate_manifest_integrity_fields(&manifest).unwrap_err();
        assert!(matches!(&error, PackError::ManifestParseError(message) if
            message.contains("resources[].hash")));
    }

    #[test]
    fn bounded_verify_rejects_deflated_resource_bomb() {
        let key = SigningKey::generate(&mut OsRng);
        let resource = vec![0u8; 128 * 1024];
        let resources = vec![("resources/bomb.bin".to_string(), resource.clone())];
        let pkg = make_deflated_package(&key, b"wasm", &resources);
        assert!(
            pkg.len() < resource.len() / 4,
            "fixture should be highly compressed"
        );

        let error = verify_bounded(&pkg, &key.verifying_key(), 1024).unwrap_err();
        assert!(
            matches!(&error, PackError::InvalidPackage(message) if
                message.contains("resources/bomb.bin") && message.contains("exceeds limit 1024")),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn bounded_verify_rejects_cumulative_payload_bomb() {
        let key = SigningKey::generate(&mut OsRng);
        let resources = vec![("resources/second.bin".to_string(), vec![0u8; 700])];
        let pkg = make_deflated_package(&key, &[0u8; 700], &resources);

        let error = verify_bounded(&pkg, &key.verifying_key(), 1024).unwrap_err();
        assert!(
            matches!(&error, PackError::InvalidPackage(message) if
                message.contains("cumulative bytes") && message.contains("exceeds limit 1024")),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn verify_with_limits_rejects_excessive_payload_entry_count() {
        let key = SigningKey::generate(&mut OsRng);
        let resources = vec![
            ("resources/a.bin".to_string(), b"a".to_vec()),
            ("resources/b.bin".to_string(), b"b".to_vec()),
        ];
        let pkg = make_deflated_package(&key, b"wasm", &resources);
        let limits = PackageVerificationLimits {
            max_entry_bytes: 1024,
            max_total_bytes: 1024,
            max_entries: 2,
        };

        let error = verify_with_limits(&pkg, &key.verifying_key(), limits).unwrap_err();
        assert!(
            matches!(&error, PackError::InvalidPackage(message) if
                message.contains("3 entries") && message.contains("exceeds limit 2")),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn verify_rejects_deflated_oversized_signature() {
        let cursor = Cursor::new(Vec::new());
        let mut zip = zip::ZipWriter::new(cursor);
        let options = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated);
        zip.start_file("manifest.sig", options).unwrap();
        zip.write_all(&vec![0u8; 128 * 1024]).unwrap();
        let pkg = zip.finish().unwrap().into_inner();

        let key = SigningKey::generate(&mut OsRng);
        let error = verify(&pkg, &key.verifying_key()).unwrap_err();
        assert!(
            matches!(&error, PackError::InvalidPackage(message) if
                message.contains("manifest.sig") && message.contains("exceeds limit 64")),
            "unexpected error: {error:?}"
        );
    }

    fn make_package_with_protos(
        signing_key: &SigningKey,
        binary: &[u8],
        protos: Vec<(String, Vec<u8>)>,
    ) -> Vec<u8> {
        use crate::manifest::ProtoFileEntry;
        let manifest = test_manifest();
        let proto_entries: Vec<ProtoFileEntry> = protos
            .iter()
            .map(|(name, _)| ProtoFileEntry {
                name: name.clone(),
                path: format!("proto/{name}"),
                hash: String::new(),
            })
            .collect();
        let mut m = manifest;
        m.proto_files = proto_entries;
        // PackOptions.proto_files uses (name, data) where name is the raw filename
        // pack() internally creates path as "proto/{name}" and writes to ZIP at that path
        let opts = PackOptions {
            manifest: m,
            binary_bytes: binary.to_vec(),
            resources: vec![],
            proto_files: protos,
            signing_key: signing_key.clone(),
            lock_file: None,
        };
        pack(&opts).unwrap()
    }

    #[test]
    fn with_proto_files_roundtrip() {
        let key = SigningKey::generate(&mut OsRng);
        let protos = vec![
            (
                "echo.proto".to_string(),
                b"syntax = \"proto3\";\nservice Echo {}".to_vec(),
            ),
            (
                "common.proto".to_string(),
                b"syntax = \"proto3\";\nmessage Empty {}".to_vec(),
            ),
        ];
        let pkg = make_package_with_protos(&key, b"wasm", protos);
        let result = verify(&pkg, &key.verifying_key()).unwrap();
        assert_eq!(result.manifest.proto_files.len(), 2);
        assert_eq!(result.manifest.proto_files[0].name, "echo.proto");
        assert_eq!(result.manifest.proto_files[1].name, "common.proto");
    }

    #[test]
    fn tampered_proto_detected() {
        let key = SigningKey::generate(&mut OsRng);
        let protos = vec![(
            "echo.proto".to_string(),
            b"syntax = \"proto3\";\nservice Echo {}".to_vec(),
        )];
        let pkg = make_package_with_protos(&key, b"wasm", protos);
        // Tamper the proto content in the ZIP
        let mut tampered = pkg.clone();
        let needle = b"Echo";
        if let Some(pos) = tampered.windows(needle.len()).position(|w| w == needle) {
            tampered[pos] ^= 0xFF;
        }
        let result = verify(&tampered, &key.verifying_key());
        assert!(result.is_err(), "tampered proto should fail: {:?}", result);
    }
}
