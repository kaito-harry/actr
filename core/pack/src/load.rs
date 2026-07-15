use std::io::Cursor;

use crate::error::PackError;
use crate::manifest::PackageManifest;
use crate::util::read_zip_entry_bounded;

const MAX_MANIFEST_BYTES: usize = 1024 * 1024;

/// Read the manifest from an .actr package without full verification.
pub fn read_manifest(actr_bytes: &[u8]) -> Result<PackageManifest, PackError> {
    let manifest_str = read_manifest_raw(actr_bytes)?;
    PackageManifest::from_toml(&manifest_str)
}

/// Read the raw manifest TOML string from an .actr package.
///
/// Returns the exact bytes stored in the package as a UTF-8 string,
/// preserving the original text for signing purposes.
pub fn read_manifest_raw(actr_bytes: &[u8]) -> Result<String, PackError> {
    let cursor = Cursor::new(actr_bytes);
    let mut archive = zip::ZipArchive::new(cursor)?;

    let manifest_bytes = read_zip_entry_bounded(&mut archive, "manifest.toml", MAX_MANIFEST_BYTES)
        .map_err(|e| match e {
            PackError::ZipError(zip::result::ZipError::FileNotFound) => PackError::ManifestNotFound,
            other => other,
        })?;

    String::from_utf8(manifest_bytes)
        .map_err(|e| PackError::ManifestParseError(format!("manifest is not valid UTF-8: {e}")))
}

/// Load the binary bytes from an .actr package.
///
/// Reads the manifest to determine the binary path, then extracts the binary
/// under [`crate::DEFAULT_MAX_VERIFIED_ENTRY_BYTES`]. Use
/// [`load_binary_bounded`] to choose a runtime-specific limit.
pub fn load_binary(actr_bytes: &[u8]) -> Result<Vec<u8>, PackError> {
    load_binary_bounded(actr_bytes, crate::verify::DEFAULT_MAX_VERIFIED_ENTRY_BYTES)
}

/// Load the packaged binary while bounding both the manifest and decompressed
/// binary before allocation. This is the untrusted-package loading path used
/// by native WASM execution.
pub fn load_binary_bounded(
    actr_bytes: &[u8],
    max_binary_bytes: usize,
) -> Result<Vec<u8>, PackError> {
    let cursor = Cursor::new(actr_bytes);
    let mut archive = zip::ZipArchive::new(cursor)?;

    let manifest_bytes = read_zip_entry_bounded(&mut archive, "manifest.toml", MAX_MANIFEST_BYTES)
        .map_err(|e| match e {
            PackError::ZipError(zip::result::ZipError::FileNotFound) => PackError::ManifestNotFound,
            other => other,
        })?;
    let manifest_str = std::str::from_utf8(&manifest_bytes)
        .map_err(|e| PackError::ManifestParseError(format!("manifest is not valid UTF-8: {e}")))?;
    let manifest = PackageManifest::from_toml(manifest_str)?;

    read_zip_entry_bounded(&mut archive, &manifest.binary.path, max_binary_bytes).map_err(|e| {
        match e {
            PackError::ZipError(zip::result::ZipError::FileNotFound) => {
                PackError::BinaryNotFound(manifest.binary.path.clone())
            }
            other => other,
        }
    })
}

/// Read the raw 64-byte Ed25519 signature from an .actr package.
pub fn read_signature(actr_bytes: &[u8]) -> Result<Vec<u8>, PackError> {
    let cursor = Cursor::new(actr_bytes);
    let mut archive = zip::ZipArchive::new(cursor)?;
    let sig =
        read_zip_entry_bounded(&mut archive, "manifest.sig", 64).map_err(|error| match error {
            PackError::ZipError(zip::result::ZipError::FileNotFound) => {
                PackError::SignatureNotFound
            }
            other => other,
        })?;
    if sig.len() != 64 {
        return Err(PackError::SignatureVerificationFailed(format!(
            "manifest.sig must be exactly 64 bytes, got {}",
            sig.len()
        )));
    }
    Ok(sig)
}

/// Read manifest.lock.toml from an .actr package (if present).
///
/// Returns None if the package does not contain a lock file.
pub fn read_lock_file(actr_bytes: &[u8]) -> Result<Option<Vec<u8>>, PackError> {
    let manifest = read_manifest(actr_bytes)?;
    let Some(lock_file) = manifest.lock_file else {
        return Ok(None);
    };
    read_lock_file_bounded(
        actr_bytes,
        &lock_file.path,
        crate::verify::DEFAULT_MAX_VERIFIED_ENTRY_BYTES,
    )
}

/// Read the packaged lock file at `path` with a decompressed-size limit.
///
/// Callers must still consult the verified manifest before using the result:
/// an unlisted ZIP entry is not authenticated merely because it has the
/// conventional `manifest.lock.toml` name. Verified-package consumers should
/// pass `manifest.lock_file.path`.
pub fn read_lock_file_bounded(
    actr_bytes: &[u8],
    path: &str,
    max_bytes: usize,
) -> Result<Option<Vec<u8>>, PackError> {
    let cursor = Cursor::new(actr_bytes);
    let mut archive = zip::ZipArchive::new(cursor)?;
    match read_zip_entry_bounded(&mut archive, path, max_bytes) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(PackError::ZipError(zip::result::ZipError::FileNotFound)) => Ok(None),
        Err(error) => Err(error),
    }
}

/// Read a single manifest-declared JS glue file from an .actr package.
///
/// Returns the first `.js` file under `resources/` that is NOT named
/// `actor.sw.js` (which is the Service Worker runtime itself, not guest
/// wasm-bindgen glue). Returns `Ok(None)` when the package has no eligible
/// glue script.
///
/// The resource path is selected from `manifest.resources`, never from raw ZIP
/// names, so an unsigned entry appended to an otherwise valid package is
/// ignored. Used by the Web runtime after signature verification.
pub fn read_glue_js(actr_bytes: &[u8]) -> Result<Option<String>, PackError> {
    let manifest = read_manifest(actr_bytes)?;
    read_glue_js_bounded(
        actr_bytes,
        &manifest,
        crate::verify::DEFAULT_MAX_VERIFIED_ENTRY_BYTES,
    )
}

/// Read the manifest-declared JS glue file under a decompressed-size limit.
///
/// Callers that already verified a package should pass the returned verified
/// manifest, ensuring the selected path and hash are authenticated.
pub fn read_glue_js_bounded(
    actr_bytes: &[u8],
    manifest: &PackageManifest,
    max_bytes: usize,
) -> Result<Option<String>, PackError> {
    let target = manifest
        .resources
        .iter()
        .map(|resource| resource.path.as_str())
        .find(|name| {
            name.starts_with("resources/")
                && name.ends_with(".js")
                && !name.ends_with("actor.sw.js")
        });

    let Some(name) = target else {
        return Ok(None);
    };

    let cursor = Cursor::new(actr_bytes);
    let mut archive = zip::ZipArchive::new(cursor)?;
    let content = read_zip_entry_bounded(&mut archive, name, max_bytes)?;
    String::from_utf8(content)
        .map(Some)
        .map_err(|error| PackError::InvalidPackage(format!("read `{name}` as UTF-8: {error}")))
}

/// Read all manifest-declared proto files from an .actr package.
///
/// Returns a list of (filename, content) pairs.
/// Returns an empty vec if the package has no proto files.
pub fn read_proto_files(actr_bytes: &[u8]) -> Result<Vec<(String, Vec<u8>)>, PackError> {
    let manifest = read_manifest(actr_bytes)?;
    read_proto_files_bounded(
        actr_bytes,
        &manifest,
        crate::verify::DEFAULT_MAX_VERIFIED_ENTRY_BYTES,
    )
}

/// Read manifest-declared proto files under one cumulative decompression
/// budget. Selecting paths from a verified manifest prevents unsigned ZIP
/// entries from being forwarded by package consumers.
pub fn read_proto_files_bounded(
    actr_bytes: &[u8],
    manifest: &PackageManifest,
    max_total_bytes: usize,
) -> Result<Vec<(String, Vec<u8>)>, PackError> {
    if manifest.proto_files.len() > crate::verify::DEFAULT_MAX_VERIFIED_ENTRIES {
        return Err(PackError::InvalidPackage(format!(
            "manifest declares {} proto files, exceeds limit {}",
            manifest.proto_files.len(),
            crate::verify::DEFAULT_MAX_VERIFIED_ENTRIES
        )));
    }
    let cursor = Cursor::new(actr_bytes);
    let mut archive = zip::ZipArchive::new(cursor)?;
    let mut result = Vec::new();
    let mut total = 0usize;
    for proto in &manifest.proto_files {
        let full_path = &proto.path;
        let remaining = max_total_bytes.saturating_sub(total);
        let content = read_zip_entry_bounded(&mut archive, full_path, remaining).map_err(
            |error| match error {
                PackError::ZipError(zip::result::ZipError::FileNotFound) => {
                    PackError::BinaryNotFound(full_path.clone())
                }
                other => other,
            },
        )?;
        total = total.checked_add(content.len()).ok_or_else(|| {
            PackError::InvalidPackage("proto payload size overflows usize".to_string())
        })?;
        result.push((proto.name.clone(), content));
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{BinaryEntry, BinaryKind, ManifestMetadata};
    use std::io::{Cursor, Write};

    fn manifest_with_resources(resources: Vec<crate::manifest::ResourceEntry>) -> PackageManifest {
        PackageManifest {
            manufacturer: "test".to_string(),
            name: "fixture".to_string(),
            version: "0.1.0".to_string(),
            binary: BinaryEntry {
                path: "bin/actor.wasm".to_string(),
                target: "wasm32-wasip2".to_string(),
                hash: "00".repeat(32),
                size: Some(4),
                kind: Some(BinaryKind::Component),
            },
            signature_algorithm: "ed25519".to_string(),
            signing_key_id: None,
            resources,
            proto_files: Vec::new(),
            lock_file: None,
            metadata: ManifestMetadata::default(),
        }
    }

    #[test]
    fn read_manifest_rejects_deflated_oversized_entry() {
        let oversized_manifest = vec![b' '; MAX_MANIFEST_BYTES + 1];
        let cursor = Cursor::new(Vec::new());
        let mut zip = zip::ZipWriter::new(cursor);
        let options = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated);
        zip.start_file("manifest.toml", options).unwrap();
        zip.write_all(&oversized_manifest).unwrap();
        let package = zip.finish().unwrap().into_inner();
        assert!(
            package.len() < oversized_manifest.len() / 100,
            "fixture should be highly compressed"
        );

        let error = read_manifest(&package).unwrap_err();
        assert!(
            matches!(&error, PackError::InvalidPackage(message) if
                message.contains("manifest.toml") && message.contains("exceeds limit")),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn read_lock_file_rejects_deflated_oversized_entry() {
        let oversized_lock = vec![b' '; 128 * 1024];
        let cursor = Cursor::new(Vec::new());
        let mut zip = zip::ZipWriter::new(cursor);
        let options = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated);
        zip.start_file("manifest.lock.toml", options).unwrap();
        zip.write_all(&oversized_lock).unwrap();
        let package = zip.finish().unwrap().into_inner();

        let error = read_lock_file_bounded(&package, "manifest.lock.toml", 1024).unwrap_err();
        assert!(
            matches!(&error, PackError::InvalidPackage(message) if
                message.contains("manifest.lock.toml") && message.contains("exceeds limit 1024")),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn load_binary_rejects_deflated_oversized_entry() {
        let manifest = manifest_with_resources(Vec::new()).to_toml().unwrap();
        let binary = vec![0u8; 128 * 1024];
        let cursor = Cursor::new(Vec::new());
        let mut zip = zip::ZipWriter::new(cursor);
        let options = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated);
        zip.start_file("manifest.toml", options).unwrap();
        zip.write_all(manifest.as_bytes()).unwrap();
        zip.start_file("bin/actor.wasm", options).unwrap();
        zip.write_all(&binary).unwrap();
        let package = zip.finish().unwrap().into_inner();

        let error = load_binary_bounded(&package, 1024).unwrap_err();
        assert!(
            matches!(&error, PackError::InvalidPackage(message) if
                message.contains("bin/actor.wasm") && message.contains("exceeds limit 1024")),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn read_glue_js_ignores_unsigned_extra_zip_entry() {
        let manifest = manifest_with_resources(Vec::new()).to_toml().unwrap();
        let extra = vec![b'x'; 128 * 1024];
        let cursor = Cursor::new(Vec::new());
        let mut zip = zip::ZipWriter::new(cursor);
        let options = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated);
        zip.start_file("manifest.toml", options).unwrap();
        zip.write_all(manifest.as_bytes()).unwrap();
        zip.start_file("resources/unsigned.js", options).unwrap();
        zip.write_all(&extra).unwrap();
        let package = zip.finish().unwrap().into_inner();

        assert_eq!(read_glue_js(&package).unwrap(), None);
    }

    #[test]
    fn read_glue_js_bounds_declared_resource() {
        let path = "resources/glue.js";
        let manifest = manifest_with_resources(vec![crate::manifest::ResourceEntry {
            path: path.to_string(),
            hash: "00".repeat(32),
        }]);
        let cursor = Cursor::new(Vec::new());
        let mut zip = zip::ZipWriter::new(cursor);
        let options = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated);
        zip.start_file(path, options).unwrap();
        zip.write_all(&vec![b'x'; 2048]).unwrap();
        let package = zip.finish().unwrap().into_inner();

        let error = read_glue_js_bounded(&package, &manifest, 1024).unwrap_err();
        assert!(
            matches!(&error, PackError::InvalidPackage(message) if
                message.contains(path) && message.contains("exceeds limit 1024")),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn read_lock_file_ignores_unsigned_extra_zip_entry() {
        let manifest = manifest_with_resources(Vec::new()).to_toml().unwrap();
        let cursor = Cursor::new(Vec::new());
        let mut zip = zip::ZipWriter::new(cursor);
        let options = zip::write::SimpleFileOptions::default();
        zip.start_file("manifest.toml", options).unwrap();
        zip.write_all(manifest.as_bytes()).unwrap();
        zip.start_file("manifest.lock.toml", options).unwrap();
        zip.write_all(b"unsigned = true").unwrap();
        let package = zip.finish().unwrap().into_inner();

        assert_eq!(read_lock_file(&package).unwrap(), None);
    }

    #[test]
    fn read_proto_files_ignores_unsigned_extra_zip_entry() {
        let manifest = manifest_with_resources(Vec::new()).to_toml().unwrap();
        let cursor = Cursor::new(Vec::new());
        let mut zip = zip::ZipWriter::new(cursor);
        let options = zip::write::SimpleFileOptions::default();
        zip.start_file("manifest.toml", options).unwrap();
        zip.write_all(manifest.as_bytes()).unwrap();
        zip.start_file("proto/unsigned.proto", options).unwrap();
        zip.write_all(b"syntax = \"proto3\";").unwrap();
        let package = zip.finish().unwrap().into_inner();

        assert!(read_proto_files(&package).unwrap().is_empty());
    }

    #[test]
    fn read_proto_files_bounds_cumulative_declared_payload() {
        let path = "proto/large.proto";
        let mut manifest = manifest_with_resources(Vec::new());
        manifest.proto_files = vec![crate::manifest::ProtoFileEntry {
            name: "large.proto".to_string(),
            path: path.to_string(),
            hash: "00".repeat(32),
        }];
        let cursor = Cursor::new(Vec::new());
        let mut zip = zip::ZipWriter::new(cursor);
        let options = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated);
        zip.start_file(path, options).unwrap();
        zip.write_all(&vec![b'x'; 2048]).unwrap();
        let package = zip.finish().unwrap().into_inner();

        let error = read_proto_files_bounded(&package, &manifest, 1024).unwrap_err();
        assert!(
            matches!(&error, PackError::InvalidPackage(message) if
                message.contains(path) && message.contains("exceeds limit 1024")),
            "unexpected error: {error:?}"
        );
    }
}
