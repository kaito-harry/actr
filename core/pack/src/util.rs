//! Internal helpers shared between load/verify/pack modules.

use std::io::{Read, Seek};

use sha2::{Digest, Sha256};

use crate::error::PackError;

/// Read a ZIP entry without allowing its declared or actual decompressed size
/// to exceed `max_bytes`.
pub(crate) fn read_zip_entry_bounded<R: Read + Seek>(
    archive: &mut zip::ZipArchive<R>,
    name: &str,
    max_bytes: usize,
) -> Result<Vec<u8>, PackError> {
    let entry = archive.by_name(name)?;
    if entry.size() > u64::try_from(max_bytes).unwrap_or(u64::MAX) {
        return Err(PackError::InvalidPackage(format!(
            "ZIP entry `{name}` declares {} bytes, exceeds limit {max_bytes}",
            entry.size()
        )));
    }
    let capacity = usize::try_from(entry.size())
        .unwrap_or(max_bytes)
        .min(max_bytes);
    let mut buf = Vec::with_capacity(capacity);
    let read_limit = u64::try_from(max_bytes)
        .unwrap_or(u64::MAX)
        .saturating_add(1);
    let mut limited = entry.take(read_limit);
    limited.read_to_end(&mut buf)?;
    if buf.len() > max_bytes {
        return Err(PackError::InvalidPackage(format!(
            "ZIP entry `{name}` exceeds decompressed limit {max_bytes}"
        )));
    }
    Ok(buf)
}

/// Hash a ZIP entry without buffering its decompressed contents, rejecting the
/// entry if either its declared or actual decompressed size exceeds
/// `max_bytes`.
pub(crate) fn sha256_zip_entry_bounded<R: Read + Seek>(
    archive: &mut zip::ZipArchive<R>,
    name: &str,
    max_bytes: usize,
) -> Result<(String, usize), PackError> {
    let entry = archive.by_name(name)?;
    if entry.size() > u64::try_from(max_bytes).unwrap_or(u64::MAX) {
        return Err(PackError::InvalidPackage(format!(
            "ZIP entry `{name}` declares {} bytes, exceeds limit {max_bytes}",
            entry.size()
        )));
    }

    let read_limit = u64::try_from(max_bytes)
        .unwrap_or(u64::MAX)
        .saturating_add(1);
    let mut limited = entry.take(read_limit);
    let mut hasher = Sha256::new();
    let mut total = 0usize;
    let mut chunk = [0u8; 16 * 1024];
    loop {
        let read = limited.read(&mut chunk)?;
        if read == 0 {
            break;
        }
        total = total.checked_add(read).ok_or_else(|| {
            PackError::InvalidPackage(format!(
                "ZIP entry `{name}` decompressed size overflows usize"
            ))
        })?;
        if total > max_bytes {
            return Err(PackError::InvalidPackage(format!(
                "ZIP entry `{name}` exceeds decompressed limit {max_bytes}"
            )));
        }
        hasher.update(&chunk[..read]);
    }
    Ok((hex::encode(hasher.finalize()), total))
}

/// Compute a lowercase hex-encoded SHA-256 digest of `data`.
pub(crate) fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}
