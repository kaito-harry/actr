//! Trust provider — pluggable verifier for `.actr` package signatures.
//!
//! Replaces the old `TrustMode` enum. A `TrustProvider` answers the only
//! question Hyper cares about at load time: "is this package bytes-authentic
//! enough for me to execute?". How it answers is up to the provider:
//!
//! - [`StaticTrust`] — one pre-configured Ed25519 public key, accepts any
//!   manufacturer. Offline / air-gapped deployments.
//! - [`RegistryTrust`] — fetch MFR public keys from an AIS HTTP registry by
//!   `(manufacturer, signing_key_id)`, cached locally.
//! - [`ChainTrust`] — try a list of providers in order; first success wins.
//!
//! Both built-in Ed25519-based providers delegate to [`actr_pack::verify`]
//! (or its configurable bounded variant), which performs the full signature +
//! binary-hash + resource-hash chain.
//! Custom providers (e.g. wasm-side keyless verification, HSM, threshold
//! signatures) may implement [`TrustProvider`] however they want — the trait
//! only obliges them to take raw bytes in and return a verified manifest out.

use std::sync::Arc;

use actr_pack::VerifiedPackage;
use async_trait::async_trait;
use ed25519_dalek::VerifyingKey;

use crate::error::{HyperError, HyperResult};
use crate::verify::cert_cache::MfrCertCache;

/// Verifier for `.actr` package signatures.
///
/// An implementation fully takes raw package bytes and returns the parsed,
/// trusted package — or errors. Callers must not use any field of the
/// returned [`VerifiedPackage`] before calling this.
#[async_trait]
pub trait TrustProvider: Send + Sync + std::fmt::Debug {
    async fn verify_package(&self, bytes: &[u8]) -> HyperResult<VerifiedPackage>;

    /// Verify while limiting decompression of signed package payloads. Hyper's
    /// workload-loading path always calls this method with its configured
    /// `max_component_bytes`; built-in providers use that value for both each
    /// entry and the cumulative payload set.
    ///
    /// The default preserves compatibility for custom providers. Providers
    /// that decompress package entries should override it and enforce the
    /// supplied limit, as the built-in providers do.
    async fn verify_package_bounded(
        &self,
        bytes: &[u8],
        _max_entry_bytes: usize,
    ) -> HyperResult<VerifiedPackage> {
        self.verify_package(bytes).await
    }
}

// ── shared helper for the Ed25519 + pubkey path ──────────────────────────────

/// Verify an `.actr` package against a single Ed25519 public key.
///
/// Shared helper used by [`StaticTrust`] and [`RegistryTrust`].
pub(crate) fn verify_ed25519_manifest(
    bytes: &[u8],
    pubkey: &VerifyingKey,
) -> HyperResult<VerifiedPackage> {
    let verified = actr_pack::verify(bytes, pubkey).map_err(pack_err_to_hyper)?;

    tracing::info!(
        actr_type = %verified.manifest.actr_type_str(),
        ".actr package verified"
    );

    Ok(verified)
}

fn verify_ed25519_manifest_bounded(
    bytes: &[u8],
    pubkey: &VerifyingKey,
    max_entry_bytes: usize,
) -> HyperResult<VerifiedPackage> {
    let verified =
        actr_pack::verify_bounded(bytes, pubkey, max_entry_bytes).map_err(pack_err_to_hyper)?;

    tracing::info!(
        actr_type = %verified.manifest.actr_type_str(),
        max_entry_bytes,
        ".actr package verified with decompression limits"
    );

    Ok(verified)
}

fn pack_err_to_hyper(e: actr_pack::PackError) -> HyperError {
    match e {
        actr_pack::PackError::SignatureVerificationFailed(msg) => {
            HyperError::SignatureVerificationFailed(msg)
        }
        actr_pack::PackError::BinaryHashMismatch { .. } => HyperError::BinaryHashMismatch,
        actr_pack::PackError::SignatureNotFound => {
            HyperError::SignatureVerificationFailed("signature not found in package".to_string())
        }
        actr_pack::PackError::BinaryNotFound(path) => {
            HyperError::InvalidManifest(format!("binary not found: {path}"))
        }
        actr_pack::PackError::ManifestNotFound => HyperError::ManifestNotFound,
        actr_pack::PackError::ManifestParseError(msg) => HyperError::InvalidManifest(msg),
        other => HyperError::InvalidManifest(other.to_string()),
    }
}

fn parse_pubkey(bytes: &[u8]) -> HyperResult<VerifyingKey> {
    let arr: [u8; 32] = bytes
        .try_into()
        .map_err(|_| HyperError::Config("Ed25519 pubkey must be exactly 32 bytes".to_string()))?;
    VerifyingKey::from_bytes(&arr)
        .map_err(|e| HyperError::Config(format!("invalid Ed25519 pubkey: {e}")))
}

// ── StaticTrust ──────────────────────────────────────────────────────────────

/// Pre-configured single Ed25519 public key. Accepts packages from any
/// manufacturer as long as they verify against this key.
///
/// Intended for dev / air-gapped / self-hosted deployments where the
/// manufacturer's public key is shipped alongside the package (typically as
/// `public-key.json`) instead of queried from a registry.
#[derive(Debug, Clone)]
pub struct StaticTrust {
    pubkey: VerifyingKey,
}

impl StaticTrust {
    /// Construct from 32 raw Ed25519 public key bytes.
    pub fn new(pubkey: impl AsRef<[u8]>) -> HyperResult<Self> {
        Ok(Self {
            pubkey: parse_pubkey(pubkey.as_ref())?,
        })
    }

    /// Development-only trust provider seeded with an all-zero Ed25519 public
    /// key. Accepts **no real package** (signatures against a zero key always
    /// fail), but lets test and example code wire a valid `TrustProvider`
    /// without pulling a real key file.
    ///
    /// Never use in production — the only reason this exists is so
    /// `Node::from_config_file` can distinguish an explicit opt-in to dev
    /// mode from a missing trust configuration (which is a hard error).
    /// Emits no warning of its own; callers should log at their discretion
    /// (`Node::from_config_file` emits a `tracing::warn!` when it selects
    /// this provider from a `kind = "dev_only"` config entry).
    pub fn dev_only() -> Self {
        // `from_bytes` accepts all-zero 32 bytes (it is a valid curve point,
        // just a broken one for signing) so `.unwrap()` here is sound.
        Self {
            pubkey: VerifyingKey::from_bytes(&[0u8; 32]).expect("all-zero pubkey parses"),
        }
    }
}

#[async_trait]
impl TrustProvider for StaticTrust {
    async fn verify_package(&self, bytes: &[u8]) -> HyperResult<VerifiedPackage> {
        verify_ed25519_manifest(bytes, &self.pubkey)
    }

    async fn verify_package_bounded(
        &self,
        bytes: &[u8],
        max_entry_bytes: usize,
    ) -> HyperResult<VerifiedPackage> {
        verify_ed25519_manifest_bounded(bytes, &self.pubkey, max_entry_bytes)
    }
}

// ── RegistryTrust ────────────────────────────────────────────────────────────

/// Resolve manufacturer public keys from an AIS HTTP registry and verify
/// Ed25519 signatures against them. Internal cache with configurable TTL
/// (default 1h).
///
/// The package manifest must carry `signing_key_id`; otherwise the provider
/// errors out — rebuild with the latest `actr build` to embed one.
#[derive(Debug, Clone)]
pub struct RegistryTrust {
    cache: Arc<MfrCertCache>,
}

impl RegistryTrust {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            cache: MfrCertCache::new(endpoint),
        }
    }

    async fn verify_package_inner(
        &self,
        bytes: &[u8],
        max_entry_bytes: Option<usize>,
    ) -> HyperResult<VerifiedPackage> {
        let pack_manifest = actr_pack::read_manifest(bytes).map_err(|e| match e {
            actr_pack::PackError::ManifestNotFound => HyperError::ManifestNotFound,
            actr_pack::PackError::ManifestParseError(msg) => HyperError::InvalidManifest(msg),
            other => HyperError::InvalidManifest(other.to_string()),
        })?;

        let key_id = pack_manifest.signing_key_id.as_deref().ok_or_else(|| {
            HyperError::InvalidManifest(
                "package manifest missing `signing_key_id`; rebuild with the latest `actr build`"
                    .to_string(),
            )
        })?;

        let pubkey = self
            .cache
            .get_or_fetch(&pack_manifest.manufacturer, Some(key_id))
            .await?;

        match max_entry_bytes {
            Some(max_entry_bytes) => {
                verify_ed25519_manifest_bounded(bytes, &pubkey, max_entry_bytes)
            }
            None => verify_ed25519_manifest(bytes, &pubkey),
        }
    }
}

#[async_trait]
impl TrustProvider for RegistryTrust {
    async fn verify_package(&self, bytes: &[u8]) -> HyperResult<VerifiedPackage> {
        self.verify_package_inner(bytes, None).await
    }

    async fn verify_package_bounded(
        &self,
        bytes: &[u8],
        max_entry_bytes: usize,
    ) -> HyperResult<VerifiedPackage> {
        self.verify_package_inner(bytes, Some(max_entry_bytes))
            .await
    }
}

// ── ChainTrust ───────────────────────────────────────────────────────────────

/// Try a list of providers in order; the first `Ok(_)` wins.
///
/// Useful for "local cache first, registry fallback" setups or for rolling
/// key migrations where an old static key and a new registry-backed provider
/// coexist.
#[derive(Debug, Clone)]
pub struct ChainTrust {
    providers: Vec<Arc<dyn TrustProvider>>,
}

impl ChainTrust {
    pub fn new(providers: Vec<Arc<dyn TrustProvider>>) -> Self {
        Self { providers }
    }

    /// Shortcut for a two-provider chain.
    pub fn of(first: Arc<dyn TrustProvider>, second: Arc<dyn TrustProvider>) -> Self {
        Self::new(vec![first, second])
    }
}

#[async_trait]
impl TrustProvider for ChainTrust {
    async fn verify_package(&self, bytes: &[u8]) -> HyperResult<VerifiedPackage> {
        let mut last_err: Option<HyperError> = None;
        for p in &self.providers {
            match p.verify_package(bytes).await {
                Ok(m) => return Ok(m),
                Err(e) => last_err = Some(e),
            }
        }
        Err(last_err.unwrap_or_else(|| {
            HyperError::SignatureVerificationFailed("empty trust chain".to_string())
        }))
    }

    async fn verify_package_bounded(
        &self,
        bytes: &[u8],
        max_entry_bytes: usize,
    ) -> HyperResult<VerifiedPackage> {
        let mut last_err: Option<HyperError> = None;
        for provider in &self.providers {
            match provider
                .verify_package_bounded(bytes, max_entry_bytes)
                .await
            {
                Ok(package) => return Ok(package),
                Err(error) => last_err = Some(error),
            }
        }
        Err(last_err.unwrap_or_else(|| {
            HyperError::SignatureVerificationFailed("empty trust chain".to_string())
        }))
    }
}

#[cfg(test)]
#[path = "trust_tests.rs"]
mod tests;
