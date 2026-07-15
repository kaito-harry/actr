//! `.actr` package verification + extraction — the browser-side equivalent of
//! [`actr_hyper::Hyper::attach`]'s prelude.
//!
//! Previously this logic was duplicated in JavaScript (`actor.sw.js`'s
//! `verifyActrPackage` + manual ZIP parsing). Now we delegate to the same
//! Rust code that the native host runs, compiled to WASM and exposed to JS
//! via `wasm-bindgen`.
//!
//! The module honours `actr-config::TrustAnchor` semantics:
//!   - `kind = "static"` — pre-configured Ed25519 pubkey (from `pubkey_b64`);
//!     used to run `actr_pack::verify`.
//!   - `kind = "registry"` — look up MFR keys over HTTP. **Not yet implemented
//!     on the browser side**; surfacing it produces a hard error instead of a
//!     silent skip (the native host has this via `RegistryTrust`; porting to
//!     the SW is a follow-up).
//!
//! Verification is **mandatory**. There is no "skip verify" path: a
//! misconfigured trust anchor errors out and the package never loads.

use actr_pack::PackageManifest;
use base64::Engine as _;
use ed25519_dalek::VerifyingKey;
use serde::Deserialize;
use wasm_bindgen::prelude::*;

/// Trust anchor as received from the runtime config JSON. Matches
/// `actr_config::TrustAnchor`; we keep a local copy to avoid pulling
/// `actr-config` into the SW crate just for serde.
#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum TrustAnchorCfg {
    Static {
        #[serde(default)]
        pubkey_b64: Option<String>,
        #[serde(default)]
        #[allow(dead_code)] // browser has no FS; pubkey_file is resolved host-side
        pubkey_file: Option<String>,
    },
    Registry {
        #[allow(dead_code)] // carried for error messaging only
        endpoint: String,
    },
}

/// Output of [`verify_and_extract_actr_package`].
///
/// Kept as an opaque handle exposed to JS via getters. Avoids round-tripping
/// binary bytes through JSON.
#[wasm_bindgen]
pub struct ExtractedPackage {
    manifest_json: String,
    binary: Vec<u8>,
    glue_js: Option<String>,
}

#[wasm_bindgen]
impl ExtractedPackage {
    /// Verified package manifest, serialized as JSON. Fields mirror
    /// `actr_pack::PackageManifest`.
    #[wasm_bindgen(getter)]
    pub fn manifest_json(&self) -> String {
        self.manifest_json.clone()
    }

    /// Verified binary bytes (WASM module) extracted from the `.actr` ZIP.
    #[wasm_bindgen(getter)]
    pub fn binary(&self) -> Vec<u8> {
        self.binary.clone()
    }

    /// wasm-bindgen JS glue text extracted from `resources/*.js`, if any.
    /// Returns `None` when the package carries no glue (guest-bridge mode or
    /// pure-Rust packages).
    #[wasm_bindgen(getter)]
    pub fn glue_js(&self) -> Option<String> {
        self.glue_js.clone()
    }
}

/// Verify a `.actr` package against the provided trust anchors and return
/// its extracted parts.
///
/// Browser-side equivalent of the `Hyper::verify_package` → `load_binary`
/// step on native. Always runs the full signature + binary hash chain;
/// there is no "skip verify" escape hatch.
///
/// # Parameters
/// - `package_bytes` — the raw `.actr` ZIP bytes
/// - `trust_anchors_json` — JSON array of `TrustAnchor` entries
///   (shape matches `actr_config::TrustAnchor`). The SW honours the first
///   usable `kind = "static"` entry; `kind = "registry"` entries cause a
///   hard error until the SW learns to do async AIS lookups.
///
/// # Errors
/// Raises a `JsError` with a descriptive message on:
/// - malformed trust config
/// - no usable static anchor (empty, missing `pubkey_b64`, or only `registry`)
/// - invalid / wrong-size public key
/// - signature mismatch, tampered binary, missing manifest, etc.
#[wasm_bindgen]
pub fn verify_and_extract_actr_package(
    package_bytes: &[u8],
    trust_anchors_json: &str,
) -> Result<ExtractedPackage, JsError> {
    let anchors: Vec<TrustAnchorCfg> = serde_json::from_str(trust_anchors_json)
        .map_err(|e| JsError::new(&format!("trust config is not valid JSON: {e}")))?;

    let pubkey_b64 = resolve_static_pubkey(&anchors).ok_or_else(|| {
        JsError::new(
            "no usable static trust anchor in config: browser runtime \
             requires a `kind=\"static\"` entry with `pubkey_b64` \
             (registry anchors need async AIS lookup — not yet implemented \
             in the browser; configure a static anchor or run via the \
             native host)",
        )
    })?;

    let vk = parse_pubkey(&pubkey_b64)?;

    let verified = actr_pack::verify(package_bytes, &vk)
        .map_err(|e| JsError::new(&format!("package verification failed: {e}")))?;

    let binary =
        actr_pack::load_binary_bounded(package_bytes, actr_pack::DEFAULT_MAX_VERIFIED_ENTRY_BYTES)
            .map_err(|e| JsError::new(&format!("extract binary: {e}")))?;

    let glue_js = actr_pack::read_glue_js_bounded(
        package_bytes,
        &verified.manifest,
        actr_pack::DEFAULT_MAX_VERIFIED_ENTRY_BYTES,
    )
    .map_err(|e| JsError::new(&format!("extract glue.js: {e}")))?;

    Ok(ExtractedPackage {
        manifest_json: manifest_to_json(&verified.manifest)?,
        binary,
        glue_js,
    })
}

fn resolve_static_pubkey(anchors: &[TrustAnchorCfg]) -> Option<String> {
    for anchor in anchors {
        if let TrustAnchorCfg::Static {
            pubkey_b64: Some(b64),
            ..
        } = anchor
        {
            if !b64.is_empty() && b64 != "__MFR_PUBKEY_PLACEHOLDER__" {
                return Some(b64.clone());
            }
        }
    }
    None
}

fn parse_pubkey(b64: &str) -> Result<VerifyingKey, JsError> {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(b64)
        .map_err(|e| JsError::new(&format!("invalid base64 pubkey: {e}")))?;
    let arr: [u8; 32] = bytes.as_slice().try_into().map_err(|_| {
        JsError::new(&format!(
            "pubkey must be exactly 32 bytes, got {}",
            bytes.len()
        ))
    })?;
    VerifyingKey::from_bytes(&arr)
        .map_err(|e| JsError::new(&format!("invalid Ed25519 pubkey: {e}")))
}

fn manifest_to_json(manifest: &PackageManifest) -> Result<String, JsError> {
    serde_json::to_string(manifest).map_err(|e| JsError::new(&format!("serialize manifest: {e}")))
}
