use super::*;
use ed25519_dalek::{Signer, SigningKey};
use rand::rngs::OsRng;

fn make_minimal_package(signing_key: &SigningKey) -> Vec<u8> {
    make_package_with_binary(signing_key, b"wasm")
}

fn make_package_with_binary(signing_key: &SigningKey, binary: &[u8]) -> Vec<u8> {
    let manifest = actr_pack::PackageManifest {
        manufacturer: "test-mfr".to_string(),
        name: "Test".to_string(),
        version: "1.0.0".to_string(),
        binary: actr_pack::BinaryEntry {
            path: "bin/actor.wasm".to_string(),
            target: "wasm32-wasip1".to_string(),
            hash: String::new(),
            size: None,
            kind: None,
        },
        signature_algorithm: "ed25519".to_string(),
        signing_key_id: Some(actr_pack::compute_key_id(
            &signing_key.verifying_key().to_bytes(),
        )),
        resources: vec![],
        proto_files: vec![],
        lock_file: None,
        metadata: actr_pack::ManifestMetadata::default(),
    };
    actr_pack::pack(&actr_pack::PackOptions {
        manifest,
        binary_bytes: binary.to_vec(),
        resources: vec![],
        proto_files: vec![],
        lock_file: None,
        signing_key: signing_key.clone(),
    })
    .unwrap()
}

#[tokio::test]
async fn static_trust_accepts_valid_package() {
    let key = SigningKey::generate(&mut OsRng);
    let vk = key.verifying_key();
    let pkg = make_minimal_package(&key);

    let trust = StaticTrust::new(vk.to_bytes()).unwrap();
    let verified = trust.verify_package(&pkg).await.unwrap();
    assert_eq!(verified.manifest.manufacturer, "test-mfr");
}

#[tokio::test]
async fn static_trust_rejects_wrong_key() {
    let key = SigningKey::generate(&mut OsRng);
    let wrong = SigningKey::generate(&mut OsRng);
    let pkg = make_minimal_package(&key);

    let trust = StaticTrust::new(wrong.verifying_key().to_bytes()).unwrap();
    assert!(matches!(
        trust.verify_package(&pkg).await,
        Err(HyperError::SignatureVerificationFailed(_))
    ));
}

#[tokio::test]
async fn static_trust_enforces_bounded_verification_limit() {
    let key = SigningKey::generate(&mut OsRng);
    let pkg = make_package_with_binary(&key, &[0u8; 2048]);
    let trust = StaticTrust::new(key.verifying_key().to_bytes()).unwrap();

    let error = trust.verify_package_bounded(&pkg, 1024).await.unwrap_err();
    assert!(
        matches!(&error, HyperError::InvalidManifest(message) if
            message.contains("bin/actor.wasm") && message.contains("exceeds limit 1024")),
        "unexpected error: {error:?}"
    );
}

#[tokio::test]
async fn chain_first_match_wins() {
    let key = SigningKey::generate(&mut OsRng);
    let other = SigningKey::generate(&mut OsRng);
    let pkg = make_minimal_package(&key);

    let wrong: Arc<dyn TrustProvider> =
        Arc::new(StaticTrust::new(other.verifying_key().to_bytes()).unwrap());
    let right: Arc<dyn TrustProvider> =
        Arc::new(StaticTrust::new(key.verifying_key().to_bytes()).unwrap());

    let chain = ChainTrust::of(wrong, right);
    let verified = chain.verify_package(&pkg).await.unwrap();
    assert_eq!(verified.manifest.manufacturer, "test-mfr");
}

#[tokio::test]
async fn chain_all_fail_returns_last_error() {
    let key = SigningKey::generate(&mut OsRng);
    let wrong1 = SigningKey::generate(&mut OsRng);
    let wrong2 = SigningKey::generate(&mut OsRng);
    let pkg = make_minimal_package(&key);

    let chain = ChainTrust::of(
        Arc::new(StaticTrust::new(wrong1.verifying_key().to_bytes()).unwrap()),
        Arc::new(StaticTrust::new(wrong2.verifying_key().to_bytes()).unwrap()),
    );
    assert!(matches!(
        chain.verify_package(&pkg).await,
        Err(HyperError::SignatureVerificationFailed(_))
    ));
}

// Just so the minimum-bound test doesn't compile away unused Signer import.
#[allow(dead_code)]
fn _signer_sanity(key: &SigningKey) -> ed25519_dalek::Signature {
    key.sign(b"x")
}

#[test]
fn pack_err_to_hyper_maps_each_variant() {
    use actr_pack::PackError;

    assert!(matches!(
        pack_err_to_hyper(PackError::SignatureVerificationFailed("bad".into())),
        HyperError::SignatureVerificationFailed(_)
    ));
    assert!(matches!(
        pack_err_to_hyper(PackError::BinaryHashMismatch { path: "bin".into() }),
        HyperError::BinaryHashMismatch
    ));
    assert!(matches!(
        pack_err_to_hyper(PackError::SignatureNotFound),
        HyperError::SignatureVerificationFailed(_)
    ));
    assert!(matches!(
        pack_err_to_hyper(PackError::BinaryNotFound("p".into())),
        HyperError::InvalidManifest(_)
    ));
    assert!(matches!(
        pack_err_to_hyper(PackError::ManifestNotFound),
        HyperError::ManifestNotFound
    ));
    assert!(matches!(
        pack_err_to_hyper(PackError::ManifestParseError("bad".into())),
        HyperError::InvalidManifest(_)
    ));
}

#[tokio::test]
async fn chain_trust_empty_chain_errors() {
    let chain = ChainTrust::new(vec![]);
    let err = chain.verify_package(b"whatever").await.unwrap_err();
    match err {
        HyperError::SignatureVerificationFailed(msg) => {
            assert!(msg.contains("empty trust chain"), "got: {msg}");
        }
        other => panic!("expected SignatureVerificationFailed, got {other:?}"),
    }
}

#[tokio::test]
async fn registry_trust_rejects_package_without_signing_key_id() {
    // Build a package whose manifest has no signing_key_id. RegistryTrust
    // must surface InvalidManifest before hitting the AIS cache.
    let signing_key = SigningKey::generate(&mut rand::rngs::OsRng);
    let manifest = actr_pack::PackageManifest {
        manufacturer: "m".into(),
        name: "n".into(),
        version: "1.0.0".into(),
        binary: actr_pack::BinaryEntry {
            path: "bin/x".into(),
            target: "wasm32-wasip2".into(),
            hash: String::new(),
            size: None,
            kind: Some(actr_pack::BinaryKind::Component),
        },
        signature_algorithm: "ed25519".into(),
        signing_key_id: None, // missing on purpose
        resources: vec![],
        proto_files: vec![],
        lock_file: None,
        metadata: actr_pack::ManifestMetadata::default(),
    };
    let pkg = actr_pack::pack(&actr_pack::PackOptions {
        manifest,
        binary_bytes: b"bin".to_vec(),
        resources: vec![],
        proto_files: vec![],
        lock_file: None,
        signing_key: signing_key.clone(),
    })
    .unwrap();

    let registry = RegistryTrust::new("http://ais.invalid");
    let err = registry.verify_package(&pkg).await.unwrap_err();
    assert!(
        matches!(err, HyperError::InvalidManifest(_)),
        "expected InvalidManifest for missing signing_key_id, got {err:?}"
    );
}
