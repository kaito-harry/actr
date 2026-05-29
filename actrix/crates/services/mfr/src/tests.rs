/// MFR 模块集成测试
///
/// 所有需要数据库的测试使用 in-process SQLite 内存库，每个测试独立 pool，互不干扰。
use sqlx::SqlitePool;

use crate::{
    MfrError, crypto,
    manager::{KeySource, MfrManager, PublishRequest, lookup_package},
    model::{ActrPackage, GitHubRepoChallenge, Manufacturer, MfrStatus, PkgStatus, PublishNonce},
    reserved::validate_github_login,
};

#[derive(serde::Serialize)]
struct SignablePublishBody<'a> {
    manufacturer: &'a str,
    name: &'a str,
    version: &'a str,
    target: &'a str,
    manifest: &'a str,
    signature: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    proto_files: Option<&'a serde_json::Value>,
    nonce: &'a str,
}

// ─── 测试辅助 ────────────────────────────────────────────────────────────────

async fn setup_test_pool() -> SqlitePool {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .connect("sqlite::memory:")
        .await
        .expect("failed to create in-memory sqlite pool");

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS mfr (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            public_key TEXT NOT NULL DEFAULT '',
            key_id TEXT NOT NULL DEFAULT '',
            contact TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            created_at INTEGER NOT NULL,
            updated_at INTEGER,
            verified_at INTEGER,
            suspended_at INTEGER,
            revoked_at INTEGER,
            key_expires_at INTEGER
        )",
    )
    .execute(&pool)
    .await
    .expect("failed to create mfr table");

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS mfr_challenge (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            mfr_id INTEGER NOT NULL REFERENCES mfr(id),
            token TEXT NOT NULL,
            verify_url TEXT NOT NULL DEFAULT '',
            expires_at INTEGER NOT NULL,
            verified_at INTEGER,
            created_at INTEGER NOT NULL
        )",
    )
    .execute(&pool)
    .await
    .expect("failed to create mfr_challenge table");

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS mfr_package (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            mfr_id INTEGER NOT NULL REFERENCES mfr(id),
            manufacturer TEXT NOT NULL,
            name TEXT NOT NULL,
            version TEXT NOT NULL,
            type_str TEXT NOT NULL,
            target TEXT NOT NULL,
            key_id INTEGER NOT NULL DEFAULT 1,
            manifest TEXT NOT NULL,
            signature TEXT NOT NULL,
            proto_files TEXT,
            status TEXT NOT NULL DEFAULT 'active',
            published_at INTEGER NOT NULL,
            revoked_at INTEGER,
            UNIQUE(manufacturer, name, version, target)
        )",
    )
    .execute(&pool)
    .await
    .expect("failed to create mfr_package table");

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS mfr_key_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            mfr_id INTEGER NOT NULL REFERENCES mfr(id),
            key_id TEXT NOT NULL,
            public_key TEXT NOT NULL,
            fingerprint TEXT,
            status TEXT NOT NULL DEFAULT 'Active',
            created_at INTEGER NOT NULL,
            retired_at INTEGER NOT NULL DEFAULT 0,
            revoked_at INTEGER
        )",
    )
    .execute(&pool)
    .await
    .expect("failed to create mfr_key_history table");

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS mfr_publish_nonce (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            mfr_id     INTEGER NOT NULL REFERENCES mfr(id),
            nonce      BLOB    NOT NULL UNIQUE,
            status     TEXT    NOT NULL DEFAULT 'pending',
            created_at INTEGER NOT NULL,
            expires_at INTEGER NOT NULL
        )",
    )
    .execute(&pool)
    .await
    .expect("failed to create mfr_publish_nonce table");

    pool
}

fn valid_public_key() -> String {
    let (_, public_key) = crypto::generate_keypair();
    public_key
}

/// Generate a valid nonce + nonce_sig for publish requests in tests.
///
/// Creates a real nonce in the DB, then signs the challenge payload with the given key.
/// Returns (nonce_b64, nonce_sig_b64) ready to use in PublishRequest.
#[allow(clippy::too_many_arguments)]
async fn make_publish_nonce(
    pool: &SqlitePool,
    mfr_id: i64,
    manufacturer: &str,
    name: &str,
    version: &str,
    target: &str,
    manifest: &str,
    signature: &str,
    proto_files: Option<&serde_json::Value>,
    signing_key: &ed25519_dalek::SigningKey,
) -> (String, String) {
    use base64::Engine as _;
    use ed25519_dalek::Signer;
    use sha2::{Digest, Sha256};

    let nonce_bytes = PublishNonce::create(pool, mfr_id)
        .await
        .expect("failed to create test nonce");
    let nonce_b64 = base64::engine::general_purpose::STANDARD.encode(&nonce_bytes);

    let signable_body = SignablePublishBody {
        manufacturer,
        name,
        version,
        target,
        manifest,
        signature,
        proto_files,
        nonce: &nonce_b64,
    };
    let signable_body_bytes =
        serde_json::to_vec(&signable_body).expect("failed to serialize signable publish body");
    let body_hash = hex::encode(Sha256::digest(&signable_body_bytes));
    let nonce_hex = hex::encode(&nonce_bytes);
    let payload = format!(
        "ACTR-PUBLISH-V1\nmanufacturer={}\nmethod=POST\npath=/mfr/pkg/publish\nnonce={}\nbody_sha256={}",
        manufacturer, nonce_hex, body_hash
    );
    let sig = signing_key.sign(payload.as_bytes());
    let nonce_sig_b64 = base64::engine::general_purpose::STANDARD.encode(sig.to_bytes());

    (nonce_b64, nonce_sig_b64)
}

fn test_manifest(manufacturer: &str, name: &str, version: &str, target: &str) -> String {
    format!(
        "manufacturer = \"{manufacturer}\"\nname = \"{name}\"\nversion = \"{version}\"\n\n[binary]\npath = \"bin/actor.wasm\"\ntarget = \"{target}\"\nhash = \"sha256:abc123\"\n"
    )
}

// ─── reserved.rs tests ──────────────────────────────────────────────────

#[test]
fn test_validate_github_login_accepts_former_reserved() {
    // Previously reserved names are now valid manufacturer names
    assert!(validate_github_login("self").is_ok());
    assert!(validate_github_login("acme").is_ok());
    assert!(validate_github_login("actrix").is_ok());
}

#[test]
fn test_validate_github_login_too_long() {
    let long = "a".repeat(40);
    assert!(matches!(
        validate_github_login(&long),
        Err(MfrError::InvalidName(_))
    ));
}

#[test]
fn test_validate_github_login_empty() {
    assert!(matches!(
        validate_github_login(""),
        Err(MfrError::InvalidName(_))
    ));
}

#[test]
fn test_validate_github_login_hyphen_boundary() {
    assert!(matches!(
        validate_github_login("-abc"),
        Err(MfrError::InvalidName(_))
    ));
    assert!(matches!(
        validate_github_login("abc-"),
        Err(MfrError::InvalidName(_))
    ));
}

#[test]
fn test_validate_github_login_consecutive_hyphens() {
    assert!(matches!(
        validate_github_login("a--b"),
        Err(MfrError::InvalidName(_))
    ));
}

#[test]
fn test_validate_github_login_invalid_chars() {
    assert!(matches!(
        validate_github_login("my_company"),
        Err(MfrError::InvalidName(_))
    ));
    assert!(matches!(
        validate_github_login("my company"),
        Err(MfrError::InvalidName(_))
    ));
    assert!(matches!(
        validate_github_login("com.example"),
        Err(MfrError::InvalidName(_))
    ));
}

#[test]
fn test_validate_github_login_valid() {
    assert!(validate_github_login("octocat").is_ok());
    assert!(validate_github_login("my-company").is_ok());
    assert!(validate_github_login("user123").is_ok());
    assert!(validate_github_login("a").is_ok());
    let max = "a".repeat(39);
    assert!(validate_github_login(&max).is_ok());
}

// ─── crypto.rs 纯单元测试 ────────────────────────────────────────────────────

#[test]
fn test_generate_keypair_roundtrip() {
    use base64::Engine as _;

    let (private_b64, public_b64) = crypto::generate_keypair();
    assert!(!private_b64.is_empty());
    assert!(!public_b64.is_empty());

    let priv_bytes = base64::engine::general_purpose::STANDARD
        .decode(&private_b64)
        .expect("private key should be valid base64");
    assert_eq!(
        priv_bytes.len(),
        32,
        "Ed25519 private key should be 32 bytes"
    );

    let pub_bytes = base64::engine::general_purpose::STANDARD
        .decode(&public_b64)
        .expect("public key should be valid base64");
    assert_eq!(pub_bytes.len(), 32, "Ed25519 public key should be 32 bytes");
}

#[test]
fn test_generate_keypair_unique() {
    let (priv1, pub1) = crypto::generate_keypair();
    let (priv2, pub2) = crypto::generate_keypair();
    assert_ne!(priv1, priv2);
    assert_ne!(pub1, pub2);
}

#[test]
fn test_verify_signature_valid() {
    use base64::Engine as _;
    use ed25519_dalek::{Signer, SigningKey};
    use rand::rngs::OsRng;

    let signing_key = SigningKey::generate(&mut OsRng);
    let verifying_key = signing_key.verifying_key();
    let message = b"hello mfr";
    let sig = signing_key.sign(message);

    let sig_b64 = base64::engine::general_purpose::STANDARD.encode(sig.to_bytes());
    let pub_b64 = base64::engine::general_purpose::STANDARD.encode(verifying_key.to_bytes());

    let result = crypto::verify_signature(message, &sig_b64, &pub_b64)
        .expect("verify_signature should not error on valid inputs");
    assert!(result, "valid signature should verify as true");
}

#[test]
fn test_verify_signature_invalid_zeros() {
    use base64::Engine as _;
    let (_, pub_b64) = crypto::generate_keypair();
    let bad_sig = base64::engine::general_purpose::STANDARD.encode([0u8; 64]);
    let result = crypto::verify_signature(b"message", &bad_sig, &pub_b64);
    assert!(
        matches!(result, Ok(false) | Err(MfrError::Crypto(_))),
        "all-zero signature should fail: {result:?}"
    );
}

#[test]
fn test_verify_signature_wrong_key() {
    use base64::Engine as _;
    use ed25519_dalek::{Signer, SigningKey};
    use rand::rngs::OsRng;

    let key1 = SigningKey::generate(&mut OsRng);
    let key2 = SigningKey::generate(&mut OsRng);
    let message = b"test message";
    let sig = key1.sign(message);

    let sig_b64 = base64::engine::general_purpose::STANDARD.encode(sig.to_bytes());
    let wrong_pub_b64 =
        base64::engine::general_purpose::STANDARD.encode(key2.verifying_key().to_bytes());

    let result = crypto::verify_signature(message, &sig_b64, &wrong_pub_b64)
        .expect("should not return error for valid encoding");
    assert!(!result, "signature with wrong key should verify as false");
}

#[test]
fn test_verify_signature_bad_pubkey_encoding() {
    let bad_pub = "not-valid-base64!!!";
    let result = crypto::verify_signature(b"msg", "anysig", bad_pub);
    assert!(
        matches!(result, Err(MfrError::Crypto(_))),
        "bad base64 pubkey should return Crypto error"
    );
}

// ─── model/manufacturer.rs 测试（需 DB）─────────────────────────────────────

#[tokio::test]
async fn test_manufacturer_create_and_get() {
    let pool = setup_test_pool().await;
    let mfr = Manufacturer::create(&pool, "testco", Some("admin@testco.com"))
        .await
        .expect("create should succeed");

    assert_eq!(mfr.name, "testco");
    assert_eq!(mfr.status, MfrStatus::Pending);
    assert!(mfr.verified_at.is_none());
    assert_eq!(mfr.contact.as_deref(), Some("admin@testco.com"));

    let found = Manufacturer::get(&pool, mfr.id)
        .await
        .expect("get should succeed")
        .expect("should find created manufacturer");
    assert_eq!(found.name, "testco");
    assert_eq!(found.id, mfr.id);
}

#[tokio::test]
async fn test_manufacturer_get_nonexistent() {
    let pool = setup_test_pool().await;
    let found = Manufacturer::get(&pool, 9999).await.unwrap();
    assert!(found.is_none());
}

#[tokio::test]
async fn test_manufacturer_get_by_name() {
    let pool = setup_test_pool().await;
    let mfr = Manufacturer::create(&pool, "namedco", None).await.unwrap();

    let found = Manufacturer::get_by_name(&pool, "namedco")
        .await
        .unwrap()
        .expect("should find by name");
    assert_eq!(found.id, mfr.id);

    let missing = Manufacturer::get_by_name(&pool, "nobody").await.unwrap();
    assert!(missing.is_none());
}

#[tokio::test]
async fn test_manufacturer_duplicate_name() {
    let pool = setup_test_pool().await;
    Manufacturer::create(&pool, "dupco", None).await.unwrap();
    let result = Manufacturer::create(&pool, "dupco", None).await;
    assert!(
        matches!(result, Err(MfrError::AlreadyExists(_))),
        "duplicate name should return AlreadyExists"
    );
}

#[tokio::test]
async fn test_manufacturer_activate() {
    let pool = setup_test_pool().await;
    let mut mfr = Manufacturer::create(&pool, "activeco", None).await.unwrap();
    let public_key = valid_public_key();

    mfr.activate(&pool, public_key.clone())
        .await
        .expect("activate from pending should succeed");

    assert_eq!(mfr.status, MfrStatus::Active);
    assert_eq!(mfr.public_key, public_key);
    assert!(mfr.verified_at.is_some());
    assert!(mfr.updated_at.is_some());

    let from_db = Manufacturer::get(&pool, mfr.id).await.unwrap().unwrap();
    assert_eq!(from_db.status, MfrStatus::Active);
    assert_eq!(from_db.public_key, public_key);
}

#[tokio::test]
async fn test_manufacturer_lifecycle_full() {
    let pool = setup_test_pool().await;
    let mut mfr = Manufacturer::create(&pool, "lifecycle", None)
        .await
        .unwrap();

    mfr.activate(&pool, valid_public_key()).await.unwrap();
    assert_eq!(mfr.status, MfrStatus::Active);

    mfr.suspend(&pool).await.unwrap();
    assert_eq!(mfr.status, MfrStatus::Suspended);
    assert!(mfr.suspended_at.is_some());

    mfr.reinstate(&pool).await.unwrap();
    assert_eq!(mfr.status, MfrStatus::Active);

    mfr.revoke(&pool).await.unwrap();
    assert_eq!(mfr.status, MfrStatus::Revoked);
    assert!(mfr.revoked_at.is_some());
}

#[tokio::test]
async fn test_manufacturer_invalid_transitions_from_pending() {
    let pool = setup_test_pool().await;
    let mut mfr = Manufacturer::create(&pool, "transco", None).await.unwrap();

    let err = mfr.suspend(&pool).await.unwrap_err();
    assert!(matches!(err, MfrError::InvalidStatus(_)));

    let err = mfr.reinstate(&pool).await.unwrap_err();
    assert!(matches!(err, MfrError::InvalidStatus(_)));
}

#[tokio::test]
async fn test_manufacturer_cannot_activate_twice() {
    let pool = setup_test_pool().await;
    let mut mfr = Manufacturer::create(&pool, "twiceco", None).await.unwrap();

    mfr.activate(&pool, "key1".to_string()).await.unwrap();
    let err = mfr.activate(&pool, "key2".to_string()).await.unwrap_err();
    assert!(matches!(err, MfrError::InvalidStatus(_)));
}

#[tokio::test]
async fn test_manufacturer_list_all() {
    let pool = setup_test_pool().await;
    Manufacturer::create(&pool, "list1", None).await.unwrap();
    Manufacturer::create(&pool, "list2", None).await.unwrap();

    let all = Manufacturer::list(&pool, None).await.unwrap();
    assert_eq!(all.len(), 2);
}

#[tokio::test]
async fn test_manufacturer_list_by_status() {
    let pool = setup_test_pool().await;
    Manufacturer::create(&pool, "statuslist1", None)
        .await
        .unwrap();
    let mut mfr2 = Manufacturer::create(&pool, "statuslist2", None)
        .await
        .unwrap();
    mfr2.activate(&pool, valid_public_key()).await.unwrap();

    let active = Manufacturer::list(&pool, Some(MfrStatus::Active))
        .await
        .unwrap();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].name, "statuslist2");

    let pending = Manufacturer::list(&pool, Some(MfrStatus::Pending))
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].name, "statuslist1");
}

#[tokio::test]
async fn test_manufacturer_delete() {
    let pool = setup_test_pool().await;
    let mfr = Manufacturer::create(&pool, "delco", None).await.unwrap();
    let id = mfr.id;

    Manufacturer::delete(&pool, id).await.unwrap();

    let found = Manufacturer::get(&pool, id).await.unwrap();
    assert!(found.is_none());
}

// ─── model/challenge.rs 测试（需 DB）─────────────────────────────────────────

#[tokio::test]
async fn test_challenge_create() {
    let pool = setup_test_pool().await;
    let mfr = Manufacturer::create(&pool, "chco", None).await.unwrap();

    let ch = GitHubRepoChallenge::create(&pool, mfr.id).await.unwrap();

    assert!(
        ch.token.starts_with("actrix-verify="),
        "token should start with 'actrix-verify=', got: {}",
        ch.token
    );
    assert!(ch.verify_url.is_empty());
    assert!(ch.verified_at.is_none());
    assert!(ch.expires_at > ch.created_at);
    assert_eq!(ch.mfr_id, mfr.id);
}

#[tokio::test]
async fn test_challenge_get_active_found() {
    let pool = setup_test_pool().await;
    let mfr = Manufacturer::create(&pool, "activech", None).await.unwrap();
    let ch = GitHubRepoChallenge::create(&pool, mfr.id).await.unwrap();

    let active = GitHubRepoChallenge::get_active(&pool, mfr.id)
        .await
        .unwrap();
    assert!(active.is_some());
    assert_eq!(active.unwrap().id, ch.id);
}

#[tokio::test]
async fn test_challenge_get_active_none_when_empty() {
    let pool = setup_test_pool().await;
    let mfr = Manufacturer::create(&pool, "nochco", None).await.unwrap();

    let active = GitHubRepoChallenge::get_active(&pool, mfr.id)
        .await
        .unwrap();
    assert!(active.is_none());
}

#[tokio::test]
async fn test_challenge_mark_verified() {
    let pool = setup_test_pool().await;
    let mfr = Manufacturer::create(&pool, "verch", None).await.unwrap();
    let mut ch = GitHubRepoChallenge::create(&pool, mfr.id).await.unwrap();

    ch.mark_verified(&pool, "https://github.com/verch/actr-mfr-verify")
        .await
        .unwrap();
    assert!(ch.verified_at.is_some());
    assert_eq!(ch.verify_url, "https://github.com/verch/actr-mfr-verify");

    let active = GitHubRepoChallenge::get_active(&pool, mfr.id)
        .await
        .unwrap();
    assert!(
        active.is_none(),
        "verified challenge should not appear in get_active"
    );
}

#[tokio::test]
async fn test_challenge_token_unique() {
    let pool = setup_test_pool().await;
    let mfr = Manufacturer::create(&pool, "tokenco", None).await.unwrap();

    let ch1 = GitHubRepoChallenge::create(&pool, mfr.id).await.unwrap();
    let ch2 = GitHubRepoChallenge::create(&pool, mfr.id).await.unwrap();

    assert_ne!(
        ch1.token, ch2.token,
        "each challenge should have a unique token"
    );
}

// ─── model/package.rs 测试（需 DB）──────────────────────────────────────────

#[tokio::test]
async fn test_package_publish_and_get() {
    use base64::Engine as _;
    use ed25519_dalek::SigningKey;
    use rand::rngs::OsRng;

    let pool = setup_test_pool().await;
    let mut mfr = Manufacturer::create(&pool, "pkgco", None).await.unwrap();
    let key = SigningKey::generate(&mut OsRng);
    let pub_b64 = base64::engine::general_purpose::STANDARD.encode(key.verifying_key().to_bytes());
    mfr.activate(&pool, pub_b64).await.unwrap();

    let pkg = ActrPackage::publish(
        &pool,
        mfr.id,
        "pkgco",
        "client",
        "1.0.0",
        "wasm32-wasip1",
        "manifest content",
        "sig123",
        None,
    )
    .await
    .unwrap();

    assert_eq!(pkg.type_str, "pkgco:client:1.0.0");
    assert_eq!(pkg.status, PkgStatus::Active);
    assert_eq!(pkg.manufacturer, "pkgco");
    assert_eq!(pkg.name, "client");
    assert_eq!(pkg.version, "1.0.0");

    let found = ActrPackage::get_by_type(&pool, "pkgco:client:1.0.0")
        .await
        .unwrap()
        .expect("should find published package");
    assert_eq!(found.id, pkg.id);
}

#[tokio::test]
async fn test_package_get_by_type_not_found() {
    let pool = setup_test_pool().await;
    let found = ActrPackage::get_by_type(&pool, "nobody:nothing:v0")
        .await
        .unwrap();
    assert!(found.is_none());
}

#[tokio::test]
async fn test_package_duplicate_rejected() {
    let pool = setup_test_pool().await;
    let mut mfr = Manufacturer::create(&pool, "dupkg", None).await.unwrap();
    mfr.activate(&pool, valid_public_key()).await.unwrap();

    ActrPackage::publish(
        &pool,
        mfr.id,
        "dupkg",
        "svc",
        "1.0.0",
        "wasm32-wasip1",
        "m",
        "s",
        None,
    )
    .await
    .unwrap();
    let result = ActrPackage::publish(
        &pool,
        mfr.id,
        "dupkg",
        "svc",
        "1.0.0",
        "wasm32-wasip1",
        "m2",
        "s2",
        None,
    )
    .await;
    assert!(
        matches!(result, Err(MfrError::PackageAlreadyPublished)),
        "duplicate publish should return PackageAlreadyPublished"
    );
}

#[tokio::test]
async fn test_package_revoke() {
    let pool = setup_test_pool().await;
    let mut mfr = Manufacturer::create(&pool, "revpkg", None).await.unwrap();
    mfr.activate(&pool, valid_public_key()).await.unwrap();

    let mut pkg = ActrPackage::publish(
        &pool,
        mfr.id,
        "revpkg",
        "svc",
        "1.0.0",
        "wasm32-wasip1",
        "m",
        "s",
        None,
    )
    .await
    .unwrap();
    pkg.revoke(&pool).await.unwrap();

    assert_eq!(pkg.status, PkgStatus::Revoked);
    assert!(pkg.revoked_at.is_some());

    let found = ActrPackage::get_by_type(&pool, "revpkg:svc:1.0.0")
        .await
        .unwrap();
    assert!(
        found.is_none(),
        "revoked package should not be found by get_by_type"
    );
}

#[tokio::test]
async fn test_package_list_by_mfr() {
    let pool = setup_test_pool().await;
    let mut mfr = Manufacturer::create(&pool, "listpkg", None).await.unwrap();
    mfr.activate(&pool, valid_public_key()).await.unwrap();

    ActrPackage::publish(
        &pool,
        mfr.id,
        "listpkg",
        "alpha",
        "1.0.0",
        "wasm32-wasip1",
        "m",
        "s",
        None,
    )
    .await
    .unwrap();
    ActrPackage::publish(
        &pool,
        mfr.id,
        "listpkg",
        "beta",
        "1.0.0",
        "wasm32-wasip1",
        "m",
        "s",
        None,
    )
    .await
    .unwrap();

    let pkgs = ActrPackage::list_by_mfr(&pool, mfr.id).await.unwrap();
    assert_eq!(pkgs.len(), 2);
}

#[tokio::test]
async fn test_package_get_by_id() {
    let pool = setup_test_pool().await;
    let mut mfr = Manufacturer::create(&pool, "idpkg", None).await.unwrap();
    mfr.activate(&pool, valid_public_key()).await.unwrap();

    let pkg = ActrPackage::publish(
        &pool,
        mfr.id,
        "idpkg",
        "svc",
        "1.0.0",
        "wasm32-wasip1",
        "m",
        "s",
        None,
    )
    .await
    .unwrap();

    let found = ActrPackage::get_by_id(&pool, pkg.id).await.unwrap();
    assert!(found.is_some());
    assert_eq!(found.unwrap().type_str, "idpkg:svc:1.0.0");
}

// ─── manager.rs 测试（需 DB）─────────────────────────────────────────────────

#[tokio::test]
async fn test_lookup_package_no_reserved_shortcut() {
    let pool = setup_test_pool().await;
    // No reserved names anymore; unregistered names return false
    assert!(
        !lookup_package(&pool, "self:anything:1.0.0", None, None)
            .await
            .unwrap()
    );
    assert!(
        !lookup_package(&pool, "acme:client:1.0.0", None, None)
            .await
            .unwrap()
    );
    assert!(
        !lookup_package(&pool, "actrix:core:1.0.0", None, None)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn test_lookup_package_not_registered() {
    let pool = setup_test_pool().await;
    assert!(
        !lookup_package(&pool, "unknown:svc:1.0.0", None, None)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn test_lookup_package_active() {
    let pool = setup_test_pool().await;
    let mut mfr = Manufacturer::create(&pool, "lookco", None).await.unwrap();
    mfr.activate(&pool, valid_public_key()).await.unwrap();
    ActrPackage::publish(
        &pool,
        mfr.id,
        "lookco",
        "svc",
        "1.0.0",
        "wasm32-wasip1",
        "m",
        "s",
        None,
    )
    .await
    .unwrap();

    assert!(
        lookup_package(&pool, "lookco:svc:1.0.0", None, None)
            .await
            .unwrap()
    );
    assert!(
        !lookup_package(&pool, "lookco:svc:v2", None, None)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn test_lookup_package_revoked() {
    let pool = setup_test_pool().await;
    let mut mfr = Manufacturer::create(&pool, "revokedlook", None)
        .await
        .unwrap();
    mfr.activate(&pool, valid_public_key()).await.unwrap();
    let mut pkg = ActrPackage::publish(
        &pool,
        mfr.id,
        "revokedlook",
        "svc",
        "1.0.0",
        "wasm32-wasip1",
        "m",
        "s",
        None,
    )
    .await
    .unwrap();
    pkg.revoke(&pool).await.unwrap();

    assert!(
        !lookup_package(&pool, "revokedlook:svc:1.0.0", None, None)
            .await
            .unwrap(),
        "revoked package should not be found"
    );
}

#[tokio::test]
async fn test_manager_apply_formerly_reserved_accepted() {
    let pool = setup_test_pool().await;
    let manager = MfrManager::new(pool);
    // Previously reserved names are now accepted
    let result = manager.apply("acme", None).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_manager_apply_valid() {
    let pool = setup_test_pool().await;
    let manager = MfrManager::new(pool);
    let (mfr, challenge) = manager
        .apply("octocat", Some("admin@octocat.dev"))
        .await
        .unwrap();

    assert_eq!(mfr.name, "octocat");
    assert_eq!(mfr.status, MfrStatus::Pending);
    assert!(challenge.token.starts_with("actrix-verify="));
    assert!(challenge.verify_url.is_empty());
}

#[tokio::test]
async fn test_manager_apply_invalid_login() {
    let pool = setup_test_pool().await;
    let manager = MfrManager::new(pool);
    // Underscore is not allowed in GitHub logins
    let result = manager.apply("my_company", None).await;
    assert!(matches!(result, Err(MfrError::InvalidName(_))));
}

#[tokio::test]
async fn test_manager_get_status() {
    let pool = setup_test_pool().await;
    let manager = MfrManager::new(pool);
    let (mfr, _) = manager.apply("statusco", None).await.unwrap();

    let status = manager.get_status(mfr.id).await.unwrap();
    assert_eq!(status.name, "statusco");
    assert_eq!(status.status, MfrStatus::Pending);
}

#[tokio::test]
async fn test_manager_get_status_not_found() {
    let pool = setup_test_pool().await;
    let manager = MfrManager::new(pool);
    let result = manager.get_status(9999).await;
    assert!(matches!(result, Err(MfrError::NotFound)));
}

#[tokio::test]
async fn test_manager_admin_approve() {
    let pool = setup_test_pool().await;
    let manager = MfrManager::new(pool);
    let (mfr, _) = manager.apply("approveco", None).await.unwrap();

    let response = manager.admin_approve(mfr.id, None).await.unwrap();
    assert_eq!(response.certificate.mfr_name, "approveco");
    assert_eq!(response.key_source, KeySource::Generated);
    assert!(response.private_key.is_some());
    assert!(!response.private_key.as_ref().unwrap().is_empty());
    assert!(!response.certificate.mfr_pubkey.is_empty());
    assert!(response.certificate.expires_at > response.certificate.issued_at);
}

#[tokio::test]
async fn test_manager_admin_suspend_reinstate() {
    let pool = setup_test_pool().await;
    let manager = MfrManager::new(pool);
    let (mfr, _) = manager.apply("suspco", None).await.unwrap();
    manager.admin_approve(mfr.id, None).await.unwrap();

    manager.admin_suspend(mfr.id).await.unwrap();
    let status = manager.get_status(mfr.id).await.unwrap();
    assert_eq!(status.status, MfrStatus::Suspended);

    manager.admin_reinstate(mfr.id).await.unwrap();
    let status = manager.get_status(mfr.id).await.unwrap();
    assert_eq!(status.status, MfrStatus::Active);
}

#[tokio::test]
async fn test_manager_admin_delete() {
    let pool = setup_test_pool().await;
    let manager = MfrManager::new(pool);
    let (mfr, _) = manager.apply("deleteco", None).await.unwrap();
    let id = mfr.id;

    manager.admin_delete(id).await.unwrap();
    let result = manager.get_status(id).await;
    assert!(matches!(result, Err(MfrError::NotFound)));
}

#[tokio::test]
async fn test_manager_publish_invalid_signature() {
    use base64::Engine as _;
    use ed25519_dalek::SigningKey;
    use rand::rngs::OsRng;

    let pool = setup_test_pool().await;
    let key = SigningKey::generate(&mut OsRng);
    let pub_b64 = base64::engine::general_purpose::STANDARD.encode(key.verifying_key().to_bytes());

    let manager = MfrManager::new(pool.clone());
    let (mfr, _) = manager.apply("sigco", None).await.unwrap();
    manager.admin_approve(mfr.id, Some(&pub_b64)).await.unwrap();

    let manifest = test_manifest("sigco", "svc", "1.0.0", "wasm32-wasip1");
    let bad_sig = base64::engine::general_purpose::STANDARD.encode([0u8; 64]);

    // Valid nonce but bad manifest signature
    let (nonce_b64, nonce_sig_b64) = make_publish_nonce(
        &pool,
        mfr.id,
        "sigco",
        "svc",
        "1.0.0",
        "wasm32-wasip1",
        &manifest,
        &bad_sig,
        None,
        &key,
    )
    .await;

    let result = manager
        .publish_package(PublishRequest {
            manufacturer: "sigco".to_string(),
            name: "svc".to_string(),
            version: "1.0.0".to_string(),
            target: "wasm32-wasip1".to_string(),
            manifest,
            signature: bad_sig,
            proto_files: None,
            nonce: Some(nonce_b64),
            nonce_sig: Some(nonce_sig_b64),
        })
        .await;
    assert!(
        matches!(
            result,
            Err(MfrError::InvalidSignature) | Err(MfrError::Crypto(_))
        ),
        "invalid signature should be rejected: {result:?}"
    );
}

#[tokio::test]
async fn test_manager_publish_valid_signature() {
    use base64::Engine as _;
    use ed25519_dalek::{Signer, SigningKey};
    use rand::rngs::OsRng;

    let pool = setup_test_pool().await;

    let signing_key = SigningKey::generate(&mut OsRng);
    let pub_b64 =
        base64::engine::general_purpose::STANDARD.encode(signing_key.verifying_key().to_bytes());

    let mut mfr = Manufacturer::create(&pool, "validpub", None).await.unwrap();
    mfr.activate(&pool, pub_b64).await.unwrap();

    let manifest = test_manifest("validpub", "client", "1.0.0", "wasm32-wasip1");
    let sig = signing_key.sign(manifest.as_bytes());
    let sig_b64 = base64::engine::general_purpose::STANDARD.encode(sig.to_bytes());

    let (nonce_b64, nonce_sig_b64) = make_publish_nonce(
        &pool,
        mfr.id,
        "validpub",
        "client",
        "1.0.0",
        "wasm32-wasip1",
        &manifest,
        &sig_b64,
        None,
        &signing_key,
    )
    .await;

    let manager = MfrManager::new(pool);
    let pkg = manager
        .publish_package(PublishRequest {
            manufacturer: "validpub".to_string(),
            name: "client".to_string(),
            version: "1.0.0".to_string(),
            target: "wasm32-wasip1".to_string(),
            manifest,
            signature: sig_b64,
            proto_files: None,
            nonce: Some(nonce_b64),
            nonce_sig: Some(nonce_sig_b64),
        })
        .await
        .unwrap();

    assert_eq!(pkg.type_str, "validpub:client:1.0.0");
    assert_eq!(pkg.status, PkgStatus::Active);
}

#[tokio::test]
async fn test_manager_publish_inactive_mfr() {
    let pool = setup_test_pool().await;
    Manufacturer::create(&pool, "pendingmfr", None)
        .await
        .unwrap();

    let manager = MfrManager::new(pool);
    let result = manager
        .publish_package(PublishRequest {
            manufacturer: "pendingmfr".to_string(),
            name: "svc".to_string(),
            version: "1.0.0".to_string(),
            target: "wasm32-wasip1".to_string(),
            manifest: "m".to_string(),
            signature: "s".to_string(),
            proto_files: None,
            nonce: None,
            nonce_sig: None,
        })
        .await;
    assert!(
        matches!(result, Err(MfrError::InvalidStatus(_))),
        "publishing for pending MFR should fail with InvalidStatus"
    );
}

#[tokio::test]
async fn test_manager_resolve_by_name() {
    let pool = setup_test_pool().await;
    let manager = MfrManager::new(pool);
    let (mfr, _) = manager.apply("resolveco", None).await.unwrap();
    manager.admin_approve(mfr.id, None).await.unwrap();

    let info = manager.resolve_by_name("resolveco").await.unwrap();
    assert_eq!(info.name, "resolveco");
    assert!(!info.public_key.is_empty());
}

#[tokio::test]
async fn test_manager_resolve_by_name_not_active() {
    let pool = setup_test_pool().await;
    let manager = MfrManager::new(pool);
    manager.apply("pendingres", None).await.unwrap();

    let result = manager.resolve_by_name("pendingres").await;
    assert!(matches!(result, Err(MfrError::InvalidStatus(_))));
}

#[tokio::test]
async fn test_manager_get_and_revoke_package() {
    use base64::Engine as _;
    use ed25519_dalek::{Signer, SigningKey};
    use rand::rngs::OsRng;

    let pool = setup_test_pool().await;
    let key = SigningKey::generate(&mut OsRng);
    let pub_b64 = base64::engine::general_purpose::STANDARD.encode(key.verifying_key().to_bytes());

    let mut mfr = Manufacturer::create(&pool, "revmgr", None).await.unwrap();
    mfr.activate(&pool, pub_b64).await.unwrap();

    let manifest = test_manifest("revmgr", "svc", "1.0.0", "wasm32-wasip1");
    let sig = key.sign(manifest.as_bytes());
    let sig_b64 = base64::engine::general_purpose::STANDARD.encode(sig.to_bytes());

    let (nonce_b64, nonce_sig_b64) = make_publish_nonce(
        &pool,
        mfr.id,
        "revmgr",
        "svc",
        "1.0.0",
        "wasm32-wasip1",
        &manifest,
        &sig_b64,
        None,
        &key,
    )
    .await;

    let manager = MfrManager::new(pool);
    let pkg = manager
        .publish_package(PublishRequest {
            manufacturer: "revmgr".to_string(),
            name: "svc".to_string(),
            version: "1.0.0".to_string(),
            target: "wasm32-wasip1".to_string(),
            manifest,
            signature: sig_b64,
            proto_files: None,
            nonce: Some(nonce_b64),
            nonce_sig: Some(nonce_sig_b64),
        })
        .await
        .unwrap();

    let found = manager.get_package("revmgr:svc:1.0.0").await.unwrap();
    assert_eq!(found.id, pkg.id);

    manager.revoke_package(pkg.id).await.unwrap();

    let result = manager.get_package("revmgr:svc:1.0.0").await;
    assert!(matches!(result, Err(MfrError::NotFound)));
}

#[tokio::test]
async fn test_manager_admin_list() {
    let pool = setup_test_pool().await;
    let manager = MfrManager::new(pool);

    manager.apply("adminlist1", None).await.unwrap();
    let (mfr2, _) = manager.apply("adminlist2", None).await.unwrap();
    manager.admin_approve(mfr2.id, None).await.unwrap();

    let all = manager.admin_list(None).await.unwrap();
    assert_eq!(all.len(), 2);

    let active = manager.admin_list(Some(MfrStatus::Active)).await.unwrap();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].name, "adminlist2");
}

#[tokio::test]
async fn test_manager_list_packages_by_mfr() {
    use base64::Engine as _;
    use ed25519_dalek::{Signer, SigningKey};
    use rand::rngs::OsRng;

    let pool = setup_test_pool().await;
    let key = SigningKey::generate(&mut OsRng);
    let pub_b64 = base64::engine::general_purpose::STANDARD.encode(key.verifying_key().to_bytes());

    let mut mfr = Manufacturer::create(&pool, "listmgr", None).await.unwrap();
    mfr.activate(&pool, pub_b64).await.unwrap();

    let manager = MfrManager::new(pool.clone());

    for pkg_name in &["alpha", "beta"] {
        let manifest = test_manifest("listmgr", pkg_name, "1.0.0", "wasm32-wasip1");
        let sig = key.sign(manifest.as_bytes());
        let sig_b64 = base64::engine::general_purpose::STANDARD.encode(sig.to_bytes());
        let (nonce_b64, nonce_sig_b64) = make_publish_nonce(
            &pool,
            mfr.id,
            "listmgr",
            pkg_name,
            "1.0.0",
            "wasm32-wasip1",
            &manifest,
            &sig_b64,
            None,
            &key,
        )
        .await;
        manager
            .publish_package(PublishRequest {
                manufacturer: "listmgr".to_string(),
                name: pkg_name.to_string(),
                version: "1.0.0".to_string(),
                target: "wasm32-wasip1".to_string(),
                manifest,
                signature: sig_b64,
                proto_files: None,
                nonce: Some(nonce_b64),
                nonce_sig: Some(nonce_sig_b64),
            })
            .await
            .unwrap();
    }

    let pkgs = manager.list_packages(Some("listmgr")).await.unwrap();
    assert_eq!(pkgs.len(), 2);

    let all = manager.list_packages(None).await.unwrap();
    assert_eq!(all.len(), 2);
}

// ─── dual key mode 测试 ──────────────────────────────────────────────────────

#[test]
fn test_validate_public_key_valid() {
    let (_, pub_b64) = crypto::generate_keypair();
    assert!(crypto::validate_public_key(&pub_b64).is_ok());
}

#[test]
fn test_validate_public_key_bad_base64() {
    let result = crypto::validate_public_key("not-valid-base64!!!");
    assert!(
        matches!(result, Err(MfrError::Crypto(_))),
        "bad base64 should return Crypto error"
    );
}

#[test]
fn test_validate_public_key_wrong_length() {
    use base64::Engine as _;
    let short = base64::engine::general_purpose::STANDARD.encode([0u8; 16]);
    let result = crypto::validate_public_key(&short);
    assert!(
        matches!(result, Err(MfrError::Crypto(_))),
        "16-byte key should be rejected"
    );
}

#[test]
fn test_validate_public_key_all_zeros() {
    use base64::Engine as _;
    let zeros = base64::engine::general_purpose::STANDARD.encode([0u8; 32]);
    // All-zero bytes may or may not be a valid curve point depending on the library.
    // We just verify it does not panic — either Ok or Err(Crypto) is acceptable.
    let result = crypto::validate_public_key(&zeros);
    assert!(
        result.is_ok() || matches!(result, Err(MfrError::Crypto(_))),
        "all-zero key should return Ok or Crypto error, not panic"
    );
}

#[tokio::test]
async fn test_manager_admin_approve_with_uploaded_key() {
    use base64::Engine as _;
    use ed25519_dalek::SigningKey;
    use rand::rngs::OsRng;

    let pool = setup_test_pool().await;
    let manager = MfrManager::new(pool);
    let (mfr, _) = manager.apply("uploadco", None).await.unwrap();

    // User generates their own keypair
    let user_key = SigningKey::generate(&mut OsRng);
    let user_pub_b64 =
        base64::engine::general_purpose::STANDARD.encode(user_key.verifying_key().to_bytes());

    let response = manager
        .admin_approve(mfr.id, Some(&user_pub_b64))
        .await
        .unwrap();

    assert_eq!(response.key_source, KeySource::Uploaded);
    assert!(
        response.private_key.is_none(),
        "uploaded mode should NOT return a private key"
    );
    assert_eq!(response.certificate.mfr_pubkey, user_pub_b64);
    assert_eq!(response.certificate.mfr_name, "uploadco");
}

#[tokio::test]
async fn test_manager_admin_approve_with_invalid_key_rejected() {
    let pool = setup_test_pool().await;
    let manager = MfrManager::new(pool);
    let (mfr, _) = manager.apply("badkeyco", None).await.unwrap();

    let result = manager.admin_approve(mfr.id, Some("not-a-valid-key")).await;
    assert!(
        matches!(result, Err(MfrError::Crypto(_))),
        "invalid public key should be rejected with Crypto error: {result:?}"
    );

    // Verify MFR is still pending (not corrupted)
    let status = manager.get_status(mfr.id).await.unwrap();
    assert_eq!(
        status.status,
        MfrStatus::Pending,
        "MFR should remain pending after failed activation"
    );
}

#[tokio::test]
async fn test_manager_admin_approve_generated_mode_default() {
    let pool = setup_test_pool().await;
    let manager = MfrManager::new(pool);
    let (mfr, _) = manager.apply("gendefault", None).await.unwrap();

    // No public_key passed => generated mode
    let response = manager.admin_approve(mfr.id, None).await.unwrap();

    assert_eq!(response.key_source, KeySource::Generated);
    assert!(
        response.private_key.is_some(),
        "generated mode should return a private key"
    );
    assert!(!response.private_key.as_ref().unwrap().is_empty());
}

#[tokio::test]
async fn test_manager_uploaded_key_can_publish() {
    use base64::Engine as _;
    use ed25519_dalek::{Signer, SigningKey};
    use rand::rngs::OsRng;

    let pool = setup_test_pool().await;
    let manager = MfrManager::new(pool);
    let (mfr, _) = manager.apply("pubupload", None).await.unwrap();

    // User generates their own keypair and uploads only the public key
    let user_key = SigningKey::generate(&mut OsRng);
    let user_pub_b64 =
        base64::engine::general_purpose::STANDARD.encode(user_key.verifying_key().to_bytes());

    manager
        .admin_approve(mfr.id, Some(&user_pub_b64))
        .await
        .unwrap();

    // Now sign and publish using the user's private key
    let manifest = test_manifest("pubupload", "widget", "1.0.0", "wasm32-wasip1");
    let sig = user_key.sign(manifest.as_bytes());
    let sig_b64 = base64::engine::general_purpose::STANDARD.encode(sig.to_bytes());

    let (nonce_b64, nonce_sig_b64) = make_publish_nonce(
        manager.pool(),
        mfr.id,
        "pubupload",
        "widget",
        "1.0.0",
        "wasm32-wasip1",
        &manifest,
        &sig_b64,
        None,
        &user_key,
    )
    .await;

    let pkg = manager
        .publish_package(PublishRequest {
            manufacturer: "pubupload".to_string(),
            name: "widget".to_string(),
            version: "1.0.0".to_string(),
            target: "wasm32-wasip1".to_string(),
            manifest,
            signature: sig_b64,
            proto_files: None,
            nonce: Some(nonce_b64),
            nonce_sig: Some(nonce_sig_b64),
        })
        .await
        .unwrap();

    assert_eq!(pkg.type_str, "pubupload:widget:1.0.0");
    assert_eq!(pkg.status, PkgStatus::Active);
}

// ─── Key rotation + publish/verify tests ─────────────────────────────────────

#[tokio::test]
async fn test_key_rotation_then_publish_with_new_key() {
    use base64::Engine as _;
    use ed25519_dalek::{Signer, SigningKey};
    use rand::rngs::OsRng;

    let pool = setup_test_pool().await;
    let manager = MfrManager::new(pool.clone());

    // Step 1: Apply and approve with initial key (key_A)
    let (mfr, _) = manager.apply("rotpub", None).await.unwrap();
    let key_a = SigningKey::generate(&mut OsRng);
    let pub_a = base64::engine::general_purpose::STANDARD.encode(key_a.verifying_key().to_bytes());
    manager.admin_approve(mfr.id, Some(&pub_a)).await.unwrap();

    // Step 2: Publish with key_A — should succeed
    let manifest_a = test_manifest("rotpub", "svc", "1.0.0", "wasm32-wasip1");
    let sig_a = key_a.sign(manifest_a.as_bytes());
    let sig_a_b64 = base64::engine::general_purpose::STANDARD.encode(sig_a.to_bytes());
    let (nonce_a, nonce_sig_a) = make_publish_nonce(
        &pool,
        mfr.id,
        "rotpub",
        "svc",
        "1.0.0",
        "wasm32-wasip1",
        &manifest_a,
        &sig_a_b64,
        None,
        &key_a,
    )
    .await;
    let pkg_a = manager
        .publish_package(PublishRequest {
            manufacturer: "rotpub".to_string(),
            name: "svc".to_string(),
            version: "1.0.0".to_string(),
            target: "wasm32-wasip1".to_string(),
            manifest: manifest_a,
            signature: sig_a_b64,
            proto_files: None,
            nonce: Some(nonce_a),
            nonce_sig: Some(nonce_sig_a),
        })
        .await
        .unwrap();
    assert_eq!(pkg_a.type_str, "rotpub:svc:1.0.0");

    // Step 3: Rotate key → key_B
    let key_b = SigningKey::generate(&mut OsRng);
    let pub_b = base64::engine::general_purpose::STANDARD.encode(key_b.verifying_key().to_bytes());
    manager.renew_key(mfr.id, Some(&pub_b)).await.unwrap();

    // Step 4: Publish with key_B — should succeed (new version)
    let manifest_b = test_manifest("rotpub", "svc", "2.0.0", "wasm32-wasip1");
    let sig_b = key_b.sign(manifest_b.as_bytes());
    let sig_b_b64 = base64::engine::general_purpose::STANDARD.encode(sig_b.to_bytes());
    let (nonce_b, nonce_sig_b) = make_publish_nonce(
        &pool,
        mfr.id,
        "rotpub",
        "svc",
        "2.0.0",
        "wasm32-wasip1",
        &manifest_b,
        &sig_b_b64,
        None,
        &key_b,
    )
    .await;
    let pkg_b = manager
        .publish_package(PublishRequest {
            manufacturer: "rotpub".to_string(),
            name: "svc".to_string(),
            version: "2.0.0".to_string(),
            target: "wasm32-wasip1".to_string(),
            manifest: manifest_b,
            signature: sig_b_b64,
            proto_files: None,
            nonce: Some(nonce_b),
            nonce_sig: Some(nonce_sig_b),
        })
        .await
        .unwrap();
    assert_eq!(pkg_b.type_str, "rotpub:svc:2.0.0");

    // Step 5: Publish with OLD key_A — should FAIL
    // After key rotation, MFR's public_key is key_B. The nonce_sig signed by key_A
    // won't verify against key_B → Unauthorized before reaching manifest sig check.
    let manifest_c = test_manifest("rotpub", "svc", "3.0.0", "wasm32-wasip1");
    let sig_c = key_a.sign(manifest_c.as_bytes());
    let sig_c_b64 = base64::engine::general_purpose::STANDARD.encode(sig_c.to_bytes());
    // Sign nonce with old key_a — verification will fail since MFR now uses key_b
    let (nonce_c, nonce_sig_c) = make_publish_nonce(
        &pool,
        mfr.id,
        "rotpub",
        "svc",
        "3.0.0",
        "wasm32-wasip1",
        &manifest_c,
        &sig_c_b64,
        None,
        &key_a,
    )
    .await;
    let result = manager
        .publish_package(PublishRequest {
            manufacturer: "rotpub".to_string(),
            name: "svc".to_string(),
            version: "3.0.0".to_string(),
            target: "wasm32-wasip1".to_string(),
            manifest: manifest_c,
            signature: sig_c_b64,
            proto_files: None,
            nonce: Some(nonce_c),
            nonce_sig: Some(nonce_sig_c),
        })
        .await;
    assert!(
        matches!(result, Err(MfrError::Unauthorized)),
        "old key should be rejected after rotation (nonce sig fails): {result:?}"
    );
}

#[tokio::test]
async fn test_key_rotation_historical_key_resolved() {
    use base64::Engine as _;
    use ed25519_dalek::SigningKey;
    use rand::rngs::OsRng;

    let pool = setup_test_pool().await;
    let manager = MfrManager::new(pool);

    // Apply + approve with key_A
    let (mfr, _) = manager.apply("histkey", None).await.unwrap();
    let key_a = SigningKey::generate(&mut OsRng);
    let pub_a = base64::engine::general_purpose::STANDARD.encode(key_a.verifying_key().to_bytes());
    let resp_a = manager.admin_approve(mfr.id, Some(&pub_a)).await.unwrap();
    let key_id_a = resp_a.certificate.key_id.clone();

    // Rotate to key_B
    let key_b = SigningKey::generate(&mut OsRng);
    let pub_b = base64::engine::general_purpose::STANDARD.encode(key_b.verifying_key().to_bytes());
    let resp_b = manager.renew_key(mfr.id, Some(&pub_b)).await.unwrap();
    let key_id_b = resp_b.certificate.key_id.clone();

    // Resolve current key (key_B)
    let current = manager.resolve_by_name("histkey").await.unwrap();
    assert_eq!(current.key_id, key_id_b);
    assert_eq!(current.public_key, pub_b);

    // Resolve old key (key_A) by key_id — should still work
    let historical = manager
        .resolve_key_by_id("histkey", &key_id_a)
        .await
        .unwrap();
    assert_eq!(historical.key_id, key_id_a);
    assert_eq!(historical.public_key, pub_a);

    // Resolve non-existent key_id — should fail
    let result = manager
        .resolve_key_by_id("histkey", "mfr-nonexistent")
        .await;
    assert!(matches!(result, Err(MfrError::NotFound)));
}

// ─── Error path tests ────────────────────────────────────────────────────────

#[tokio::test]
async fn test_publish_with_expired_key_rejected() {
    use base64::Engine as _;
    use ed25519_dalek::{Signer, SigningKey};
    use rand::rngs::OsRng;

    let pool = setup_test_pool().await;
    let key = SigningKey::generate(&mut OsRng);
    let pub_b64 = base64::engine::general_purpose::STANDARD.encode(key.verifying_key().to_bytes());

    // Create and activate manufacturer
    let mut mfr = Manufacturer::create(&pool, "expiredco", None)
        .await
        .unwrap();
    mfr.activate(&pool, pub_b64).await.unwrap();

    // Force key_expires_at to past (already expired)
    sqlx::query("UPDATE mfr SET key_expires_at = ? WHERE id = ?")
        .bind(1000i64) // Unix timestamp year ~1970
        .bind(mfr.id)
        .execute(&pool)
        .await
        .unwrap();

    let manifest = test_manifest("expiredco", "svc", "1.0.0", "wasm32-wasip1");
    let sig = key.sign(manifest.as_bytes());
    let sig_b64 = base64::engine::general_purpose::STANDARD.encode(sig.to_bytes());

    let manager = MfrManager::new(pool);
    let result = manager
        .publish_package(PublishRequest {
            manufacturer: "expiredco".to_string(),
            name: "svc".to_string(),
            version: "1.0.0".to_string(),
            target: "wasm32-wasip1".to_string(),
            manifest,
            signature: sig_b64,
            proto_files: None,
            nonce: None,
            nonce_sig: None,
        })
        .await;
    assert!(
        matches!(result, Err(MfrError::CertificateExpired)),
        "expired key should be rejected: {result:?}"
    );
}

#[tokio::test]
async fn test_publish_with_wrong_key_rejected() {
    use base64::Engine as _;
    use ed25519_dalek::{Signer, SigningKey};
    use rand::rngs::OsRng;

    let pool = setup_test_pool().await;
    let key_registered = SigningKey::generate(&mut OsRng);
    let key_attacker = SigningKey::generate(&mut OsRng);
    let pub_registered =
        base64::engine::general_purpose::STANDARD.encode(key_registered.verifying_key().to_bytes());

    let mut mfr = Manufacturer::create(&pool, "wrongkeyco", None)
        .await
        .unwrap();
    mfr.activate(&pool, pub_registered).await.unwrap();

    // Attacker signs with their own key, not the registered one
    let manifest = test_manifest("wrongkeyco", "svc", "1.0.0", "wasm32-wasip1");
    let sig = key_attacker.sign(manifest.as_bytes());
    let sig_b64 = base64::engine::general_purpose::STANDARD.encode(sig.to_bytes());

    // Attacker signs nonce with their key — nonce_sig won't verify against registered key
    let (nonce_b64, nonce_sig_b64) = make_publish_nonce(
        &pool,
        mfr.id,
        "wrongkeyco",
        "svc",
        "1.0.0",
        "wasm32-wasip1",
        &manifest,
        &sig_b64,
        None,
        &key_attacker,
    )
    .await;

    let manager = MfrManager::new(pool);
    let result = manager
        .publish_package(PublishRequest {
            manufacturer: "wrongkeyco".to_string(),
            name: "svc".to_string(),
            version: "1.0.0".to_string(),
            target: "wasm32-wasip1".to_string(),
            manifest,
            signature: sig_b64,
            proto_files: None,
            nonce: Some(nonce_b64),
            nonce_sig: Some(nonce_sig_b64),
        })
        .await;
    assert!(
        matches!(result, Err(MfrError::Unauthorized)),
        "wrong key should be rejected (nonce sig fails): {result:?}"
    );
}

#[tokio::test]
async fn test_publish_for_revoked_mfr_rejected() {
    use base64::Engine as _;
    use ed25519_dalek::{Signer, SigningKey};
    use rand::rngs::OsRng;

    let pool = setup_test_pool().await;
    let key = SigningKey::generate(&mut OsRng);
    let pub_b64 = base64::engine::general_purpose::STANDARD.encode(key.verifying_key().to_bytes());

    let mut mfr = Manufacturer::create(&pool, "revokedmfr", None)
        .await
        .unwrap();
    mfr.activate(&pool, pub_b64).await.unwrap();
    mfr.revoke(&pool).await.unwrap();

    let manifest = test_manifest("revokedmfr", "svc", "1.0.0", "wasm32-wasip1");
    let sig = key.sign(manifest.as_bytes());
    let sig_b64 = base64::engine::general_purpose::STANDARD.encode(sig.to_bytes());

    let manager = MfrManager::new(pool);
    let result = manager
        .publish_package(PublishRequest {
            manufacturer: "revokedmfr".to_string(),
            name: "svc".to_string(),
            version: "1.0.0".to_string(),
            target: "wasm32-wasip1".to_string(),
            manifest,
            signature: sig_b64,
            proto_files: None,
            nonce: None,
            nonce_sig: None,
        })
        .await;
    assert!(
        matches!(result, Err(MfrError::InvalidStatus(_))),
        "revoked MFR should not be able to publish: {result:?}"
    );
}

#[tokio::test]
async fn test_publish_for_suspended_mfr_rejected() {
    use base64::Engine as _;
    use ed25519_dalek::{Signer, SigningKey};
    use rand::rngs::OsRng;

    let pool = setup_test_pool().await;
    let key = SigningKey::generate(&mut OsRng);
    let pub_b64 = base64::engine::general_purpose::STANDARD.encode(key.verifying_key().to_bytes());

    let mut mfr = Manufacturer::create(&pool, "suspendedmfr", None)
        .await
        .unwrap();
    mfr.activate(&pool, pub_b64).await.unwrap();
    mfr.suspend(&pool).await.unwrap();

    let manifest = test_manifest("suspendedmfr", "svc", "1.0.0", "wasm32-wasip1");
    let sig = key.sign(manifest.as_bytes());
    let sig_b64 = base64::engine::general_purpose::STANDARD.encode(sig.to_bytes());

    let manager = MfrManager::new(pool);
    let result = manager
        .publish_package(PublishRequest {
            manufacturer: "suspendedmfr".to_string(),
            name: "svc".to_string(),
            version: "1.0.0".to_string(),
            target: "wasm32-wasip1".to_string(),
            manifest,
            signature: sig_b64,
            proto_files: None,
            nonce: None,
            nonce_sig: None,
        })
        .await;
    assert!(
        matches!(result, Err(MfrError::InvalidStatus(_))),
        "suspended MFR should not be able to publish: {result:?}"
    );
}

// ─── Multi-manufacturer / multi-package tests ────────────────────────────────

#[tokio::test]
async fn test_multi_manufacturer_independent_publish() {
    use base64::Engine as _;
    use ed25519_dalek::{Signer, SigningKey};
    use rand::rngs::OsRng;

    let pool = setup_test_pool().await;

    // Create two independent manufacturers with different keys
    let key_alpha = SigningKey::generate(&mut OsRng);
    let key_beta = SigningKey::generate(&mut OsRng);
    let pub_alpha =
        base64::engine::general_purpose::STANDARD.encode(key_alpha.verifying_key().to_bytes());
    let pub_beta =
        base64::engine::general_purpose::STANDARD.encode(key_beta.verifying_key().to_bytes());

    let mut mfr_alpha = Manufacturer::create(&pool, "alpha-corp", None)
        .await
        .unwrap();
    mfr_alpha.activate(&pool, pub_alpha).await.unwrap();
    let mut mfr_beta = Manufacturer::create(&pool, "beta-corp", None)
        .await
        .unwrap();
    mfr_beta.activate(&pool, pub_beta).await.unwrap();

    let manager = MfrManager::new(pool.clone());

    // Alpha publishes services
    for (name, ver) in &[("gateway", "1.0.0"), ("analytics", "2.0.0")] {
        let manifest = test_manifest("alpha-corp", name, ver, "wasm32-wasip1");
        let sig = key_alpha.sign(manifest.as_bytes());
        let sig_b64 = base64::engine::general_purpose::STANDARD.encode(sig.to_bytes());
        let (nonce_b64, nonce_sig_b64) = make_publish_nonce(
            &pool,
            mfr_alpha.id,
            "alpha-corp",
            name,
            ver,
            "wasm32-wasip1",
            &manifest,
            &sig_b64,
            None,
            &key_alpha,
        )
        .await;
        manager
            .publish_package(PublishRequest {
                manufacturer: "alpha-corp".to_string(),
                name: name.to_string(),
                version: ver.to_string(),
                target: "wasm32-wasip1".to_string(),
                manifest,
                signature: sig_b64,
                proto_files: None,
                nonce: Some(nonce_b64),
                nonce_sig: Some(nonce_sig_b64),
            })
            .await
            .unwrap();
    }

    // Beta publishes services
    for (name, ver) in &[("stream", "1.0.0"), ("auth", "1.0.0"), ("cache", "3.0.0")] {
        let manifest = test_manifest("beta-corp", name, ver, "wasm32-wasip1");
        let sig = key_beta.sign(manifest.as_bytes());
        let sig_b64 = base64::engine::general_purpose::STANDARD.encode(sig.to_bytes());
        let (nonce_b64, nonce_sig_b64) = make_publish_nonce(
            &pool,
            mfr_beta.id,
            "beta-corp",
            name,
            ver,
            "wasm32-wasip1",
            &manifest,
            &sig_b64,
            None,
            &key_beta,
        )
        .await;
        manager
            .publish_package(PublishRequest {
                manufacturer: "beta-corp".to_string(),
                name: name.to_string(),
                version: ver.to_string(),
                target: "wasm32-wasip1".to_string(),
                manifest,
                signature: sig_b64,
                proto_files: None,
                nonce: Some(nonce_b64),
                nonce_sig: Some(nonce_sig_b64),
            })
            .await
            .unwrap();
    }

    // Verify isolation: each MFR sees only their own packages
    let alpha_pkgs = manager.list_packages(Some("alpha-corp")).await.unwrap();
    assert_eq!(alpha_pkgs.len(), 2);
    assert!(alpha_pkgs.iter().all(|p| p.manufacturer == "alpha-corp"));

    let beta_pkgs = manager.list_packages(Some("beta-corp")).await.unwrap();
    assert_eq!(beta_pkgs.len(), 3);
    assert!(beta_pkgs.iter().all(|p| p.manufacturer == "beta-corp"));

    // All packages visible in global listing
    let all = manager.list_packages(None).await.unwrap();
    assert_eq!(all.len(), 5);

    // Cross-manufacturer publish should FAIL: alpha's key signing for beta's name
    // The nonce is issued for beta-corp (mfr_beta.id), but signed with alpha's key.
    // Nonce sig verification will fail since MFR beta-corp uses beta's public key.
    let cross_manifest = test_manifest("beta-corp", "hack", "1.0.0", "wasm32-wasip1");
    let cross_sig = key_alpha.sign(cross_manifest.as_bytes());
    let cross_sig_b64 = base64::engine::general_purpose::STANDARD.encode(cross_sig.to_bytes());
    let (nonce_cross, nonce_sig_cross) = make_publish_nonce(
        &pool,
        mfr_beta.id,
        "beta-corp",
        "hack",
        "1.0.0",
        "wasm32-wasip1",
        &cross_manifest,
        &cross_sig_b64,
        None,
        &key_alpha,
    )
    .await;
    let result = manager
        .publish_package(PublishRequest {
            manufacturer: "beta-corp".to_string(),
            name: "hack".to_string(),
            version: "1.0.0".to_string(),
            target: "wasm32-wasip1".to_string(),
            manifest: cross_manifest,
            signature: cross_sig_b64,
            proto_files: None,
            nonce: Some(nonce_cross),
            nonce_sig: Some(nonce_sig_cross),
        })
        .await;
    assert!(
        matches!(result, Err(MfrError::Unauthorized)),
        "cross-manufacturer publish should fail (nonce sig mismatch): {result:?}"
    );
}

#[tokio::test]
async fn test_multi_target_same_package() {
    use base64::Engine as _;
    use ed25519_dalek::{Signer, SigningKey};
    use rand::rngs::OsRng;

    let pool = setup_test_pool().await;
    let key = SigningKey::generate(&mut OsRng);
    let pub_b64 = base64::engine::general_purpose::STANDARD.encode(key.verifying_key().to_bytes());

    let mut mfr = Manufacturer::create(&pool, "multiarch", None)
        .await
        .unwrap();
    mfr.activate(&pool, pub_b64).await.unwrap();

    let manager = MfrManager::new(pool.clone());

    // Same package name+version, different targets
    for target in &[
        "wasm32-wasip1",
        "x86_64-unknown-linux-gnu",
        "aarch64-apple-darwin",
    ] {
        let manifest = format!(
            "manufacturer = \"multiarch\"\nname = \"server\"\nversion = \"1.0.0\"\n\n[binary]\npath = \"server.wasm\"\ntarget = \"{target}\"\nhash = \"sha256:abc123\""
        );
        let sig = key.sign(manifest.as_bytes());
        let sig_b64 = base64::engine::general_purpose::STANDARD.encode(sig.to_bytes());
        let (nonce_b64, nonce_sig_b64) = make_publish_nonce(
            &pool,
            mfr.id,
            "multiarch",
            "server",
            "1.0.0",
            target,
            &manifest,
            &sig_b64,
            None,
            &key,
        )
        .await;
        manager
            .publish_package(PublishRequest {
                manufacturer: "multiarch".to_string(),
                name: "server".to_string(),
                version: "1.0.0".to_string(),
                target: target.to_string(),
                manifest,
                signature: sig_b64,
                proto_files: None,
                nonce: Some(nonce_b64),
                nonce_sig: Some(nonce_sig_b64),
            })
            .await
            .unwrap();
    }

    let pkgs = manager.list_packages(Some("multiarch")).await.unwrap();
    assert_eq!(
        pkgs.len(),
        3,
        "same package with 3 targets should produce 3 entries"
    );
    let targets: Vec<&str> = pkgs.iter().map(|p| p.target.as_str()).collect();
    assert!(targets.contains(&"wasm32-wasip1"));
    assert!(targets.contains(&"x86_64-unknown-linux-gnu"));
    assert!(targets.contains(&"aarch64-apple-darwin"));
}

#[tokio::test]
async fn test_publish_tampered_proto_files_rejected() {
    use base64::Engine as _;
    use ed25519_dalek::{Signer, SigningKey};
    use rand::rngs::OsRng;

    let pool = setup_test_pool().await;
    let key = SigningKey::generate(&mut OsRng);
    let pub_b64 = base64::engine::general_purpose::STANDARD.encode(key.verifying_key().to_bytes());

    let mut mfr = Manufacturer::create(&pool, "protoco", None).await.unwrap();
    mfr.activate(&pool, pub_b64).await.unwrap();

    let manifest = test_manifest("protoco", "svc", "1.0.0", "wasm32-wasip1");
    let sig = key.sign(manifest.as_bytes());
    let sig_b64 = base64::engine::general_purpose::STANDARD.encode(sig.to_bytes());
    let proto_original = serde_json::json!({
        "protobufs": [
            { "relative_path": "api/v1/service.proto", "content_b64": "YWJj" }
        ]
    });
    let proto_tampered = serde_json::json!({
        "protobufs": [
            { "relative_path": "api/v1/service.proto", "content_b64": "ZGVm" }
        ]
    });

    let (nonce_b64, nonce_sig_b64) = make_publish_nonce(
        &pool,
        mfr.id,
        "protoco",
        "svc",
        "1.0.0",
        "wasm32-wasip1",
        &manifest,
        &sig_b64,
        Some(&proto_original),
        &key,
    )
    .await;

    let manager = MfrManager::new(pool);
    let result = manager
        .publish_package(PublishRequest {
            manufacturer: "protoco".to_string(),
            name: "svc".to_string(),
            version: "1.0.0".to_string(),
            target: "wasm32-wasip1".to_string(),
            manifest,
            signature: sig_b64,
            proto_files: Some(proto_tampered),
            nonce: Some(nonce_b64),
            nonce_sig: Some(nonce_sig_b64),
        })
        .await;

    assert!(
        matches!(result, Err(MfrError::Unauthorized)),
        "tampered proto_files should invalidate nonce authorization: {result:?}"
    );
}

#[tokio::test]
async fn test_publish_manifest_missing_required_field_rejected() {
    use base64::Engine as _;
    use ed25519_dalek::{Signer, SigningKey};
    use rand::rngs::OsRng;

    let pool = setup_test_pool().await;
    let key = SigningKey::generate(&mut OsRng);
    let pub_b64 = base64::engine::general_purpose::STANDARD.encode(key.verifying_key().to_bytes());

    let mut mfr = Manufacturer::create(&pool, "missingfield", None)
        .await
        .unwrap();
    mfr.activate(&pool, pub_b64).await.unwrap();

    let manifest = r#"manufacturer = "missingfield"
name = "svc"

[binary]
path = "bin/actor.wasm"
target = "wasm32-wasip1"
hash = "sha256:abc123"
"#;
    let sig = key.sign(manifest.as_bytes());
    let sig_b64 = base64::engine::general_purpose::STANDARD.encode(sig.to_bytes());
    let (nonce_b64, nonce_sig_b64) = make_publish_nonce(
        &pool,
        mfr.id,
        "missingfield",
        "svc",
        "1.0.0",
        "wasm32-wasip1",
        manifest,
        &sig_b64,
        None,
        &key,
    )
    .await;

    let manager = MfrManager::new(pool);
    let result = manager
        .publish_package(PublishRequest {
            manufacturer: "missingfield".to_string(),
            name: "svc".to_string(),
            version: "1.0.0".to_string(),
            target: "wasm32-wasip1".to_string(),
            manifest: manifest.to_string(),
            signature: sig_b64,
            proto_files: None,
            nonce: Some(nonce_b64),
            nonce_sig: Some(nonce_sig_b64),
        })
        .await;

    assert!(
        matches!(result, Err(MfrError::InvalidRequest(ref msg)) if msg.contains("missing required field 'version'")),
        "manifest missing version should be rejected: {result:?}"
    );
}
