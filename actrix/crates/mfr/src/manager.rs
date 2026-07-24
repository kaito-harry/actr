use crate::{
    MfrError, crypto, github,
    model::{
        ActrPackage, GitHubRepoChallenge, Manufacturer, MfrKeyHistory, MfrStatus, PkgStatus,
        PublishNonce, key_history::KeyHistoryStatus,
    },
    reserved,
};
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum KeySource {
    /// Server generated the keypair; private_key is present.
    Generated,
    /// User uploaded their own public key; private_key is absent.
    Uploaded,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ActivateResponse {
    /// How the key was provisioned.
    pub key_source: KeySource,
    /// Ed25519 private key, base64. Present ONLY when key_source == Generated.
    /// Returned ONCE — never stored by actrix.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub private_key: Option<String>,
    pub certificate: MfrCertificate,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MfrCertificate {
    pub key_id: String,
    pub mfr_name: String,
    pub mfr_pubkey: String,
    pub issued_at: i64,
    /// End of this key's authority to publish new packages. Natural expiry
    /// does not invalidate signatures made by this key.
    pub expires_at: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PublishRequest {
    pub manufacturer: String,
    pub name: String,
    pub version: String,
    /// Target platform (e.g. "wasm32-wasip1", "x86_64-unknown-linux-gnu")
    #[serde(default = "default_target")]
    pub target: String,
    /// Full content of actr.toml (with binary_hash field populated)
    pub manifest: String,
    /// base64 Ed25519 signature by MFR private key over manifest bytes
    pub signature: String,
    /// Proto files JSON for filing/audit (optional)
    #[serde(default)]
    pub proto_files: Option<serde_json::Value>,
    /// Challenge-Response nonce (base64-encoded 32 bytes, obtained from POST /mfr/pkg/nonce)
    #[serde(default)]
    pub nonce: Option<String>,
    /// Ed25519 signature over the request authorization payload:
    /// ACTR-PUBLISH-V1 + manufacturer + method + path + hex(nonce) + sha256(signable_body)
    #[serde(default)]
    pub nonce_sig: Option<String>,
}

/// Request body for POST /mfr/pkg/nonce
#[derive(Debug, Serialize, Deserialize)]
pub struct NonceRequest {
    pub manufacturer: String,
}

fn default_target() -> String {
    "wasm32-wasip1".to_string()
}

#[derive(Serialize)]
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

fn signable_publish_body_bytes(req: &PublishRequest, nonce: &str) -> Result<Vec<u8>, MfrError> {
    serde_json::to_vec(&SignablePublishBody {
        manufacturer: &req.manufacturer,
        name: &req.name,
        version: &req.version,
        target: &req.target,
        manifest: &req.manifest,
        signature: &req.signature,
        proto_files: req.proto_files.as_ref(),
        nonce,
    })
    .map_err(|e| {
        MfrError::InvalidRequest(format!("failed to serialize signable publish body: {e}"))
    })
}

fn build_publish_auth_payload(
    manufacturer: &str,
    nonce_hex: &str,
    body_sha256_hex: &str,
) -> String {
    format!(
        "ACTR-PUBLISH-V1\nmanufacturer={manufacturer}\nmethod=POST\npath=/mfr/pkg/publish\nnonce={nonce_hex}\nbody_sha256={body_sha256_hex}"
    )
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MfrPublicInfo {
    pub id: i64,
    pub key_id: String,
    pub name: String,
    pub public_key: String,
    pub certificate: MfrCertificate,
}

pub struct MfrManager {
    pool: SqlitePool,
    /// Domain of this actrix node, used as the verification filename.
    domain: String,
    /// How long (seconds) to retain expired/used nonce records for auditing.
    /// Default: 86400 (24 hours).
    nonce_retain_secs: i64,
}

impl MfrManager {
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            pool,
            domain: String::new(),
            nonce_retain_secs: 86400, // 24 hours
        }
    }

    pub fn with_domain(mut self, domain: String) -> Self {
        self.domain = domain;
        self
    }

    /// Set nonce retention duration in seconds (for cleanup of expired/used nonces).
    pub fn with_nonce_retain_secs(mut self, secs: i64) -> Self {
        self.nonce_retain_secs = secs;
        self
    }

    pub fn domain(&self) -> &str {
        &self.domain
    }

    /// Access the underlying database pool (used by tests for nonce setup).
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    fn validate_publish_authority(mfr: &Manufacturer) -> Result<(), MfrError> {
        if mfr.status != MfrStatus::Active {
            return Err(MfrError::InvalidStatus(format!(
                "MFR '{}' is not active",
                mfr.name
            )));
        }

        let key_expires_at = mfr.key_expires_at.ok_or(MfrError::CertificateExpired)?;
        if chrono::Utc::now().timestamp() >= key_expires_at {
            return Err(MfrError::CertificateExpired);
        }

        Ok(())
    }

    // ========== Publish Nonce Challenge-Response ==========

    /// Request a one-time nonce for publish authentication.
    ///
    /// Also performs lazy cleanup of old nonce records.
    pub async fn request_nonce(&self, manufacturer: &str) -> Result<Vec<u8>, MfrError> {
        let mfr = Manufacturer::get_by_name(&self.pool, manufacturer)
            .await?
            .ok_or(MfrError::NotFound)?;
        Self::validate_publish_authority(&mfr)?;

        // Lazy cleanup of old nonces
        let cleaned = PublishNonce::cleanup(&self.pool, self.nonce_retain_secs).await?;
        if cleaned > 0 {
            platform::recording::debug!("cleaned up {} expired publish nonces", cleaned);
        }

        // Create new nonce
        let nonce = PublishNonce::create(&self.pool, mfr.id).await?;
        platform::recording::info!(
            "publish nonce issued for manufacturer '{}' (mfr_id={})",
            manufacturer,
            mfr.id
        );
        Ok(nonce)
    }

    /// Step 1: Apply for manufacturer registration via GitHub identity.
    /// The GitHub login (user or org) becomes the manufacturer name.
    /// Returns a challenge token that the user must place in a public repo.
    pub async fn apply(
        &self,
        github_login: &str,
        contact: Option<&str>,
    ) -> Result<(Manufacturer, GitHubRepoChallenge), MfrError> {
        let login = github_login.to_ascii_lowercase();
        reserved::validate_github_login(&login)?;
        let mfr = Manufacturer::create(&self.pool, &login, contact).await?;
        let challenge = GitHubRepoChallenge::create(&self.pool, mfr.id).await?;
        platform::recording::info!("MFR application received: github_login={}", login,);
        Ok((mfr, challenge))
    }

    /// Step 2: Verify ownership by checking a public GitHub repo.
    ///
    /// Looks for `{mfr.name}/actr-mfr-verify/{domain}.txt` containing the challenge token.
    ///
    /// If `user_public_key` is Some, the user's own Ed25519 public key is used (uploaded mode).
    /// If None, a new keypair is generated and the private key is returned once (generated mode).
    pub async fn verify_github(
        &self,
        mfr_id: i64,
        user_public_key: Option<&str>,
    ) -> Result<ActivateResponse, MfrError> {
        let mut mfr = Manufacturer::get(&self.pool, mfr_id)
            .await?
            .ok_or(MfrError::NotFound)?;

        if mfr.status != MfrStatus::Pending {
            return Err(MfrError::InvalidStatus(format!(
                "cannot verify MFR with status: {}",
                mfr.status
            )));
        }

        let mut challenge = GitHubRepoChallenge::get_active(&self.pool, mfr_id)
            .await?
            .ok_or(MfrError::ChallengeNotFound)?;

        let filename = github::verify_filename(&self.domain);
        let verified = github::verify_repo(&mfr.name, &challenge.token, &self.domain).await?;
        if !verified {
            return Err(MfrError::VerificationFailed(format!(
                "{filename} does not contain the expected challenge token"
            )));
        }

        let url = github::repo_url(&mfr.name);
        challenge.mark_verified(&self.pool, &url).await?;

        let response = self.activate_with_key(&mut mfr, user_public_key).await?;
        platform::recording::info!(
            "MFR verified via GitHub repo: mfr_id={}, name={}, key_source={:?}",
            mfr_id,
            mfr.name,
            response.key_source
        );
        Ok(response)
    }

    /// Admin: manually approve without GitHub verification (for private deployments).
    ///
    /// If `user_public_key` is Some, the user's own Ed25519 public key is used (uploaded mode).
    /// If None, a new keypair is generated and the private key is returned once (generated mode).
    pub async fn admin_approve(
        &self,
        mfr_id: i64,
        user_public_key: Option<&str>,
    ) -> Result<ActivateResponse, MfrError> {
        let mut mfr = Manufacturer::get(&self.pool, mfr_id)
            .await?
            .ok_or(MfrError::NotFound)?;

        let response = self.activate_with_key(&mut mfr, user_public_key).await?;
        platform::recording::info!(
            "MFR manually approved by admin: mfr_id={}, name={}, key_source={:?}",
            mfr_id,
            mfr.name,
            response.key_source
        );
        Ok(response)
    }

    /// Common key provisioning logic for both verify_github and admin_approve.
    ///
    /// - `user_public_key = None` → generate a new Ed25519 keypair, return private key.
    /// - `user_public_key = Some(b64)` → validate and use the provided public key, no private key returned.
    async fn activate_with_key(
        &self,
        mfr: &mut Manufacturer,
        user_public_key: Option<&str>,
    ) -> Result<ActivateResponse, MfrError> {
        let (key_source, private_key, public_key) = match user_public_key {
            Some(pk) => {
                crypto::validate_public_key(pk)?;
                (KeySource::Uploaded, None, pk.to_string())
            }
            None => {
                let (priv_key, pub_key) = crypto::generate_keypair();
                (KeySource::Generated, Some(priv_key), pub_key)
            }
        };

        mfr.activate(&self.pool, public_key).await?;

        let expires_at = mfr.key_expires_at.ok_or(MfrError::CertificateExpired)?;
        Ok(ActivateResponse {
            key_source,
            private_key,
            certificate: MfrCertificate {
                key_id: mfr.key_id.clone(),
                mfr_name: mfr.name.clone(),
                mfr_pubkey: mfr.public_key.clone(),
                issued_at: mfr.verified_at.unwrap_or(mfr.created_at),
                expires_at,
            },
        })
    }

    /// Get the active (unexpired, unverified) challenge for a pending MFR.
    pub async fn get_challenge(&self, mfr_id: i64) -> Result<GitHubRepoChallenge, MfrError> {
        let mfr = Manufacturer::get(&self.pool, mfr_id)
            .await?
            .ok_or(MfrError::NotFound)?;
        if mfr.status != MfrStatus::Pending {
            return Err(MfrError::InvalidStatus(format!(
                "MFR is not pending (status: {})",
                mfr.status
            )));
        }
        GitHubRepoChallenge::get_active(&self.pool, mfr_id)
            .await?
            .ok_or(MfrError::ChallengeNotFound)
    }

    pub async fn get_status(&self, mfr_id: i64) -> Result<Manufacturer, MfrError> {
        Manufacturer::get(&self.pool, mfr_id)
            .await?
            .ok_or(MfrError::NotFound)
    }

    pub async fn resolve_by_name(&self, name: &str) -> Result<MfrPublicInfo, MfrError> {
        let mfr = Manufacturer::get_by_name(&self.pool, name)
            .await?
            .ok_or(MfrError::NotFound)?;
        if mfr.status != MfrStatus::Active {
            return Err(MfrError::InvalidStatus(format!(
                "MFR '{}' is not active",
                name
            )));
        }
        let expires_at = mfr.key_expires_at.ok_or(MfrError::CertificateExpired)?;
        let cert = MfrCertificate {
            key_id: mfr.key_id.clone(),
            mfr_name: mfr.name.clone(),
            mfr_pubkey: mfr.public_key.clone(),
            issued_at: mfr.verified_at.unwrap_or(mfr.created_at),
            expires_at,
        };
        Ok(MfrPublicInfo {
            id: mfr.id,
            key_id: mfr.key_id.clone(),
            name: mfr.name,
            public_key: mfr.public_key,
            certificate: cert,
        })
    }

    /// Resolve a specific historical (or active) key by exact key_id.
    pub async fn resolve_key_by_id(
        &self,
        name: &str,
        key_id: &str,
    ) -> Result<MfrPublicInfo, MfrError> {
        let mfr = Manufacturer::get_by_name(&self.pool, name)
            .await?
            .ok_or(MfrError::NotFound)?;
        if mfr.status != MfrStatus::Active {
            return Err(MfrError::InvalidStatus(format!(
                "MFR '{}' is not active",
                name
            )));
        }

        // Is it the current key?
        if mfr.key_id == key_id {
            return self.resolve_by_name(name).await;
        }

        use crate::model::key_history::MfrKeyHistory;
        let history = MfrKeyHistory::get_by_key_id(&self.pool, mfr.id, key_id)
            .await?
            .ok_or(MfrError::NotFound)?;

        let cert = MfrCertificate {
            key_id: history.key_id.clone(),
            mfr_name: mfr.name.clone(),
            mfr_pubkey: history.public_key.clone(),
            issued_at: history.created_at,
            expires_at: history.retired_at, // Old keys technically "expired" at retirement time
        };

        Ok(MfrPublicInfo {
            id: mfr.id,
            key_id: history.key_id.clone(),
            name: mfr.name,
            public_key: history.public_key,
            certificate: cert,
        })
    }

    /// Verify the MFR key that authenticated a published package.
    ///
    /// Natural expiration and normal retirement do not invalidate an existing
    /// package. Explicitly revoked keys do. New packages store signing_key_id
    /// directly; legacy rows fall back to the signed manifest or, if needed,
    /// cryptographic identification across the MFR's non-revoked keys.
    pub async fn verify_published_package_signing_key(
        &self,
        package: &ActrPackage,
    ) -> Result<String, MfrError> {
        let mfr = Manufacturer::get(&self.pool, package.mfr_id)
            .await?
            .ok_or(MfrError::NotFound)?;
        if mfr.name != package.manufacturer {
            return Err(MfrError::InvalidRequest(
                "package manufacturer does not match its MFR record".to_string(),
            ));
        }
        if mfr.status != MfrStatus::Active {
            return Err(MfrError::InvalidStatus(format!(
                "MFR '{}' is not active",
                mfr.name
            )));
        }

        let manifest_key_id = package
            .manifest
            .parse::<toml::Value>()
            .ok()
            .and_then(|manifest| {
                manifest
                    .get("signing_key_id")
                    .and_then(|value| value.as_str())
                    .map(str::to_owned)
            });

        if let (Some(stored), Some(manifest)) = (
            package.signing_key_id.as_deref(),
            manifest_key_id.as_deref(),
        ) && stored != manifest
        {
            return Err(MfrError::InvalidSignature);
        }

        if let Some(key_id) = package
            .signing_key_id
            .as_deref()
            .or(manifest_key_id.as_deref())
        {
            let key = self.resolve_key_by_id(&mfr.name, key_id).await?;
            let valid = crypto::verify_signature(
                package.manifest.as_bytes(),
                &package.signature,
                &key.public_key,
            )?;
            return if valid {
                Ok(key.key_id)
            } else {
                Err(MfrError::InvalidSignature)
            };
        }

        // Legacy package: identify the signing key by verifying the stored
        // signature against the current key and all non-revoked historical keys.
        if !mfr.key_id.is_empty() && !mfr.public_key.is_empty() {
            let valid = crypto::verify_signature(
                package.manifest.as_bytes(),
                &package.signature,
                &mfr.public_key,
            )?;
            if valid {
                return Ok(mfr.key_id);
            }
        }

        for history in MfrKeyHistory::list_by_mfr(&self.pool, mfr.id).await? {
            if history.status == KeyHistoryStatus::Revoked {
                continue;
            }
            let valid = crypto::verify_signature(
                package.manifest.as_bytes(),
                &package.signature,
                &history.public_key,
            )?;
            if valid {
                return Ok(history.key_id);
            }
        }

        Err(MfrError::InvalidSignature)
    }

    /// Admin: Rotate the signing key for a manufacturer.
    pub async fn renew_key(
        &self,
        mfr_id: i64,
        user_public_key: Option<&str>,
    ) -> Result<ActivateResponse, MfrError> {
        let mut mfr = Manufacturer::get(&self.pool, mfr_id)
            .await?
            .ok_or(MfrError::NotFound)?;

        let (key_source, private_key, public_key) = match user_public_key {
            Some(pk) => {
                crypto::validate_public_key(pk)?;
                (KeySource::Uploaded, None, pk.to_string())
            }
            None => {
                let (priv_key, pub_key) = crypto::generate_keypair();
                (KeySource::Generated, Some(priv_key), pub_key)
            }
        };

        mfr.renew_key(&self.pool, public_key).await?;

        let expires_at = mfr.key_expires_at.ok_or(MfrError::CertificateExpired)?;

        platform::recording::info!(
            "MFR key renewed: mfr_id={}, name={}, new_key_id={}, key_source={:?}",
            mfr_id,
            mfr.name,
            mfr.key_id,
            key_source
        );

        Ok(ActivateResponse {
            key_source,
            private_key,
            certificate: MfrCertificate {
                key_id: mfr.key_id.clone(),
                mfr_name: mfr.name.clone(),
                mfr_pubkey: mfr.public_key.clone(),
                issued_at: mfr.updated_at.unwrap_or(mfr.created_at),
                expires_at,
            },
        })
    }

    pub async fn publish_package(&self, req: PublishRequest) -> Result<ActrPackage, MfrError> {
        let mfr = Manufacturer::get_by_name(&self.pool, &req.manufacturer)
            .await?
            .ok_or(MfrError::NotFound)?;
        Self::validate_publish_authority(&mfr)?;

        // --- Challenge-Response nonce verification ---
        // Flow: find_pending → verify nonce_sig over the full signable request body
        //       → verify manifest_sig → atomic consume.
        let nonce_entry = match (&req.nonce, &req.nonce_sig) {
            (Some(nonce_b64), Some(nonce_sig_b64)) => {
                use base64::Engine as _;
                use sha2::{Digest, Sha256};

                // Decode nonce
                let nonce_bytes = base64::engine::general_purpose::STANDARD
                    .decode(nonce_b64)
                    .map_err(|_| MfrError::InvalidRequest("invalid nonce base64".to_string()))?;

                // Step 1: Read-only lookup — does NOT consume the nonce
                let entry = PublishNonce::find_pending(&self.pool, &nonce_bytes).await?;

                // Ensure nonce was issued for this manufacturer
                if entry.mfr_id != mfr.id {
                    platform::recording::warn!(
                        "publish nonce mfr_id mismatch: nonce={}, request={}",
                        entry.mfr_id,
                        mfr.id
                    );
                    return Err(MfrError::Unauthorized);
                }

                // Step 2: Verify nonce signature (before consuming).
                // The signature authorizes this exact publish request body (excluding nonce_sig).
                let body_bytes = signable_publish_body_bytes(&req, nonce_b64)?;
                let body_sha256 = hex::encode(Sha256::digest(&body_bytes));
                let nonce_hex = hex::encode(&nonce_bytes);
                let payload =
                    build_publish_auth_payload(&req.manufacturer, &nonce_hex, &body_sha256);

                let valid =
                    crypto::verify_signature(payload.as_bytes(), nonce_sig_b64, &mfr.public_key)?;
                if !valid {
                    platform::recording::warn!(
                        "publish nonce signature invalid: manufacturer={}",
                        req.manufacturer
                    );
                    return Err(MfrError::Unauthorized);
                }

                entry
            }
            _ => {
                platform::recording::warn!(
                    "publish request missing nonce/nonce_sig: manufacturer={}",
                    req.manufacturer
                );
                return Err(MfrError::Unauthorized);
            }
        };

        // Step 3: Verify manifest signature
        let valid =
            crypto::verify_signature(req.manifest.as_bytes(), &req.signature, &mfr.public_key)?;
        if !valid {
            return Err(MfrError::InvalidSignature);
        }

        // Step 4: Atomically consume nonce (only after both signatures verified)
        PublishNonce::consume(&self.pool, nonce_entry.id).await?;
        platform::recording::info!(
            "publish nonce verified and consumed: manufacturer={}",
            req.manufacturer
        );

        // Step 5: Verify outer fields match manifest content (parse as TOML)
        let manifest_toml: toml::Value = toml::from_str(&req.manifest)
            .map_err(|e| MfrError::InvalidRequest(format!("manifest is not valid TOML: {}", e)))?;

        // Check manufacturer
        let m = manifest_toml
            .get("manufacturer")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                MfrError::InvalidRequest("manifest missing required field 'manufacturer'".into())
            })?;
        if m != req.manufacturer {
            return Err(MfrError::InvalidRequest(format!(
                "manifest manufacturer '{}' != request '{}'",
                m, req.manufacturer
            )));
        }

        // Check name
        let n = manifest_toml
            .get("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                MfrError::InvalidRequest("manifest missing required field 'name'".into())
            })?;
        if n != req.name {
            return Err(MfrError::InvalidRequest(format!(
                "manifest name '{}' != request '{}'",
                n, req.name
            )));
        }

        // Check version
        let v = manifest_toml
            .get("version")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                MfrError::InvalidRequest("manifest missing required field 'version'".into())
            })?;
        if v != req.version {
            return Err(MfrError::InvalidRequest(format!(
                "manifest version '{}' != request '{}'",
                v, req.version
            )));
        }

        // If a modern manifest declares its signing key, it must identify the
        // current publish-authority key. The database field remains
        // authoritative so legacy manifests without this field stay publishable.
        if let Some(manifest_key_id) = manifest_toml
            .get("signing_key_id")
            .and_then(|value| value.as_str())
            && manifest_key_id != mfr.key_id
        {
            return Err(MfrError::InvalidRequest(format!(
                "manifest signing_key_id '{}' != current MFR key '{}'",
                manifest_key_id, mfr.key_id
            )));
        }

        // Check target (nested under [binary])
        let t = manifest_toml
            .get("binary")
            .and_then(|b| b.get("target"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                MfrError::InvalidRequest("manifest missing required field 'binary.target'".into())
            })?;
        if t != req.target {
            return Err(MfrError::InvalidRequest(format!(
                "manifest binary.target '{}' != request '{}'",
                t, req.target
            )));
        }

        // Serialize proto_files JSON to string for storage
        let proto_files_str = req.proto_files.as_ref().map(|v| v.to_string());

        let pkg = ActrPackage::publish(
            &self.pool,
            mfr.id,
            &req.manufacturer,
            &req.name,
            &req.version,
            &req.target,
            &req.manifest,
            &req.signature,
            &mfr.key_id,
            proto_files_str.as_deref(),
        )
        .await?;

        if req.proto_files.is_some() {
            platform::recording::info!(
                "actor package published with proto filing: type_str={}, mfr_id={}",
                pkg.type_str,
                mfr.id
            );
        } else {
            platform::recording::info!(
                "actor package published: type_str={}, mfr_id={}",
                pkg.type_str,
                mfr.id
            );
        }
        Ok(pkg)
    }

    pub async fn get_package(&self, type_str: &str) -> Result<ActrPackage, MfrError> {
        ActrPackage::get_by_type(&self.pool, type_str)
            .await?
            .ok_or(MfrError::NotFound)
    }

    pub async fn list_packages(
        &self,
        mfr_name: Option<&str>,
    ) -> Result<Vec<ActrPackage>, MfrError> {
        if let Some(name) = mfr_name {
            let mfr = Manufacturer::get_by_name(&self.pool, name)
                .await?
                .ok_or(MfrError::NotFound)?;
            ActrPackage::list_by_mfr(&self.pool, mfr.id).await
        } else {
            Ok(sqlx::query_as::<_, ActrPackage>(
                "SELECT * FROM mfr_package ORDER BY published_at DESC",
            )
            .fetch_all(&self.pool)
            .await?)
        }
    }

    pub async fn revoke_package(&self, pkg_id: i64) -> Result<(), MfrError> {
        let mut pkg = ActrPackage::get_by_id(&self.pool, pkg_id)
            .await?
            .ok_or(MfrError::NotFound)?;
        pkg.revoke(&self.pool).await?;
        platform::recording::warn!(
            "actor package revoked: pkg_id={}, type_str={}",
            pkg_id,
            pkg.type_str
        );
        Ok(())
    }

    // Admin methods
    pub async fn admin_list(
        &self,
        status: Option<MfrStatus>,
    ) -> Result<Vec<Manufacturer>, MfrError> {
        Manufacturer::list(&self.pool, status).await
    }

    pub async fn admin_suspend(&self, mfr_id: i64) -> Result<(), MfrError> {
        let mut mfr = Manufacturer::get(&self.pool, mfr_id)
            .await?
            .ok_or(MfrError::NotFound)?;
        mfr.suspend(&self.pool).await?;
        platform::recording::warn!(
            "MFR suspended by admin: mfr_id={}, name={}",
            mfr_id,
            mfr.name
        );
        Ok(())
    }

    pub async fn admin_reinstate(&self, mfr_id: i64) -> Result<(), MfrError> {
        let mut mfr = Manufacturer::get(&self.pool, mfr_id)
            .await?
            .ok_or(MfrError::NotFound)?;
        mfr.reinstate(&self.pool).await?;
        platform::recording::info!(
            "MFR reinstated by admin: mfr_id={}, name={}",
            mfr_id,
            mfr.name
        );
        Ok(())
    }

    pub async fn admin_list_keys(
        &self,
        mfr_id: i64,
    ) -> Result<Vec<crate::model::key_history::MfrKeyHistory>, MfrError> {
        // ensure MFR exists
        let _ = Manufacturer::get(&self.pool, mfr_id)
            .await?
            .ok_or(MfrError::NotFound)?;
        crate::model::key_history::MfrKeyHistory::list_by_mfr(&self.pool, mfr_id).await
    }

    pub async fn admin_revoke_historical_key(&self, history_id: i64) -> Result<(), MfrError> {
        crate::model::key_history::MfrKeyHistory::revoke(&self.pool, history_id).await?;
        platform::recording::warn!("MFR historical key revoked: history_id={}", history_id);
        Ok(())
    }

    pub async fn admin_delete(&self, mfr_id: i64) -> Result<(), MfrError> {
        Manufacturer::delete(&self.pool, mfr_id).await?;
        platform::recording::warn!("MFR deleted by admin: mfr_id={}", mfr_id);
        Ok(())
    }
}

/// Public API for AIS integration: check if a type_str is a valid, active package.
/// Reserved names always return true.
///
/// When `target` is provided, lookup is narrowed to that specific platform.
/// When `manifest_hash` is provided, the stored manifest is SHA-256 compared for content integrity.
pub async fn lookup_package(
    pool: &SqlitePool,
    type_str: &str,
    target: Option<&str>,
    manifest_hash: Option<&[u8]>,
) -> Result<bool, MfrError> {
    let pkg = if let Some(t) = target {
        ActrPackage::get_by_type_and_target(pool, type_str, t).await?
    } else {
        ActrPackage::get_by_type(pool, type_str).await?
    };

    match pkg {
        Some(p) if p.status == PkgStatus::Active => {
            // C1: manifest hash comparison (defense-in-depth)
            if let Some(expected_hash) = manifest_hash {
                use sha2::{Digest, Sha256};
                let mut hasher = Sha256::new();
                hasher.update(p.manifest.as_bytes());
                let stored_hash = hasher.finalize();
                if stored_hash.as_slice() != expected_hash {
                    platform::recording::warn!(
                        "manifest hash mismatch for type_str={}, target={:?}",
                        type_str,
                        target
                    );
                    return Ok(false);
                }
            }
            Ok(true)
        }
        _ => Ok(false),
    }
}
