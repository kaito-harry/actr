use actrix_proto::{
    CreateRealmRequest, CreateRealmResponse, DeleteConfigOverrideRequest,
    DeleteConfigOverrideResponse, DeleteRealmRequest, DeleteRealmResponse, GetConfigRequest,
    GetConfigResponse, GetNodeInfoRequest, GetNodeInfoResponse, GetRealmRequest, GetRealmResponse,
    ListConfigOverridesRequest, ListConfigOverridesResponse, ListRealmsRequest, ListRealmsResponse,
    NodeAdminService, NonceCredential, SetConfigOverrideRequest, SetConfigOverrideResponse,
    ShutdownRequest, ShutdownResponse, UpdateConfigRequest, UpdateConfigResponse,
    UpdateRealmRequest, UpdateRealmResponse,
};
use nonce_auth::{CredentialVerifier, NonceError, storage::NonceStorage};
use std::sync::Arc;
use std::time::Duration;
use tonic::{Request, Response, Status};

/// 请求体需要提供认证载荷与凭证
pub trait CredentialPayload {
    fn credential(&self) -> &NonceCredential;
    fn auth_payload(&self, node_id: &str) -> String;
    /// Digest of the request body (excluding the `credential` field) for mutable
    /// operations. Appended to `auth_payload` as `{op}:{node_id}:{subject}:{digest}`
    /// so the signature binds the security-critical body fields (e.g. realm secret
    /// hashes) against in-transit tampering. Returns empty for read-only ops, which
    /// have no mutable body beyond what `auth_payload` already binds.
    fn body_digest(&self) -> String {
        String::new()
    }
}

/// SHA-256 hex digest of a canonical string.
fn sha256_hex(s: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(s.as_bytes());
    hex::encode(hasher.finalize())
}

fn opt_str(v: Option<&str>) -> &str {
    v.unwrap_or("")
}

fn opt_u64(v: Option<u64>) -> String {
    v.map(|x| x.to_string()).unwrap_or_default()
}

fn opt_i32(v: Option<i32>) -> String {
    v.map(|x| x.to_string()).unwrap_or_default()
}

fn opt_bool(v: Option<bool>) -> String {
    v.map(|b| b.to_string()).unwrap_or_default()
}

#[derive(Clone)]
struct VerifierState {
    node_id: String,
    shared_secret: Arc<Vec<u8>>,
    nonce_storage: Arc<dyn NonceStorage + Send + Sync>,
    max_clock_skew_secs: u64,
}

impl VerifierState {
    async fn verify(&self, credential: &NonceCredential, payload: String) -> Result<(), Status> {
        let nonce_credential = nonce_auth::NonceCredential {
            timestamp: credential.timestamp,
            nonce: credential.nonce.clone(),
            signature: credential.signature.clone(),
        };

        let verifier = CredentialVerifier::new(self.nonce_storage.clone())
            .with_secret(&self.shared_secret)
            .with_time_window(Duration::from_secs(self.max_clock_skew_secs))
            .with_storage_ttl(Duration::from_secs(self.max_clock_skew_secs + 300));

        verifier
            .verify(&nonce_credential, payload.as_bytes())
            .await
            .map_err(|e| map_nonce_error(e, "credential verification failed"))
    }
}

/// 在进入业务实现前统一做 NonceCredential 校验的包装服务
#[derive(Clone)]
pub struct AuthService<S> {
    inner: S,
    verifier: Arc<VerifierState>,
}

impl<S> AuthService<S> {
    pub fn new(
        inner: S,
        node_id: impl Into<String>,
        shared_secret: Arc<Vec<u8>>,
        nonce_storage: Arc<dyn NonceStorage + Send + Sync>,
        max_clock_skew_secs: u64,
    ) -> Self {
        let time_window = if max_clock_skew_secs == 0 {
            300
        } else {
            max_clock_skew_secs
        };

        Self {
            inner,
            verifier: Arc::new(VerifierState {
                node_id: node_id.into(),
                shared_secret,
                nonce_storage,
                max_clock_skew_secs: time_window,
            }),
        }
    }

    async fn verify_body<T: CredentialPayload>(&self, body: &T) -> Result<(), Status> {
        let mut payload = body.auth_payload(&self.verifier.node_id);
        let digest = body.body_digest();
        if !digest.is_empty() {
            payload.push(':');
            payload.push_str(&digest);
        }
        self.verifier.verify(body.credential(), payload).await
    }
}

#[tonic::async_trait]
impl<S> NodeAdminService for AuthService<S>
where
    S: NodeAdminService + Send + Sync + Clone + 'static,
{
    async fn update_config(
        &self,
        request: Request<UpdateConfigRequest>,
    ) -> Result<Response<UpdateConfigResponse>, Status> {
        self.verify_body(request.get_ref()).await?;
        self.inner.update_config(request).await
    }

    async fn get_config(
        &self,
        request: Request<GetConfigRequest>,
    ) -> Result<Response<GetConfigResponse>, Status> {
        self.verify_body(request.get_ref()).await?;
        self.inner.get_config(request).await
    }

    async fn create_realm(
        &self,
        request: Request<CreateRealmRequest>,
    ) -> Result<Response<CreateRealmResponse>, Status> {
        self.verify_body(request.get_ref()).await?;
        self.inner.create_realm(request).await
    }

    async fn get_realm(
        &self,
        request: Request<GetRealmRequest>,
    ) -> Result<Response<GetRealmResponse>, Status> {
        self.verify_body(request.get_ref()).await?;
        self.inner.get_realm(request).await
    }

    async fn update_realm(
        &self,
        request: Request<UpdateRealmRequest>,
    ) -> Result<Response<UpdateRealmResponse>, Status> {
        self.verify_body(request.get_ref()).await?;
        self.inner.update_realm(request).await
    }

    async fn delete_realm(
        &self,
        request: Request<DeleteRealmRequest>,
    ) -> Result<Response<DeleteRealmResponse>, Status> {
        self.verify_body(request.get_ref()).await?;
        self.inner.delete_realm(request).await
    }

    async fn list_realms(
        &self,
        request: Request<ListRealmsRequest>,
    ) -> Result<Response<ListRealmsResponse>, Status> {
        self.verify_body(request.get_ref()).await?;
        self.inner.list_realms(request).await
    }

    async fn get_node_info(
        &self,
        request: Request<GetNodeInfoRequest>,
    ) -> Result<Response<GetNodeInfoResponse>, Status> {
        self.verify_body(request.get_ref()).await?;
        self.inner.get_node_info(request).await
    }

    async fn shutdown(
        &self,
        request: Request<ShutdownRequest>,
    ) -> Result<Response<ShutdownResponse>, Status> {
        self.verify_body(request.get_ref()).await?;
        self.inner.shutdown(request).await
    }

    async fn list_config_overrides(
        &self,
        request: Request<ListConfigOverridesRequest>,
    ) -> Result<Response<ListConfigOverridesResponse>, Status> {
        self.verify_body(request.get_ref()).await?;
        self.inner.list_config_overrides(request).await
    }

    async fn set_config_override(
        &self,
        request: Request<SetConfigOverrideRequest>,
    ) -> Result<Response<SetConfigOverrideResponse>, Status> {
        self.verify_body(request.get_ref()).await?;
        self.inner.set_config_override(request).await
    }

    async fn delete_config_override(
        &self,
        request: Request<DeleteConfigOverrideRequest>,
    ) -> Result<Response<DeleteConfigOverrideResponse>, Status> {
        self.verify_body(request.get_ref()).await?;
        self.inner.delete_config_override(request).await
    }
}

// ========= 请求类型的载荷构造实现 =========

impl CredentialPayload for UpdateConfigRequest {
    fn credential(&self) -> &NonceCredential {
        &self.credential
    }

    fn auth_payload(&self, node_id: &str) -> String {
        format!(
            "update_config:{node_id}:{}:{}",
            self.config_type, self.config_key
        )
    }
}

impl CredentialPayload for GetConfigRequest {
    fn credential(&self) -> &NonceCredential {
        &self.credential
    }

    fn auth_payload(&self, node_id: &str) -> String {
        format!(
            "get_config:{node_id}:{}:{}",
            self.config_type, self.config_key
        )
    }
}

impl CredentialPayload for CreateRealmRequest {
    fn credential(&self) -> &NonceCredential {
        &self.credential
    }

    fn auth_payload(&self, node_id: &str) -> String {
        format!("create_realm:{node_id}:{}", self.realm_id.unwrap_or(0))
    }

    fn body_digest(&self) -> String {
        let canon = format!(
            "realm_id={}&name={}&enabled={}&status={}&expires_at={}&secret_current_hash={}&secret_previous_hash={}&secret_previous_valid_until={}",
            self.realm_id.unwrap_or(0),
            self.name,
            self.enabled,
            opt_str(self.status.as_deref()),
            self.expires_at,
            opt_str(self.secret_current_hash.as_deref()),
            opt_str(self.secret_previous_hash.as_deref()),
            opt_u64(self.secret_previous_valid_until),
        );
        sha256_hex(&canon)
    }
}

impl CredentialPayload for GetRealmRequest {
    fn credential(&self) -> &NonceCredential {
        &self.credential
    }

    fn auth_payload(&self, node_id: &str) -> String {
        format!("get_realm:{node_id}:{}", self.realm_id)
    }
}

impl CredentialPayload for UpdateRealmRequest {
    fn credential(&self) -> &NonceCredential {
        &self.credential
    }

    fn auth_payload(&self, node_id: &str) -> String {
        format!("update_realm:{node_id}:{}", self.realm_id)
    }

    fn body_digest(&self) -> String {
        let canon = format!(
            "realm_id={}&name={}&enabled={}&status={}&expires_at={}&secret_current_hash={}&secret_previous_hash={}&secret_previous_valid_until={}",
            self.realm_id,
            opt_str(self.name.as_deref()),
            opt_bool(self.enabled),
            opt_str(self.status.as_deref()),
            opt_u64(self.expires_at),
            opt_str(self.secret_current_hash.as_deref()),
            opt_str(self.secret_previous_hash.as_deref()),
            opt_u64(self.secret_previous_valid_until),
        );
        sha256_hex(&canon)
    }
}

impl CredentialPayload for DeleteRealmRequest {
    fn credential(&self) -> &NonceCredential {
        &self.credential
    }

    fn auth_payload(&self, node_id: &str) -> String {
        format!("delete_realm:{node_id}:{}", self.realm_id)
    }
}

impl CredentialPayload for ListRealmsRequest {
    fn credential(&self) -> &NonceCredential {
        &self.credential
    }

    fn auth_payload(&self, node_id: &str) -> String {
        format!("list_realms:{node_id}")
    }
}

impl CredentialPayload for GetNodeInfoRequest {
    fn credential(&self) -> &NonceCredential {
        &self.credential
    }

    fn auth_payload(&self, node_id: &str) -> String {
        format!("node_info:{node_id}")
    }
}

impl CredentialPayload for ShutdownRequest {
    fn credential(&self) -> &NonceCredential {
        &self.credential
    }

    fn auth_payload(&self, node_id: &str) -> String {
        format!("shutdown:{node_id}")
    }

    fn body_digest(&self) -> String {
        let canon = format!(
            "graceful={}&timeout_secs={}&reason={}",
            self.graceful,
            opt_i32(self.timeout_secs),
            opt_str(self.reason.as_deref()),
        );
        sha256_hex(&canon)
    }
}

impl CredentialPayload for ListConfigOverridesRequest {
    fn credential(&self) -> &NonceCredential {
        &self.credential
    }

    fn auth_payload(&self, node_id: &str) -> String {
        format!("list_config_overrides:{node_id}")
    }
}

impl CredentialPayload for SetConfigOverrideRequest {
    fn credential(&self) -> &NonceCredential {
        &self.credential
    }

    fn auth_payload(&self, node_id: &str) -> String {
        format!("set_config_override:{node_id}:{}", self.key)
    }
}

impl CredentialPayload for DeleteConfigOverrideRequest {
    fn credential(&self) -> &NonceCredential {
        &self.credential
    }

    fn auth_payload(&self, node_id: &str) -> String {
        format!("delete_config_override:{node_id}:{}", self.key)
    }
}

fn map_nonce_error(err: NonceError, context: &str) -> Status {
    match err {
        NonceError::DuplicateNonce => {
            Status::unauthenticated(format!("{context}: nonce already used"))
        }
        NonceError::TimestampOutOfWindow => {
            Status::unauthenticated(format!("{context}: timestamp out of range"))
        }
        NonceError::InvalidSignature => {
            Status::unauthenticated(format!("{context}: invalid signature"))
        }
        other => Status::internal(format!("{context}: {other}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Canonical body string for realm requests is the cross-repo contract:
    /// the superv client must build the identical string for the same logical
    /// fields. Any change to field order/rendering here must be mirrored on the
    /// superv side (see its `realm_body_digest` test, same expected digest).
    const REALM_CANON: &str = "realm_id=42&name=managed&enabled=true&status=Active&expires_at=1900000000&secret_current_hash=hash-42&secret_previous_hash=&secret_previous_valid_until=";

    fn dummy_credential() -> NonceCredential {
        NonceCredential {
            timestamp: 0,
            nonce: String::new(),
            signature: String::new(),
        }
    }

    #[test]
    fn create_realm_body_digest_matches_canonical() {
        let req = CreateRealmRequest {
            realm_id: Some(42),
            name: "managed".to_string(),
            enabled: true,
            credential: dummy_credential(),
            expires_at: 1_900_000_000,
            status: Some("Active".to_string()),
            secret_current_hash: Some("hash-42".to_string()),
            secret_previous_hash: None,
            secret_previous_valid_until: None,
        };
        assert_eq!(sha256_hex(REALM_CANON), req.body_digest());
    }

    #[test]
    fn update_realm_body_digest_matches_canonical() {
        let req = UpdateRealmRequest {
            realm_id: 42,
            name: Some("managed".to_string()),
            enabled: Some(true),
            credential: dummy_credential(),
            status: Some("Active".to_string()),
            expires_at: Some(1_900_000_000),
            secret_current_hash: Some("hash-42".to_string()),
            secret_previous_hash: None,
            secret_previous_valid_until: None,
        };
        assert_eq!(sha256_hex(REALM_CANON), req.body_digest());
    }

    #[test]
    fn shutdown_body_digest_matches_canonical() {
        let req = ShutdownRequest {
            graceful: true,
            timeout_secs: Some(30),
            reason: Some("deploy".to_string()),
            credential: dummy_credential(),
        };
        let canon = "graceful=true&timeout_secs=30&reason=deploy";
        assert_eq!(sha256_hex(canon), req.body_digest());
    }

    #[test]
    fn read_ops_have_empty_body_digest() {
        let get_realm = GetRealmRequest {
            realm_id: 42,
            credential: dummy_credential(),
        };
        assert_eq!(get_realm.body_digest(), "");
    }
}
