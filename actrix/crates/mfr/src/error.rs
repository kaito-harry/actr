use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::json;

#[derive(Debug, thiserror::Error)]
pub enum MfrError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("not found")]
    NotFound,
    #[error("key revoked: {0}")]
    KeyRevoked(String),
    #[error("already exists: {0}")]
    AlreadyExists(String),
    #[error("reserved name: {0}")]
    ReservedName(String),
    #[error("invalid name: {0}")]
    InvalidName(String),
    #[error("invalid status for this operation: {0}")]
    InvalidStatus(String),
    #[error("verification failed: {0}")]
    VerificationFailed(String),
    #[error("challenge expired or not found")]
    ChallengeNotFound,
    #[error("invalid signature")]
    InvalidSignature,
    #[error("package already published, revoke first")]
    PackageAlreadyPublished,
    #[error("github api error: {0}")]
    GitHub(String),
    #[error("crypto error: {0}")]
    Crypto(String),
    #[error("unauthorized")]
    Unauthorized,
    #[error("certificate expired: signing key has expired, please renew")]
    CertificateExpired,
    #[error("invalid request: {0}")]
    InvalidRequest(String),
}

impl IntoResponse for MfrError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            MfrError::NotFound => (StatusCode::NOT_FOUND, self.to_string()),
            MfrError::AlreadyExists(_) | MfrError::PackageAlreadyPublished => {
                (StatusCode::CONFLICT, self.to_string())
            }
            MfrError::ReservedName(_) | MfrError::InvalidName(_) | MfrError::InvalidRequest(_) => {
                (StatusCode::BAD_REQUEST, self.to_string())
            }
            MfrError::InvalidStatus(_) => (StatusCode::UNPROCESSABLE_ENTITY, self.to_string()),
            MfrError::InvalidSignature | MfrError::Unauthorized => {
                (StatusCode::UNAUTHORIZED, self.to_string())
            }
            MfrError::VerificationFailed(_) | MfrError::ChallengeNotFound => {
                (StatusCode::BAD_REQUEST, self.to_string())
            }
            MfrError::KeyRevoked(_) | MfrError::CertificateExpired => {
                (StatusCode::FORBIDDEN, self.to_string())
            }
            _ => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal error".to_string(),
            ),
        };
        (status, Json(json!({"error": message}))).into_response()
    }
}
