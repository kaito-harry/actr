//! AId 错误类型定义
//!
//! 定义了 AId Token 相关操作的各种错误类型

use crate::realm::error::RealmError;
use thiserror::Error;

/// AId Token 相关错误类型
#[derive(Debug, Error)]
pub enum AidError {
    #[error("Invalid credential format")]
    InvalidFormat,

    #[error("Token has expired")]
    Expired,

    #[error("Invalid timestamp: {0}")]
    InvalidTimestamp(String),

    #[error("Base64 decode error: {0}")]
    Base64DecodeError(#[from] base64::DecodeError),

    #[error("Invalid signature: {0}")]
    InvalidSignature(String),

    #[error("Proto decode failure: {0}")]
    DecodeFailure(String),

    #[error("Token generation failed: {0}")]
    GenerationFailed(String),

    #[error("Invalid credential prefix, expected 'ar-'")]
    InvalidPrefix,

    #[error("Empty credential ID")]
    EmptyId,

    #[error("Hex decode error: {0}")]
    HexDecodeError(String),

    #[error("Realm error: {0}")]
    RealmError(#[from] RealmError),
}
