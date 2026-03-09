//! Actor Identity Token 管理模块
//!
//! 提供 Actor Identity Token 验证功能（签发功能已移至 ais crate）

pub mod error;
pub mod validator;
pub mod verifier;

pub use actr_protocol::AIdCredential;
pub use error::AidError;
pub use validator::AIdCredentialValidator;
pub use verifier::AIdCredentialVerifier;
