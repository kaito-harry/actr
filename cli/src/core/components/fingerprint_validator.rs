//! Default FingerprintValidator implementation

use anyhow::Result;
use async_trait::async_trait;
use sha2::{Digest, Sha256};
use std::io::Read;
use std::path::Path;
use walkdir::WalkDir;

use super::{Fingerprint, FingerprintValidator, ResolvedDependency, ServiceInfo};

/// Default fingerprint validator
pub struct DefaultFingerprintValidator;

impl DefaultFingerprintValidator {
    pub fn new() -> Self {
        Self
    }

    /// Compute SHA256 of a file's content
    fn hash_file(path: &Path) -> Result<Vec<u8>> {
        let mut hasher = Sha256::new();
        let mut file = std::fs::File::open(path)?;
        let mut buffer = [0u8; 8192];

        loop {
            let count = file.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            hasher.update(&buffer[..count]);
        }

        Ok(hasher.finalize().to_vec())
    }
}

impl Default for DefaultFingerprintValidator {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl FingerprintValidator for DefaultFingerprintValidator {
    async fn compute_service_fingerprint(&self, service: &ServiceInfo) -> Result<Fingerprint> {
        Ok(Fingerprint {
            algorithm: "sha256".to_string(),
            value: service.fingerprint.clone(),
        })
    }

    async fn verify_fingerprint(
        &self,
        expected: &Fingerprint,
        actual: &Fingerprint,
    ) -> Result<bool> {
        Ok(expected.algorithm == actual.algorithm && expected.value == actual.value)
    }

    async fn compute_project_fingerprint(&self, project_path: &Path) -> Result<Fingerprint> {
        let mut hasher = Sha256::new();
        let mut proto_files: Vec<_> = WalkDir::new(project_path)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("proto"))
            .collect();

        // Sort files to ensure deterministic hash
        proto_files.sort_by(|a, b| a.path().cmp(b.path()));

        for entry in proto_files {
            let file_hash = Self::hash_file(entry.path())?;
            hasher.update(&file_hash);
        }

        Ok(Fingerprint {
            algorithm: "sha256".to_string(),
            value: hex::encode(hasher.finalize()),
        })
    }

    async fn generate_lock_fingerprint(&self, deps: &[ResolvedDependency]) -> Result<Fingerprint> {
        let mut hasher = Sha256::new();
        let mut dep_names: Vec<_> = deps.iter().map(|d| &d.spec.name).collect();
        dep_names.sort();

        for name in dep_names {
            hasher.update(name.as_bytes());
            if let Some(dep) = deps.iter().find(|d| d.spec.name == *name) {
                hasher.update(dep.fingerprint.as_bytes());
            }
        }

        Ok(Fingerprint {
            algorithm: "sha256".to_string(),
            value: hex::encode(hasher.finalize()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{Fingerprint, ServiceInfo};
    use tempfile::TempDir;

    fn fp(algo: &str, value: &str) -> Fingerprint {
        Fingerprint {
            algorithm: algo.to_string(),
            value: value.to_string(),
        }
    }

    #[tokio::test]
    async fn verify_fingerprint_compares_algorithm_and_value() {
        let validator = DefaultFingerprintValidator::new();
        assert!(
            validator
                .verify_fingerprint(&fp("sha256", "abc"), &fp("sha256", "abc"))
                .await
                .unwrap()
        );
        // Different value → mismatch.
        assert!(
            !validator
                .verify_fingerprint(&fp("sha256", "abc"), &fp("sha256", "xyz"))
                .await
                .unwrap()
        );
        // Different algorithm → mismatch.
        assert!(
            !validator
                .verify_fingerprint(&fp("sha256", "abc"), &fp("md5", "abc"))
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn compute_service_fingerprint_echoes_service_fingerprint() {
        let validator = DefaultFingerprintValidator::new();
        let service = ServiceInfo {
            name: "echo".into(),
            tags: vec![],
            fingerprint: "deadbeef".into(),
            actr_type: actr_protocol::ActrType::from_string_repr("acme:Echo:1.0.0").unwrap(),
            published_at: None,
            description: None,
            methods: vec![],
        };
        let result = validator
            .compute_service_fingerprint(&service)
            .await
            .unwrap();
        assert_eq!(result.algorithm, "sha256");
        assert_eq!(result.value, "deadbeef");
    }

    #[tokio::test]
    async fn compute_project_fingerprint_is_deterministic_hex() {
        let validator = DefaultFingerprintValidator::new();
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("a.proto"), "syntax = \"proto3\";\n").unwrap();
        std::fs::write(dir.path().join("b.proto"), "message X {}\n").unwrap();

        let first = validator
            .compute_project_fingerprint(dir.path())
            .await
            .unwrap();
        let second = validator
            .compute_project_fingerprint(dir.path())
            .await
            .unwrap();
        assert_eq!(first.algorithm, "sha256");
        assert_eq!(first.value.len(), 64);
        assert_eq!(
            first.value, second.value,
            "fingerprint must be deterministic"
        );

        // Empty directory yields a valid (all-zero-input) hash.
        let empty = TempDir::new().unwrap();
        let empty_fp = validator
            .compute_project_fingerprint(empty.path())
            .await
            .unwrap();
        assert_ne!(empty_fp.value, first.value);
    }

    #[tokio::test]
    async fn generate_lock_fingerprint_is_stable_across_input_order() {
        let validator = DefaultFingerprintValidator::new();
        let mk = |name: &str, fp_val: &str| ResolvedDependency {
            spec: crate::core::DependencySpec {
                alias: name.into(),
                name: name.into(),
                actr_type: None,
                fingerprint: None,
            },
            fingerprint: fp_val.into(),
            proto_files: vec![],
        };
        let ordered = vec![mk("a", "1"), mk("b", "2")];
        let reversed = vec![mk("b", "2"), mk("a", "1")];
        let f1 = validator.generate_lock_fingerprint(&ordered).await.unwrap();
        let f2 = validator
            .generate_lock_fingerprint(&reversed)
            .await
            .unwrap();
        assert_eq!(
            f1.value, f2.value,
            "lock fingerprint must be order-independent"
        );
        assert_eq!(f1.algorithm, "sha256");
    }
}
