use actr_config::ManifestConfig;
use anyhow::Result;
use async_trait::async_trait;

use super::{
    ConflictReport, ConflictType, DependencyGraph, DependencyResolver, DependencySpec,
    ResolvedDependency, ServiceDetails,
};

pub struct DefaultDependencyResolver;

impl DefaultDependencyResolver {
    pub fn new() -> Self {
        Self
    }
}

impl Default for DefaultDependencyResolver {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DependencyResolver for DefaultDependencyResolver {
    async fn resolve_spec(&self, config: &ManifestConfig) -> Result<Vec<DependencySpec>> {
        let specs: Vec<DependencySpec> = config
            .dependencies
            .iter()
            .map(|dependency| DependencySpec {
                alias: dependency.alias.clone(),
                name: dependency
                    .service
                    .as_ref()
                    .map(|service| service.name.clone())
                    .unwrap_or_else(|| dependency.alias.clone()),
                actr_type: dependency.actr_type.clone(),
                fingerprint: dependency
                    .service
                    .as_ref()
                    .map(|service| service.fingerprint.clone()),
            })
            .collect();

        Ok(specs)
    }

    async fn resolve_dependencies(
        &self,
        specs: &[DependencySpec],
        service_details: &[ServiceDetails],
    ) -> Result<Vec<ResolvedDependency>> {
        let mut resolved = Vec::with_capacity(specs.len());

        for spec in specs {
            // Find matching service details
            let matching_details = service_details.iter().find(|details| {
                details.info.name == spec.name
                    || details.info.actr_type.to_string_repr() == spec.name
                    || spec
                        .actr_type
                        .as_ref()
                        .is_some_and(|ty| details.info.actr_type == *ty)
            });

            let (fingerprint, proto_files) = match matching_details {
                Some(details) => (
                    details.info.fingerprint.clone(),
                    details.proto_files.clone(),
                ),
                None => (spec.fingerprint.clone().unwrap_or_default(), Vec::new()),
            };

            resolved.push(ResolvedDependency {
                spec: spec.clone(),
                fingerprint,
                proto_files,
            });
        }

        Ok(resolved)
    }

    async fn check_conflicts(&self, deps: &[ResolvedDependency]) -> Result<Vec<ConflictReport>> {
        let mut conflicts = Vec::new();

        for i in 0..deps.len() {
            for j in (i + 1)..deps.len() {
                // Conflict if same alias is used
                if deps[i].spec.alias == deps[j].spec.alias {
                    // Same alias is always a conflict if they point to different things
                    if deps[i].spec.name != deps[j].spec.name
                        || deps[i].fingerprint != deps[j].fingerprint
                    {
                        conflicts.push(ConflictReport {
                            dependency_a: deps[i].spec.alias.clone(),
                            dependency_b: deps[j].spec.alias.clone(),
                            conflict_type: ConflictType::VersionConflict,
                            description: format!(
                                "Dependency alias '{}' is duplicated with different targets",
                                deps[i].spec.alias
                            ),
                        });
                        continue;
                    }
                }

                // Conflict if same package name has different fingerprints
                if deps[i].spec.name != deps[j].spec.name {
                    continue;
                }

                if !deps[i].fingerprint.is_empty()
                    && !deps[j].fingerprint.is_empty()
                    && deps[i].fingerprint != deps[j].fingerprint
                {
                    conflicts.push(ConflictReport {
                        dependency_a: format!("{} ({})", deps[i].spec.name, deps[i].spec.alias),
                        dependency_b: format!("{} ({})", deps[j].spec.name, deps[j].spec.alias),
                        conflict_type: ConflictType::FingerprintMismatch,
                        description: format!(
                            "Dependency {} has conflicting fingerprints",
                            deps[i].spec.name
                        ),
                    });
                }
            }
        }

        Ok(conflicts)
    }

    async fn build_dependency_graph(&self, deps: &[ResolvedDependency]) -> Result<DependencyGraph> {
        let nodes: Vec<String> = deps.iter().map(|d| d.spec.alias.clone()).collect();

        Ok(DependencyGraph {
            nodes,
            edges: Vec::new(),
            has_cycles: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{DependencySpec, ProtoFile, ServiceDetails, ServiceInfo};

    fn actr_type(s: &str) -> actr_protocol::ActrType {
        actr_protocol::ActrType::from_string_repr(s).unwrap()
    }

    fn service_info(name: &str, fp_val: &str, type_repr: &str) -> ServiceInfo {
        ServiceInfo {
            name: name.into(),
            tags: vec![],
            fingerprint: fp_val.into(),
            actr_type: actr_type(type_repr),
            published_at: None,
            description: None,
            methods: vec![],
        }
    }

    fn spec(alias: &str, name: &str, actr: Option<&str>, fp: Option<&str>) -> DependencySpec {
        DependencySpec {
            alias: alias.into(),
            name: name.into(),
            actr_type: actr.map(actr_type),
            fingerprint: fp.map(str::to_string),
        }
    }

    fn resolved(spec: DependencySpec, fp_val: &str) -> ResolvedDependency {
        ResolvedDependency {
            spec,
            fingerprint: fp_val.into(),
            proto_files: vec![],
        }
    }

    #[tokio::test]
    async fn resolve_dependencies_matches_by_name_actr_type_or_falls_back() {
        let resolver = DefaultDependencyResolver::new();
        let specs = vec![
            spec("echo", "echo", None, None),
            spec("other", "different", Some("acme:Other:1.0.0"), None),
            spec("orphan", "orphan", None, Some("manual-fp")),
        ];
        let details = vec![
            ServiceDetails {
                info: service_info("echo", "echo-fp", "acme:Echo:1.0.0"),
                proto_files: vec![ProtoFile {
                    name: "echo.proto".into(),
                    path: "echo.proto".into(),
                    content: "syntax = \"proto3\";".into(),
                    services: vec![],
                }],
                dependencies: vec![],
            },
            ServiceDetails {
                info: service_info("other-service", "other-fp", "acme:Other:1.0.0"),
                proto_files: vec![],
                dependencies: vec![],
            },
        ];

        let result = resolver
            .resolve_dependencies(&specs, &details)
            .await
            .unwrap();
        assert_eq!(result.len(), 3);
        // Matched by name.
        assert_eq!(result[0].fingerprint, "echo-fp");
        assert_eq!(result[0].proto_files.len(), 1);
        // Matched by actr_type.
        assert_eq!(result[1].fingerprint, "other-fp");
        // No match → falls back to spec fingerprint, empty protos.
        assert_eq!(result[2].fingerprint, "manual-fp");
        assert!(result[2].proto_files.is_empty());
    }

    #[tokio::test]
    async fn resolve_dependencies_no_match_uses_empty_fingerprint() {
        let resolver = DefaultDependencyResolver::new();
        let specs = vec![spec("ghost", "ghost", None, None)];
        let result = resolver.resolve_dependencies(&specs, &[]).await.unwrap();
        assert_eq!(result[0].fingerprint, "");
        assert!(result[0].proto_files.is_empty());
    }

    #[tokio::test]
    async fn check_conflicts_reports_alias_dup_and_fingerprint_mismatch() {
        let resolver = DefaultDependencyResolver::new();
        // Same alias, different names → version conflict.
        let deps = vec![
            resolved(spec("dup", "a", None, None), "fp-a"),
            resolved(spec("dup", "b", None, None), "fp-b"),
        ];
        let conflicts = resolver.check_conflicts(&deps).await.unwrap();
        assert_eq!(conflicts.len(), 1);
        assert!(matches!(
            conflicts[0].conflict_type,
            ConflictType::VersionConflict
        ));

        // Same name, different non-empty fingerprints → fingerprint mismatch.
        let deps = vec![
            resolved(spec("alias1", "shared", None, None), "fp-1"),
            resolved(spec("alias2", "shared", None, None), "fp-2"),
        ];
        let conflicts = resolver.check_conflicts(&deps).await.unwrap();
        assert_eq!(conflicts.len(), 1);
        assert!(matches!(
            conflicts[0].conflict_type,
            ConflictType::FingerprintMismatch
        ));

        // No conflicts when deps are clean.
        let deps = vec![
            resolved(spec("a", "svc-a", None, None), "fp-a"),
            resolved(spec("b", "svc-b", None, None), "fp-b"),
        ];
        assert!(resolver.check_conflicts(&deps).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn build_dependency_graph_collects_aliases() {
        let resolver = DefaultDependencyResolver::new();
        let deps = vec![
            resolved(spec("alpha", "a", None, None), "1"),
            resolved(spec("beta", "b", None, None), "2"),
        ];
        let graph = resolver.build_dependency_graph(&deps).await.unwrap();
        assert_eq!(graph.nodes, vec!["alpha".to_string(), "beta".to_string()]);
        assert!(graph.edges.is_empty());
        assert!(!graph.has_cycles);
    }
}
