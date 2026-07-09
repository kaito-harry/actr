use crate::commands::SupportedLanguage;
#[cfg(test)]
use crate::commands::codegen::proto_model::{
    MethodModel, ProtoFileModel, ProtoModel, ServiceModel, TypeOwnerIndex,
};
use crate::error::{ActrCliError, Result};
#[cfg(test)]
use actr_protocol::ActrType;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

pub const ACTR_GEN_META_FILE: &str = "actr-gen-meta.json";

/// Structured reference to a proto message type used as an RPC input/output.
///
/// `input_ref`/`output_ref` on [`MethodMetadata`] carry the bare message name
/// (`type_name`) together with the declaring package and proto file, so each
/// language generator can emit owner-qualified type references instead of
/// assuming the current service's package.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TypeRef {
    /// Fully-qualified proto type as written in the RPC signature
    /// (leading `.` stripped), e.g. `ask.ContinuePromptResultStreamsRequest`.
    pub proto_type: String,
    /// Owner-relative proto type name, e.g. `ContinuePromptResultStreamsRequest`
    /// or `Outer.InnerRequest`.
    pub type_name: String,
    /// Declaring proto package, e.g. `ask`.
    pub proto_package: String,
    /// Declaring proto file (path relative to the proto root), e.g.
    /// `remote/ask-service/ask.proto`.
    pub proto_file: String,
    /// Optional language-specific generated type name. Kotlin uses this to
    /// preserve descriptor options such as `java_package`,
    /// `java_outer_classname`, and `java_multiple_files`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generated_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ActrGenMetadata {
    pub plugin_version: String,
    pub language: String,
    #[serde(default)]
    pub local_services: Vec<LocalServiceMetadata>,
    #[serde(default)]
    pub remote_services: Vec<RemoteServiceMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalServiceMetadata {
    pub name: String,
    pub package: String,
    pub proto_file: String,
    pub handler_interface: String,
    pub workload_type: String,
    pub dispatcher_type: String,
    pub methods: Vec<MethodMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteServiceMetadata {
    pub name: String,
    pub package: String,
    pub proto_file: String,
    pub actr_type: String,
    pub client_type: String,
    pub methods: Vec<MethodMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MethodMetadata {
    pub name: String,
    pub snake_name: String,
    pub route_key: String,
    pub input_ref: TypeRef,
    pub output_ref: TypeRef,
}

impl ActrGenMetadata {
    #[cfg(test)]
    pub fn from_proto_model(language: SupportedLanguage, proto_model: &ProtoModel) -> Result<Self> {
        let owner_index = TypeOwnerIndex::from_files(&proto_model.files);
        let local_services = proto_model
            .local_services
            .iter()
            .map(|service| build_local_service_metadata(service, &proto_model.files, &owner_index))
            .collect::<Result<Vec<_>>>()?;
        let remote_services = proto_model
            .remote_services
            .iter()
            .map(|service| build_remote_service_metadata(service, &proto_model.files, &owner_index))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            plugin_version: "actr-cli".to_string(),
            language: language_key(language).to_string(),
            local_services,
            remote_services,
        })
    }
}

pub fn metadata_path(output_dir: &Path) -> PathBuf {
    output_dir.join(ACTR_GEN_META_FILE)
}

pub fn load_metadata(output_dir: &Path) -> Result<Option<ActrGenMetadata>> {
    let path = metadata_path(output_dir);
    if !path.exists() {
        return Ok(None);
    }

    let content = std::fs::read_to_string(&path).map_err(|e| {
        ActrCliError::config_error(format!("Failed to read {}: {e}", path.display()))
    })?;
    let metadata = serde_json::from_str(&content).map_err(|e| {
        ActrCliError::config_error(format!("Failed to parse {}: {e}", path.display()))
    })?;
    Ok(Some(metadata))
}

pub fn load_required_metadata(
    output_dir: &Path,
    language: SupportedLanguage,
) -> Result<ActrGenMetadata> {
    let path = metadata_path(output_dir);
    let metadata = load_metadata(output_dir)?.ok_or_else(|| {
        ActrCliError::config_error(format!(
            "ACTR plugin metadata is required but {} was not found",
            path.display()
        ))
    })?;
    let expected_language = language_key(language);
    if metadata.language != expected_language {
        return Err(ActrCliError::config_error(format!(
            "ACTR plugin metadata language mismatch in {}: expected `{}`, found `{}`",
            path.display(),
            expected_language,
            metadata.language
        )));
    }
    Ok(metadata)
}

pub fn write_metadata(output_dir: &Path, metadata: &ActrGenMetadata) -> Result<PathBuf> {
    std::fs::create_dir_all(output_dir).map_err(|e| {
        ActrCliError::config_error(format!(
            "Failed to create metadata output directory {}: {e}",
            output_dir.display()
        ))
    })?;

    let path = metadata_path(output_dir);
    let content = serde_json::to_string_pretty(metadata)?;
    std::fs::write(&path, content).map_err(|e| {
        ActrCliError::config_error(format!("Failed to write {}: {e}", path.display()))
    })?;

    Ok(path)
}

pub(crate) fn language_key(language: SupportedLanguage) -> &'static str {
    match language {
        SupportedLanguage::Rust => "rust",
        SupportedLanguage::Python => "python",
        SupportedLanguage::Swift => "swift",
        SupportedLanguage::Kotlin => "kotlin",
        SupportedLanguage::TypeScript => "typescript",
    }
}

#[cfg(test)]
fn build_local_service_metadata(
    service: &ServiceModel,
    files: &[ProtoFileModel],
    owner_index: &TypeOwnerIndex,
) -> Result<LocalServiceMetadata> {
    let current_file = files
        .iter()
        .find(|file| file.relative_path == service.relative_path);
    Ok(LocalServiceMetadata {
        name: service.name.clone(),
        package: service.package.clone(),
        proto_file: service.relative_path.to_string_lossy().to_string(),
        handler_interface: format!("{}Handler", service.name),
        workload_type: format!("{}Workload", service.name),
        dispatcher_type: format!("{}Dispatcher", service.name),
        methods: service
            .methods
            .iter()
            .map(|method| build_method_metadata(method, service, current_file, owner_index))
            .collect::<Result<Vec<_>>>()?,
    })
}

#[cfg(test)]
fn build_remote_service_metadata(
    service: &ServiceModel,
    files: &[ProtoFileModel],
    owner_index: &TypeOwnerIndex,
) -> Result<RemoteServiceMetadata> {
    let current_file = files
        .iter()
        .find(|file| file.relative_path == service.relative_path);
    Ok(RemoteServiceMetadata {
        name: service.name.clone(),
        package: service.package.clone(),
        proto_file: service.relative_path.to_string_lossy().to_string(),
        actr_type: service.actr_type.clone().unwrap_or_else(|| {
            ActrType {
                manufacturer: "acme".to_string(),
                name: service.name.clone(),
                version: "1.0.0".to_string(),
            }
            .to_string_repr()
        }),
        client_type: format!("{}Client", service.name),
        methods: service
            .methods
            .iter()
            .map(|method| build_method_metadata(method, service, current_file, owner_index))
            .collect::<Result<Vec<_>>>()?,
    })
}

#[cfg(test)]
fn build_method_metadata(
    method: &MethodModel,
    service: &ServiceModel,
    current_file: Option<&ProtoFileModel>,
    owner_index: &TypeOwnerIndex,
) -> Result<MethodMetadata> {
    let input_ref = resolve_type_ref(
        &method.input_type,
        service,
        &method.name,
        "input",
        current_file,
        owner_index,
    )?;
    let output_ref = resolve_type_ref(
        &method.output_type,
        service,
        &method.name,
        "output",
        current_file,
        owner_index,
    )?;
    Ok(MethodMetadata {
        name: method.name.clone(),
        snake_name: method.snake_name.clone(),
        route_key: method.route_key.clone(),
        input_ref,
        output_ref,
    })
}

#[cfg(test)]
fn resolve_type_ref(
    referenced: &str,
    service: &ServiceModel,
    method_name: &str,
    kind: &str,
    current_file: Option<&ProtoFileModel>,
    owner_index: &TypeOwnerIndex,
) -> Result<TypeRef> {
    let normalized = referenced.trim().trim_start_matches('.');
    let type_name = normalized
        .rsplit('.')
        .next()
        .unwrap_or(normalized)
        .to_string();

    let fallback = || TypeRef {
        proto_type: normalized.to_string(),
        type_name: type_name.clone(),
        proto_package: service.package.clone(),
        proto_file: service.relative_path.to_string_lossy().to_string(),
        generated_type: None,
    };

    let Some(current) = current_file else {
        if normalized.contains('.') {
            return Err(unresolved_qualified_type_error(
                kind,
                normalized,
                service,
                method_name,
            ));
        }
        return Ok(fallback());
    };

    match owner_index.resolve(referenced, current) {
        Ok(Some(owner)) => Ok(TypeRef {
            proto_type: normalized.to_string(),
            type_name: owner.type_name,
            proto_package: owner.proto_package,
            proto_file: owner.proto_file,
            generated_type: None,
        }),
        Ok(None) if normalized.contains('.') => Err(unresolved_qualified_type_error(
            kind,
            normalized,
            service,
            method_name,
        )),
        Ok(None) => Ok(fallback()),
        Err(candidates) => {
            let declared_files = candidates
                .iter()
                .map(|owner| owner.proto_file.clone())
                .collect::<Vec<_>>()
                .join(", ");
            Err(ActrCliError::config_error(format!(
                "Cannot uniquely resolve {} type `{}` for {}.{}: declared in multiple proto files [{}]",
                kind, normalized, service.name, method_name, declared_files
            )))
        }
    }
}

#[cfg(test)]
fn unresolved_qualified_type_error(
    kind: &str,
    normalized: &str,
    service: &ServiceModel,
    method_name: &str,
) -> ActrCliError {
    ActrCliError::config_error(format!(
        "Cannot resolve {} type `{}` for {}.{}: qualified RPC types must be declared in one of the parsed proto files",
        kind, normalized, service.name, method_name
    ))
}

#[cfg(test)]
#[path = "metadata_tests.rs"]
mod tests;
