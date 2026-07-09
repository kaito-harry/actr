use crate::commands::codegen::proto_model::ProtoModel;
use crate::commands::codegen::scaffold::ScaffoldCatalog;
use crate::error::Result;
use actr_config::ManifestConfig;
use async_trait::async_trait;
use std::path::PathBuf;

/// Type of scaffold code to generate
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ScaffoldType {
    /// Generate export-side scaffold only
    Server,
    /// Generate dependency-side scaffold only
    Client,
    /// Generate both export and dependency scaffolds
    #[default]
    Both,
}

/// Context for code generation
#[derive(Debug, Clone)]
pub struct GenContext {
    pub proto_files: Vec<PathBuf>,
    pub proto_model: ProtoModel,
    pub input_path: PathBuf,
    pub output: PathBuf,
    pub config_path: PathBuf,
    pub config: ManifestConfig,
    pub no_scaffold: bool,
    pub overwrite_user_code: bool,
    pub no_format: bool,
    pub debug: bool,
    pub skip_validation: bool,
}

/// Interface for language-specific code generators
#[async_trait]
pub trait LanguageGenerator: Send + Sync {
    /// Generate infrastructure code (e.g., protobuf types, actors)
    async fn generate_infrastructure(&self, context: &GenContext) -> Result<Vec<PathBuf>>;

    /// Generate user code scaffold
    async fn generate_scaffold(
        &self,
        context: &GenContext,
        catalog: &ScaffoldCatalog,
    ) -> Result<Vec<PathBuf>>;

    /// Format generated code using language-specific tools
    async fn format_code(&self, context: &GenContext, files: &[PathBuf]) -> Result<()>;

    /// Validate generated code (e.g., using a compiler)
    async fn validate_code(&self, context: &GenContext) -> Result<()>;

    /// Finalize generation (e.g., set files read-only). Default is no-op.
    async fn finalize_generation(&self, _context: &GenContext) -> Result<()> {
        Ok(())
    }

    /// Print next steps
    fn print_next_steps(&self, context: &GenContext);
}
