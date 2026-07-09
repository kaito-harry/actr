mod kotlin;
mod metadata;
mod proto_model;
mod python;
mod rust;
mod scaffold;
mod swift;
mod traits;
mod typescript;

pub use crate::commands::SupportedLanguage;
use crate::error::Result;
use kotlin::KotlinGenerator;
pub use metadata::{
    ACTR_GEN_META_FILE, ActrGenMetadata, TypeRef, load_metadata, load_required_metadata,
    metadata_path, write_metadata,
};
pub use proto_model::{
    MethodModel, ProtoFileModel, ProtoModel, ProtoSide, ServiceModel, TypeOwner, TypeOwnerIndex,
};
use python::PythonGenerator;
use rust::RustGenerator;
pub use scaffold::{ScaffoldCatalog, ScaffoldMethod, ScaffoldService};
use swift::SwiftGenerator;
use tracing::info;
pub use traits::{GenContext, LanguageGenerator, ScaffoldType};
use typescript::TypeScriptGenerator;

pub struct GeneratorFactory;

impl GeneratorFactory {
    pub fn get_generator(language: SupportedLanguage) -> Box<dyn LanguageGenerator> {
        match language {
            SupportedLanguage::Rust => Box::new(RustGenerator),
            SupportedLanguage::Python => Box::new(PythonGenerator),
            SupportedLanguage::Swift => Box::new(SwiftGenerator),
            SupportedLanguage::Kotlin => Box::new(KotlinGenerator),
            SupportedLanguage::TypeScript => Box::new(TypeScriptGenerator),
        }
    }
}

fn should_validate(skip_validation: bool) -> bool {
    !skip_validation
}

pub async fn execute_codegen(language: SupportedLanguage, context: &GenContext) -> Result<()> {
    let generator = GeneratorFactory::get_generator(language);
    run_codegen_pipeline(language, generator.as_ref(), context).await
}

/// Runs the language-independent generation pipeline. All `GenContext` flag
/// gates (`no_scaffold`, `no_format`, `skip_validation`) are enforced here so
/// individual generators never need to re-check them.
async fn run_codegen_pipeline(
    language: SupportedLanguage,
    generator: &dyn LanguageGenerator,
    context: &GenContext,
) -> Result<()> {
    let mut all_files = generator.generate_infrastructure(context).await?;
    let metadata = load_required_metadata(&context.output, language)?;
    if !context.no_scaffold {
        let catalog = ScaffoldCatalog::from_metadata(&metadata);
        all_files.extend(generator.generate_scaffold(context, &catalog).await?);
    }
    if !context.no_format {
        generator.format_code(context, &all_files).await?;
    }

    if should_validate(context.skip_validation) {
        generator.validate_code(context).await?;
    } else {
        info!("⏭️  Skipped code validation (--skip-validation)");
    }

    info!("Code generation completed");

    generator.finalize_generation(context).await?;

    generator.print_next_steps(context);
    Ok(())
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
