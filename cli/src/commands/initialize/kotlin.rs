use super::{InitContext, ProjectInitializer, create_local_proto};
use crate::error::Result;
use crate::templates::ProjectTemplateName;
use crate::utils::read_fixture_text;
use async_trait::async_trait;
use std::path::{Path, PathBuf};
use tracing::info;

pub struct KotlinInitializer;

#[async_trait]
impl ProjectInitializer for KotlinInitializer {
    async fn generate_project_structure(&self, context: &InitContext) -> Result<()> {
        match context.template {
            ProjectTemplateName::Echo => self.generate_echo_project(context).await,
            ProjectTemplateName::Empty => Err(crate::error::ActrCliError::Unsupported(
                "Empty template is not supported for Kotlin yet".to_string(),
            )),
            ProjectTemplateName::DataStream => self.generate_data_stream_project(context).await,
        }
    }

    fn print_next_steps(&self, context: &InitContext) {
        let _project_name_pascal = to_pascal_case(&context.project_name);
        let package_path = to_package_name(&context.project_name).replace('.', "/");

        info!("");
        info!("Next steps:");
        if !context.is_current_dir {
            info!("  cd {}", context.project_dir.display());
        }
        info!("  actr deps install  # Install remote protobuf dependencies from manifest.toml");

        match context.template {
            ProjectTemplateName::Echo => {
                info!(
                    "  actr gen -l kotlin -i protos/remote/echo-echo-server/echo.proto -o app/src/main/java/{}/generated",
                    package_path
                );
            }
            ProjectTemplateName::Empty => {
                info!("  # Empty template is not supported for Kotlin yet");
            }
            ProjectTemplateName::DataStream => {
                info!("  actr gen -l kotlin  # Generate code for stream-echo-server-python");
            }
        }
        info!("  ./gradlew assembleDebug");
        info!("  # Install APK: adb install app/build/outputs/apk/debug/app-debug.apk");
        info!("");
        info!("💡 Tips:");
        info!("  - For Android emulator, use ws://10.0.2.2:PORT to reach host localhost");
        info!("  - actr-kotlin library is fetched from Actrium GitHub Packages");
        info!(
            "  - Generated framework code is in app/src/main/java/{}/generated/",
            package_path
        );
        info!("  - Run tests: ./gradlew connectedDebugAndroidTest");
    }
}

impl KotlinInitializer {
    async fn generate_echo_project(&self, context: &InitContext) -> Result<()> {
        // Note: proto files are no longer created during init, they will be pulled via actr deps install

        let project_name_pascal = to_pascal_case(&context.project_name);
        let package_name = to_package_name(&context.project_name);
        let package_path = package_name.replace('.', "/");

        let signaling_host = extract_signaling_host(&context.signaling_url);

        let replacements = vec![
            ("{{PROJECT_NAME}}".to_string(), context.project_name.clone()),
            (
                "{{PROJECT_NAME_PASCAL}}".to_string(),
                project_name_pascal.clone(),
            ),
            ("{{PACKAGE_NAME}}".to_string(), package_name.clone()),
            ("{{PACKAGE_PATH}}".to_string(), package_path.clone()),
            (
                "{{SIGNALING_URL}}".to_string(),
                context.signaling_url.clone(),
            ),
            (
                "{{AIS_ENDPOINT_URL}}".to_string(),
                derive_ais_endpoint_url(&context.signaling_url),
            ),
            ("{{SIGNALING_HOST}}".to_string(), signaling_host),
            ("{{MANUFACTURER}}".to_string(), context.manufacturer.clone()),
        ];

        let fixtures_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures");
        let app_dir = context.project_dir.join("app");
        let java_dir = app_dir.join("src/main/java").join(&package_path);

        let files = vec![
            // Root level files
            (
                fixtures_root.join("kotlin/settings.gradle.kts"),
                context.project_dir.join("settings.gradle.kts"),
            ),
            (
                fixtures_root.join("kotlin/build.gradle.kts"),
                context.project_dir.join("build.gradle.kts"),
            ),
            (
                fixtures_root.join("kotlin/gradle.properties"),
                context.project_dir.join("gradle.properties"),
            ),
            (
                fixtures_root.join("kotlin/echo/manifest.toml"),
                context.project_dir.join("manifest.toml"),
            ),
            (
                fixtures_root.join("kotlin/gitignore"),
                context.project_dir.join(".gitignore"),
            ),
            // App module files
            (
                fixtures_root.join("kotlin/app/build.gradle.kts"),
                app_dir.join("build.gradle.kts"),
            ),
            (
                fixtures_root.join("kotlin/app/src/main/AndroidManifest.xml"),
                app_dir.join("src/main/AndroidManifest.xml"),
            ),
            // Resources
            (
                fixtures_root.join("kotlin/app/src/main/res/values/strings.xml"),
                app_dir.join("src/main/res/values/strings.xml"),
            ),
            (
                fixtures_root.join("kotlin/app/src/main/res/values/colors.xml"),
                app_dir.join("src/main/res/values/colors.xml"),
            ),
            (
                fixtures_root.join("kotlin/app/src/main/res/values/themes.xml"),
                app_dir.join("src/main/res/values/themes.xml"),
            ),
            (
                fixtures_root.join("kotlin/app/src/main/res/layout/activity_main.xml"),
                app_dir.join("src/main/res/layout/activity_main.xml"),
            ),
            // Kotlin source files
            (
                fixtures_root.join("kotlin/echo/MainActivity.kt"),
                java_dir.join("MainActivity.kt"),
            ),
            // Android Test files
            (
                fixtures_root.join("kotlin/echo/EchoIntegrationTest.kt"),
                app_dir
                    .join("src/androidTest/java")
                    .join(&package_path)
                    .join("EchoIntegrationTest.kt"),
            ),
        ];

        for (fixture_path, output_path) in files {
            let template = read_fixture_text(&fixture_path)?;
            let rendered = apply_placeholders(&template, &replacements);
            write_file(&output_path, &rendered)?;
        }

        // Copy gradle wrapper
        copy_gradle_wrapper(&context.project_dir)?;

        // Create local.proto file
        create_local_proto(
            &context.project_dir,
            &context.project_name,
            "protos/local",
            context.template,
            None,
        )?;

        info!("📁 Created Android Echo project structure");
        Ok(())
    }

    async fn generate_data_stream_project(&self, context: &InitContext) -> Result<()> {
        let project_name_pascal = to_pascal_case(&context.project_name);
        let package_name = to_package_name(&context.project_name);
        let package_path = package_name.replace('.', "/");

        let signaling_host = extract_signaling_host(&context.signaling_url);

        let replacements = vec![
            ("{{PROJECT_NAME}}".to_string(), context.project_name.clone()),
            (
                "{{PROJECT_NAME_PASCAL}}".to_string(),
                project_name_pascal.clone(),
            ),
            ("{{PACKAGE_NAME}}".to_string(), package_name.clone()),
            ("{{PACKAGE_PATH}}".to_string(), package_path.clone()),
            (
                "{{SIGNALING_URL}}".to_string(),
                context.signaling_url.clone(),
            ),
            (
                "{{AIS_ENDPOINT_URL}}".to_string(),
                derive_ais_endpoint_url(&context.signaling_url),
            ),
            ("{{SIGNALING_HOST}}".to_string(), signaling_host),
            ("{{MANUFACTURER}}".to_string(), context.manufacturer.clone()),
        ];

        let fixtures_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures");
        let app_dir = context.project_dir.join("app");
        let java_dir = app_dir.join("src/main/java").join(&package_path);

        let files = vec![
            // Root level files
            (
                fixtures_root.join("kotlin/settings.gradle.kts"),
                context.project_dir.join("settings.gradle.kts"),
            ),
            (
                fixtures_root.join("kotlin/build.gradle.kts"),
                context.project_dir.join("build.gradle.kts"),
            ),
            (
                fixtures_root.join("kotlin/gradle.properties"),
                context.project_dir.join("gradle.properties"),
            ),
            (
                fixtures_root.join("kotlin/data-stream/manifest.toml"),
                context.project_dir.join("manifest.toml"),
            ),
            (
                fixtures_root.join("kotlin/gitignore"),
                context.project_dir.join(".gitignore"),
            ),
            // App module files
            (
                fixtures_root.join("kotlin/app/build.gradle.kts"),
                app_dir.join("build.gradle.kts"),
            ),
            (
                fixtures_root.join("kotlin/app/src/main/AndroidManifest.xml"),
                app_dir.join("src/main/AndroidManifest.xml"),
            ),
            // Resources
            (
                fixtures_root.join("kotlin/app/src/main/res/values/strings.xml"),
                app_dir.join("src/main/res/values/strings.xml"),
            ),
            (
                fixtures_root.join("kotlin/app/src/main/res/values/colors.xml"),
                app_dir.join("src/main/res/values/colors.xml"),
            ),
            (
                fixtures_root.join("kotlin/app/src/main/res/values/themes.xml"),
                app_dir.join("src/main/res/values/themes.xml"),
            ),
            (
                fixtures_root.join("kotlin/data-stream/activity_main.xml"),
                app_dir.join("src/main/res/layout/activity_main.xml"),
            ),
            // Kotlin source files
            (
                fixtures_root.join("kotlin/data-stream/MainActivity.kt"),
                java_dir.join("MainActivity.kt"),
            ),
            (
                fixtures_root.join("kotlin/data-stream/MyUnifiedHandler.kt"),
                java_dir.join("MyUnifiedHandler.kt"),
            ),
            // Android Test files
            (
                fixtures_root.join("kotlin/data-stream/DataStreamIntegrationTest.kt"),
                app_dir
                    .join("src/androidTest/java")
                    .join(&package_path)
                    .join("DataStreamIntegrationTest.kt"),
            ),
        ];

        for (fixture_path, output_path) in files {
            let template = read_fixture_text(&fixture_path)?;
            let rendered = apply_placeholders(&template, &replacements);
            write_file(&output_path, &rendered)?;
        }

        // Copy gradle wrapper
        copy_gradle_wrapper(&context.project_dir)?;

        // Create local stream_client.proto file for data-stream template
        create_data_stream_local_proto(&context.project_dir)?;

        info!("📁 Created Android DataStream project structure");
        Ok(())
    }
}

/// Extract host from signaling URL
/// e.g., "ws://10.30.3.206:8081/signaling/ws" -> "10.30.3.206"
/// or "wss://actrix1.develenv.com/signaling/ws" -> "actrix1.develenv.com"
fn extract_signaling_host(signaling_url: &str) -> String {
    signaling_url
        .trim_start_matches("ws://")
        .trim_start_matches("wss://")
        .split('/')
        .next()
        .unwrap_or("10.0.2.2")
        .split(':')
        .next()
        .unwrap_or("10.0.2.2")
        .to_string()
}

fn derive_ais_endpoint_url(signaling_url: &str) -> String {
    let trimmed = signaling_url.trim_end_matches('/');
    if trimmed.is_empty() {
        return String::new();
    }

    let normalized = if let Some(rest) = trimmed.strip_prefix("wss://") {
        format!("https://{rest}")
    } else if let Some(rest) = trimmed.strip_prefix("ws://") {
        format!("http://{rest}")
    } else {
        trimmed.to_string()
    };

    if let Some(prefix) = normalized.strip_suffix("/signaling/ws") {
        format!("{prefix}/ais")
    } else if let Some(prefix) = normalized.strip_suffix("/signaling") {
        format!("{prefix}/ais")
    } else if let Some(prefix) = normalized.strip_suffix("/ws") {
        format!("{prefix}/ais")
    } else {
        format!("{normalized}/ais")
    }
}

fn write_file(path: &Path, content: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, content)?;
    Ok(())
}

fn apply_placeholders(template: &str, replacements: &[(String, String)]) -> String {
    let mut rendered = template.to_string();
    for (key, value) in replacements {
        rendered = rendered.replace(key, value);
    }
    rendered
}

/// Create the local stream_client.proto file for data-stream template
fn create_data_stream_local_proto(project_dir: &Path) -> Result<()> {
    let proto_dir = project_dir.join("protos/local/stream_client");
    std::fs::create_dir_all(&proto_dir)?;

    let proto_content = r#"syntax = "proto3";

// Must use the same package as the remote proto for route key matching
// Server callback uses route key: stream_server.StreamClient.PrepareClientStream
package stream_server;

// Import the remote proto to reuse RegisterStreamResponse
import "remote/stream-echo-server-python/stream_server.proto";

// Request from server to client to prepare for receiving data stream
// NOTE: This is only defined here (server side has its own definition that matches)
message PrepareClientStreamRequest {
  string stream_id = 1;
  int32 expected_count = 2;
}

// Response for prepare client stream (local only)
message PrepareClientStreamResponse {
  bool ready = 1;
  string message = 2;
}

// Request to start a stream (local only, different name to avoid conflict)
message ClientStartStreamRequest {
  string client_id = 1;
  string stream_id = 2;
  int32 message_count = 3;
}

// Response for starting a stream (local only, different name to avoid conflict)
message ClientStartStreamResponse {
  bool accepted = 1;
  string message = 2;
}

// StreamClient service - must match server's expectation
// Server will callback using: stream_server.StreamClient.PrepareClientStream
service StreamClient {
  // Called by the server to prepare the client for receiving data stream
  rpc PrepareClientStream(PrepareClientStreamRequest) returns (PrepareClientStreamResponse);
  // Called locally to start a stream transfer (uses different message names to avoid conflict)
  rpc StartStream(ClientStartStreamRequest) returns (ClientStartStreamResponse);
}
"#;

    std::fs::write(proto_dir.join("stream_client.proto"), proto_content)?;
    info!("📄 Created local stream_client.proto");
    Ok(())
}

fn to_pascal_case(input: &str) -> String {
    let mut result = String::new();
    let mut start_of_word = true;

    for c in input.chars() {
        if !c.is_alphanumeric() {
            start_of_word = true;
            continue;
        }

        if c.is_uppercase() {
            result.push(c);
            start_of_word = false;
        } else if start_of_word {
            result.push(c.to_uppercase().next().unwrap_or(c));
            start_of_word = false;
        } else {
            result.push(c.to_lowercase().next().unwrap_or(c));
        }
    }

    result
}

fn to_package_name(project_name: &str) -> String {
    // Convert project name to valid Android package name
    // e.g., "my-echo-client" -> "io.actrium.myechoclient"
    let clean_name: String = project_name
        .chars()
        .filter(|c| c.is_alphanumeric())
        .collect::<String>()
        .to_lowercase();
    format!("io.actrium.{}", clean_name)
}

fn copy_gradle_wrapper(project_dir: &Path) -> Result<()> {
    // Create gradle wrapper directory
    let wrapper_dir = project_dir.join("gradle/wrapper");
    std::fs::create_dir_all(&wrapper_dir)?;

    // Create gradle-wrapper.properties
    // Note: AGP 8.12+ requires Gradle 8.13+
    let wrapper_properties = r#"distributionBase=GRADLE_USER_HOME
distributionPath=wrapper/dists
distributionUrl=https\://services.gradle.org/distributions/gradle-8.13-bin.zip
networkTimeout=10000
validateDistributionUrl=true
zipStoreBase=GRADLE_USER_HOME
zipStorePath=wrapper/dists
"#;
    std::fs::write(
        wrapper_dir.join("gradle-wrapper.properties"),
        wrapper_properties,
    )?;

    // Copy gradle-wrapper.jar (binary file)
    let wrapper_jar = include_bytes!("../../../fixtures/kotlin/gradle-wrapper.jar");
    std::fs::write(wrapper_dir.join("gradle-wrapper.jar"), wrapper_jar)?;

    // Create gradlew script
    let gradlew = include_str!("../../../fixtures/kotlin/gradlew");
    if !gradlew.is_empty() {
        std::fs::write(project_dir.join("gradlew"), gradlew)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(
                project_dir.join("gradlew"),
                std::fs::Permissions::from_mode(0o755),
            )?;
        }
    } else {
        // Fallback: create a minimal gradlew that downloads the wrapper
        let gradlew_fallback = r#"#!/bin/sh
echo "Please download gradle wrapper from https://gradle.org/releases/"
echo "Or run: gradle wrapper --gradle-version 8.11.1"
exit 1
"#;
        std::fs::write(project_dir.join("gradlew"), gradlew_fallback)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(
                project_dir.join("gradlew"),
                std::fs::Permissions::from_mode(0o755),
            )?;
        }
    }

    info!("📦 Created Gradle wrapper configuration");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_signaling_host_parses_all_variants() {
        assert_eq!(
            extract_signaling_host("ws://10.30.3.206:8081/signaling/ws"),
            "10.30.3.206"
        );
        assert_eq!(
            extract_signaling_host("wss://actrix1.develenv.com/signaling/ws"),
            "actrix1.develenv.com"
        );
        assert_eq!(extract_signaling_host("ws://localhost:8080"), "localhost");
        // Empty string falls through splits and returns the empty string itself.
        assert_eq!(extract_signaling_host(""), "");
        assert!(
            extract_signaling_host("not-a-url").contains("not-a-url"),
            "{:?}",
            extract_signaling_host("not-a-url")
        );
    }

    #[test]
    fn derive_ais_endpoint_no_suffix_uses_slash_ais() {
        assert_eq!(
            derive_ais_endpoint_url("ws://example.com:8080/signaling/ws"),
            "http://example.com:8080/ais"
        );
        assert_eq!(
            derive_ais_endpoint_url("wss://example.com/signaling"),
            "https://example.com/ais"
        );
        assert_eq!(
            derive_ais_endpoint_url("http://example.com/ws"),
            "http://example.com/ais"
        );
        assert!(derive_ais_endpoint_url("").is_empty());
    }

    #[test]
    fn to_pascal_case_handles_kebab_and_snake() {
        assert_eq!(to_pascal_case("echo-service"), "EchoService");
        assert_eq!(to_pascal_case("my_app"), "MyApp");
        assert_eq!(to_pascal_case("hello-world_app"), "HelloWorldApp");
    }

    #[test]
    fn to_package_name_generates_android_style() {
        let pkg = to_package_name("my-echo-client");
        assert!(pkg.starts_with("io.actrium."));
        assert!(!pkg.contains('-'));
    }

    #[test]
    fn apply_placeholders_substitutes_all_keys() {
        let result = apply_placeholders(
            "{{NAME}} at {{PLACE}}",
            &[
                ("{{NAME}}".into(), "X".into()),
                ("{{PLACE}}".into(), "Y".into()),
            ],
        );
        assert_eq!(result, "X at Y");
    }
}
