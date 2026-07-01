//! Observability module for logging and tracing initialization.
//!
//! This module provides unified initialization for logging (via `tracing`) and
//! optional distributed tracing (via OpenTelemetry). It supports injecting
//! custom platform-specific layers (e.g., Android Logcat, iOS os_log) while
//! providing a sensible default (stderr fmt layer) when none is provided.

use actr_config::ObservabilityConfig;
use actr_protocol::ActorResult;
#[cfg(feature = "opentelemetry")]
use opentelemetry::{KeyValue, trace::TracerProvider as _};
#[cfg(feature = "opentelemetry")]
use opentelemetry_otlp::WithExportConfig;
#[cfg(feature = "opentelemetry")]
use opentelemetry_sdk::{
    propagation::TraceContextPropagator, resource::Resource, trace::SdkTracerProvider,
};
#[cfg(feature = "opentelemetry")]
use tracing_subscriber::filter::Targets;
use tracing_subscriber::{
    Layer, filter::EnvFilter, fmt, layer::SubscriberExt, prelude::*, registry::LookupSpan,
};

/// Type alias for a boxed tracing layer that can be dynamically composed.
///
/// Platform-specific bindings (e.g., `libactr` for Swift/Kotlin) can create
/// layers using `tracing-android` or `tracing-oslog` and pass them here.
type BoxedLayer<S> = Box<dyn Layer<S> + Send + Sync + 'static>;

/// Guard for observability resources. Shuts down tracing exporter on drop.
#[derive(Default)]
pub struct ObservabilityGuard {
    #[cfg(feature = "opentelemetry")]
    tracer_provider: Option<SdkTracerProvider>,
}

impl Drop for ObservabilityGuard {
    fn drop(&mut self) {
        #[cfg(feature = "opentelemetry")]
        if let Some(provider) = self.tracer_provider.take() {
            if let Err(err) = provider.shutdown() {
                tracing::warn!("Failed to shutdown tracer provider: {err:?}");
            }
        }
    }
}

/// Initialize logging + (optional) tracing subscriber with default fmt layer.
///
/// This is the original API for backward compatibility. It uses a stderr-based
/// fmt layer for local logging output.
///
/// - `RUST_LOG` wins over configured level; fallback to `info` if unset.
/// - Tracing exporter only activates when both the `opentelemetry` feature is enabled and
///   `cfg.tracing_enabled` is true.
/// - Invalid endpoints fail fast; runtime delivery errors log but do not abort.
pub fn init_observability(
    cfg: &actr_config::ObservabilityConfig,
) -> ActorResult<ObservabilityGuard> {
    init_observability_with_layer(cfg, None::<BoxedLayer<tracing_subscriber::Registry>>)
}

/// Initialize logging + (optional) tracing subscriber with a custom platform layer.
///
/// This extended API allows platform-specific bindings to inject their own
/// logging layer (e.g., `tracing-android` for Logcat, `tracing-oslog` for Apple).
///
/// # Arguments
///
/// * `cfg` - Observability configuration (filter level, OTel settings)
/// * `platform_layer` - Optional custom layer for platform-specific logging.
///   If `None`, a default `fmt::layer()` outputting to stderr will be used.
///
/// # Example
///
/// ```rust,ignore
/// // In libactr for Android:
/// let android_layer = tracing_android::layer("actr")
///     .expect("Failed to create Android layer");
/// let guard = init_observability_with_layer(&cfg, Some(android_layer.boxed()))?;
/// ```
pub fn init_observability_with_layer<L>(
    cfg: &ObservabilityConfig,
    platform_layer: Option<L>,
) -> ActorResult<ObservabilityGuard>
where
    L: Layer<tracing_subscriber::Registry> + Send + Sync + 'static,
{
    let level_directive = std::env::var("RUST_LOG")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| cfg.filter_level.clone());
    let env_filter =
        EnvFilter::try_new(level_directive.clone()).unwrap_or_else(|_| EnvFilter::new("info"));

    init_subscriber_internal(cfg, env_filter, platform_layer)
}

// ============================================================================
// Internal implementation
// ============================================================================

#[cfg(not(feature = "opentelemetry"))]
fn init_subscriber_internal<L>(
    _cfg: &ObservabilityConfig,
    env_filter: EnvFilter,
    platform_layer: Option<L>,
) -> ActorResult<ObservabilityGuard>
where
    L: Layer<tracing_subscriber::Registry> + Send + Sync + 'static,
{
    // Apply the filter to the output layer using with_filter()
    // This ensures the filter properly gates events before they reach the output layer
    let filtered_layer = if let Some(layer) = platform_layer {
        layer.with_filter(env_filter).boxed()
    } else {
        create_default_fmt_layer().with_filter(env_filter).boxed()
    };

    let _ = tracing_subscriber::registry()
        .with(filtered_layer)
        .try_init();

    Ok(ObservabilityGuard::default())
}

#[cfg(feature = "opentelemetry")]
fn init_subscriber_internal<L>(
    cfg: &ObservabilityConfig,
    env_filter: EnvFilter,
    platform_layer: Option<L>,
) -> ActorResult<ObservabilityGuard>
where
    L: Layer<tracing_subscriber::Registry> + Send + Sync + 'static,
{
    // Apply the filter to the output layer using with_filter()
    // This ensures the filter properly gates events before they reach the output layer
    // Note: OTel layer receives all events for distributed tracing purposes
    let filtered_output_layer = if let Some(layer) = platform_layer {
        layer.with_filter(env_filter).boxed()
    } else {
        create_default_fmt_layer().with_filter(env_filter).boxed()
    };

    // Add OTel layer if enabled, with target-level filter to suppress noisy third-party crates
    let mut tracer_provider = None;
    if cfg.tracing_enabled {
        let provider = build_otel_provider(cfg)?;
        let tracer = provider.tracer("actr-runtime");
        let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

        // Filter: use configured filter_level as default, suppress noisy third-party crates
        let otel_default_level = cfg
            .filter_level
            .parse::<tracing::Level>()
            .unwrap_or(tracing::Level::INFO);
        let otel_filter = Targets::new()
            .with_default(otel_default_level)
            .with_target("tungstenite", tracing::Level::ERROR) // OFF equivalent
            .with_target("tokio_tungstenite", tracing::Level::ERROR) // OFF equivalent
            .with_target("wasmtime", tracing::Level::WARN)
            .with_target("webrtc_mdns::conn", tracing::Level::WARN)
            .with_target("webrtc_ice::agent::agent_internal", tracing::Level::WARN)
            .with_target("webrtc_sctp", tracing::Level::WARN);

        let _ = tracing_subscriber::registry()
            .with(filtered_output_layer)
            .with(otel_layer.with_filter(otel_filter))
            .try_init();
        tracer_provider = Some(provider);
    } else {
        let _ = tracing_subscriber::registry()
            .with(filtered_output_layer)
            .try_init();
    }

    Ok(ObservabilityGuard { tracer_provider })
}

/// Create the default fmt layer for stderr output.
fn create_default_fmt_layer<S>() -> impl Layer<S>
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
{
    // Enable ANSI colors on Linux/Unix platforms for better terminal readability
    // Disable on mobile platforms (iOS/Android) where colors are not useful
    let enable_ansi = cfg!(all(
        unix,
        not(target_os = "ios"),
        not(target_os = "android")
    ));

    fmt::layer()
        .with_writer(std::io::stderr)
        .with_target(true)
        .with_level(true)
        .with_line_number(true)
        .with_file(true)
        .with_ansi(enable_ansi)
}

#[cfg(test)]
mod tests {
    use super::*;
    use actr_config::ObservabilityConfig;

    fn cfg() -> ObservabilityConfig {
        ObservabilityConfig {
            filter_level: "info".into(),
            tracing_enabled: false,
            tracing_endpoint: String::new(),
            tracing_service_name: "actr-test".into(),
        }
    }

    #[test]
    fn init_observability_returns_guard() {
        // Global subscriber init is best-effort (try_init errors are swallowed),
        // so this returns Ok whether or not a subscriber was already installed.
        let guard = init_observability(&cfg()).unwrap();
        let _ = guard; // drop runs without panic
    }

    #[test]
    fn init_with_default_layer() {
        let _guard =
            init_observability_with_layer(&cfg(), None::<BoxedLayer<tracing_subscriber::Registry>>)
                .unwrap();
    }

    #[test]
    fn init_with_custom_platform_layer() {
        // A plain fmt layer stands in for a platform-specific layer (android/oslog).
        let layer = fmt::layer();
        let _guard = init_observability_with_layer(&cfg(), Some(layer)).unwrap();
    }

    #[test]
    fn default_fmt_layer_builds_without_panic() {
        // Exercises the cfg!(unix/mobile) branch and writer configuration.
        let _layer = create_default_fmt_layer::<tracing_subscriber::Registry>();
    }

    #[test]
    fn observability_guard_default_constructs() {
        // Without the opentelemetry feature the guard holds no provider; drop is a no-op.
        let guard = ObservabilityGuard::default();
        drop(guard);
    }

    #[test]
    fn env_filter_falls_back_on_invalid_level() {
        // An unparseable configured level must not panic — EnvFilter falls back to "info".
        let bad = ObservabilityConfig {
            filter_level: "not-a-real-level!!!".into(),
            ..cfg()
        };
        let _guard = init_observability(&bad).unwrap();
    }
}

#[cfg(feature = "opentelemetry")]
fn build_otel_provider(config: &ObservabilityConfig) -> ActorResult<SdkTracerProvider> {
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(config.tracing_endpoint.clone())
        .build()
        .map_err(|e| {
            actr_protocol::ActrError::Internal(format!("OTLP exporter build failed: {e}"))
        })?;

    let resource = Resource::builder()
        .with_service_name(config.tracing_service_name.clone())
        .with_attributes([KeyValue::new("telemetry.sdk.language", "rust")])
        .build();

    let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_resource(resource)
        .with_batch_exporter(exporter)
        .build();

    opentelemetry::global::set_tracer_provider(tracer_provider.clone());
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

    Ok(tracer_provider)
}
