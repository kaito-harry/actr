//! Distributed tracing support for RpcEnvelope (Web implementation)
//!
//! This module provides W3C Trace Context propagation for RpcEnvelope in the Web environment.
//! It mirrors the implementation in actr's runtime to ensure consistent distributed tracing
//! across native and Web platforms.

use actr_protocol::RpcEnvelope;
use opentelemetry::{
    Context, propagation::Extractor, propagation::Injector, trace::TraceContextExt,
};
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Set the given span's parent from RpcEnvelope context (or current Context if invalid).
///
/// This extracts the W3C Trace Context from the envelope's `traceparent` and `tracestate`
/// fields and sets it as the parent of the given span, enabling distributed tracing across
/// process boundaries.
///
/// # Example
///
/// ```rust,ignore
/// let envelope = receive_envelope().await?;
/// let span = tracing::info_span!("handle_request", request_id = %envelope.request_id);
/// set_parent_from_rpc_envelope(&span, &envelope);
/// let _guard = span.enter();
/// ```
#[allow(dead_code)]
pub(crate) fn set_parent_from_rpc_envelope(span: &Span, envelope: &RpcEnvelope) {
    let context = extract_trace_context_from_rpc(envelope);
    span.set_parent(context);
}

/// Inject current span context into RpcEnvelope.
///
/// This serializes the current span's trace context into W3C Trace Context format
/// and stores it in the envelope's `traceparent` and `tracestate` fields.
///
/// If the current span context is not valid (e.g., no active tracing), the injection
/// is skipped and a warning is logged.
///
/// # Example
///
/// ```rust,ignore
/// let mut envelope = RpcEnvelope {
///     traceparent: None,
///     tracestate: None,
///     // ... other fields
/// };
/// inject_span_context_to_rpc(&tracing::Span::current(), &mut envelope);
/// send_envelope(envelope).await?;
/// ```
pub(crate) fn inject_span_context_to_rpc(span: &Span, envelope: &mut RpcEnvelope) {
    let mut injector = RpcEnvelopeInjector(envelope);
    let context = span.context();
    let span_ref = context.span();
    let span_context = span_ref.span_context();

    if !span_context.is_valid() {
        log::warn!("⚠️ inject_span_context_to_rpc: span context is not valid, skipping injection");
        return;
    }

    opentelemetry::global::get_text_map_propagator(|propagator| {
        propagator.inject_context(&context, &mut injector)
    });
}

/// Copy trace context fields from a request envelope into a response envelope.
///
/// This keeps trace propagation consistent when the response does not explicitly
/// set trace context fields.
#[allow(dead_code)]
pub(crate) fn copy_trace_context_from_rpc(request: &RpcEnvelope, response: &mut RpcEnvelope) {
    if response.traceparent.is_none() {
        response.traceparent = request.traceparent.clone();
    }
    if response.tracestate.is_none() {
        response.tracestate = request.tracestate.clone();
    }
}

/// Extract trace context from RpcEnvelope.
///
/// This deserializes the W3C Trace Context from the envelope's `traceparent` and
/// `tracestate` fields. If the extracted context is invalid, it returns the current
/// context as a fallback.
///
/// # Returns
///
/// - Valid extracted context if envelope contains valid W3C Trace Context
/// - Current context if extraction fails or context is invalid
fn extract_trace_context_from_rpc(envelope: &RpcEnvelope) -> Context {
    let context = opentelemetry::global::get_text_map_propagator(|propagator| {
        propagator.extract(&RpcEnvelopeExtractor(envelope))
    });
    let span_ref = context.span();
    let span_context = span_ref.span_context();
    if span_context.is_valid() {
        context
    } else {
        Context::current()
    }
}

/// Extractor implementation for reading W3C Trace Context from RpcEnvelope
struct RpcEnvelopeExtractor<'a>(&'a RpcEnvelope);

impl<'a> Extractor for RpcEnvelopeExtractor<'a> {
    fn get(&self, key: &str) -> Option<&str> {
        match key {
            "traceparent" => self.0.traceparent.as_deref(),
            "tracestate" => self.0.tracestate.as_deref(),
            _ => None,
        }
    }

    fn keys(&self) -> Vec<&str> {
        vec!["traceparent", "tracestate"]
    }
}

/// Injector implementation for writing W3C Trace Context into RpcEnvelope
struct RpcEnvelopeInjector<'a>(&'a mut RpcEnvelope);

impl<'a> Injector for RpcEnvelopeInjector<'a> {
    fn set(&mut self, key: &str, value: String) {
        match key {
            "traceparent" => self.0.traceparent = Some(value),
            "tracestate" => self.0.tracestate = Some(value),
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use actr_protocol::RpcEnvelope;

    #[test]
    fn test_extractor_get_traceparent() {
        let envelope = RpcEnvelope {
            route_key: "test.route".to_string(),
            payload: None,
            error: None,
            direction: Some(actr_protocol::Direction::Request as i32),
            traceparent: Some(
                "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string(),
            ),
            tracestate: None,
            request_id: "test-id".to_string(),
            metadata: vec![],
            timeout_ms: 30000,
        };

        let extractor = RpcEnvelopeExtractor(&envelope);
        assert_eq!(
            extractor.get("traceparent"),
            Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
        );
        assert_eq!(extractor.get("tracestate"), None);
        assert_eq!(extractor.get("unknown"), None);
    }

    #[test]
    fn test_extractor_get_tracestate() {
        let envelope = RpcEnvelope {
            route_key: "test.route".to_string(),
            payload: None,
            error: None,
            direction: Some(actr_protocol::Direction::Request as i32),
            traceparent: None,
            tracestate: Some("vendor1=value1,vendor2=value2".to_string()),
            request_id: "test-id".to_string(),
            metadata: vec![],
            timeout_ms: 30000,
        };

        let extractor = RpcEnvelopeExtractor(&envelope);
        assert_eq!(extractor.get("traceparent"), None);
        assert_eq!(
            extractor.get("tracestate"),
            Some("vendor1=value1,vendor2=value2")
        );
    }

    #[test]
    fn test_extractor_keys() {
        let envelope = RpcEnvelope {
            route_key: "test.route".to_string(),
            payload: None,
            error: None,
            direction: Some(actr_protocol::Direction::Request as i32),
            traceparent: None,
            tracestate: None,
            request_id: "test-id".to_string(),
            metadata: vec![],
            timeout_ms: 30000,
        };

        let extractor = RpcEnvelopeExtractor(&envelope);
        let keys = extractor.keys();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&"traceparent"));
        assert!(keys.contains(&"tracestate"));
    }

    #[test]
    fn test_injector_set_traceparent() {
        let mut envelope = RpcEnvelope {
            route_key: "test.route".to_string(),
            payload: None,
            error: None,
            direction: Some(actr_protocol::Direction::Request as i32),
            traceparent: None,
            tracestate: None,
            request_id: "test-id".to_string(),
            metadata: vec![],
            timeout_ms: 30000,
        };

        let mut injector = RpcEnvelopeInjector(&mut envelope);
        injector.set(
            "traceparent",
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string(),
        );

        assert_eq!(
            envelope.traceparent,
            Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string())
        );
        assert_eq!(envelope.tracestate, None);
    }

    #[test]
    fn test_injector_set_tracestate() {
        let mut envelope = RpcEnvelope {
            route_key: "test.route".to_string(),
            payload: None,
            error: None,
            direction: Some(actr_protocol::Direction::Request as i32),
            traceparent: None,
            tracestate: None,
            request_id: "test-id".to_string(),
            metadata: vec![],
            timeout_ms: 30000,
        };

        let mut injector = RpcEnvelopeInjector(&mut envelope);
        injector.set("tracestate", "vendor1=value1".to_string());

        assert_eq!(envelope.traceparent, None);
        assert_eq!(envelope.tracestate, Some("vendor1=value1".to_string()));
    }

    #[test]
    fn test_injector_set_unknown_key() {
        let mut envelope = RpcEnvelope {
            route_key: "test.route".to_string(),
            payload: None,
            error: None,
            direction: Some(actr_protocol::Direction::Request as i32),
            traceparent: None,
            tracestate: None,
            request_id: "test-id".to_string(),
            metadata: vec![],
            timeout_ms: 30000,
        };

        let mut injector = RpcEnvelopeInjector(&mut envelope);
        injector.set("unknown", "value".to_string());

        // Should not modify envelope
        assert_eq!(envelope.traceparent, None);
        assert_eq!(envelope.tracestate, None);
    }

    #[test]
    fn test_injector_set_multiple_values() {
        let mut envelope = RpcEnvelope {
            route_key: "test.route".to_string(),
            payload: None,
            error: None,
            direction: Some(actr_protocol::Direction::Request as i32),
            traceparent: None,
            tracestate: None,
            request_id: "test-id".to_string(),
            metadata: vec![],
            timeout_ms: 30000,
        };

        let mut injector = RpcEnvelopeInjector(&mut envelope);
        injector.set(
            "traceparent",
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string(),
        );
        injector.set("tracestate", "vendor1=value1".to_string());

        assert_eq!(
            envelope.traceparent,
            Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string())
        );
        assert_eq!(envelope.tracestate, Some("vendor1=value1".to_string()));
    }
}
