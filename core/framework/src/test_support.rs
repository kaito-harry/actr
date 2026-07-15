//! Test helpers for actr-framework
//!
//! Currently provides a simple `DummyContext` implementation of the `Context` trait
//! for unit tests that need a context but do not exercise transport logic.

use crate::{Context, Dest, MaybeSendBoxFuture, MaybeSendSync, MediaSample};
use actr_protocol::{ActorResult, ActrError, ActrId, ActrType, RpcRequest};
use async_trait::async_trait;
use futures_util::future::BoxFuture;

/// Minimal `Context` implementation for tests.
#[derive(Clone, Debug)]
pub struct DummyContext {
    self_id: ActrId,
    caller_id: Option<ActrId>,
    request_id: String,
}

impl DummyContext {
    /// Create a new dummy context with random request id.
    pub fn new(self_id: ActrId) -> Self {
        Self {
            self_id,
            caller_id: None,
            request_id: uuid::Uuid::new_v4().to_string(),
        }
    }

    /// Override request id for deterministic testing.
    pub fn with_request_id(mut self, request_id: impl Into<String>) -> Self {
        self.request_id = request_id.into();
        self
    }

    fn not_implemented(feature: &str) -> ActrError {
        ActrError::NotImplemented(feature.to_string())
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Context for DummyContext {
    fn self_id(&self) -> &ActrId {
        &self.self_id
    }

    fn caller_id(&self) -> Option<&ActrId> {
        self.caller_id.as_ref()
    }

    fn request_id(&self) -> &str {
        &self.request_id
    }

    async fn call<R: RpcRequest>(&self, _target: &Dest, _request: R) -> ActorResult<R::Response> {
        Err(Self::not_implemented("DummyContext::call"))
    }

    async fn tell<R: RpcRequest>(&self, _target: &Dest, _message: R) -> ActorResult<()> {
        Err(Self::not_implemented("DummyContext::tell"))
    }

    async fn register_stream<F>(&self, _stream_id: String, _callback: F) -> ActorResult<()>
    where
        F: Fn(actr_protocol::DataChunk, ActrId) -> MaybeSendBoxFuture<'static, ActorResult<()>>
            + MaybeSendSync
            + 'static,
    {
        Ok(())
    }

    async fn unregister_stream(&self, _stream_id: &str) -> ActorResult<()> {
        Ok(())
    }

    async fn send_data_chunk(
        &self,
        _target: &Dest,
        _chunk: actr_protocol::DataChunk,
        _payload_type: actr_protocol::PayloadType,
    ) -> ActorResult<()> {
        Err(Self::not_implemented("DummyContext::send_data_chunk"))
    }

    async fn discover_route_candidate(&self, _target_type: &ActrType) -> ActorResult<ActrId> {
        Err(Self::not_implemented(
            "DummyContext::discover_route_candidate",
        ))
    }

    async fn call_raw(
        &self,
        _target: &ActrId,
        _route_key: &str,
        _payload: bytes::Bytes,
    ) -> ActorResult<bytes::Bytes> {
        Err(Self::not_implemented("DummyContext::call_raw"))
    }

    async fn register_media_track<F>(&self, _track_id: String, _callback: F) -> ActorResult<()>
    where
        F: Fn(MediaSample, ActrId) -> BoxFuture<'static, ActorResult<()>> + Send + Sync + 'static,
    {
        Ok(())
    }

    async fn unregister_media_track(&self, _track_id: &str) -> ActorResult<()> {
        Ok(())
    }

    async fn send_media_sample(
        &self,
        _target: &Dest,
        _track_id: &str,
        _sample: MediaSample,
    ) -> ActorResult<()> {
        Err(Self::not_implemented("DummyContext::send_media_sample"))
    }

    async fn add_media_track(
        &self,
        _target: &Dest,
        _track_id: &str,
        _codec: &str,
        _media_type: &str,
    ) -> ActorResult<()> {
        Err(Self::not_implemented("DummyContext::add_media_track"))
    }

    async fn remove_media_track(&self, _target: &Dest, _track_id: &str) -> ActorResult<()> {
        Err(Self::not_implemented("DummyContext::remove_media_track"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn dummy_context_exposes_ids() {
        let id = ActrId::default();
        let ctx = DummyContext::new(id.clone()).with_request_id("r1");

        assert_eq!(ctx.self_id(), &id);
        assert_eq!(ctx.request_id(), "r1");
    }
}
