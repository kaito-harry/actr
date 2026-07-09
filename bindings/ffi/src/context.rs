use actr_framework::{Bytes, Context, DataChunk, Dest, MediaSample as FrameworkMediaSample};
use actr_hyper::context::RuntimeContext;
use actr_protocol::{ActrId, PayloadType};
use async_trait::async_trait;
use std::any::TypeId;
use std::sync::Arc;

use crate::error::run_on_tokio_runtime;
use crate::{ActrError, ActrResult};

/// Callback interface for DataChunk events.
#[uniffi::export(callback_interface)]
#[async_trait]
pub trait DataChunkCallback: Send + Sync + 'static {
    /// Handle an incoming DataChunk.
    async fn on_stream(
        &self,
        chunk: crate::types::DataChunk,
        sender: crate::types::ActrId,
    ) -> ActrResult<()>;
}

/// Callback interface for MediaTrack events.
#[uniffi::export(callback_interface)]
#[async_trait]
pub trait MediaTrackCallback: Send + Sync + 'static {
    /// Handle an incoming media sample from a WebRTC native track.
    async fn on_sample(
        &self,
        sample: crate::types::MediaSample,
        sender: crate::types::ActrId,
    ) -> ActrResult<()>;
}

/// Context provided to the workload
#[derive(uniffi::Object, Clone)]
pub struct ContextBridge {
    pub(crate) inner: RuntimeContext,
}

impl ContextBridge {
    /// Try to create a ContextBridge from a generic Context implementation.
    ///
    /// This performs a runtime type check and fails if the context is not a
    /// `RuntimeContext`.
    pub(crate) fn try_from_context<C: Context + 'static>(ctx: &C) -> ActrResult<Arc<Self>> {
        if TypeId::of::<C>() != TypeId::of::<RuntimeContext>() {
            return Err(ActrError::Internal {
                msg: format!(
                    "Context type mismatch: expected RuntimeContext, got {}",
                    std::any::type_name::<C>()
                ),
            });
        }

        let runtime_ctx = unsafe { &*(ctx as *const C as *const RuntimeContext) };
        Ok(Arc::new(Self {
            inner: runtime_ctx.clone(),
        }))
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl ContextBridge {
    /// Call a remote actor via RPC (simplified for FFI)
    ///
    /// # Arguments
    /// - `target`: Target actor ID
    /// - `route_key`: RPC route key (e.g., "echo.EchoService.Echo")
    /// - `payload_type`: Payload transmission type (RpcReliable, RpcSignal, etc.)
    /// - `payload`: Request payload bytes (protobuf encoded)
    /// - `timeout_ms`: Timeout in milliseconds
    ///
    /// # Returns
    /// Response payload bytes (protobuf encoded)
    pub async fn call_raw(
        &self,
        target: crate::types::ActrId,
        route_key: String,
        payload_type: crate::types::PayloadType,
        payload: Vec<u8>,
        timeout_ms: i64,
    ) -> crate::error::ActrResult<Vec<u8>> {
        let target_id: ActrId = target.into();
        let proto_payload_type: PayloadType = payload_type.into();
        let inner = self.inner.clone();
        let resp = run_on_tokio_runtime("remote call", async move {
            inner
                .call_raw(
                    &Dest::Peer(target_id),
                    route_key,
                    proto_payload_type,
                    Bytes::from(payload),
                    timeout_ms,
                )
                .await
        })
        .await?;
        Ok(resp.to_vec())
    }

    /// Send a one-way message to an actor (fire-and-forget)
    ///
    /// # Arguments
    /// - `target`: Target actor ID
    /// - `route_key`: RPC route key (e.g., "echo.EchoService.Echo")
    /// - `payload_type`: Payload transmission type (RpcReliable, RpcSignal, etc.)
    /// - `payload`: Message payload bytes (protobuf encoded)
    pub async fn tell_raw(
        &self,
        target: crate::types::ActrId,
        route_key: String,
        payload_type: crate::types::PayloadType,
        payload: Vec<u8>,
    ) -> crate::error::ActrResult<()> {
        let target_id: ActrId = target.into();
        let proto_payload_type: PayloadType = payload_type.into();
        let inner = self.inner.clone();
        run_on_tokio_runtime("remote tell", async move {
            inner
                .tell_raw(
                    &Dest::Peer(target_id),
                    route_key,
                    proto_payload_type,
                    Bytes::from(payload),
                )
                .await
        })
        .await
    }

    /// Send a DataChunk to a remote actor (Fast Path)
    ///
    /// # Arguments
    /// - `target`: Target actor ID
    /// - `chunk`: DataChunk containing stream_id, sequence, payload, etc.
    /// - `payload_type`: Stream lane selection for delivery guarantees.
    pub async fn send_data_chunk(
        &self,
        target: crate::types::ActrId,
        chunk: crate::types::DataChunk,
        payload_type: crate::types::PayloadType,
    ) -> crate::error::ActrResult<()> {
        let target_id: ActrId = target.into();
        let chunk: DataChunk = chunk.into();
        let payload_type: PayloadType = payload_type.into();
        self.inner
            .send_data_chunk(&Dest::Peer(target_id), chunk, payload_type)
            .await?;
        Ok(())
    }

    /// Register a DataChunk callback for a stream ID.
    pub async fn register_stream(
        &self,
        stream_id: String,
        callback: Box<dyn DataChunkCallback>,
    ) -> crate::error::ActrResult<()> {
        let callback: Arc<dyn DataChunkCallback> = Arc::from(callback);
        self.inner
            .register_stream(stream_id, move |chunk, sender| {
                let callback = callback.clone();
                Box::pin(async move {
                    let chunk: crate::types::DataChunk = chunk.into();
                    let sender: crate::types::ActrId = sender.into();
                    callback
                        .on_stream(chunk, sender)
                        .await
                        .map_err(actr_protocol::ActrError::from)
                })
            })
            .await?;
        Ok(())
    }

    /// Unregister a DataChunk callback for a stream ID.
    pub async fn unregister_stream(&self, stream_id: String) -> crate::error::ActrResult<()> {
        self.inner.unregister_stream(&stream_id).await?;
        Ok(())
    }

    /// Discover an actor of the specified type
    ///
    /// # Arguments
    /// - `target_type`: Actor type to discover (manufacturer + name)
    ///
    /// # Returns
    /// The ActrId of a discovered actor
    pub async fn discover(
        &self,
        target_type: crate::types::ActrType,
    ) -> crate::error::ActrResult<crate::types::ActrId> {
        let proto_type: actr_protocol::ActrType = target_type.into();
        let id = self.inner.discover_route_candidate(&proto_type).await?;
        Ok(id.into())
    }

    /// Add a media track to the WebRTC connection with the target
    pub async fn add_media_track(
        &self,
        target: crate::types::ActrId,
        track_id: String,
        codec: String,
        media_type: String,
    ) -> crate::error::ActrResult<()> {
        let target_id: ActrId = target.into();
        self.inner
            .add_media_track(&Dest::Peer(target_id), &track_id, &codec, &media_type)
            .await?;
        Ok(())
    }

    /// Remove a media track from the WebRTC connection with the target.
    pub async fn remove_media_track(
        &self,
        target: crate::types::ActrId,
        track_id: String,
    ) -> crate::error::ActrResult<()> {
        let target_id: ActrId = target.into();
        self.inner
            .remove_media_track(&Dest::Peer(target_id), &track_id)
            .await?;
        Ok(())
    }

    /// Send a media sample via WebRTC native RTP track
    pub async fn send_media_sample(
        &self,
        target: crate::types::ActrId,
        track_id: String,
        sample: crate::types::MediaSample,
    ) -> crate::error::ActrResult<()> {
        let target_id: ActrId = target.into();
        let framework_sample: FrameworkMediaSample = sample.into();
        self.inner
            .send_media_sample(&Dest::Peer(target_id), &track_id, framework_sample)
            .await?;
        Ok(())
    }

    /// Register a callback for incoming media track samples
    pub async fn register_media_track(
        &self,
        track_id: String,
        callback: Box<dyn MediaTrackCallback>,
    ) -> crate::error::ActrResult<()> {
        let callback: Arc<dyn MediaTrackCallback> = Arc::from(callback);
        self.inner
            .register_media_track(track_id, move |sample, sender| {
                let callback = callback.clone();
                Box::pin(async move {
                    let sample: crate::types::MediaSample = sample.into();
                    let sender: crate::types::ActrId = sender.into();
                    callback
                        .on_sample(sample, sender)
                        .await
                        .map_err(actr_protocol::ActrError::from)
                })
            })
            .await?;
        Ok(())
    }

    /// Unregister a media track callback
    pub async fn unregister_media_track(&self, track_id: String) -> crate::error::ActrResult<()> {
        self.inner.unregister_media_track(&track_id).await?;
        Ok(())
    }
}
