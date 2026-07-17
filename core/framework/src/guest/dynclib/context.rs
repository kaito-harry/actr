//! Dynclib guest-side `Context` implementation backed by the compressed ABI.

use crate::guest::dynclib_abi::{
    self as abi, AbiPayload, AbiReply, GuestDataChunkV1, HostCallRawV1, HostCallV1, HostDiscoverV1,
    HostRegisterStreamV1, HostSendDataChunkV1, HostTellV1, HostUnregisterStreamV1,
    InvocationContextV1, abi_error_to_actr, dest_to_v1, reply_to_actr_error,
};
use crate::guest::vtable::HostVTable;
use crate::{Context, Dest, MediaSample};
use actr_protocol::{ActorResult, ActrError, ActrId, ActrType, DataChunk, PayloadType};
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::future::BoxFuture;
use prost::Message as ProstMessage;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

/// cdylib guest-side actor execution context.
pub struct DynclibContext {
    vtable: *const HostVTable,
    bridge_token: u64,
    retained: bool,
    self_id: ActrId,
    caller_id: Option<ActrId>,
    request_id: String,
}

unsafe impl Send for DynclibContext {}
unsafe impl Sync for DynclibContext {}

type StreamCallback =
    Arc<dyn Fn(DataChunk, ActrId) -> BoxFuture<'static, ActorResult<()>> + Send + Sync>;

fn stream_callbacks() -> &'static Mutex<HashMap<String, StreamCallback>> {
    static CALLBACKS: OnceLock<Mutex<HashMap<String, StreamCallback>>> = OnceLock::new();
    CALLBACKS.get_or_init(|| Mutex::new(HashMap::new()))
}

pub fn dispatch_registered_stream(payload: GuestDataChunkV1) -> ActorResult<()> {
    let callback = {
        let callbacks = stream_callbacks()
            .lock()
            .map_err(|_| ActrError::Internal("stream callback registry poisoned".into()))?;
        callbacks.get(&payload.chunk.stream_id).cloned()
    };

    let Some(callback) = callback else {
        return Err(ActrError::NotFound(format!(
            "no stream callback registered for '{}'",
            payload.chunk.stream_id
        )));
    };

    let fut = callback(payload.chunk, payload.sender);
    crate::guest::dynclib::block_on(payload.bridge_token, fut)?
}

#[doc(hidden)]
pub fn clear_stream_callbacks() {
    if let Ok(mut callbacks) = stream_callbacks().lock() {
        callbacks.clear();
    }
}

impl DynclibContext {
    /// Construct a context from host-injected invocation data.
    ///
    /// # Safety
    ///
    /// `vtable` must remain valid for the lifetime of the returned context.
    pub unsafe fn from_invocation(
        vtable: *const HostVTable,
        ctx: InvocationContextV1,
        bridge_token: u64,
    ) -> Result<Self, ActrError> {
        if vtable.is_null() {
            return Err(ActrError::Internal("HostVTable pointer is null".into()));
        }

        let retain_code = unsafe { ((*vtable).retain_context)(bridge_token) };
        if retain_code != abi::code::SUCCESS {
            return Err(ActrError::Internal(format!(
                "failed to retain dynclib bridge token {bridge_token}: {retain_code}"
            )));
        }

        Ok(Self {
            vtable,
            bridge_token,
            retained: true,
            self_id: ctx.self_id,
            caller_id: ctx.caller_id,
            request_id: ctx.request_id,
        })
    }

    fn vt(&self) -> &HostVTable {
        unsafe { &*self.vtable }
    }

    fn invoke_frame(&self, frame: abi::AbiFrame) -> Result<AbiReply, ActrError> {
        let frame_bytes = abi::encode_message(&frame).map_err(abi_error_to_actr)?;
        let mut reply_ptr: *mut u8 = std::ptr::null_mut();
        let mut reply_len: usize = 0;

        let bridge_token =
            crate::guest::dynclib::runtime::active_bridge_token().unwrap_or(self.bridge_token);
        let code = unsafe {
            (self.vt().invoke)(
                bridge_token,
                frame_bytes.as_ptr(),
                frame_bytes.len(),
                &mut reply_ptr,
                &mut reply_len,
            )
        };

        if code != abi::code::SUCCESS {
            return Err(abi_error_to_actr(code));
        }

        let bytes = if reply_ptr.is_null() || reply_len == 0 {
            Vec::new()
        } else {
            let data = unsafe { std::slice::from_raw_parts(reply_ptr, reply_len).to_vec() };
            unsafe { (self.vt().free_host_buf)(reply_ptr, reply_len) };
            data
        };

        abi::decode_message::<AbiReply>(&bytes).map_err(abi_error_to_actr)
    }
}

impl Clone for DynclibContext {
    fn clone(&self) -> Self {
        let code = unsafe { (self.vt().retain_context)(self.bridge_token) };
        if code != abi::code::SUCCESS {
            tracing::error!(
                bridge_token = self.bridge_token,
                code,
                "failed to retain dynclib context during clone"
            );
        }

        Self {
            vtable: self.vtable,
            bridge_token: self.bridge_token,
            retained: code == abi::code::SUCCESS,
            self_id: self.self_id.clone(),
            caller_id: self.caller_id.clone(),
            request_id: self.request_id.clone(),
        }
    }
}

impl Drop for DynclibContext {
    fn drop(&mut self) {
        if self.retained {
            unsafe { (self.vt().release_context)(self.bridge_token) };
        }
    }
}

#[async_trait]
impl Context for DynclibContext {
    fn self_id(&self) -> &ActrId {
        &self.self_id
    }

    fn caller_id(&self) -> Option<&ActrId> {
        self.caller_id.as_ref()
    }

    fn request_id(&self) -> &str {
        &self.request_id
    }

    async fn call<R: actr_protocol::RpcRequest>(
        &self,
        target: &Dest,
        request: R,
    ) -> ActorResult<R::Response> {
        let payload = HostCallV1 {
            route_key: R::route_key().to_string(),
            dest: dest_to_v1(target),
            payload: request.encode_to_vec(),
        };
        let frame = payload.to_frame().map_err(abi_error_to_actr)?;
        let reply = self.invoke_frame(frame)?;

        if reply.status != abi::code::SUCCESS {
            return Err(reply_to_actr_error(reply));
        }

        R::Response::decode(reply.payload.as_slice())
            .map_err(|e| ActrError::DecodeFailure(format!("response decode failed: {e}")))
    }

    async fn tell<R: actr_protocol::RpcRequest>(
        &self,
        target: &Dest,
        message: R,
    ) -> ActorResult<()> {
        let payload = HostTellV1 {
            route_key: R::route_key().to_string(),
            dest: dest_to_v1(target),
            payload: message.encode_to_vec(),
        };
        let frame = payload.to_frame().map_err(abi_error_to_actr)?;
        let reply = self.invoke_frame(frame)?;

        if reply.status != abi::code::SUCCESS {
            return Err(reply_to_actr_error(reply));
        }

        Ok(())
    }

    async fn register_stream<F>(&self, stream_id: String, callback: F) -> ActorResult<()>
    where
        F: Fn(DataChunk, ActrId) -> BoxFuture<'static, ActorResult<()>> + Send + Sync + 'static,
    {
        stream_callbacks()
            .lock()
            .map_err(|_| ActrError::Internal("stream callback registry poisoned".into()))?
            .insert(stream_id.clone(), Arc::new(callback));

        let payload = HostRegisterStreamV1 { stream_id };
        let reply = self.invoke_frame(payload.to_frame().map_err(abi_error_to_actr)?)?;
        if reply.status != abi::code::SUCCESS {
            return Err(reply_to_actr_error(reply));
        }
        Ok(())
    }

    async fn unregister_stream(&self, stream_id: &str) -> ActorResult<()> {
        stream_callbacks()
            .lock()
            .map_err(|_| ActrError::Internal("stream callback registry poisoned".into()))?
            .remove(stream_id);

        let payload = HostUnregisterStreamV1 {
            stream_id: stream_id.to_string(),
        };
        let reply = self.invoke_frame(payload.to_frame().map_err(abi_error_to_actr)?)?;
        if reply.status != abi::code::SUCCESS {
            return Err(reply_to_actr_error(reply));
        }
        Ok(())
    }

    async fn send_data_chunk(
        &self,
        target: &Dest,
        chunk: DataChunk,
        payload_type: PayloadType,
    ) -> ActorResult<()> {
        let payload = HostSendDataChunkV1 {
            dest: dest_to_v1(target),
            chunk,
            payload_type: payload_type as i32,
        };
        let reply = self.invoke_frame(payload.to_frame().map_err(abi_error_to_actr)?)?;
        if reply.status != abi::code::SUCCESS {
            return Err(reply_to_actr_error(reply));
        }
        Ok(())
    }

    async fn discover_route_candidate(&self, target_type: &ActrType) -> ActorResult<ActrId> {
        let payload = HostDiscoverV1 {
            target_type: target_type.clone(),
        };
        let frame = payload.to_frame().map_err(abi_error_to_actr)?;
        let reply = self.invoke_frame(frame)?;

        if reply.status != abi::code::SUCCESS {
            return Err(reply_to_actr_error(reply));
        }

        ActrId::decode(reply.payload.as_slice())
            .map_err(|e| ActrError::DecodeFailure(format!("discover result decode failed: {e}")))
    }

    async fn call_raw(
        &self,
        target: &ActrId,
        route_key: &str,
        payload: Bytes,
    ) -> ActorResult<Bytes> {
        let request = HostCallRawV1 {
            route_key: route_key.to_string(),
            target: target.clone(),
            payload: payload.to_vec(),
        };
        let frame = request.to_frame().map_err(abi_error_to_actr)?;
        let reply = self.invoke_frame(frame)?;

        if reply.status != abi::code::SUCCESS {
            return Err(reply_to_actr_error(reply));
        }

        Ok(Bytes::from(reply.payload))
    }

    async fn register_media_track<F>(&self, _track_id: String, _callback: F) -> ActorResult<()>
    where
        F: Fn(MediaSample, ActrId) -> BoxFuture<'static, ActorResult<()>> + Send + Sync + 'static,
    {
        Err(ActrError::NotImplemented(
            "WebRTC media tracks are not supported in dynclib environment".into(),
        ))
    }

    async fn unregister_media_track(&self, _track_id: &str) -> ActorResult<()> {
        Err(ActrError::NotImplemented(
            "WebRTC media tracks are not supported in dynclib environment".into(),
        ))
    }

    async fn send_media_sample(
        &self,
        _target: &Dest,
        _track_id: &str,
        _sample: MediaSample,
    ) -> ActorResult<()> {
        Err(ActrError::NotImplemented(
            "WebRTC media tracks are not supported in dynclib environment".into(),
        ))
    }

    async fn add_media_track(
        &self,
        _target: &Dest,
        _track_id: &str,
        _codec: &str,
        _media_type: &str,
    ) -> ActorResult<()> {
        Err(ActrError::NotImplemented(
            "WebRTC media tracks are not supported in dynclib environment".into(),
        ))
    }

    async fn remove_media_track(&self, _target: &Dest, _track_id: &str) -> ActorResult<()> {
        Err(ActrError::NotImplemented(
            "WebRTC media tracks are not supported in dynclib environment".into(),
        ))
    }
}
