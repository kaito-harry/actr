use bytes::Bytes;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{error, info};

use crate::generated::echo::{EchoRequest, EchoResponse};
use actr_framework::{Context, Dest, Workload};
use actr_protocol::RpcEnvelope;
use actr_runtime::prelude::*;

#[derive(Clone)]
pub struct ClientWorkload {
    pub server_id: Arc<Mutex<Option<ActrId>>>,
}

impl ClientWorkload {
    pub fn new() -> Self {
        Self {
            server_id: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn set_server_id(&self, server_id: ActrId) {
        *self.server_id.lock().await = Some(server_id);
    }
}

impl Workload for ClientWorkload {
    type Dispatcher = ClientDispatcher;
}

pub struct ClientDispatcher;

#[async_trait::async_trait]
impl actr_framework::MessageDispatcher for ClientDispatcher {
    type Workload = ClientWorkload;

    #[tracing::instrument(skip_all, name = "ClientDispatcher.dispatch", fields(request_id = %envelope.request_id))]
    async fn dispatch<C: Context>(
        workload: &Self::Workload,
        envelope: RpcEnvelope,
        ctx: &C,
    ) -> actr_protocol::ActorResult<Bytes> {
        info!(
            "[ClientWorkload] [...] App request，route_key={}",
            envelope.route_key
        );

        let payload = envelope.payload.as_ref().ok_or_else(|| {
            actr_protocol::ActrError::DecodeFailure(
                "RpcEnvelope [...] payload".to_string(),
            )
        })?;
        let request: EchoRequest = actr_protocol::prost::Message::decode(&**payload)
            .map_err(|e| actr_protocol::ActrError::DecodeFailure(e.to_string()))?;

        info!("[ClientWorkload] App message: {}", request.message);

        let server_id = workload.server_id.lock().await.clone();
        let server_id = match server_id {
            Some(id) => id,
            None => {
                error!("[ClientWorkload] Server ID [...]");
                return Err(actr_protocol::ActrError::Unavailable(
                    "Server ID [...]config".to_string(),
                ));
            }
        };

        info!("[ClientWorkload] via/through WebSocket [...]request[...]service[...]...");

        // via/through Dest::Peer [...]service[...]（willusing/use WebSocket channel）
        let response: EchoResponse = ctx.call(&Dest::Peer(server_id), request).await?;

        info!(
            "[ClientWorkload] [...]service[...]response: {}",
            response.reply
        );

        Ok(Bytes::from(actr_protocol::prost::Message::encode_to_vec(
            &response,
        )))
    }
}
