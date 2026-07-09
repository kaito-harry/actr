use actr_framework::{Context, Dest, Workload};
use actr_protocol::{ActrType, RpcEnvelope};
use actr_hyper::prelude::*;
use bytes::Bytes;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{info, error};

mod generated;
use generated::greeter::{GreetRequest, GreetResponse};

#[derive(Clone)]
pub struct AllowedClientWorkload {
    server_id: Arc<Mutex<Option<ActrId>>>,
}

impl AllowedClientWorkload {
    fn new() -> Self {
        Self {
            server_id: Arc::new(Mutex::new(None)),
        }
    }

    async fn set_server_id(&self, server_id: ActrId) {
        *self.server_id.lock().await = Some(server_id);
    }
}

impl Workload for AllowedClientWorkload {
    type Dispatcher = AllowedClientDispatcher;
}

pub struct AllowedClientDispatcher;

#[async_trait::async_trait]
impl actr_framework::MessageDispatcher for AllowedClientDispatcher {
    type Workload = AllowedClientWorkload;

    async fn dispatch<C: Context>(
        workload: &Self::Workload,
        envelope: RpcEnvelope,
        ctx: &C,
    ) -> actr_protocol::ActorResult<Bytes> {
        let payload = envelope.payload.as_ref().ok_or_else(|| {
            actr_protocol::ActrError::DecodeFailure("Missing payload".to_string())
        })?;
        let request: GreetRequest = actr_protocol::prost::Message::decode(&**payload)
            .map_err(|e| actr_protocol::ActrError::DecodeFailure(e.to_string()))?;

        let server_id = workload.server_id.lock().await.clone();
        let server_id = match server_id {
            Some(id) => id,
            None => {
                error!("[AllowedClient] Server ID not set");
                return Err(actr_protocol::ActrError::Unavailable(
                    "Server ID not configured".to_string(),
                ));
            }
        };

        info!("[AllowedClient] Forwarding greeting to server...");
        
        let response: GreetResponse = ctx.call(&Dest::Peer(server_id), request).await?;

        info!("[AllowedClient] Got response: {}", response.message);

        Ok(Bytes::from(actr_protocol::prost::Message::encode_to_vec(
            &response,
        )))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    info!("🚀 Allowed Client starting...");

    let config_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("actr.toml");
    let config = actr_config::ConfigParser::from_file(&config_path)?;
    let workload = AllowedClientWorkload::new();

    let node = unimplemented!(
        "source-defined workload examples were removed; migrate this example to a package-backed host"
    );
    let actr_ref = node.start().await?;
    
    info!("✅ Allowed client registered");
    
    // Discover server
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    
    let server_type = ActrType {
        manufacturer: "acme".to_string(),
        name: "greeter.GreeterService".to_string(),
        version: "1.0.0".to_string(),
    };
    
    info!("🔍 Discovering greeter.GreeterService...");
    let servers = actr_ref.discover_route_candidates(&server_type, 1).await?;
    
    if servers.is_empty() {
        error!("❌ No server found");
        return Ok(());
    }
    
    let server_id = &servers[0];
    info!("✅ Found server: {:?}", server_id);
    
    // Set server ID in workload
    workload.set_server_id(server_id.clone()).await;
    
    // Send greeting (should succeed due to ACL)
    info!("📤 Sending greeting request (should succeed with ACL)...");
    let request = GreetRequest {
        name: "Allowed Client".to_string(),
    };
    
    match actr_ref.call(request).await {
        Ok(response) => {
            info!("✅ SUCCESS: Received response: {}", response.message);
            info!("🎉 ACL test PASSED - Allowed client can access server");
        }
        Err(e) => {
            error!("❌ FAILED: {}", e);
            error!("💥 ACL test FAILED - Allowed client was blocked (unexpected)");
        }
    }
    
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    Ok(())
}
