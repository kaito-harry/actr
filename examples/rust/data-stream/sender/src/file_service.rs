//! # File [...]
//!
//! [...] `actr gen` [...]。
//! [...]。

use crate::generated::{
    file_transfer::*, local_file::*, file_actor::LocalFileServiceHandler,
};
use actr_framework::Context;
use actr_protocol::{ActrType, DataChunk};
use actr_hyper::prelude::*;
use bytes::Bytes;

pub struct MyFileService {
    receiver_id: Mutex<Option<ActrId>>,
}

impl MyFileService {
    pub fn new() -> Self {
        Self {
            receiver_id: Mutex::new(None),
        }
    }

    async fn get_receiver_id<C: Context>(&self, ctx: &C) -> ActorResult<ActrId> {
        let receiver_id = self.receiver_id.lock().await.clone();
        if let Some(receiver_id) = receiver_id {
            return Ok(receiver_id);
        }

        let mut receiver_id_cell = self.receiver_id.lock().await;
        let target_type = ActrType {
            manufacturer: "acme".to_string(),
            name: "FileTransferService".to_string(),
            version: "1.0.0".to_string(),
        };
        info!(
            "🌐 Discovering receiver via signaling for type: {}",
            target_type.to_string_repr()
        );
        let receiver_id = ctx.discover_route_candidate(&target_type).await?;
        info!("🎯 Discovered receiver: {}", receiver_id.to_string_repr());
        *receiver_id_cell = Some(receiver_id.clone());
        Ok(receiver_id)
    }
}

#[async_trait::async_trait]
impl LocalFileServiceHandler for MyFileService {
    async fn send_file<C: Context>(
        &self,
        req: SendFileRequest,
        ctx: &C,
    ) -> ActorResult<SendFileResponse> {
        let filename = req.filename;
        info!("📤 Starting file transfer:");
        info!("   Filename: {}", filename);

        let receiver_id = self.get_receiver_id(ctx).await?;
        let (content, chunks) = create_content();

        // Phase 1: StartTransfer RPC (Control Plane)
        info!("📡 Phase 1: Sending StartTransfer RPC...");
        let start_req = StartTransferRequest {
            stream_id: "test-stream-001".to_string(),
            filename: filename,
            total_size: content.len() as u64,
            chunk_count: chunks.len() as u32,
        };

        let start_resp: StartTransferResponse = ctx
            .call(&Dest::Peer(receiver_id.clone()), start_req)
            .await?;
        if !start_resp.ready {
            return Ok(SendFileResponse { success: false });
        }

        info!("✅ StartTransfer RPC succeeded: {}", start_resp.message);

        // Phase 2: Send DataChunks (Data Plane - Fast Path)
        info!("📦 Phase 2: Sending {} DataChunks...", chunks.len());

        for (i, chunk) in chunks.iter().enumerate() {
            let data_chunk = DataChunk {
                stream_id: "test-stream-001".to_string(),
                sequence: i as u64,
                payload: chunk.clone().into(),
                metadata: vec![],
                timestamp_ms: Some(chrono::Utc::now().timestamp_millis()),
            };

            ctx.send_data_chunk(
                &Dest::Peer(receiver_id.clone()),
                data_chunk,
                actr_protocol::PayloadType::StreamReliable,
            )
                .await?;

            let progress = ((i + 1) as f64 / chunks.len() as f64 * 100.0) as u32;
            info!(
                "   Sent chunk #{}/{}: {} bytes ({}%)",
                i + 1,
                chunks.len(),
                chunk.len(),
                progress
            );
        }

        info!("✅ All chunks sent successfully");

        // Phase 3: EndTransfer RPC (Control Plane)
        info!("🏁 Phase 3: Sending EndTransfer RPC...");
        let end_req = EndTransferRequest {
            stream_id: "test-stream-001".to_string(),
            success: true,
        };

        let end_resp: EndTransferResponse =
            ctx.call(&Dest::Peer(receiver_id.clone()), end_req).await?;

        info!("✅ EndTransfer RPC succeeded!");
        info!("📊 Transfer Statistics:");
        info!("   Acknowledged: {}", end_resp.acknowledged);
        info!("   Chunks received: {}", end_resp.chunks_received);
        info!("   Bytes received: {}", end_resp.bytes_received);
        info!("🎉 File transfer completed successfully!");

        Ok(SendFileResponse { success: true })
    }
}

fn create_content() -> (String, Vec<Bytes>) {
    let content = "Hello DataChunk! This is a test file content. ".repeat(100);
    let chunk_size = 1024;
    let chunks: Vec<Bytes> = content
        .as_bytes()
        .chunks(chunk_size)
        .map(Bytes::copy_from_slice)
        .collect();

    info!("   Total size: {} bytes", content.len());
    info!("   Chunk size: {} bytes", chunk_size);
    info!("   Chunk count: {}", chunks.len());

    (content, chunks)
}
