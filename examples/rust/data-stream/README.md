# DataChunk API Example

This example demonstrates the DataChunk API for streaming application data between Actors.

## Overview

The DataChunk API provides a Fast Path for streaming non-media data (file transfers, game state updates, custom protocols) between Actors without going through the RPC envelope mechanism.

**Key Concepts:**
- **DataChunk**: Protocol buffer message for streaming data chunks
- **Stream ID**: Identifies a specific data stream (e.g., "file-transfer")
- **Sequence Number**: Ensures ordered delivery of chunks
- **Fast Path**: Bypasses RPC envelope for lower latency

## Architecture

```
Sender (datastream.Sender)
  └─ SenderWorkload
      └─ ctx.send_data_chunk(&dest, chunk)
           │
           ├─ RuntimeContext::send_data_chunk()
           ├─ Gate::send_data_chunk()
           └─ PeerGate::send_data_chunk()
                │
                └─ PeerTransport::send()
                     └─ WebRTC DataChannel / WebSocket

Receiver (datastream.Receiver)
  └─ ReceiverWorkload
      └─ ctx.register_stream("file-transfer", callback)
           │
           └─ DataChunkRegistry::register()
                │
                └─ InboundPacketDispatcher::dispatch()
                     └─ Callback invoked with DataChunk
```

## Components

### Sender
- **sender/src/main.rs**: Entry point, builds `ActrNode` and starts it
- **sender/src/sender_workload.rs**: Workload that sends 10 data chunks
- **sender/src/generated/**: Auto-generated code from proto files (via `actr gen`)

### Receiver
- **receiver/src/main.rs**: Entry point, builds `ActrNode` and starts it
- **receiver/src/receiver_workload.rs**: Workload that registers callback to receive chunks
- **receiver/src/generated/**: Auto-generated code from proto files (via `actr gen`)

## Running the Example

### Prerequisites

1. **Actrix (Signaling Server)** must be running. The `start.sh` script will automatically start it, or you can run it manually:
   ```bash
   actrix --config actrix-config.toml
   ```

2. **Code Generation**: The `start.sh` script automatically generates code using `actr gen`. The sender/receiver crates also run `actr gen` in `build.rs` during `cargo build`/`cargo run`—`actr` must be available in `PATH`; otherwise build will fail. To generate manually:
    ```bash
    cd data-stream/sender
    actr gen --input=proto --output=src/generated --clean --no-scaffold
   
   cd ../receiver
   actr gen --input=proto --output=src/generated --clean
   ```
   
   Note: Sender uses `--no-scaffold` because it doesn't need the user code template (uses `sender_workload.rs` instead).

### Option 1: Use start script (Recommended)

From the workspace root:
```bash
bash data-stream/start.sh
```

This will:
- Generate code using `actr gen`
- Build and start actrix (signaling server)
- Build binaries
- Start receiver and sender automatically

### Option 2: Manual Start (For debugging)

From the workspace root:

Terminal 1 - Start Receiver:
```bash
RUST_LOG=info cargo run --bin data-stream-receiver
```

Terminal 2 - Start Sender (after receiver is ready):
```bash
RUST_LOG=info cargo run --bin data-stream-sender
```

You can also use the `-p` flag (equivalent):
```bash
cargo run -p data-stream-receiver
cargo run -p data-stream-sender
```

## Expected Output

### Receiver Output:
```
🚀 DataChunk Receiver starting
✅ ActrNode created
✅ ActrNode started!
🎉 Receiver ready to receive data streams
📦 Received chunk #1: stream_id=file-transfer, sequence=0, size=67 bytes
📄 Content preview: Chunk #0: Hello from DataChunk API! This is chunk number 0.
📦 Received chunk #2: stream_id=file-transfer, sequence=1, size=67 bytes
...
📊 Final statistics:
   Total chunks received: 10
   Total bytes received: 670
```

### Sender Output:
```
🚀 DataChunk Sender starting
✅ ActrNode created
✅ ActrNode started!
📤 SenderWorkload will start sending after 3 seconds...
📤 Sending chunk #0 (sequence=0, size=67 bytes)
✅ Chunk #0 sent successfully
📤 Sending chunk #1 (sequence=1, size=67 bytes)
✅ Chunk #1 sent successfully
...
✅ All chunks sent!
```

## Key API Methods

### Sender Side

```rust
use actr_protocol::DataChunk;
use actr_framework::{Context, Dest};

// Create a DataChunk
let chunk = DataChunk {
    stream_id: "file-transfer".to_string(),
    sequence: 0,
    payload: bytes::Bytes::from("Hello!"),
    metadata: vec![],
    timestamp_ms: Some(chrono::Utc::now().timestamp_millis()),
};

// Send to receiver
let dest = Dest::Peer(receiver_id);
ctx.send_data_chunk(&dest, chunk).await?;
```

### Receiver Side

```rust
// Register callback for stream_id
ctx.register_stream("file-transfer".to_string(), |chunk: DataChunk, sender_id: ActrId| {
    Box::pin(async move {
        println!("Received chunk: sequence={}, size={}", chunk.sequence, chunk.payload.len());
        Ok(())
    })
}).await?;

// Unregister when done
ctx.unregister_stream("file-transfer").await?;
```

## Use Cases

- **File Transfer**: Stream large files in chunks
- **Game State Sync**: Send game state updates at high frequency
- **Custom Protocols**: Implement your own streaming protocols
- **Log Streaming**: Stream application logs to monitoring services
- **Sensor Data**: Stream IoT sensor data

## Comparison with RPC

| Feature | RPC (call/tell) | DataChunk |
|---------|----------------|------------|
| Use Case | Request-response | Streaming data |
| Overhead | Higher (RpcEnvelope) | Lower (direct protobuf) |
| Ordering | Single request | Sequence numbers |
| Callback | Per-message handler | Stream callback |
| Best For | Commands, queries | Bulk data, high frequency |

## Next Steps

1. Try modifying chunk size and frequency
2. Implement proper file transfer with error handling
3. Add flow control and backpressure
4. Experiment with StreamLatencyFirst vs StreamReliable PayloadTypes
