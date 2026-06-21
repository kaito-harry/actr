//! Hand-written business logic — the only thing that should change when
//! the echo semantics change.  Everything else (proto types, dispatcher
//! glue) is mechanically derivable from echo.proto.

use crate::proto::{EchoRequest, EchoResponse};

pub fn handle_echo(req: EchoRequest) -> EchoResponse {
    EchoResponse {
        reply: format!("Echo: {}", req.message),
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64,
    }
}
