//! Hand-written business logic — the only thing that should change when
//! the echo semantics change.  Mirror of cli/fixtures/rust/echo_service.rs.hbs
//! verbatim.

use crate::generated::echo::{EchoRequest, EchoResponse};
use crate::generated::echo_actor::EchoServiceHandler;
use actr_framework::Context;
use actr_protocol::ActorResult;

#[derive(Default)]
pub struct EchoServiceImpl;

// Match the cfg-attr the generated EchoServiceHandler trait uses: wasm
// targets need `?Send` because the Component Model dispatcher is single-
// threaded.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl EchoServiceHandler for EchoServiceImpl {
    async fn echo<C: Context>(
        &self,
        req: EchoRequest,
        _ctx: &C,
    ) -> ActorResult<EchoResponse> {
        Ok(EchoResponse {
            reply: format!("Echo: {}", req.message),
            // Wasm guests can't read SystemTime under wasip2 without WASI
            // clocks; report 0 here.  The polyglot driver only checks the
            // reply string.
            timestamp: 0,
        })
    }
}

impl EchoServiceImpl {
    pub fn new() -> Self {
        Self
    }
}
