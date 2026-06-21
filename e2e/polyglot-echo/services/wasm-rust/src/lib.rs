//! polyglot-echo wasm-rust — package-backed Actor-RTC EchoService workload.
//!
//! Mirror of `cli/fixtures/rust/lib.rs.service.hbs` but pinned to the wasm
//! Component Model build path (no `cdylib` feature on actr-framework — see
//! Cargo.toml).  Run `regenerate.sh` to populate `src/generated/` from
//! `protos/local/echo.proto`; without it the `generated_stub` fallback
//! below keeps the crate compilable.

#[cfg(actr_has_generated)]
pub mod generated;

#[cfg(not(actr_has_generated))]
pub mod generated_stub {
    pub mod echo {
        #[derive(Clone, PartialEq, prost::Message)]
        pub struct EchoRequest {
            #[prost(string, tag = "1")]
            pub message: String,
        }

        #[derive(Clone, PartialEq, prost::Message)]
        pub struct EchoResponse {
            #[prost(string, tag = "1")]
            pub reply: String,
            #[prost(uint64, tag = "2")]
            pub timestamp: u64,
        }
    }

    pub mod echo_actor {
        use actr_framework::{Context, MessageDispatcher, Workload};
        use actr_protocol::{ActorResult, PayloadType, RpcEnvelope, RpcRequest};
        use async_trait::async_trait;
        use bytes::Bytes;
        use prost::Message as ProstMessage;

        use super::echo::{EchoRequest, EchoResponse};

        impl RpcRequest for EchoRequest {
            type Response = EchoResponse;

            fn route_key() -> &'static str {
                "echo.EchoService.Echo"
            }

            fn payload_type() -> PayloadType {
                PayloadType::RpcReliable
            }
        }

        #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
        #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
        pub trait EchoServiceHandler: Send + Sync + 'static {
            async fn echo<C: Context>(&self, req: EchoRequest, ctx: &C) -> ActorResult<EchoResponse>;
        }

        pub struct EchoServiceWorkload<T: EchoServiceHandler>(pub T);

        impl<T: EchoServiceHandler> EchoServiceWorkload<T> {
            pub fn new(handler: T) -> Self {
                Self(handler)
            }
        }

        pub struct EchoServiceDispatcher<T: EchoServiceHandler>(std::marker::PhantomData<T>);

        #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
        #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
        impl<T: EchoServiceHandler> MessageDispatcher for EchoServiceDispatcher<T> {
            type Workload = EchoServiceWorkload<T>;

            async fn dispatch<C: Context>(
                workload: &Self::Workload,
                envelope: RpcEnvelope,
                ctx: &C,
            ) -> ActorResult<Bytes> {
                match envelope.route_key.as_str() {
                    "echo.EchoService.Echo" => {
                        let payload = envelope.payload.as_ref().ok_or_else(|| {
                            actr_protocol::ActrError::DecodeFailure(
                                "Missing payload in RpcEnvelope".to_string(),
                            )
                        })?;
                        let req = EchoRequest::decode(&**payload).map_err(|error| {
                            actr_protocol::ActrError::DecodeFailure(format!(
                                "Failed to decode EchoRequest: {error}"
                            ))
                        })?;
                        let response = workload.0.echo(req, ctx).await?;
                        Ok(Bytes::from(response.encode_to_vec()))
                    }
                    _ => Err(actr_protocol::ActrError::UnknownRoute(
                        envelope.route_key.to_string(),
                    )),
                }
            }
        }

        impl<T: EchoServiceHandler> Workload for EchoServiceWorkload<T> {
            type Dispatcher = EchoServiceDispatcher<T>;
        }
    }
}

#[cfg(not(actr_has_generated))]
pub use generated_stub as generated;

pub mod echo_service;

use actr_framework::entry;
use generated::echo_actor::EchoServiceWorkload;

pub use crate::echo_service::EchoServiceImpl;

entry!(
    EchoServiceWorkload<EchoServiceImpl>,
    EchoServiceWorkload::new(EchoServiceImpl::new())
);
