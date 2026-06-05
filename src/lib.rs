pub mod errors;
pub mod request;
pub mod response;
pub mod params;
pub mod listener;
pub mod peer;
pub mod peer_builder;
pub mod service;
pub mod server;
pub mod server_builder;
pub mod transport;

#[doc(hidden)]
pub use serde_json as __serde_json;
#[doc(hidden)]
pub use async_trait as __async_trait;
pub use rustsonrpc_macros::{rpc_method, rpc_service};

#[macro_export]
macro_rules! params {
    () => {
        None
    };
    ($($json:tt)+) => {
        Some($crate::__serde_json::json!($($json)+))
    };
}

pub mod prelude {
    pub use crate::{
        peer_builder::PeerBuilder,
        errors::{JsonRpcError, JsonRpcResult},
        listener::{Listener, TcpListener, WsListener},
        params::{DynamicParams, IntoParams},
        peer::Peer,
        params, rpc_method, rpc_service,
        request::JsonRpcRequest,
        response::JsonRpcResponse,
        server::Server,
        server_builder::ServerBuilder,
        service::Service,
        transport::{
            MessageReader, MessageWriter, StdioTransport, TcpTransport, Transport, WsTransport,
        },
    };
    pub use serde_json::Value;
}
