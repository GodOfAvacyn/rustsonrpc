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
pub use serde as __serde;
#[doc(hidden)]
pub use async_trait as __async_trait;
pub use rustsonrpc_macros::{rpc_method, rpc_service, Params};
pub use errors::Context;
pub use params::{DynamicParams, IntoParams};

use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;

use crate::errors::{JsonRpcError, Result};

/// Serialize a value to a JSON string, mapping a serialization failure to an
/// internal error so callers stay in [`Result`](crate::errors::Result).
pub fn serialize<T: ?Sized + Serialize>(value: &T) -> Result<String> {
    serde_json::to_string(value)
        .map_err(|error| JsonRpcError::internal_error(format!("failed to serialize JSON: {error}")))
}

/// Deserialize a value from a JSON string, mapping a deserialization failure to
/// an internal error.
pub fn deserialize<T: DeserializeOwned>(text: &str) -> Result<T> {
    serde_json::from_str(text)
        .map_err(|error| JsonRpcError::internal_error(format!("failed to deserialize JSON: {error}")))
}

/// Serialize a value into a JSON [`Value`], mapping a serialization failure to
/// an internal error. Use this (not [`serialize`]) when the result is nested
/// into more JSON in memory rather than written out as text.
pub fn to_value<T: Serialize>(value: T) -> Result<Value> {
    serde_json::to_value(value)
        .map_err(|error| JsonRpcError::internal_error(format!("failed to serialize JSON: {error}")))
}

/// Deserialize a value from a JSON [`Value`], mapping a deserialization failure
/// to an internal error.
pub fn from_value<T: DeserializeOwned>(value: Value) -> Result<T> {
    serde_json::from_value(value)
        .map_err(|error| JsonRpcError::internal_error(format!("failed to deserialize JSON: {error}")))
}

#[macro_export]
macro_rules! params {
    () => {
        $crate::params::DynamicParams::empty()
    };
    ($($json:tt)+) => {
        match $crate::__serde_json::json!($($json)+) {
            $crate::__serde_json::Value::Object(values) => {
                $crate::params::DynamicParams::new(values)
            }
            _ => panic!("JSON-RPC params must be an object"),
        }
    };
}

pub mod prelude {
    pub use crate::{
        peer_builder::PeerBuilder,
        errors::{JsonRpcError, Result},
        peer::Peer,
        params, rpc_method, rpc_service, Params,
        server::Server,
        server_builder::ServerBuilder,
        service::Service,
    };
    pub use serde_json::Value;
}
