use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{error::Error, fmt};

pub type JsonRpcResult<T> = Result<T, JsonRpcError>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JsonRpcError {
    pub code: i64,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl JsonRpcError {
    pub fn parse_error() -> JsonRpcError {
        JsonRpcError {
            code: -32700,
            message: "Parse error".to_string(),
            data: None,
        }
    }

    pub fn invalid_request() -> JsonRpcError {
        JsonRpcError {
            code: -32600,
            message: "Invalid Request".to_string(),
            data: None,
        }
    }

    pub fn method_not_found() -> JsonRpcError {
        JsonRpcError {
            code: -32601,
            message: "Method not found".to_string(),
            data: None,
        }
    }

    pub fn invalid_params() -> JsonRpcError {
        JsonRpcError {
            code: -32602,
            message: "Invalid params".to_string(),
            data: None,
        }
    }

    pub fn internal_error(message: impl Into<String>) -> JsonRpcError {
        JsonRpcError {
            code: -32603,
            message: "Internal error".to_string(),
            data: Some(Value::String(message.into())),
        }
    }

    /// A transport-level failure (connect refused, connection closed, read/write
    /// I/O error, bind failure, ...). Uses the JSON-RPC implementation-defined
    /// server-error range (-32000) and carries a human-readable cause so callers
    /// don't just see an opaque "Internal error".
    pub fn transport_error(message: impl Into<String>) -> JsonRpcError {
        JsonRpcError {
            code: -32000,
            message: message.into(),
            data: None,
        }
    }

    pub fn to_response(self, id: Option<Value>) -> Value {
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": id.unwrap_or(Value::Null),
            "error": self,
        })
    }
}

impl fmt::Display for JsonRpcError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "JSON-RPC error {}: {}", self.code, self.message)
    }
}

impl Error for JsonRpcError {}
