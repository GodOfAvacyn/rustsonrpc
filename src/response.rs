use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

use crate::errors::{JsonRpcError, JsonRpcResult};

#[derive(Debug, Clone, PartialEq)]
pub struct JsonRpcResponse<T> {
    pub jsonrpc: String,
    pub id: Value,
    pub payload: JsonRpcResult<T>,
}

impl<T: Serialize> Serialize for JsonRpcResponse<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(3))?;
        map.serialize_entry("jsonrpc", &self.jsonrpc)?;
        map.serialize_entry("id", &self.id)?;
        match &self.payload {
            Ok(result) => map.serialize_entry("result", result)?,
            Err(error) => map.serialize_entry("error", error)?,
        }
        map.end()
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for JsonRpcResponse<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Wire<T> {
            jsonrpc: String,
            id: Value,
            result: Option<T>,
            error: Option<JsonRpcError>,
        }

        let wire = Wire::deserialize(deserializer)?;
        let result = match (wire.result, wire.error) {
            (Some(result), None) => Ok(result),
            (None, Some(error)) => Err(error),
            _ => {
                return Err(serde::de::Error::custom(
                    "JSON-RPC response must contain exactly one of result or error",
                ));
            }
        };

        Ok(Self {
            jsonrpc: wire.jsonrpc,
            id: wire.id,
            payload: result,
        })
    }
}
