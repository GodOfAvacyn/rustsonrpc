use serde::{de::DeserializeOwned, Serialize};
use serde_json::{Map, Value};

use crate::errors::{JsonRpcError, Result};

pub trait IntoParams {
    fn into_params(self) -> Result<Option<Value>>;
}

#[derive(Debug, Clone)]
pub struct DynamicParams {
    values: Map<String, Value>,
}

impl DynamicParams {
    pub fn new(values: Map<String, Value>) -> DynamicParams {
        DynamicParams { values }
    }

    pub fn empty() -> DynamicParams {
        DynamicParams { values: Map::new() }
    }

    pub fn from_value(value: Option<Value>) -> Result<DynamicParams> {
        match value {
            Some(Value::Object(values)) => Ok(DynamicParams::new(values)),
            None => Ok(DynamicParams::empty()),
            _ => Err(JsonRpcError::invalid_params()),
        }
    }

    pub fn get<T>(&self, name: &str) -> Result<T>
    where
        T: DeserializeOwned,
    {
        let value = self.values.get(name).cloned().unwrap_or(Value::Null);

        serde_json::from_value(value).map_err(|_| JsonRpcError::invalid_params())
    }

    pub fn with<T>(mut self, name: impl Into<String>, value: T) -> Result<DynamicParams>
    where
        T: Serialize,
    {
        let value = serde_json::to_value(value).map_err(|error| {
            JsonRpcError::internal_error(format!("failed to serialize RPC param: {error}"))
        })?;

        self.values.insert(name.into(), value);

        Ok(self)
    }
}

impl IntoParams for () {
    fn into_params(self) -> Result<Option<Value>> {
        Ok(None)
    }
}

impl IntoParams for DynamicParams {
    fn into_params(self) -> Result<Option<Value>> {
        Ok(Some(Value::Object(self.values)))
    }
}

impl IntoParams for Option<Value> {
    fn into_params(self) -> Result<Option<Value>> {
        match self {
            Some(Value::Object(_)) | None => Ok(self),
            _ => Err(JsonRpcError::invalid_params()),
        }
    }
}

impl IntoParams for Value {
    fn into_params(self) -> Result<Option<Value>> {
        match self {
            Value::Object(_) => Ok(Some(self)),
            _ => Err(JsonRpcError::invalid_params()),
        }
    }
}

impl IntoParams for Map<String, Value> {
    fn into_params(self) -> Result<Option<Value>> {
        Ok(Some(Value::Object(self)))
    }
}
