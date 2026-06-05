use async_trait::async_trait;
use serde_json::Value;

use crate::{errors::JsonRpcResult, params::DynamicParams};

/// A collection of JSON-RPC methods that a peer keeps alive as an
/// `Arc<dyn Service>` and routes calls into.
///
/// Implement this manually for full control, or use `#[rpc_service]` on an
/// inherent impl block and mark individual methods with `#[rpc_method]`.
///
/// A method is identified by an index into [`Service::methods`]: when a peer
/// receives a call for the name at position `i`, it invokes
/// [`Service::dispatch`] with `method = i`.
#[async_trait]
pub trait Service: Send + Sync + 'static {
    /// The method names this service exposes. The index of each name is the
    /// id passed back to [`Service::dispatch`].
    fn methods(&self) -> &'static [&'static str];

    /// Execute the method identified by `method` (an index into
    /// [`Service::methods`]), decoding arguments from `params`.
    async fn dispatch(&self, method: u32, params: DynamicParams) -> JsonRpcResult<Value>;
}
