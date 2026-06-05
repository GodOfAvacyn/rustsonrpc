use std::{collections::HashMap, future::Future, sync::Arc};

use serde_json::Value;

use crate::{
    errors::JsonRpcResult,
    listener::{Listener, TcpListener, WsListener},
    params::DynamicParams,
    peer::{Handler, Peer},
    peer_builder::{register_service, Registry},
    server::{OnConnect, Server},
    service::Service,
};

pub struct ServerBuilder {
    methods: HashMap<String, Handler>,
    services: HashMap<u32, Arc<dyn Service>>,
    next_service_id: u32,
    on_connect: Option<OnConnect>,
}

impl ServerBuilder {
    pub fn new() -> ServerBuilder {
        ServerBuilder {
            methods: HashMap::new(),
            services: HashMap::new(),
            next_service_id: 0,
            on_connect: None,
        }
    }

    pub fn with_service<S: Service>(mut self, service: S) -> ServerBuilder {
        register_service(
            &mut self.methods,
            &mut self.services,
            &mut self.next_service_id,
            Arc::new(service),
        );
        self
    }

    pub fn with_method<F, Fut>(mut self, name: impl Into<String>, handler: F) -> ServerBuilder
    where
        F: Fn(DynamicParams) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = JsonRpcResult<Value>> + Send + 'static,
    {
        self.methods.insert(
            name.into(),
            Handler::Method(Arc::new(move |params| Box::pin(handler(params)))),
        );
        self
    }

    pub fn on_connect<F>(mut self, handler: F) -> ServerBuilder
    where
        F: Fn(Arc<Peer>) + Send + Sync + 'static,
    {
        self.on_connect = Some(Arc::new(handler));
        self
    }

    fn into_parts(self) -> (Registry, Option<OnConnect>) {
        (
            Registry {
                methods: self.methods,
                services: self.services,
                next_service_id: self.next_service_id,
            },
            self.on_connect,
        )
    }

    pub async fn serve_tcp(
        self,
        addr: impl tokio::net::ToSocketAddrs + Send,
    ) -> JsonRpcResult<Server> {
        let listener = TcpListener::bind(addr).await?;
        let (registry, on_connect) = self.into_parts();
        Ok(Server::serve(Box::new(listener), registry, on_connect))
    }

    pub async fn serve_ws(
        self,
        addr: impl tokio::net::ToSocketAddrs + Send,
    ) -> JsonRpcResult<Server> {
        let listener = WsListener::bind(addr).await?;
        let (registry, on_connect) = self.into_parts();
        Ok(Server::serve(Box::new(listener), registry, on_connect))
    }
}

impl Default for ServerBuilder {
    fn default() -> ServerBuilder {
        ServerBuilder::new()
    }
}
