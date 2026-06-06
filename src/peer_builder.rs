use std::{collections::HashMap, future::Future, sync::Arc};

use serde_json::Value;

use crate::{
    errors::Result,
    params::DynamicParams,
    peer::{Handler, Peer},
    service::Service,
    transport::{StdioTransport, TcpTransport, Transport, WsTransport},
};

#[derive(Clone)]
pub(crate) struct Registry {
    pub methods: HashMap<String, Handler>,
    pub services: HashMap<u32, Arc<dyn Service>>,
    pub next_service_id: u32,
}

/// Install `service` into the given method/service maps, allocating it the next
/// id and routing each of its method names to it.
pub(crate) fn register_service(
    methods: &mut HashMap<String, Handler>,
    services: &mut HashMap<u32, Arc<dyn Service>>,
    next_service_id: &mut u32,
    service: Arc<dyn Service>,
) {
    let id = *next_service_id;
    *next_service_id += 1;

    for (index, name) in service.methods().iter().enumerate() {
        methods.insert(
            (*name).to_string(),
            Handler::Service {
                service: id,
                method: index as u32,
            },
        );
    }
    services.insert(id, service);
}

/// Accumulates methods and services, then builds a [`Peer`] over a transport.
pub struct PeerBuilder {
    methods: HashMap<String, Handler>,
    services: HashMap<u32, Arc<dyn Service>>,
    next_service_id: u32,
}

impl PeerBuilder {
    pub fn new() -> PeerBuilder {
        PeerBuilder {
            methods: HashMap::new(),
            services: HashMap::new(),
            next_service_id: 0,
        }
    }

    /// Register a [`Service`], routing each of its method names to it.
    pub fn with_service<S: Service>(mut self, service: S) -> PeerBuilder {
        register_service(
            &mut self.methods,
            &mut self.services,
            &mut self.next_service_id,
            Arc::new(service),
        );
        self
    }

    /// Register a standalone closure under `name`.
    pub fn with_method<F, Fut>(mut self, name: impl Into<String>, handler: F) -> PeerBuilder
    where
        F: Fn(DynamicParams) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value>> + Send + 'static,
    {
        self.methods.insert(
            name.into(),
            Handler::Method(Arc::new(move |params| Box::pin(handler(params)))),
        );
        self
    }

    fn into_registry(self) -> Registry {
        Registry {
            methods: self.methods,
            services: self.services,
            next_service_id: self.next_service_id,
        }
    }

    fn build<T: Transport>(self, transport: T) -> Peer {
        let peer = Peer::from_registry(transport, self.into_registry());
        peer.start();
        peer
    }

    pub async fn connect_tcp(self, addr: impl tokio::net::ToSocketAddrs) -> Result<Peer> {
        let transport = TcpTransport::connect(addr).await?;
        Ok(self.build(transport))
    }

    pub async fn connect_ws(self, url: &str) -> Result<Peer> {
        let transport = WsTransport::connect(url).await?;
        Ok(self.build(transport))
    }

    pub async fn connect_process(self, command: tokio::process::Command) -> Result<Peer> {
        let transport = StdioTransport::spawn(command).await?;
        Ok(self.build(transport))
    }

    pub fn stdio(self) -> Peer {
        self.build(StdioTransport::new())
    }
}

impl Default for PeerBuilder {
    fn default() -> PeerBuilder {
        PeerBuilder::new()
    }
}
