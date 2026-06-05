use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicU32, AtomicU64, Ordering},
        Arc, RwLock,
    },
};

use serde_json::{Map, Value};
use tokio::{
    sync::{oneshot, watch, Mutex},
    task::JoinHandle,
};

use crate::{
    peer_builder::Registry,
    errors::{JsonRpcError, JsonRpcResult},
    params::{DynamicParams, IntoParams},
    request::JsonRpcRequest,
    response::JsonRpcResponse,
    service::Service,
    transport::{MessageReader, MessageWriter, Transport},
};

pub struct Peer {
    methods: Methods,
    services: Services,
    pending: Pending,
    writer: Writer,
    next_id: AtomicU64,
    next_service_id: Arc<AtomicU32>,
    start: std::sync::Mutex<Option<oneshot::Sender<()>>>,
    closed: watch::Receiver<bool>,
    read_task: JoinHandle<()>,
}

impl Peer {
    pub(crate) fn from_registry<T: Transport>(transport: T, registry: Registry) -> Peer {
        let (reader, writer) = transport.split();
        let (start_tx, start_rx) = oneshot::channel();
        let (closed_tx, closed_rx) = watch::channel(false);
        let pending = Arc::new(Mutex::new(HashMap::new()));
        let writer: Writer = Arc::new(Mutex::new(Box::new(writer)));
        let methods = Arc::new(RwLock::new(registry.methods));
        let services = Arc::new(RwLock::new(registry.services));
        let next_service_id = Arc::new(AtomicU32::new(registry.next_service_id));
        let read_task = tokio::spawn(read_loop(
            Box::new(reader),
            writer.clone(),
            pending.clone(),
            methods.clone(),
            services.clone(),
            start_rx,
            closed_tx,
        ));

        Peer {
            methods,
            services,
            pending,
            writer,
            next_id: AtomicU64::new(0),
            next_service_id,
            start: std::sync::Mutex::new(Some(start_tx)),
            closed: closed_rx,
            read_task,
        }
    }

    pub(crate) fn start(&self) {
        if let Some(sender) = self.start.lock().unwrap().take() {
            let _ = sender.send(());
        }
    }

    pub async fn wait_closed(&self) {
        let mut closed = self.closed.clone();
        let _ = closed.changed().await;
    }

    pub async fn call<A>(&self, method: impl Into<String>, params: impl IntoParams) -> JsonRpcResult<A>
    where
        A: serde::de::DeserializeOwned,
    {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let message = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: method.into(),
            params: params.into_params()?,
            id: Some(Value::from(id)),
        };
        let bytes = serde_json::to_vec(&message).map_err(|_| JsonRpcError::parse_error())?;

        let (sender, receiver) = oneshot::channel();
        self.pending.lock().await.insert(id, sender);

        if let Err(err) = self.writer.lock().await.write_message(bytes).await {
            self.pending.lock().await.remove(&id);
            return Err(err);
        }

        let value = receiver
            .await
            .map_err(|error| {
                JsonRpcError::internal_error(format!("response channel closed: {error}"))
            })??;

        serde_json::from_value(value).map_err(|error| {
            JsonRpcError::internal_error(format!(
                "failed to deserialize response result: {error}"
            ))
        })
    }

    pub async fn notify(&self, method: impl Into<String>, params: impl IntoParams) -> JsonRpcResult<()> {
        let message = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: method.into(),
            params: params.into_params()?,
            id: None,
        };
        let bytes = serde_json::to_vec(&message).map_err(|_| JsonRpcError::parse_error())?;

        self.writer.lock().await.write_message(bytes).await
    }

    pub fn add_method<F, Fut>(&self, name: impl Into<String>, handler: F) -> &Self
    where
        F: Fn(DynamicParams) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = JsonRpcResult<Value>> + Send + 'static,
    {
        self.methods.write().unwrap().insert(
            name.into(),
            Handler::Method(Arc::new(move |params| Box::pin(handler(params)))),
        );
        self
    }

    pub fn add_service<S: Service>(&self, service: S) -> &Self {
        let service: Arc<dyn Service> = Arc::new(service);
        let id = self.next_service_id.fetch_add(1, Ordering::Relaxed);

        {
            let mut methods = self.methods.write().unwrap();
            for (index, name) in service.methods().iter().enumerate() {
                methods.insert(
                    (*name).to_string(),
                    Handler::Service {
                        service: id,
                        method: index as u32,
                    },
                );
            }
        }
        self.services.write().unwrap().insert(id, service);
        self
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        self.read_task.abort();
    }
}

async fn read_loop(
    mut reader: Box<dyn MessageReader>,
    writer: Writer,
    pending: Pending,
    methods: Methods,
    services: Services,
    start_rx: oneshot::Receiver<()>,
    _closed_tx: watch::Sender<bool>,
) {
    if start_rx.await.is_err() {
        return;
    }

    loop {
        let bytes = match reader.read_message().await {
            Ok(bytes) => bytes,
            Err(_) => break,
        };

        let value: Value = match serde_json::from_slice(&bytes) {
            Ok(value) => value,
            Err(_) => {
                let _ = send_value(&writer, JsonRpcError::parse_error().to_response(None)).await;
                continue;
            }
        };

        if handle_message(value, &writer, &pending, &methods, &services)
            .await
            .is_err()
        {
            break;
        }
    }
}

async fn handle_message(
    value: Value,
    writer: &Writer,
    pending: &Pending,
    methods: &Methods,
    services: &Services,
) -> JsonRpcResult<()> {
    match value {
        Value::Array(items) => {
            if items.is_empty() {
                send_value(writer, JsonRpcError::invalid_request().to_response(None)).await?;
                return Ok(());
            }

            let mut responses = Vec::new();
            for item in items {
                if let Some(response) = handle_one(item, pending, methods, services).await? {
                    responses.push(response);
                }
            }

            if !responses.is_empty() {
                send_value(writer, Value::Array(responses)).await?;
            }
        }
        Value::Object(object) => {
            if let Some(response) = handle_object(object, pending, methods, services).await? {
                send_value(writer, response).await?;
            }
        }
        _ => {
            send_value(writer, JsonRpcError::invalid_request().to_response(None)).await?;
        }
    }

    Ok(())
}

async fn handle_one(
    value: Value,
    pending: &Pending,
    methods: &Methods,
    services: &Services,
) -> JsonRpcResult<Option<Value>> {
    match value {
        Value::Object(object) => handle_object(object, pending, methods, services).await,
        _ => Ok(Some(JsonRpcError::invalid_request().to_response(None))),
    }
}

async fn handle_object(
    object: Map<String, Value>,
    pending: &Pending,
    methods: &Methods,
    services: &Services,
) -> JsonRpcResult<Option<Value>> {
    let id = object.get("id").cloned();
    if !is_valid_id(id.as_ref()) {
        return Ok(Some(JsonRpcError::invalid_request().to_response(None)));
    }

    if object.get("jsonrpc") != Some(&Value::String("2.0".to_string())) {
        return Ok(Some(JsonRpcError::invalid_request().to_response(id)));
    }

    if !object.contains_key("method") {
        if let Some(id) = id {
            deliver_response(id, &object, pending).await;
            return Ok(None);
        }

        return Ok(Some(JsonRpcError::invalid_request().to_response(None)));
    }

    let method = match object.get("method") {
        Some(Value::String(method)) => method,
        _ => {
            return Ok(Some(JsonRpcError::invalid_request().to_response(id)));
        }
    };
    let is_notification = id.is_none();
    let id = id.unwrap_or(Value::Null);
    let params = object.get("params").cloned();

    let handler = methods.read().unwrap().get(method).cloned();
    let handler = match handler {
        Some(handler) => handler,
        None => {
            if is_notification {
                return Ok(None);
            }

            return Ok(Some(JsonRpcError::method_not_found().to_response(Some(id))));
        }
    };

    let params = match DynamicParams::from_value(params) {
        Ok(params) => params,
        Err(err) => {
            if is_notification {
                return Ok(None);
            }

            return Ok(Some(err.to_response(Some(id))));
        }
    };

    let result = match handler {
        Handler::Method(handler) => handler(params).await,
        Handler::Service {
            service: service_id,
            method,
        } => {
            // Clone the Arc out from under the lock so the guard is released
            // before we await the (possibly long-running) dispatch.
            let service = services.read().unwrap().get(&service_id).cloned();
            match service {
                Some(service) => service.dispatch(method, params).await,
                None => Err(JsonRpcError::internal_error(
                    "registered service no longer exists",
                )),
            }
        }
    };
    if is_notification {
        return Ok(None);
    }

    let response = JsonRpcResponse {
        jsonrpc: "2.0".to_string(),
        id,
        payload: result,
    };

    Ok(Some(serde_json::to_value(response).unwrap_or_else(|error| {
        JsonRpcError::internal_error(format!("failed to serialize response: {error}"))
        .to_response(None)
    })))
}

async fn deliver_response(
    id: Value,
    object: &Map<String, Value>,
    pending: &Pending,
) {
    let Some(id) = id.as_u64() else {
        return;
    };
    let Some(sender) = pending.lock().await.remove(&id) else {
        return;
    };

    let has_result = object.contains_key("result");
    let has_error = object.contains_key("error");
    let result = match (has_result, has_error) {
        (true, false) => Ok(object.get("result").cloned().unwrap_or(Value::Null)),
        (false, true) => match object.get("error").cloned() {
            Some(error) => serde_json::from_value(error)
                .map_err(|_| JsonRpcError::invalid_request())
                .and_then(Err),
            None => Err(JsonRpcError::invalid_request()),
        },
        _ => Err(JsonRpcError::invalid_request()),
    };

    let _ = sender.send(result);
}

async fn send_value(
    writer: &Writer,
    value: Value,
) -> JsonRpcResult<()> {
    let bytes = serde_json::to_vec(&value).map_err(|error| {
        JsonRpcError::internal_error(format!("failed to serialize message: {error}"))
    })?;
    writer.lock().await.write_message(bytes).await
}

fn is_valid_id(id: Option<&Value>) -> bool {
    match id {
        None => true,
        Some(Value::Null | Value::String(_) | Value::Number(_)) => true,
        _ => false,
    }
}

pub type Methods = Arc<RwLock<HashMap<String, Handler>>>;
pub type Services = Arc<RwLock<HashMap<u32, Arc<dyn Service>>>>;
pub type Pending = Arc<Mutex<HashMap<u64, oneshot::Sender<JsonRpcResult<Value>>>>>;
pub type Writer = Arc<Mutex<Box<dyn MessageWriter>>>;

pub type HandlerFuture = Pin<Box<dyn Future<Output = JsonRpcResult<Value>> + Send + 'static>>;
pub type MethodFn = Arc<dyn Fn(DynamicParams) -> HandlerFuture + Send + Sync + 'static>;

#[derive(Clone)]
pub enum Handler {
    Method(MethodFn),
    Service { service: u32, method: u32 },
}

