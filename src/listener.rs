use async_trait::async_trait;
use tokio::net::{TcpListener as TokioTcpListener, ToSocketAddrs};
use tokio_tungstenite::{accept_async, MaybeTlsStream};

use crate::{
    errors::{JsonRpcError, JsonRpcResult},
    transport::{BoxedTransport, TcpTransport, Transport, WsTransport},
};

/// A source of inbound connections. Where a [`Transport`] is a single
/// established connection, a `Listener` produces a fresh one per `accept`.
///
/// `accept` yields a [`BoxedTransport`] (a type-erased transport), so the trait
/// has no associated transport type and stays object-safe — a server can hold a
/// `Box<dyn Listener>`. `bind` carries `where Self: Sized` to keep it off the
/// vtable while still requiring a concrete type to construct one.
#[async_trait]
pub trait Listener: Send + 'static {
    /// Bind to `addr` and start listening.
    async fn bind<A: ToSocketAddrs + Send>(addr: A) -> JsonRpcResult<Self>
    where
        Self: Sized;

    /// Wait for and accept the next inbound connection.
    async fn accept(&mut self) -> JsonRpcResult<BoxedTransport>;
}

/// Accepts line-delimited JSON connections over TCP.
pub struct TcpListener {
    inner: TokioTcpListener,
}

#[async_trait]
impl Listener for TcpListener {
    async fn bind<A: ToSocketAddrs + Send>(addr: A) -> JsonRpcResult<TcpListener> {
        let inner = TokioTcpListener::bind(addr)
            .await
            .map_err(|err| JsonRpcError::transport_error(format!("tcp bind failed: {err}")))?;

        Ok(TcpListener { inner })
    }

    async fn accept(&mut self) -> JsonRpcResult<BoxedTransport> {
        let (stream, _) = self
            .inner
            .accept()
            .await
            .map_err(|err| JsonRpcError::transport_error(format!("tcp accept failed: {err}")))?;

        let (reader, writer) = TcpTransport::from_stream(stream).split();
        Ok(BoxedTransport::new(Box::new(reader), Box::new(writer)))
    }
}

/// Accepts WebSocket connections: each accepted TCP stream is upgraded via the
/// WebSocket handshake before becoming a transport.
pub struct WsListener {
    inner: TokioTcpListener,
}

#[async_trait]
impl Listener for WsListener {
    async fn bind<A: ToSocketAddrs + Send>(addr: A) -> JsonRpcResult<WsListener> {
        let inner = TokioTcpListener::bind(addr)
            .await
            .map_err(|err| JsonRpcError::transport_error(format!("websocket bind failed: {err}")))?;

        Ok(WsListener { inner })
    }

    async fn accept(&mut self) -> JsonRpcResult<BoxedTransport> {
        let (stream, _) = self.inner.accept().await.map_err(|err| {
            JsonRpcError::transport_error(format!("websocket accept failed: {err}"))
        })?;

        let stream = accept_async(MaybeTlsStream::Plain(stream))
            .await
            .map_err(|err| {
                JsonRpcError::transport_error(format!("websocket handshake failed: {err}"))
            })?;

        let (reader, writer) = WsTransport::from_stream(stream).split();
        Ok(BoxedTransport::new(Box::new(reader), Box::new(writer)))
    }
}
