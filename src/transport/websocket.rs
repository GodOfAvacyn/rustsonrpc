use std::sync::{Arc, Mutex};

use futures_util::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use serde_json::{json, Map, Value};
use tokio::net::TcpStream;
use tokio_tungstenite::{
    accept_hdr_async, connect_async,
    tungstenite::{
        handshake::server::{ErrorResponse, Request, Response},
        http::HeaderMap,
        Message,
    },
    MaybeTlsStream, WebSocketStream,
};

use crate::errors::{JsonRpcError, Result};

use super::{MessageReader, MessageWriter, Transport};

pub struct WsTransport {
    stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    metadata: Value,
}

pub struct WsReader {
    reader: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
}

pub struct WsWriter {
    writer: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
}

impl WsTransport {
    pub async fn connect(url: &str) -> Result<WsTransport> {
        let (stream, response) = connect_async(url)
            .await
            .map_err(|err| JsonRpcError::transport_error(format!("websocket connect failed: {err}")))?;

        let metadata = json!({
            "address": url,
            "headers": header_map_to_value(response.headers()),
        });

        Ok(WsTransport { stream, metadata })
    }

    pub(crate) async fn accept(stream: TcpStream) -> Result<WsTransport> {
        // Grab the address before the stream is moved into the handshake.
        let address = match stream.peer_addr() {
            Ok(addr) => Value::String(addr.to_string()),
            Err(_) => Value::Null,
        };

        let captured: Arc<Mutex<Map<String, Value>>> = Arc::new(Mutex::new(Map::new()));
        let sink = captured.clone();
        let stream = accept_hdr_async(
            MaybeTlsStream::Plain(stream),
            move |request: &Request, response: Response| -> std::result::Result<Response, ErrorResponse> {
                *sink.lock().unwrap() = header_map_to_value(request.headers());
                Ok(response)
            },
        )
        .await
        .map_err(|err| {
            JsonRpcError::transport_error(format!("websocket handshake failed: {err}"))
        })?;

        let headers = Arc::try_unwrap(captured)
            .map(|cell| cell.into_inner().unwrap())
            .unwrap_or_default();

        let metadata = json!({
            "address": address,
            "headers": Value::Object(headers),
        });

        Ok(WsTransport { stream, metadata })
    }
}

fn header_map_to_value(headers: &HeaderMap) -> Map<String, Value> {
    let mut map = Map::new();
    for (name, value) in headers {
        map.insert(
            name.as_str().to_string(),
            Value::String(String::from_utf8_lossy(value.as_bytes()).into_owned()),
        );
    }
    map
}

impl Transport for WsTransport {
    type Reader = WsReader;
    type Writer = WsWriter;

    fn metadata(&self) -> Value {
        self.metadata.clone()
    }

    fn split(self) -> (WsReader, WsWriter) {
        let (writer, reader) = self.stream.split();

        (WsReader { reader }, WsWriter { writer })
    }
}

#[async_trait::async_trait]
impl MessageReader for WsReader {
    async fn read_message(&mut self) -> Result<Vec<u8>> {
        loop {
            let Some(message) = self.reader.next().await else {
                return Err(JsonRpcError::transport_error("websocket stream ended"));
            };

            match message
                .map_err(|err| JsonRpcError::transport_error(format!("websocket read failed: {err}")))?
            {
                Message::Binary(bytes) => return Ok(bytes.to_vec()),
                Message::Text(text) => return Ok(text.to_string().into_bytes()),
                Message::Close(_) => {
                    return Err(JsonRpcError::transport_error("websocket closed by peer"))
                }
                Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {}
            }
        }
    }
}

#[async_trait::async_trait]
impl MessageWriter for WsWriter {
    async fn write_message(&mut self, msg: Vec<u8>) -> Result<()> {
        self.writer
            .send(Message::Binary(msg.into()))
            .await
            .map_err(|err| JsonRpcError::transport_error(format!("websocket write failed: {err}")))
    }
}
