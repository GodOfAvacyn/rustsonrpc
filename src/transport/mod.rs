//! Transports: a single established bidirectional connection that can be split
//! into a [`MessageReader`]/[`MessageWriter`] pair and carries readable
//! [`Transport::metadata`] describing the remote end.

mod stdio;
mod tcp;
mod websocket;

pub use stdio::{StdioReader, StdioTransport, StdioWriter};
pub use tcp::{TcpReader, TcpTransport, TcpWriter};
pub use websocket::{WsReader, WsTransport, WsWriter};

use serde_json::Value;
use tokio::io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader};

use crate::errors::{JsonRpcError, Result};

#[async_trait::async_trait]
pub trait MessageReader: Send + 'static {
    async fn read_message(&mut self) -> Result<Vec<u8>>;
}

#[async_trait::async_trait]
pub trait MessageWriter: Send + 'static {
    async fn write_message(&mut self, msg: Vec<u8>) -> Result<()>;
}

pub trait Transport: Send + 'static {
    type Reader: MessageReader;
    type Writer: MessageWriter;

    fn metadata(&self) -> Value;

    fn split(self) -> (Self::Reader, Self::Writer);
}

pub(crate) async fn read_line_message<R>(reader: &mut BufReader<R>) -> Result<Vec<u8>>
where
    R: io::AsyncRead + Unpin,
{
    let mut line = Vec::new();
    let count = reader
        .read_until(b'\n', &mut line)
        .await
        .map_err(|err| JsonRpcError::transport_error(format!("read failed: {err}")))?;

    if count == 0 {
        return Err(JsonRpcError::transport_error("connection closed"));
    }

    while matches!(line.last(), Some(b'\n' | b'\r')) {
        line.pop();
    }

    Ok(line)
}

pub(crate) async fn write_line_message<W>(writer: &mut W, mut msg: Vec<u8>) -> Result<()>
where
    W: io::AsyncWrite + Unpin,
{
    msg.push(b'\n');
    writer
        .write_all(&msg)
        .await
        .map_err(|err| JsonRpcError::transport_error(format!("write failed: {err}")))?;
    writer
        .flush()
        .await
        .map_err(|err| JsonRpcError::transport_error(format!("flush failed: {err}")))
}

pub struct BoxedTransport {
    reader: Box<dyn MessageReader>,
    writer: Box<dyn MessageWriter>,
    metadata: Value,
}

impl BoxedTransport {
    pub(crate) fn new(
        reader: Box<dyn MessageReader>,
        writer: Box<dyn MessageWriter>,
        metadata: Value,
    ) -> BoxedTransport {
        BoxedTransport {
            reader,
            writer,
            metadata,
        }
    }
}

impl Transport for BoxedTransport {
    type Reader = Box<dyn MessageReader>;
    type Writer = Box<dyn MessageWriter>;

    fn metadata(&self) -> Value {
        self.metadata.clone()
    }

    fn split(self) -> (Self::Reader, Self::Writer) {
        (self.reader, self.writer)
    }
}

#[async_trait::async_trait]
impl MessageReader for Box<dyn MessageReader> {
    async fn read_message(&mut self) -> Result<Vec<u8>> {
        (**self).read_message().await
    }
}

#[async_trait::async_trait]
impl MessageWriter for Box<dyn MessageWriter> {
    async fn write_message(&mut self, msg: Vec<u8>) -> Result<()> {
        (**self).write_message(msg).await
    }
}
