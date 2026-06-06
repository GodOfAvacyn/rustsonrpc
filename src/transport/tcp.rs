use serde_json::{json, Value};
use tokio::{
    io::BufReader,
    net::TcpStream,
};

use crate::errors::{JsonRpcError, Result};

use super::{read_line_message, write_line_message, MessageReader, MessageWriter, Transport};

pub struct TcpTransport {
    stream: TcpStream,
    metadata: Value,
}

pub struct TcpReader {
    reader: BufReader<tokio::net::tcp::OwnedReadHalf>,
}

pub struct TcpWriter {
    writer: tokio::net::tcp::OwnedWriteHalf,
}

impl TcpTransport {
    pub async fn connect(addr: impl tokio::net::ToSocketAddrs) -> Result<TcpTransport> {
        let stream = TcpStream::connect(addr)
            .await
            .map_err(|err| JsonRpcError::transport_error(format!("tcp connect failed: {err}")))?;

        Ok(TcpTransport::from_stream(stream))
    }

    pub(crate) fn from_stream(stream: TcpStream) -> TcpTransport {
        let address = match stream.peer_addr() {
            Ok(addr) => Value::String(addr.to_string()),
            Err(_) => Value::Null,
        };
        let metadata = json!({ "address": address });
        TcpTransport { stream, metadata }
    }
}

impl Transport for TcpTransport {
    type Reader = TcpReader;
    type Writer = TcpWriter;

    fn metadata(&self) -> Value {
        self.metadata.clone()
    }

    fn split(self) -> (TcpReader, TcpWriter) {
        let (reader, writer) = self.stream.into_split();

        (
            TcpReader {
                reader: BufReader::new(reader),
            },
            TcpWriter { writer },
        )
    }
}

#[async_trait::async_trait]
impl MessageReader for TcpReader {
    async fn read_message(&mut self) -> Result<Vec<u8>> {
        read_line_message(&mut self.reader).await
    }
}

#[async_trait::async_trait]
impl MessageWriter for TcpWriter {
    async fn write_message(&mut self, msg: Vec<u8>) -> Result<()> {
        write_line_message(&mut self.writer, msg).await
    }
}
