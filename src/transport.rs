use std::process::Stdio;

use futures_util::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use tokio::{
    io::{self, AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader},
    net::TcpStream,
    process::{Child, ChildStdin, ChildStdout, Command},
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::Message,
    MaybeTlsStream, WebSocketStream,
};

use crate::errors::{JsonRpcError, JsonRpcResult};

#[async_trait::async_trait]
pub trait MessageReader: Send + 'static {
    async fn read_message(&mut self) -> JsonRpcResult<Vec<u8>>;
}

#[async_trait::async_trait]
pub trait MessageWriter: Send + 'static {
    async fn write_message(&mut self, msg: Vec<u8>) -> JsonRpcResult<()>;
}

pub trait Transport: Send + 'static {
    type Reader: MessageReader;
    type Writer: MessageWriter;

    fn split(self) -> (Self::Reader, Self::Writer);
}

pub struct TcpTransport {
    stream: TcpStream,
}

pub struct TcpReader {
    reader: BufReader<tokio::net::tcp::OwnedReadHalf>,
}

pub struct TcpWriter {
    writer: tokio::net::tcp::OwnedWriteHalf,
}

impl TcpTransport {
    pub async fn connect(addr: impl tokio::net::ToSocketAddrs) -> JsonRpcResult<TcpTransport> {
        let stream = TcpStream::connect(addr)
            .await
            .map_err(|err| JsonRpcError::transport_error(format!("tcp connect failed: {err}")))?;

        Ok(TcpTransport { stream })
    }

    pub(crate) fn from_stream(stream: TcpStream) -> TcpTransport {
        TcpTransport { stream }
    }
}

impl Transport for TcpTransport {
    type Reader = TcpReader;
    type Writer = TcpWriter;

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
    async fn read_message(&mut self) -> JsonRpcResult<Vec<u8>> {
        read_line_message(&mut self.reader).await
    }
}

#[async_trait::async_trait]
impl MessageWriter for TcpWriter {
    async fn write_message(&mut self, msg: Vec<u8>) -> JsonRpcResult<()> {
        write_line_message(&mut self.writer, msg).await
    }
}

/// A line-delimited JSON transport over a reader/writer pair. Defaults to this
/// process's own stdin/stdout (the "launch me" case); `spawn` instead launches a
/// child process and talks over *its* stdio.
pub struct StdioTransport<R = io::Stdin, W = io::Stdout> {
    reader: R,
    writer: W,
    child: Option<Child>,
}

pub struct StdioReader<R> {
    reader: BufReader<R>,
}

pub struct StdioWriter<W> {
    writer: W,
    // Held so a spawned child is killed when the peer (and thus this writer) is dropped.
    _child: Option<Child>,
}

impl StdioTransport {
    pub fn new() -> StdioTransport {
        StdioTransport {
            reader: io::stdin(),
            writer: io::stdout(),
            child: None,
        }
    }
}

impl StdioTransport<ChildStdout, ChildStdin> {
    pub async fn spawn(mut command: Command) -> JsonRpcResult<StdioTransport<ChildStdout, ChildStdin>> {
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|err| JsonRpcError::transport_error(format!("failed to spawn process: {err}")))?;

        let writer = child
            .stdin
            .take()
            .ok_or_else(|| JsonRpcError::transport_error("child stdin was not captured"))?;
        let reader = child
            .stdout
            .take()
            .ok_or_else(|| JsonRpcError::transport_error("child stdout was not captured"))?;

        Ok(StdioTransport {
            reader,
            writer,
            child: Some(child),
        })
    }
}

impl<R, W> Transport for StdioTransport<R, W>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    type Reader = StdioReader<R>;
    type Writer = StdioWriter<W>;

    fn split(self) -> (StdioReader<R>, StdioWriter<W>) {
        (
            StdioReader {
                reader: BufReader::new(self.reader),
            },
            StdioWriter {
                writer: self.writer,
                _child: self.child,
            },
        )
    }
}

#[async_trait::async_trait]
impl<R: AsyncRead + Unpin + Send + 'static> MessageReader for StdioReader<R> {
    async fn read_message(&mut self) -> JsonRpcResult<Vec<u8>> {
        read_line_message(&mut self.reader).await
    }
}

#[async_trait::async_trait]
impl<W: AsyncWrite + Unpin + Send + 'static> MessageWriter for StdioWriter<W> {
    async fn write_message(&mut self, msg: Vec<u8>) -> JsonRpcResult<()> {
        write_line_message(&mut self.writer, msg).await
    }
}

pub struct WsTransport {
    stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

pub struct WsReader {
    reader: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
}

pub struct WsWriter {
    writer: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
}

impl WsTransport {
    pub async fn connect(url: &str) -> JsonRpcResult<WsTransport> {
        let (stream, _) = connect_async(url)
            .await
            .map_err(|err| JsonRpcError::transport_error(format!("websocket connect failed: {err}")))?;

        Ok(WsTransport { stream })
    }

    pub(crate) fn from_stream(
        stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    ) -> WsTransport {
        WsTransport { stream }
    }
}

impl Transport for WsTransport {
    type Reader = WsReader;
    type Writer = WsWriter;

    fn split(self) -> (WsReader, WsWriter) {
        let (writer, reader) = self.stream.split();

        (WsReader { reader }, WsWriter { writer })
    }
}

#[async_trait::async_trait]
impl MessageReader for WsReader {
    async fn read_message(&mut self) -> JsonRpcResult<Vec<u8>> {
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
    async fn write_message(&mut self, msg: Vec<u8>) -> JsonRpcResult<()> {
        self.writer
            .send(Message::Binary(msg.into()))
            .await
            .map_err(|err| JsonRpcError::transport_error(format!("websocket write failed: {err}")))
    }
}

async fn read_line_message<R>(reader: &mut BufReader<R>) -> JsonRpcResult<Vec<u8>>
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

async fn write_line_message<W>(writer: &mut W, mut msg: Vec<u8>) -> JsonRpcResult<()>
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

/// A transport whose two halves are already type-erased. A
/// [`Listener`](crate::listener::Listener) produces one of these per accepted
/// connection, so a server can build a peer from it through the same
/// `Transport` path as any concrete transport.
pub struct BoxedTransport {
    reader: Box<dyn MessageReader>,
    writer: Box<dyn MessageWriter>,
}

impl BoxedTransport {
    pub(crate) fn new(
        reader: Box<dyn MessageReader>,
        writer: Box<dyn MessageWriter>,
    ) -> BoxedTransport {
        BoxedTransport { reader, writer }
    }
}

impl Transport for BoxedTransport {
    type Reader = Box<dyn MessageReader>;
    type Writer = Box<dyn MessageWriter>;

    fn split(self) -> (Self::Reader, Self::Writer) {
        (self.reader, self.writer)
    }
}

#[async_trait::async_trait]
impl MessageReader for Box<dyn MessageReader> {
    async fn read_message(&mut self) -> JsonRpcResult<Vec<u8>> {
        (**self).read_message().await
    }
}

#[async_trait::async_trait]
impl MessageWriter for Box<dyn MessageWriter> {
    async fn write_message(&mut self, msg: Vec<u8>) -> JsonRpcResult<()> {
        (**self).write_message(msg).await
    }
}
