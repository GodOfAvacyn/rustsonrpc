use std::process::Stdio;

use serde_json::Value;
use tokio::{
    io::{self, AsyncRead, AsyncWrite, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
};

use crate::errors::{JsonRpcError, Result};

use super::{read_line_message, write_line_message, MessageReader, MessageWriter, Transport};

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

impl Default for StdioTransport {
    fn default() -> StdioTransport {
        StdioTransport::new()
    }
}

impl StdioTransport<ChildStdout, ChildStdin> {
    pub async fn spawn(mut command: Command) -> Result<StdioTransport<ChildStdout, ChildStdin>> {
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

    fn metadata(&self) -> Value {
        Value::Null
    }

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
    async fn read_message(&mut self) -> Result<Vec<u8>> {
        read_line_message(&mut self.reader).await
    }
}

#[async_trait::async_trait]
impl<W: AsyncWrite + Unpin + Send + 'static> MessageWriter for StdioWriter<W> {
    async fn write_message(&mut self, msg: Vec<u8>) -> Result<()> {
        write_line_message(&mut self.writer, msg).await
    }
}
