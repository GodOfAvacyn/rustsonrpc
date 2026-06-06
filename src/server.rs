use std::sync::Arc;

use tokio::{
    sync::{watch, Mutex},
    task::JoinHandle,
};

use crate::{
    errors::{JsonRpcError, Result},
    listener::Listener,
    peer::Peer,
    peer_builder::Registry,
};

pub type OnConnect = Arc<dyn Fn(Arc<Peer>) + Send + Sync + 'static>;

#[derive(Clone)]
pub struct Server {
    inner: Arc<ServerInner>,
}

struct ServerInner {
    close: watch::Sender<bool>,
    accept_task: Mutex<Option<JoinHandle<Result<()>>>>,
}

impl Server {
    pub(crate) fn serve(
        listener: Box<dyn Listener>,
        registry: Registry,
        on_connect: Option<OnConnect>,
    ) -> Server {
        let (close, close_rx) = watch::channel(false);
        let accept_task = tokio::spawn(accept_loop(listener, registry, on_connect, close_rx));

        Server {
            inner: Arc::new(ServerInner {
                close,
                accept_task: Mutex::new(Some(accept_task)),
            }),
        }
    }

    pub fn close(&self) {
        let _ = self.inner.close.send(true);
    }

    pub async fn wait_closed(&self) -> Result<()> {
        let Some(accept_task) = self.inner.accept_task.lock().await.take() else {
            return Ok(());
        };

        accept_task.await.map_err(|error| {
            JsonRpcError::internal_error(format!("server accept task failed: {error}"))
        })?
    }

    pub async fn serve_forever(&self) -> Result<()> {
        self.wait_closed().await
    }
}

async fn accept_loop(
    mut listener: Box<dyn Listener>,
    registry: Registry,
    on_connect: Option<OnConnect>,
    mut close_rx: watch::Receiver<bool>,
) -> Result<()> {
    loop {
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok(transport) => {

                        let peer = Arc::new(Peer::from_registry(transport, registry.clone()));
                        let on_connect = on_connect.clone();
                        let close_rx = close_rx.clone();

                        tokio::spawn(async move {
                            if let Some(on_connect) = on_connect {
                                on_connect(peer.clone());
                            }
                            peer.start();
                            tokio::select! {
                                _ = peer.wait_closed() => {}
                                _ = await_close(close_rx) => {}
                            }
                        });
                    }

                    Err(_) => continue,
                }
            }
            changed = close_rx.changed() => {
                if changed.is_err() || *close_rx.borrow() {
                    return Ok(());
                }
            }
        }
    }
}

async fn await_close(mut close_rx: watch::Receiver<bool>) {
    if *close_rx.borrow() {
        return;
    }
    while close_rx.changed().await.is_ok() {
        if *close_rx.borrow() {
            return;
        }
    }
}
