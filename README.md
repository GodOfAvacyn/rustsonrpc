# rustsonrpc

`rustsonrpc` is the Rust sibling of the Python `pysonrpc` experiment: one
bidirectional JSON-RPC `Peer`, async Tokio transports, named params only, typed
results, and a small service/macro API for registering local methods.

This crate is peer-first. A connected peer can call remote methods, receive
remote calls, send notifications, and handle notifications over the same
connection.

## Client Call

```rust
use rustsonrpc::prelude::*;

let peer = PeerBuilder::new()
    .connect_tcp("127.0.0.1:8080")
    .await?;

let result: i32 = peer
    .call("add", params!({ "a": 2, "b": 3 }))
    .await?;
```

Params are named only. Pass an object with `params!`, a `serde_json::Value`
object, or `()` for no params:

```rust
let sum: i32 = peer.call("add", params!({ "a": 2, "b": 3 })).await?;
let status: Value = peer.call("health", ()).await?;
```

Array params are rejected with `Invalid params`.

## Transports

All transports return a `Peer`. After that, calls and notifications feel the
same.

```rust
let peer = PeerBuilder::new()
    .connect_tcp("127.0.0.1:8080")
    .await?;

let peer = PeerBuilder::new()
    .connect_ws("ws://127.0.0.1:8080/rpc")
    .await?;

let peer = PeerBuilder::new()
    .stdio();
```

TCP and stdio use newline-delimited JSON-RPC messages. WebSocket uses one
JSON-RPC message per WebSocket message.

## Services

Services are collections of JSON-RPC methods. Use `#[rpc_service]` on an
inherent impl block and mark exported methods with `#[rpc_method]`.

```rust
use rustsonrpc::prelude::*;

struct MathService {
    // data here
}

#[rpc_service]
impl MathService {
    pub fn new() -> Self {
        Self {}
    }

    #[rpc_method("math.add")]
    pub fn add(&self, a: i32, b: i32) -> Result<i32> {
        Ok(a + b)
    }

    #[rpc_method("math.divide")]
    pub fn divide(&self, a: i32, b: i32) -> Result<i32> {
        if b == 0 {
            return Err(JsonRpcError::invalid_params());
        }
        Ok(a / b)
    }
}
```

Register services on a particular peer:

```rust
let peer = PeerBuilder::new()
    .with_service(MathService::new())
    .connect_tcp("127.0.0.1:8080")
    .await?;
```

Handlers must return `Result<T>`, so application failures can become
structured JSON-RPC errors.

## Runtime Handlers

For ad hoc methods, register a closure on the builder:

```rust
let peer = PeerBuilder::new()
    .with_method("double", |params| async {
        let value: i32 = params.get("value")?;
        Ok(serde_json::json!(value * 2))
    })
    .connect_tcp("127.0.0.1:8080")
    .await?;
```

Or add a method to an already-connected peer:

```rust
peer.add_method("echo", |params| async {
    let text: String = params.get("text")?;
    Ok(serde_json::json!(text))
}).await;
```

## Servers

Constructing a server feels very similar to constructing a peer. You add
services and methods the same way, then serve over a transport.

```rust
let server = ServerBuilder::new()
    .with_service(MathService::new())
    .with_method("double", |params| async {
        let value: i32 = params.get("value")?;
        Ok(serde_json::json!(value * 2))
    })
    .serve_tcp("127.0.0.1:8080")
    .await?;

server.serve_forever().await?;
```

Services registered on the server are owned by the server. A reference to each
of them is cloned onto every peer. To get per-peer services or methods, use an
`on_connect` hook, which receives an `Arc<Peer>` for each new connection:

```rust
let server = ServerBuilder::new()
    .with_service(MathService::new())
    .on_connect(|peer| async move {
        peer.add_service(OtherService::new()).await;
        peer.add_method("ping", |_params| async {
            Ok(serde_json::json!("pong"))
        }).await;
        Ok(())
    })
    .serve_tcp("127.0.0.1:8080")
    .await?;
```

`Server` has `serve_forever()`, `close()`, and `wait_closed()`.

## Errors

Remote JSON-RPC error responses become `JsonRpcError` values:

```rust
match peer.call::<i32>("divide", params!({ "a": 1, "b": 0 })).await {
    Ok(value) => println!("{value}"),
    Err(error) => eprintln!("{error}"),
}
```

Handlers can raise structured JSON-RPC errors:

```rust
return Err(JsonRpcError::invalid_params());
```
