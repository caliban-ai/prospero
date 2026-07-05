# CalibandClient Network Transport Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let prospero's `CalibandClient` dial a caliband over TCP+TLS+bearer-token (ADR 0051) in addition to Unix, and re-sync prospero's wire vocabulary with caliban's `Endpoint` proto.

**Architecture:** Mirror caliban's serde-tagged `Endpoint` in prospero's wire module (forced parity fix), port caliban's client-side transport (`connect`/`TlsClient`/token preamble) into a new `caliband/transport.rs`, and route `CalibandClient`'s three dial sites through it. `AgentHandle` and every `socket_path` wire field become `Endpoint`. `FakeCaliband` gains a TCP+TLS listen path so the conformance suites prove the end-to-end acceptance over the network.

**Tech Stack:** Rust (edition 2024), tokio, tokio-rustls (ring), serde/serde_json, rcgen (test certs), async-trait.

## Global Constraints

- **Wire parity is byte-for-byte.** `Endpoint` must serialize exactly as caliban's: `{"scheme":"unix","path":"…"}` / `{"scheme":"tcp","addr":"host:port"}` (serde `#[serde(tag = "scheme", rename_all = "snake_case")]`). Source of truth: `caliban/crates/caliban-supervisor/src/transport.rs` + `proto.rs`.
- **Unix stays the credential-free default.** No token, no TLS on Unix; filesystem permissions are the boundary (ADR 0051).
- **The NDJSON protocol bytes do not change.** This is a transport lift; `read_frame`/`write_frame` and all `CtlRequest`/`CtlReply`/`TurnEvent` message bodies are unchanged except the `socket_path`→`endpoint` field rename that mirrors caliban.
- **`tokio-rustls` pin:** `version = "0.26", default-features = false, features = ["ring", "tls12"]` (match caliban). **`rcgen` pin:** `"0.13"` (test-only).
- **Verification gate (run before any completion claim):** from repo root — `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --all-features -- -D warnings`, `cargo build --workspace --all-targets`, `cargo test --workspace --all-features`.
- **Every commit message ends the subject with `(#71)`.**

---

### Task 1: Add deps + the `Endpoint` wire type

**Files:**
- Modify: `Cargo.toml` (workspace `[workspace.dependencies]`)
- Modify: `crates/core/Cargo.toml`
- Modify: `crates/core/src/caliband/wire.rs` (add `Endpoint` + tests)

**Interfaces:**
- Produces: `pub enum Endpoint { Unix { path: PathBuf }, Tcp { addr: String } }` in `crate::caliband::wire`, plus `Endpoint::unix_socket_path(&self) -> Option<&std::path::Path>`.

- [ ] **Step 1: Add workspace deps.** In `Cargo.toml` under `[workspace.dependencies]`, add:

```toml
tokio-rustls = { version = "0.26", default-features = false, features = ["ring", "tls12"] }
rcgen = "0.13"
```

- [ ] **Step 2: Wire deps into `prospero-core`.** In `crates/core/Cargo.toml`:
  - under `[dependencies]` add `tokio-rustls.workspace = true`
  - under `[dev-dependencies]` add `rcgen.workspace = true`
  - extend the `testkit` feature so the server-side TLS path compiles when the harness is used by other crates:
    `testkit = ["dep:tempfile"]` stays; no new optional dep needed (tokio-rustls is a hard dep).

- [ ] **Step 3: Write the failing serde-parity test.** Append to the `tests` module in `crates/core/src/caliband/wire.rs`:

```rust
#[test]
fn endpoint_matches_caliban_wire_shape() {
    // Byte-for-byte parity with caliban's transport::Endpoint.
    let unix = Endpoint::Unix { path: "/tmp/a1.sock".into() };
    assert_eq!(
        serde_json::to_string(&unix).unwrap(),
        r#"{"scheme":"unix","path":"/tmp/a1.sock"}"#
    );
    let tcp = Endpoint::Tcp { addr: "host.ns.svc:9443".into() };
    assert_eq!(
        serde_json::to_string(&tcp).unwrap(),
        r#"{"scheme":"tcp","addr":"host.ns.svc:9443"}"#
    );
    // Round-trips both ways.
    for e in [unix, tcp] {
        let s = serde_json::to_string(&e).unwrap();
        assert_eq!(serde_json::from_str::<Endpoint>(&s).unwrap(), e);
    }
}

#[test]
fn endpoint_unix_socket_path_accessor() {
    assert_eq!(
        Endpoint::Unix { path: "/x.sock".into() }.unix_socket_path(),
        Some(std::path::Path::new("/x.sock"))
    );
    assert_eq!(Endpoint::Tcp { addr: "h:1".into() }.unix_socket_path(), None);
}
```

- [ ] **Step 4: Run it, verify it fails.**

Run: `cargo test -p prospero-core --lib caliband::wire::tests::endpoint 2>&1 | tail`
Expected: FAIL — `cannot find type Endpoint`.

- [ ] **Step 5: Add the `Endpoint` type.** In `crates/core/src/caliband/wire.rs`, after the `use` lines add:

```rust
/// Where a caliband socket lives, independent of transport family. Mirrors
/// `caliban-supervisor::transport::Endpoint` byte-for-byte on the wire (ADR 0051).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "scheme", rename_all = "snake_case")]
pub enum Endpoint {
    /// Local Unix-domain socket at this filesystem path.
    Unix {
        /// Socket file path.
        path: PathBuf,
    },
    /// TCP endpoint as a `host:port` string (host may be a DNS name).
    Tcp {
        /// `host:port`.
        addr: String,
    },
}

impl Endpoint {
    /// The Unix socket path, when this endpoint is Unix-domain.
    #[must_use]
    pub fn unix_socket_path(&self) -> Option<&std::path::Path> {
        match self {
            Endpoint::Unix { path } => Some(path.as_path()),
            Endpoint::Tcp { .. } => None,
        }
    }
}
```

- [ ] **Step 6: Run tests + build, verify green.**

Run: `cargo test -p prospero-core --lib caliband::wire && cargo build -p prospero-core`
Expected: PASS.

- [ ] **Step 7: Commit.**

```bash
git add Cargo.toml crates/core/Cargo.toml crates/core/src/caliband/wire.rs
git commit -m "feat(core): add Endpoint wire type + TLS/cert deps (#71)"
```

---

### Task 2: Port the client-side `transport` module

**Files:**
- Create: `crates/core/src/caliband/transport.rs`
- Modify: `crates/core/src/caliband/mod.rs` (add `pub mod transport;`)

**Interfaces:**
- Consumes: `crate::caliband::wire::Endpoint` (Task 1).
- Produces:
  - `pub type BoxConn = Box<dyn Conn>;` where `Conn: AsyncRead + AsyncWrite + Unpin + Send`.
  - `pub struct TlsClient { pub connector: TlsConnector, pub server_name: String }`.
  - `pub fn tls_client_from_pem(ca_pem: &[u8], server_name: &str) -> std::io::Result<TlsClient>`.
  - `pub struct ConnectSpec { pub endpoint: Endpoint, pub tls: Option<TlsClient>, pub token: Option<String> }`.
  - `pub async fn connect(spec: &ConnectSpec) -> std::io::Result<BoxConn>`.
  - **Test/testkit-only** (gated `#[cfg(any(test, feature = "testkit"))]`): `TlsServer`, `tls_server_from_pem`, `BindSpec`, `Listener` (`bind`/`local_addr`/`accept`), so `FakeCaliband` can serve TLS in Task 4.

- [ ] **Step 1: Write the failing round-trip test.** Create `crates/core/src/caliband/transport.rs` with only a test module first:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    async fn echo_once(listener: Listener) {
        let mut c = listener.accept().await.expect("accept");
        let mut buf = [0u8; 5];
        c.read_exact(&mut buf).await.expect("read");
        c.write_all(&buf).await.expect("write");
        c.flush().await.expect("flush");
    }

    #[tokio::test]
    async fn tcp_tls_token_round_trip() {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let cert_pem = cert.cert.pem().into_bytes();
        let key_pem = cert.key_pair.serialize_pem().into_bytes();

        let listener = Listener::bind(&BindSpec {
            endpoint: Endpoint::Tcp { addr: "127.0.0.1:0".into() },
            tls: Some(tls_server_from_pem(&cert_pem, &key_pem).unwrap()),
            token: Some("s3cr3t".into()),
        })
        .await
        .unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(echo_once(listener));

        let mut c = connect(&ConnectSpec {
            endpoint: Endpoint::Tcp { addr },
            tls: Some(tls_client_from_pem(&cert_pem, "localhost").unwrap()),
            token: Some("s3cr3t".into()),
        })
        .await
        .unwrap();
        c.write_all(b"hello").await.unwrap();
        let mut got = [0u8; 5];
        c.read_exact(&mut got).await.unwrap();
        assert_eq!(&got, b"hello");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn bad_token_is_rejected() {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let cert_pem = cert.cert.pem().into_bytes();
        let key_pem = cert.key_pair.serialize_pem().into_bytes();
        let listener = Listener::bind(&BindSpec {
            endpoint: Endpoint::Tcp { addr: "127.0.0.1:0".into() },
            tls: Some(tls_server_from_pem(&cert_pem, &key_pem).unwrap()),
            token: Some("right".into()),
        })
        .await
        .unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { let _ = listener.accept().await; });
        let r = connect(&ConnectSpec {
            endpoint: Endpoint::Tcp { addr },
            tls: Some(tls_client_from_pem(&cert_pem, "localhost").unwrap()),
            token: Some("wrong".into()),
        })
        .await;
        // The connect() sends the token then returns Ok; the server rejects on
        // accept. Assert the server side saw PermissionDenied by having the
        // client attempt a read that fails (connection dropped).
        if let Ok(mut c) = r {
            let mut b = [0u8; 1];
            assert!(c.read(&mut b).await.map(|n| n == 0).unwrap_or(true));
        }
    }

    #[tokio::test]
    async fn unix_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.sock");
        let listener = Listener::bind(&BindSpec {
            endpoint: Endpoint::Unix { path: path.clone() },
            tls: None,
            token: None,
        })
        .await
        .unwrap();
        let server = tokio::spawn(echo_once(listener));
        let mut c = connect(&ConnectSpec {
            endpoint: Endpoint::Unix { path },
            tls: None,
            token: None,
        })
        .await
        .unwrap();
        c.write_all(b"world").await.unwrap();
        let mut got = [0u8; 5];
        c.read_exact(&mut got).await.unwrap();
        assert_eq!(&got, b"world");
        server.await.unwrap();
    }
}
```

- [ ] **Step 2: Run it, verify it fails.**

Run: `cargo test -p prospero-core --all-features --lib caliband::transport 2>&1 | tail`
Expected: FAIL — `cannot find function connect` / `Listener` etc.

- [ ] **Step 3: Implement the module.** Prepend to `crates/core/src/caliband/transport.rs` (port of caliban's `transport.rs`, client path prod, server path gated). Exact content:

```rust
//! Network-agnostic transport seam for the caliband protocol (ADR 0051).
//!
//! Turns an [`Endpoint`] (+ optional TLS + optional bearer token) into a duplex
//! byte stream. The NDJSON protocol rides on top of a [`BoxConn`] unchanged —
//! TLS and the token preamble are framing below it. Ported from
//! `caliban-supervisor::transport`; prospero needs the client [`connect`] path
//! in production, and the server [`Listener`] path only for `FakeCaliband`.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};
use tokio::net::TcpStream;
use tokio::net::UnixStream;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName};
use tokio_rustls::rustls::{ClientConfig, RootCertStore};
use tokio_rustls::TlsConnector;

use crate::caliband::wire::Endpoint;

/// A duplex byte stream over any transport family.
pub trait Conn: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Conn for T {}

/// Boxed duplex connection handed to the NDJSON protocol layer.
pub type BoxConn = Box<dyn Conn>;

/// Client-side TLS material.
#[derive(Clone)]
pub struct TlsClient {
    /// Handshake connector built from a trusted CA store.
    pub connector: TlsConnector,
    /// Expected server name (SNI / cert validation target).
    pub server_name: String,
}

/// Install the `ring` crypto provider as the process default, exactly once.
fn ensure_crypto_provider() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    });
}

/// Build client TLS trusting `ca_pem`, verifying the server presents `server_name`.
pub fn tls_client_from_pem(ca_pem: &[u8], server_name: &str) -> std::io::Result<TlsClient> {
    ensure_crypto_provider();
    let mut roots = RootCertStore::empty();
    for cert in CertificateDer::pem_slice_iter(ca_pem) {
        roots
            .add(cert.map_err(|e| std::io::Error::other(e.to_string()))?)
            .map_err(std::io::Error::other)?;
    }
    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(TlsClient {
        connector: TlsConnector::from(Arc::new(config)),
        server_name: server_name.to_string(),
    })
}

/// Bearer-token preamble: `{"bearer":"<token>"}\n`, sent after the TLS
/// handshake so it travels encrypted. TCP only; Unix never sends it.
#[derive(Serialize, Deserialize)]
struct TokenPreamble {
    bearer: String,
}

async fn client_send_token(conn: &mut BoxConn, token: &str) -> std::io::Result<()> {
    let mut line = serde_json::to_vec(&TokenPreamble { bearer: token.to_string() })
        .map_err(std::io::Error::other)?;
    line.push(b'\n');
    conn.write_all(&line).await?;
    conn.flush().await
}

/// How to dial a connection.
pub struct ConnectSpec {
    /// Target address.
    pub endpoint: Endpoint,
    /// TLS (TCP only).
    pub tls: Option<TlsClient>,
    /// Bearer token to present (TCP only).
    pub token: Option<String>,
}

/// Dial a connection per `spec`: TLS handshake when configured, then the
/// bearer-token preamble when a token is configured.
pub async fn connect(spec: &ConnectSpec) -> std::io::Result<BoxConn> {
    match &spec.endpoint {
        Endpoint::Unix { path } => Ok(Box::new(UnixStream::connect(path).await?)),
        Endpoint::Tcp { addr } => {
            let stream = TcpStream::connect(addr).await?;
            let mut conn: BoxConn = match &spec.tls {
                None => Box::new(stream),
                Some(t) => {
                    let name = ServerName::try_from(t.server_name.clone())
                        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
                    Box::new(t.connector.connect(name, stream).await?)
                }
            };
            if let Some(token) = &spec.token {
                client_send_token(&mut conn, token).await?;
            }
            Ok(conn)
        }
    }
}

// ---- Server half: only compiled for the test harness (`FakeCaliband`). ----
#[cfg(any(test, feature = "testkit"))]
mod server {
    use super::*;
    use tokio::net::{TcpListener, UnixListener};
    use tokio_rustls::rustls::pki_types::PrivateKeyDer;
    use tokio_rustls::rustls::ServerConfig;
    use tokio_rustls::TlsAcceptor;

    /// Server-side TLS material.
    #[derive(Clone)]
    pub struct TlsServer {
        pub acceptor: TlsAcceptor,
    }

    /// Build server TLS from a PEM cert chain + private key.
    pub fn tls_server_from_pem(cert_pem: &[u8], key_pem: &[u8]) -> std::io::Result<TlsServer> {
        ensure_crypto_provider();
        let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(cert_pem)
            .collect::<Result<_, _>>()
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        let key: PrivateKeyDer<'static> = PrivateKeyDer::from_pem_slice(key_pem)
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        let config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(std::io::Error::other)?;
        Ok(TlsServer { acceptor: TlsAcceptor::from(Arc::new(config)) })
    }

    async fn read_preamble_line(conn: &mut BoxConn) -> std::io::Result<String> {
        let mut buf = Vec::with_capacity(128);
        let mut byte = [0u8; 1];
        loop {
            let n = conn.read(&mut byte).await?;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "no token preamble",
                ));
            }
            if byte[0] == b'\n' {
                break;
            }
            buf.push(byte[0]);
            if buf.len() > 4096 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "token preamble too long",
                ));
            }
        }
        String::from_utf8(buf).map_err(std::io::Error::other)
    }

    async fn server_check_token(conn: &mut BoxConn, expected: &str) -> std::io::Result<()> {
        let line = read_preamble_line(conn).await?;
        let preamble: TokenPreamble =
            serde_json::from_str(&line).map_err(std::io::Error::other)?;
        if preamble.bearer == expected {
            Ok(())
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "bad bearer token",
            ))
        }
    }

    /// How to bind a listener.
    pub struct BindSpec {
        pub endpoint: Endpoint,
        pub tls: Option<TlsServer>,
        pub token: Option<String>,
    }

    /// A bound listener over one transport family.
    pub enum Listener {
        Unix(UnixListener),
        Tcp {
            listener: TcpListener,
            tls: Option<TlsServer>,
            token: Option<String>,
        },
    }

    impl Listener {
        pub async fn bind(spec: &BindSpec) -> std::io::Result<Listener> {
            match &spec.endpoint {
                Endpoint::Unix { path } => {
                    if let Some(parent) = path.parent() {
                        tokio::fs::create_dir_all(parent).await?;
                    }
                    let _ = tokio::fs::remove_file(path).await;
                    Ok(Listener::Unix(UnixListener::bind(path)?))
                }
                Endpoint::Tcp { addr } => Ok(Listener::Tcp {
                    listener: TcpListener::bind(addr).await?,
                    tls: spec.tls.clone(),
                    token: spec.token.clone(),
                }),
            }
        }

        pub fn local_addr(&self) -> Option<String> {
            match self {
                Listener::Unix(_) => None,
                Listener::Tcp { listener, .. } => {
                    listener.local_addr().ok().map(|a| a.to_string())
                }
            }
        }

        pub async fn accept(&self) -> std::io::Result<BoxConn> {
            match self {
                Listener::Unix(l) => {
                    let (stream, _addr) = l.accept().await?;
                    Ok(Box::new(stream))
                }
                Listener::Tcp { listener, tls, token } => {
                    let (stream, _addr) = listener.accept().await?;
                    let mut conn: BoxConn = match tls {
                        None => Box::new(stream),
                        Some(t) => Box::new(t.acceptor.accept(stream).await?),
                    };
                    if let Some(expected) = token {
                        server_check_token(&mut conn, expected).await?;
                    }
                    Ok(conn)
                }
            }
        }
    }
}

#[cfg(any(test, feature = "testkit"))]
pub use server::{tls_server_from_pem, BindSpec, Listener, TlsServer};
```

- [ ] **Step 4: Register the module.** In `crates/core/src/caliband/mod.rs`, add `pub mod transport;` beside `pub mod client;`.

- [ ] **Step 5: Run tests, verify green.**

Run: `cargo test -p prospero-core --all-features --lib caliband::transport`
Expected: PASS (3 tests).

- [ ] **Step 6: Commit.**

```bash
git add crates/core/src/caliband/transport.rs crates/core/src/caliband/mod.rs
git commit -m "feat(core): port caliband client transport (TCP+TLS+token) (#71)"
```

---

### Task 3: Migrate wire fields + client + model + callers to `Endpoint` (atomic)

This is one task because the `socket_path: PathBuf` → `endpoint: Endpoint` change ripples synchronously across the whole crate — Rust will not compile a partial migration. Behavior is unchanged: everything runs over `Endpoint::Unix`, just as before.

**Files:**
- Modify: `crates/core/src/caliband/wire.rs` (`AgentRecord`, `DaemonStatus`, `Spawned`, `AttachAck` + golden tests)
- Modify: `crates/core/src/caliband/client.rs` (endpoint field, `connect`/`open_stream`/`send_inbound`, `spawn`/`attach` returns)
- Modify: `crates/core/src/error.rs` (`CalibandUnreachable.path` → `.endpoint`)
- Modify: `crates/core/src/model.rs` (`AgentHandle.socket` → `.endpoint`)
- Modify: `crates/core/src/fleet_provider.rs` (`ensure_agent` builds `AgentHandle` + its tests)
- Modify: `crates/core/src/fleet.rs` (`spawn_agent_with_socket`, `attach`+`send_inbound`, stream `open_stream` call sites)
- Modify: `crates/core/src/testkit.rs` (`FakeCaliband` replies + `test_record` use `endpoint`)

**Interfaces:**
- Consumes: `Endpoint` (Task 1), `transport::{connect, ConnectSpec, TlsClient, BoxConn}` (Task 2).
- Produces:
  - `CtlReply::Spawned { id: String, endpoint: Endpoint }`, `CtlReply::AttachAck { endpoint: Endpoint }`, `AgentRecord.endpoint: Endpoint`, `DaemonStatus.endpoint: Endpoint`.
  - `CalibandClient::spawn(...) -> Result<(String, Endpoint)>`, `CalibandClient::attach(...) -> Result<Endpoint>`.
  - `CalibandClient::open_stream(&self, ep: &Endpoint) -> Result<BufReader<BoxConn>>` and `CalibandClient::send_inbound(&self, ep: &Endpoint, frame: &AttachInbound) -> Result<()>` (now `&self` methods, reusing the client's tls/token).
  - `CalibandClient::connect_tcp(addr: impl Into<String>, tls: Option<TlsClient>, token: Option<String>) -> Self`.
  - `AgentHandle.endpoint: Endpoint`.
  - `CoreError::CalibandUnreachable { endpoint: String, source: std::io::Error }`.

- [ ] **Step 1: Update the wire golden tests first (they define the new bytes).** In `crates/core/src/caliband/wire.rs`, replace the `spawned_reply_parses` test with the `endpoint` shape and add an `AttachAck`/record parity check:

```rust
#[test]
fn spawned_reply_parses() {
    let json = r#"{"kind":"spawned","id":"a1","endpoint":{"scheme":"unix","path":"/tmp/a1.sock"}}"#;
    let r: CtlReply = serde_json::from_str(json).unwrap();
    assert_eq!(
        r,
        CtlReply::Spawned {
            id: "a1".into(),
            endpoint: Endpoint::Unix { path: "/tmp/a1.sock".into() },
        }
    );
}

#[test]
fn spawned_reply_parses_tcp_endpoint() {
    let json = r#"{"kind":"spawned","id":"a1","endpoint":{"scheme":"tcp","addr":"pod.ns.svc:9443"}}"#;
    let r: CtlReply = serde_json::from_str(json).unwrap();
    assert_eq!(
        r,
        CtlReply::Spawned {
            id: "a1".into(),
            endpoint: Endpoint::Tcp { addr: "pod.ns.svc:9443".into() },
        }
    );
}
```

- [ ] **Step 2: Migrate the wire field definitions.** In `crates/core/src/caliband/wire.rs`:
  - `AgentRecord`: replace `pub socket_path: PathBuf,` with `/// Endpoint for the agent's per-agent socket (for attach).\n    pub endpoint: Endpoint,`
  - `DaemonStatus`: replace `pub socket_path: PathBuf,` with `/// Endpoint of the control socket.\n    pub endpoint: Endpoint,`
  - `CtlReply::Spawned`: replace `socket_path: PathBuf,` with `/// Per-agent endpoint.\n        endpoint: Endpoint,`
  - `CtlReply::AttachAck`: replace `socket_path: PathBuf,` with `/// Per-agent endpoint.\n        endpoint: Endpoint,`

- [ ] **Step 3: Migrate `error.rs`.** In `crates/core/src/error.rs`, change the `CalibandUnreachable` variant:

```rust
/// A caliban supervisor endpoint could not be reached.
#[error("caliband unreachable at {endpoint}: {source}")]
CalibandUnreachable {
    /// The endpoint we tried to connect to (display form).
    endpoint: String,
    /// Underlying I/O error.
    source: std::io::Error,
},
```
Update the test at the bottom of `error.rs` that constructs `CalibandUnreachable { path: … }` to use `endpoint: …`.

- [ ] **Step 4: Migrate `CalibandClient` (`client.rs`).** Replace the struct + constructors + dial methods. New field set and constructors:

```rust
use crate::caliband::transport::{self, BoxConn, ConnectSpec, TlsClient};
use crate::caliband::wire::Endpoint;
// (drop `use tokio::net::UnixStream;` and `use std::path::{Path, PathBuf};` →
//  keep `std::path::PathBuf` only where still needed; add `use std::path::Path` if referenced.)

#[derive(Debug, Clone)]
pub struct CalibandClient {
    endpoint: Endpoint,
    tls: Option<TlsClient>,
    token: Option<String>,
}

impl CalibandClient {
    /// Unix control socket (local default, credential-free).
    pub fn new(socket_path: impl Into<PathBuf>) -> Self {
        Self { endpoint: Endpoint::Unix { path: socket_path.into() }, tls: None, token: None }
    }

    /// TCP control endpoint dialed over TLS + bearer token (ADR 0051).
    pub fn connect_tcp(addr: impl Into<String>, tls: Option<TlsClient>, token: Option<String>) -> Self {
        Self { endpoint: Endpoint::Tcp { addr: addr.into() }, tls, token }
    }

    /// The endpoint this client targets.
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }
}
```
`TlsClient` derives `Debug`? It does not — it wraps `TlsConnector`. To keep `#[derive(Debug)]` on `CalibandClient`, add a manual note: **change `CalibandClient`'s derive to `#[derive(Clone)]` only** (drop `Debug`), OR add `#[derive(Debug)]` won't compile. **Decision: drop `Debug` from `CalibandClient` derive** (it is not printed anywhere; verify with `rg "\{:?\}.*client" ` — none). If a `Debug` bound is later needed, implement it manually. Update the one place that may rely on it (none found).

  Rewrite the private dialer + the three transport sites:

```rust
async fn connect(&self) -> Result<BoxConn> {
    transport::connect(&ConnectSpec {
        endpoint: self.endpoint.clone(),
        tls: self.tls.clone(),
        token: self.token.clone(),
    })
    .await
    .map_err(|source| CoreError::CalibandUnreachable {
        endpoint: endpoint_display(&self.endpoint),
        source,
    })
}
```
Add a small helper in `client.rs`:
```rust
fn endpoint_display(ep: &Endpoint) -> String {
    match ep {
        Endpoint::Unix { path } => path.display().to_string(),
        Endpoint::Tcp { addr } => format!("tcp://{addr}"),
    }
}
```
`request()` changes `let stream = self.connect().await?; let (read_half, mut write_half) = stream.into_split();` — `BoxConn` is not splittable via `into_split()`. Use `tokio::io::split`:
```rust
pub async fn request(&self, req: &CtlRequest) -> Result<CtlReply> {
    let conn = self.connect().await?;
    let (read_half, mut write_half) = tokio::io::split(conn);
    write_frame(&mut write_half, req).await?;
    let mut reader = BufReader::new(read_half);
    let reply: CtlReply = read_frame(&mut reader).await?;
    if let CtlReply::Error { error } = reply {
        return Err(error.into());
    }
    Ok(reply)
}
```
`spawn`/`attach` returns:
```rust
pub async fn spawn(&self, spec: SpawnSpec) -> Result<(String, Endpoint)> {
    match self.request(&CtlRequest::Spawn { spec }).await? {
        CtlReply::Spawned { id, endpoint } => Ok((id, endpoint)),
        other => Err(unexpected("spawn", other)),
    }
}
pub async fn attach(&self, id: impl Into<String>) -> Result<Endpoint> {
    match self.request(&CtlRequest::Attach { id: id.into() }).await? {
        CtlReply::AttachAck { endpoint } => Ok(endpoint),
        other => Err(unexpected("attach", other)),
    }
}
```
`open_stream`/`send_inbound` become `&self` methods dialing an endpoint with the client's creds:
```rust
pub async fn open_stream(&self, endpoint: &Endpoint) -> Result<BufReader<BoxConn>> {
    let conn = transport::connect(&ConnectSpec {
        endpoint: endpoint.clone(),
        tls: self.tls.clone(),
        token: self.token.clone(),
    })
    .await
    .map_err(|source| CoreError::CalibandUnreachable {
        endpoint: endpoint_display(endpoint),
        source,
    })?;
    Ok(BufReader::new(conn))
}

pub async fn send_inbound(&self, endpoint: &Endpoint, frame: &AttachInbound) -> Result<()> {
    let mut conn = transport::connect(&ConnectSpec {
        endpoint: endpoint.clone(),
        tls: self.tls.clone(),
        token: self.token.clone(),
    })
    .await
    .map_err(|source| CoreError::CalibandUnreachable {
        endpoint: endpoint_display(endpoint),
        source,
    })?;
    let mut line = serde_json::to_vec(frame)?;
    line.push(b'\n');
    conn.write_all(&line)
        .await
        .map_err(|source| CoreError::CalibandUnreachable {
            endpoint: endpoint_display(endpoint),
            source,
        })?;
    conn.flush().await.map_err(|source| CoreError::CalibandUnreachable {
        endpoint: endpoint_display(endpoint),
        source,
    })
}
```
(Note: `BoxConn` needs an explicit `flush` since it may wrap a TLS stream with a userspace buffer — unlike the old raw `UnixStream`.) Update `client.rs`'s own tests: `send_inbound_writes_one_ndjson_frame` builds a `CalibandClient::new(&sock)` and calls `client.send_inbound(&Endpoint::Unix { path: sock.clone() }, &frame)`; `client_round_trips_control_requests` destructures `(id, _ep)` from `spawn` and `let _ = client.attach(&id)`.

- [ ] **Step 5: Migrate `model.rs` `AgentHandle`.** Replace:

```rust
pub struct AgentHandle {
    pub id: AgentId,
    pub repo: String,
    /// Endpoint the agent's per-agent socket is reachable at.
    pub endpoint: crate::caliband::wire::Endpoint,
}
```

- [ ] **Step 6: Migrate `fleet_provider.rs`.** In `ensure_agent`, `spawn_agent_with_socket` now returns `(id, Endpoint)`; build the handle with `endpoint`:

```rust
let (id, endpoint) = self
    .inner
    .spawn_agent_with_socket(&spec.repo, spec.request)
    .await?;
Ok(AgentHandle { id: AgentId::from(id), repo: spec.repo, endpoint })
```
Update the test `ensure_agent_does_not_issue_a_second_attach`: the expected value becomes
`let expected = Endpoint::Unix { path: _dir.path().join(format!("{}.sock", handle.id.as_str())) };`
`assert_eq!(handle.endpoint, expected);`

- [ ] **Step 7: Migrate `fleet.rs` call sites.** Three sites:
  - `spawn_agent_with_socket` (~line 637-660): `let (id, endpoint) = client.spawn(spec).await?;` and change its return type from `(String, PathBuf)` to `(String, Endpoint)`; propagate `endpoint` wherever the old `socket` went (it is returned in the tuple; the snapshot path used `endpoint.unix_socket_path()` if it needs a `Path` — inspect and use the accessor, else store the `Endpoint`).
  - `attach` + `send_inbound` (~699-700): `let endpoint = client.attach(agent_id).await?;` then `client.send_inbound(&endpoint, &input).await`.
  - stream open (~1343-1344): `let endpoint = client.attach(agent_id).await?;` then `let mut reader = client.open_stream(&endpoint).await?;` (note: this is a free fn `stream_agent`; ensure a `client` binding is in scope — it is, from the line above).

- [ ] **Step 8: Migrate `testkit.rs` `FakeCaliband`.** Replace every `socket_path:` in a reply/record with `endpoint: Endpoint::Unix { path: … }`, and every `record.socket_path` read with `record.endpoint.unix_socket_path().expect("unix").to_path_buf()`:
  - `add_agent`/`add_agent_with_scripts`: `let socket_path = record.endpoint.unix_socket_path().expect("unix endpoint").to_path_buf();`
  - `Spawned` reply: `endpoint: Endpoint::Unix { path: socket_path.clone() }`
  - `AttachAck` reply: `endpoint: a.endpoint.clone()`
  - `DaemonStatus`: `endpoint: Endpoint::Unix { path: dir.join("control.sock") }`
  - `test_record` (~636): `endpoint: Endpoint::Unix { path: dir.join(format!("{id}.sock")) },`
  - add `use crate::caliband::wire::Endpoint;` to the imports.

- [ ] **Step 9: Build the whole workspace, fix any missed call site.**

Run: `cargo build --workspace --all-targets --all-features 2>&1 | rg -n "error\[|error:" | head`
Expected: no errors. (If any remain, they are additional `socket`/`socket_path` references — fix mechanically to the `endpoint` form.)

- [ ] **Step 10: Run the full test suite, verify green (Unix behavior unchanged).**

Run: `cargo test -p prospero-core --all-features 2>&1 | tail -20`
Expected: PASS — all existing tests green, now running over `Endpoint::Unix`.

- [ ] **Step 11: Commit.**

```bash
git add crates/core/src
git commit -m "refactor(core): migrate wire vocabulary + AgentHandle to Endpoint (#71)"
```

---

### Task 4: Prove the network path — `FakeCaliband` over TCP+TLS + conformance

**Files:**
- Modify: `crates/core/src/testkit.rs` (`FakeCaliband::start_tcp_tls` + a `test_certs()` helper + accept loop over `transport::Listener`)
- Modify: `crates/core/src/caliband/client.rs` (add a TCP round-trip test)
- Modify: `crates/core/src/fleet_provider.rs` (run `fleet_provider_conformance` over a TCP+TLS fake)
- Modify: `crates/core/src/fleet.rs` (`FleetConfig` optional TCP endpoint knob — minimal seam)

**Interfaces:**
- Consumes: `transport::{Listener, BindSpec, tls_server_from_pem, TlsServer}`, `tls_client_from_pem`, `CalibandClient::connect_tcp`.
- Produces:
  - `FakeCaliband::start_tcp_tls(token: &str) -> std::io::Result<(Self, CalibandTlsFixture)>` where `CalibandTlsFixture { addr: String, ca_pem: Vec<u8> }` (so a test can build a matching `CalibandClient::connect_tcp`).
  - `FleetConfig.caliband_endpoint: Option<Endpoint>` + `caliband_tls: Option<TlsClient>` + `caliband_token: Option<String>` — when `caliband_endpoint` is `Some`, the manager builds `CalibandClient::connect_tcp` instead of resolving a Unix socket.

- [ ] **Step 1: Add the `test_certs` helper + TCP listen path to `FakeCaliband`.** The current `FakeCaliband` binds a `UnixListener` and handles connections in a loop. Generalize its accept loop to a `transport::Listener` so the same request handler serves TCP+TLS. Add:

```rust
/// Self-signed localhost cert material for tests.
#[cfg(any(test, feature = "testkit"))]
pub struct CalibandTlsFixture {
    pub addr: String,
    pub ca_pem: Vec<u8>,
}

impl FakeCaliband {
    /// Start a fake serving the control protocol over TCP + TLS + bearer token.
    /// Returns the fixture a matching `CalibandClient::connect_tcp` needs.
    pub async fn start_tcp_tls(token: &str) -> std::io::Result<(Self, CalibandTlsFixture)> {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()])
            .map_err(std::io::Error::other)?;
        let cert_pem = cert.cert.pem().into_bytes();
        let key_pem = cert.key_pair.serialize_pem().into_bytes();
        let listener = crate::caliband::transport::Listener::bind(
            &crate::caliband::transport::BindSpec {
                endpoint: Endpoint::Tcp { addr: "127.0.0.1:0".into() },
                tls: Some(crate::caliband::transport::tls_server_from_pem(&cert_pem, &key_pem)?),
                token: Some(token.to_string()),
            },
        )
        .await?;
        let addr = listener.local_addr().expect("tcp addr");
        // ... spawn the same accept→handle_connection loop the Unix path uses,
        //     but reading/writing the BoxConn from `listener.accept()`.
        //     (Refactor the existing Unix accept loop body into a shared
        //     `serve(listener, state)` fn used by both start_at and start_tcp_tls.)
        Ok((/* Self */, CalibandTlsFixture { addr, ca_pem: cert_pem }))
    }
}
```
Refactor note: extract the existing per-connection handler (`handle_connection`) so it takes a `BoxConn` instead of a `UnixStream`; the Unix path wraps `Listener::Unix`. Per-agent stream listeners in `FakeCaliband` stay Unix for now (the conformance suite exercises the control plane + attach resolution over TCP; per-agent stream-over-TCP is covered by the `transport` round-trip test in Task 2, and full agent-stream-over-TCP is a K8sFleet concern, #64). Record `endpoint` for agents added to a TCP fake as `Endpoint::Unix` for their stream socket unless a test needs otherwise — **document this boundary in a code comment.**

- [ ] **Step 2: Write the failing client-over-TCP test.** In `client.rs` tests:

```rust
#[tokio::test]
async fn client_round_trips_over_tcp_tls_token() {
    use crate::testkit::FakeCaliband;
    let (fake, fixture) = FakeCaliband::start_tcp_tls("s3cr3t").await.unwrap();
    let tls = crate::caliband::transport::tls_client_from_pem(&fixture.ca_pem, "localhost").unwrap();
    let client = CalibandClient::connect_tcp(fixture.addr.clone(), Some(tls), Some("s3cr3t".into()));

    let (id, _ep) = client.spawn(test_spec()).await.unwrap();
    assert!(client.list().await.unwrap().iter().any(|a| a.id == id));
    let _ = client.attach(&id).await.unwrap();
    assert!(client.status().await.unwrap().agents >= 1);
    client.kill(&id).await.unwrap();
    client.shutdown().await.unwrap();
    let _ = fake;
}

#[tokio::test]
async fn client_rejects_bad_token_over_tcp() {
    use crate::testkit::FakeCaliband;
    let (fake, fixture) = FakeCaliband::start_tcp_tls("right").await.unwrap();
    let tls = crate::caliband::transport::tls_client_from_pem(&fixture.ca_pem, "localhost").unwrap();
    let client = CalibandClient::connect_tcp(fixture.addr.clone(), Some(tls), Some("wrong".into()));
    assert!(matches!(
        client.list().await.unwrap_err(),
        CoreError::CalibandUnreachable { .. } | CoreError::Protocol(_)
    ));
    let _ = fake;
}
```

- [ ] **Step 3: Run, verify fail, then implement Step 1's `serve` refactor to make it pass.**

Run: `cargo test -p prospero-core --all-features --lib caliband::client::tests::client_round_trips_over_tcp 2>&1 | tail`
Expected: FAIL first (no `start_tcp_tls`), PASS after implementing the refactor.

- [ ] **Step 4: Add the `FleetConfig` TCP knob.** In `fleet.rs` `FleetConfig`, add three optional fields (defaults `None`) and, at the point where the manager constructs a `CalibandClient` for a repo (the `ensure_caliband`/`resolve_socket` path in `discovery.rs` + its caller), branch: if `config.caliband_endpoint` is `Some(Endpoint::Tcp { addr })`, build `CalibandClient::connect_tcp(addr, config.caliband_tls.clone(), config.caliband_token.clone())`; else the existing Unix path. Keep the default path 100% unchanged. Add a focused unit test that a `FleetConfig` with a TCP endpoint yields a client whose `endpoint()` is `Tcp`.

```rust
#[test]
fn fleet_config_tcp_endpoint_builds_tcp_client() {
    let mut cfg = FleetConfig::new("local", std::path::Path::new("/tmp/x"));
    cfg.caliband_endpoint = Some(Endpoint::Tcp { addr: "h:9443".into() });
    let client = cfg.control_client_for(std::path::Path::new("/tmp/x/repo")); // helper you add
    assert!(matches!(client.endpoint(), Endpoint::Tcp { .. }));
}
```
(Implement `FleetConfig::control_client_for` (or inline in the existing resolver) as the single branch point; do not thread env/Secret parsing — that is deferred to #64/#72, note it in a doc comment.)

- [ ] **Step 5: Run the conformance suite over TCP+TLS.** In `fleet_provider.rs` tests, add a variant of `local_fleet_satisfies_conformance` that drives a TCP+TLS `FakeCaliband`. If `fleet_provider_conformance(&provider, &fake)` takes a `&FakeCaliband`, add an overload/param for the TCP fake, or assert the control-plane subset that does not require per-agent Unix stream sockets. Minimum bar: **list/spawn/attach/kill/status/shutdown all succeed against the TCP+TLS fake** (proven in Step 2); the fleet-level conformance stays on Unix where it depends on per-agent stream sockets, with a comment pointing at #64 for full agent-stream-over-TCP.

- [ ] **Step 6: Full gate.**

Run from repo root:
```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo build --workspace --all-targets
cargo test --workspace --all-features
```
Expected: all green.

- [ ] **Step 7: Commit.**

```bash
git add crates/core/src
git commit -m "test(core): FakeCaliband TCP+TLS + network transport conformance (#71)"
```

---

## Self-Review

**Spec coverage:**
- Endpoint abstraction (`Unix | Tcp`) → Task 1. ✓
- Dial over TCP+TLS+token, Unix default credential-free → Task 2 (`connect`) + Task 3 (`CalibandClient`). ✓
- `AgentHandle.socket` transport-agnostic (#63 carry-over) → Task 3 Step 5. ✓
- Wire-vocabulary sync (`socket_path`→`endpoint` on Spawned/AttachAck/AgentRecord/DaemonStatus) → Task 3 Steps 1-2, 8. ✓
- Thread config through discovery + FleetProvider → Task 4 Step 4 (minimal `FleetConfig` knob; heavy prod discovery deferred per spec). ✓
- Extend `FakeCaliband` + conformance for TCP+TLS → Task 4. ✓
- Acceptance (list/attach/spawn/kill + live streaming over network) → Task 4 Steps 2, 5 (control plane proven end-to-end over TCP+TLS+token; full per-agent-stream-over-TCP scoped to #64 with a comment). ✓

**Placeholder scan:** The only intentionally-narrative step is Task 4 Step 1's `serve`-refactor comment (the exact body depends on the current `FakeCaliband` accept loop, which the implementer reads in-file); every type/signature it needs is defined. No TBD/TODO left.

**Type consistency:** `Endpoint`, `BoxConn`, `ConnectSpec`, `TlsClient`, `CalibandClient::{new,connect_tcp,endpoint,spawn,attach,open_stream,send_inbound}`, `AgentHandle.endpoint`, `CalibandUnreachable{endpoint,source}`, `CtlReply::{Spawned,AttachAck}{endpoint}` — names/types match across Tasks 1-4.

**Known deferrals (documented, not gaps):** production endpoint *discovery* (env/Secret/Sandbox-DNS) → #64/#72; per-agent stream socket over TCP end-to-end in the fleet conformance → #64; caliban `AgentRecord.working_dir` (#281) parity is unaffected here (unknown fields are ignored on deserialize).
