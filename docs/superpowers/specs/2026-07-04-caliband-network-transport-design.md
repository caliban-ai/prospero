# Design: CalibandClient network transport (TCP+TLS+token) — prospero #71

- **Date:** 2026-07-04
- **Issue:** caliban-ai/prospero#71 · epic #274 (k8s), P1/P2 transport lift
- **Design authority:** caliban **ADR 0051** (`caliband network transport: NDJSON over TCP+TLS with a bearer token`), caliban #280 (server side, done)
- **Reference impl to port:** `caliban/crates/caliban-supervisor/src/transport.rs` (client half), `.../src/proto.rs` (`Endpoint` proto shape)

## Problem

Prospero's `CalibandClient` (`crates/core/src/caliband/client.rs`) connects to
caliband only over `UnixStream` — the control socket and each per-agent
attach/stream socket. Caliband already ships a remote transport (NDJSON over
**TCP + rustls TLS + bearer token**, ADR 0051), but prospero cannot consume it,
so the control plane stays pinned to the same host as every caliband it drives.
Remote / at-scale (k8s) operation is blocked on the client side.

**Forced sub-problem — wire skew.** Caliban #280 generalized its proto socket
fields from `socket_path: PathBuf` to `endpoint: Endpoint` (serde-tagged
`{"scheme":"unix","path":…}` / `{"scheme":"tcp","addr":…}`) on `Spawned`,
`AttachAck`, `AgentRecord`, and `DaemonStatus`. Prospero's `wire.rs` still uses
`socket_path: PathBuf`, so prospero is **already out of sync with current
caliban** and cannot parse those replies. Per ADR 0003 (couple to caliban via
the NDJSON wire format), this must be re-synced regardless of the transport
lift, and it is the spine of this change.

## Scope (decided)

**One PR, full client-side seam.** Wire-sync + transport module + client dial +
`AgentHandle` change + a minimal-but-real config surface + `FakeCaliband` TCP
path proving the end-to-end acceptance in the conformance suites.

**Out of scope (deferred):** heavy operator-facing production discovery
plumbing — how prospero *resolves* a caliband's TCP endpoint in a live cluster
(env/Secret/Sandbox-DNS wiring). That overlaps prospero #64 (K8sFleet session
plane) and #72 (workspace-scoped discovery) and is tracked there. This PR
delivers the dial + a constructor for a TCP endpoint; it does not add the
cluster-discovery source.

## Architecture

Six units, each independently testable:

### 1. `Endpoint` — wire vocabulary (`caliband/wire.rs`)

Mirror caliban's type **byte-for-byte** on the wire:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "scheme", rename_all = "snake_case")]
pub enum Endpoint {
    Unix { path: PathBuf },
    Tcp { addr: String },
}
```

Replace `socket_path: PathBuf` → `endpoint: Endpoint` on `CtlReply::Spawned`,
`CtlReply::AttachAck`, `AgentRecord`, `DaemonStatus`. Add a
`unix_socket_path(&self) -> Option<&Path>` helper mirroring caliban's, for the
Unix-only call sites that still want a path.

**Depends on:** nothing. **Consumers:** everything below + all snapshot/store code.

### 2. `transport` module (`caliband/transport.rs`) — client half

Port from caliban's `transport.rs`, client-only:

- `Conn` trait alias + `BoxConn = Box<dyn Conn>` (duplex byte stream).
- `TlsClient { connector, server_name }` + `tls_client_from_pem(ca_pem, server_name)`.
- `ConnectSpec { endpoint: Endpoint, tls: Option<TlsClient>, token: Option<String> }`.
- `async fn connect(spec) -> io::Result<BoxConn>`: Unix → `UnixStream`; Tcp →
  `TcpStream`, optional rustls handshake, then optional `{"bearer":"…"}\n`
  token preamble (sent after handshake so it rides encrypted).
- `ensure_crypto_provider()` (install `ring` once), matching ADR 0051.

**Depends on:** `Endpoint`, `tokio-rustls`. **Consumers:** `client.rs`, `discovery.rs`.

### 3. `CalibandClient` (`caliband/client.rs`)

Field change: `socket_path: PathBuf` → `endpoint: Endpoint` plus optional
`tls: Option<TlsClient>` and `token: Option<String>`. Constructors:

- `new(path)` — Unix, credential-free (back-compat; keeps existing call sites
  compiling via `impl Into<PathBuf>` → `Endpoint::Unix`).
- `connect_tcp(addr, tls, token)` — network dial.

`connect()`, `open_stream()`, `send_inbound()` route through
`transport::connect` and return/accept `BoxConn`. `read_frame`/`write_frame`
(already generic over `AsyncBufReadExt`/`AsyncWriteExt`) ride the `BoxConn`
unchanged. The `CalibandUnreachable { path, source }` error field becomes a
display of the `Endpoint` (rename to `endpoint: String`) so a TCP failure is
legible; `source: io::Error` unchanged.

`open_stream`/`send_inbound` currently take `&Path`; they take an `&Endpoint`
(+ the client's tls/token) instead — the per-agent endpoint now comes from
`AttachAck`/`Spawned` as an `Endpoint`.

### 4. `AgentHandle` (`model.rs`)

`socket: PathBuf` → `endpoint: Endpoint` (the #63 carry-over: make the handle
transport-agnostic). `LocalFleet::ensure_agent` fills it from the `Endpoint`
that `spawn` now returns.

### 5. Config seam (`discovery.rs`, `FleetProvider`)

`resolve_socket`/`ensure_caliband` keep returning a Unix `CalibandClient` as the
default (unchanged behavior). Add a thin constructor path so a caller holding a
`(addr, TlsClient, token)` can build a TCP `CalibandClient`. No env/Secret
source added here (deferred). `FleetConfig` gains an optional
`caliband_endpoint: Option<Endpoint>` + optional TLS/token that, when set,
makes the manager dial TCP instead of resolving a Unix socket — the minimal real
knob that proves the seam is threaded, not a full operator config.

### 6. Tests — `FakeCaliband` + conformance

- `FakeCaliband` gains a `start_tcp_tls(addr, cert, key, token)` constructor: a
  `TcpListener` + rustls acceptor + token check (the server half of
  `transport.rs`, test-scoped). Existing `start_at(path)` (Unix) unchanged.
- Fleet/store conformance suites (`testkit::fleet_provider_conformance`) run
  once over Unix and once over TCP+TLS+token, asserting
  list/attach/spawn/kill + live stream all work identically.

## Data flow

`FleetManager` → `CalibandClient` (Unix **or** Tcp+tls+token) →
`transport::connect` → `BoxConn` → NDJSON `read_frame`/`write_frame`. Reply
`Spawned/AttachAck` now carries an `Endpoint`; `open_stream`/`send_inbound` dial
that endpoint with the same client credentials. Unchanged: the NDJSON protocol
bytes, the one-request-one-reply control model, the stream-json normalizer.

## Error handling

- Dial failure (Unix or TCP) → `CoreError::CalibandUnreachable { endpoint, source }`
  → repo degrades to `Unreachable` (existing behavior, wider address space).
- TLS handshake failure → surfaced as an `io::Error` in `CalibandUnreachable`.
- Bad/missing bearer token on a TCP dial → server closes with
  `PermissionDenied`; prospero sees it as `CalibandUnreachable`.
- Unix connections never send/expect a token or TLS (filesystem perms are the
  boundary, as today).

## Testing strategy (TDD)

1. **Wire serde parity** — `Endpoint` round-trips and matches caliban's exact
   JSON (`{"scheme":"tcp","addr":"h:1"}`, `{"scheme":"unix","path":"/x"}`);
   `Spawned`/`AttachAck`/`AgentRecord`/`DaemonStatus` parse the new shape.
   (Guard against the wire-skew regression that motivated this.)
2. **Transport round-trip** — Unix echo; TCP+TLS+token echo; bad-token
   rejection; TCP-no-token path.
3. **Client over both transports** — `client_round_trips_control_requests`
   parameterized over a Unix `FakeCaliband` and a TCP+TLS one.
4. **Conformance over TCP+TLS** — `fleet_provider_conformance` green on both.

## Consequences

- **Positive:** prospero re-synced with caliban's wire proto (fixes latent
  skew); control plane can drive a caliband on another host; the transport
  module is the exact seam gRPC (#314) later slots into. Local dev flow
  untouched (Unix, no token, no TLS).
- **Negative:** the `Endpoint` generalization ripples through every
  `socket_path` site + `AgentHandle`. We hand-own TLS/token config rather than
  inheriting gRPC batteries (accepted by ADR 0051).
- **Revisit if:** #314 (gRPC) supersedes; or prospero #2 (multi-tenant authn)
  demands per-agent mTLS over the shared daemon token.
