//! Thin async client for a single caliband control socket.
//!
//! One request → one reply per connection (matching caliban's protocol): each
//! call opens the Unix socket, writes one NDJSON request frame, reads one reply
//! frame, and closes. Cheap for a local control plane and trivially debuggable.

use std::path::PathBuf;

use tokio::io::{AsyncWriteExt, BufReader};

use crate::caliband::transport::{self, BoxConn, ConnectSpec, TlsClient};
use crate::caliband::wire::{
    AgentRecord, AttachInbound, CtlReply, CtlRequest, DaemonStatus, Endpoint, SpawnSpec,
};
use crate::caliband::{read_frame, write_frame};
use crate::error::{CoreError, Result};

/// Display form of an endpoint for error messages.
fn endpoint_display(ep: &Endpoint) -> String {
    match ep {
        Endpoint::Unix { path } => path.display().to_string(),
        Endpoint::Tcp { addr } => format!("tcp://{addr}"),
    }
}

/// A client bound to one caliband control endpoint (Unix, or TCP+TLS+token).
#[derive(Clone)]
pub struct CalibandClient {
    endpoint: Endpoint,
    tls: Option<TlsClient>,
    token: Option<String>,
}

impl CalibandClient {
    /// Create a client for a local Unix control socket (credential-free default).
    pub fn new(socket_path: impl Into<PathBuf>) -> Self {
        Self {
            endpoint: Endpoint::Unix {
                path: socket_path.into(),
            },
            tls: None,
            token: None,
        }
    }

    /// Create a client that dials a TCP control endpoint over TLS + bearer token
    /// (ADR 0051). `tls`/`token` are `None` only in plaintext/no-auth test setups.
    pub fn connect_tcp(
        addr: impl Into<String>,
        tls: Option<TlsClient>,
        token: Option<String>,
    ) -> Self {
        Self {
            endpoint: Endpoint::Tcp { addr: addr.into() },
            tls,
            token,
        }
    }

    /// The control endpoint this client targets.
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// Connect to the control endpoint, mapping connection failures to the
    /// `CalibandUnreachable` error so callers can degrade the repo to
    /// `Unreachable` rather than treating it as fatal.
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

    /// Send one request and return the raw reply, surfacing `Error` replies as
    /// typed [`CoreError`]s.
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

    /// List all agents registered with this daemon.
    pub async fn list(&self) -> Result<Vec<AgentRecord>> {
        match self.request(&CtlRequest::List).await? {
            CtlReply::Listed { agents } => Ok(agents),
            other => Err(unexpected("list", other)),
        }
    }

    /// Spawn a new agent; returns `(id, per-agent endpoint)`.
    pub async fn spawn(&self, spec: SpawnSpec) -> Result<(String, Endpoint)> {
        match self.request(&CtlRequest::Spawn { spec }).await? {
            CtlReply::Spawned { id, endpoint } => Ok((id, endpoint)),
            other => Err(unexpected("spawn", other)),
        }
    }

    /// Resolve an agent's per-agent endpoint for attaching.
    pub async fn attach(&self, id: impl Into<String>) -> Result<Endpoint> {
        match self.request(&CtlRequest::Attach { id: id.into() }).await? {
            CtlReply::AttachAck { endpoint } => Ok(endpoint),
            other => Err(unexpected("attach", other)),
        }
    }

    /// Kill an agent.
    pub async fn kill(&self, id: impl Into<String>) -> Result<()> {
        match self.request(&CtlRequest::Kill { id: id.into() }).await? {
            CtlReply::Killed => Ok(()),
            other => Err(unexpected("kill", other)),
        }
    }

    /// Respawn an agent; returns the new id.
    pub async fn respawn(&self, id: impl Into<String>) -> Result<String> {
        match self.request(&CtlRequest::Respawn { id: id.into() }).await? {
            CtlReply::Respawned { id } => Ok(id),
            other => Err(unexpected("respawn", other)),
        }
    }

    /// Remove an agent from the registry.
    pub async fn rm(&self, id: impl Into<String>, force: bool) -> Result<()> {
        match self
            .request(&CtlRequest::Rm {
                id: id.into(),
                force,
            })
            .await?
        {
            CtlReply::Removed => Ok(()),
            other => Err(unexpected("rm", other)),
        }
    }

    /// Probe daemon status.
    pub async fn status(&self) -> Result<DaemonStatus> {
        match self.request(&CtlRequest::Status).await? {
            CtlReply::Status(s) => Ok(s),
            other => Err(unexpected("status", other)),
        }
    }

    /// Ask the daemon to drain and shut down.
    pub async fn shutdown(&self) -> Result<()> {
        match self.request(&CtlRequest::Shutdown).await? {
            CtlReply::ShutdownAck => Ok(()),
            other => Err(unexpected("shutdown", other)),
        }
    }

    /// Open a streaming reader over a per-agent endpoint (from [`Self::attach`]),
    /// dialing with this client's TLS/token. Lines read from this reader are
    /// caliban stream-json frames.
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

    /// Write a single inbound control frame to an interactive agent's per-agent
    /// endpoint (from [`Self::attach`]). Opens a fresh write-only connection,
    /// matching caliban's "all attach connections feed a shared inbox" model.
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
        // Flush explicitly: a TLS `BoxConn` wraps a userspace buffer, unlike the
        // old raw UnixStream, so write_all alone may not reach the kernel.
        conn.flush()
            .await
            .map_err(|source| CoreError::CalibandUnreachable {
                endpoint: endpoint_display(endpoint),
                source,
            })
    }
}

fn unexpected(op: &str, reply: CtlReply) -> CoreError {
    CoreError::Protocol(format!("unexpected reply to {op}: {reply:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::caliband::wire::AttachInbound;
    use tokio::io::{AsyncBufReadExt, BufReader};
    use tokio::net::UnixListener;

    #[tokio::test]
    async fn send_inbound_writes_one_ndjson_frame() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("a.sock");
        let listener = UnixListener::bind(&sock).unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut line = String::new();
            BufReader::new(stream).read_line(&mut line).await.unwrap();
            line
        });
        let client = CalibandClient::new(&sock);
        client
            .send_inbound(
                &Endpoint::Unix { path: sock.clone() },
                &AttachInbound::UserMessage { text: "go".into() },
            )
            .await
            .unwrap();
        assert_eq!(
            server.await.unwrap().trim_end(),
            r#"{"type":"UserMessage","text":"go"}"#
        );
    }

    fn test_spec() -> SpawnSpec {
        SpawnSpec {
            label: None,
            frontmatter_path: None,
            initial_prompt: "hi".into(),
            model: None,
            provider: None,
            tool_allowlist: None,
            isolation_worktree: false,
            inherit_hooks: true,
            interactive: false,
        }
    }

    #[tokio::test]
    async fn client_round_trips_control_requests() {
        use crate::testkit::FakeCaliband;
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("ctl.sock");
        let mut fake = FakeCaliband::start_at(&sock).await.unwrap();
        let client = CalibandClient::new(&sock);
        assert_eq!(
            client.endpoint(),
            &Endpoint::Unix {
                path: sock.clone()
            }
        );

        let (id, _endpoint) = client.spawn(test_spec()).await.unwrap();
        assert!(client.list().await.unwrap().iter().any(|a| a.id == id));
        let _ = client.attach(&id).await.unwrap();
        assert!(client.status().await.unwrap().agents >= 1);
        client.kill(&id).await.unwrap();

        let (id2, _) = client.spawn(test_spec()).await.unwrap();
        assert!(!client.respawn(&id2).await.unwrap().is_empty());

        let (id3, _) = client.spawn(test_spec()).await.unwrap();
        client.rm(&id3, true).await.unwrap();

        // Error-reply path: an unknown id maps to AgentNotFound.
        assert!(matches!(
            client.kill("nope").await.unwrap_err(),
            CoreError::AgentNotFound(_)
        ));

        client.shutdown().await.unwrap();
        let _ = &mut fake;
    }

    #[tokio::test]
    async fn connect_error_maps_to_unreachable() {
        let client = CalibandClient::new("/nonexistent/dir/ctl.sock");
        assert!(matches!(
            client.list().await.unwrap_err(),
            CoreError::CalibandUnreachable { .. }
        ));
    }
}
