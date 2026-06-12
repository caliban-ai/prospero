//! Thin async client for a single caliband control socket.
//!
//! One request → one reply per connection (matching caliban's protocol): each
//! call opens the Unix socket, writes one NDJSON request frame, reads one reply
//! frame, and closes. Cheap for a local control plane and trivially debuggable.

use std::path::{Path, PathBuf};

use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

use crate::caliband::wire::{AgentRecord, AttachInbound, CtlReply, CtlRequest, DaemonStatus, SpawnSpec};
use crate::caliband::{read_frame, write_frame};
use crate::error::{CoreError, Result};

/// A client bound to one caliband control socket path.
#[derive(Debug, Clone)]
pub struct CalibandClient {
    socket_path: PathBuf,
}

impl CalibandClient {
    /// Create a client for the given control socket path.
    pub fn new(socket_path: impl Into<PathBuf>) -> Self {
        Self {
            socket_path: socket_path.into(),
        }
    }

    /// The control socket path this client targets.
    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    /// Connect to the control socket, mapping connection failures to the
    /// `CalibandUnreachable` error so callers can degrade the repo to
    /// `Unreachable` rather than treating it as fatal.
    async fn connect(&self) -> Result<UnixStream> {
        UnixStream::connect(&self.socket_path)
            .await
            .map_err(|source| CoreError::CalibandUnreachable {
                path: self.socket_path.display().to_string(),
                source,
            })
    }

    /// Send one request and return the raw reply, surfacing `Error` replies as
    /// typed [`CoreError`]s.
    pub async fn request(&self, req: &CtlRequest) -> Result<CtlReply> {
        let stream = self.connect().await?;
        let (read_half, mut write_half) = stream.into_split();
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

    /// Spawn a new agent; returns `(id, per-agent socket path)`.
    pub async fn spawn(&self, spec: SpawnSpec) -> Result<(String, PathBuf)> {
        match self.request(&CtlRequest::Spawn { spec }).await? {
            CtlReply::Spawned { id, socket_path } => Ok((id, socket_path)),
            other => Err(unexpected("spawn", other)),
        }
    }

    /// Resolve an agent's per-agent socket path for attaching.
    pub async fn attach(&self, id: impl Into<String>) -> Result<PathBuf> {
        match self.request(&CtlRequest::Attach { id: id.into() }).await? {
            CtlReply::AttachAck { socket_path } => Ok(socket_path),
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

    /// Open a streaming reader over a per-agent socket (from [`Self::attach`]).
    /// Lines read from this reader are caliban stream-json frames.
    pub async fn open_stream(socket_path: &Path) -> Result<BufReader<UnixStream>> {
        let stream = UnixStream::connect(socket_path).await.map_err(|source| {
            CoreError::CalibandUnreachable {
                path: socket_path.display().to_string(),
                source,
            }
        })?;
        Ok(BufReader::new(stream))
    }

    /// Write a single inbound control frame to an interactive agent's per-agent
    /// socket (path from [`Self::attach`]). Opens a fresh write-only connection,
    /// matching caliban's "all attach connections feed a shared inbox" model.
    pub async fn send_inbound(socket_path: &Path, frame: &AttachInbound) -> Result<()> {
        let mut stream = UnixStream::connect(socket_path).await.map_err(|source| {
            CoreError::CalibandUnreachable {
                path: socket_path.display().to_string(),
                source,
            }
        })?;
        let mut line = serde_json::to_vec(frame)?;
        line.push(b'\n');
        stream
            .write_all(&line)
            .await
            .map_err(|source| CoreError::CalibandUnreachable {
                path: socket_path.display().to_string(),
                source,
            })?;
        let _ = stream.flush().await;
        Ok(())
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
        CalibandClient::send_inbound(&sock, &AttachInbound::UserMessage { text: "go".into() })
            .await
            .unwrap();
        assert_eq!(server.await.unwrap().trim_end(), r#"{"type":"UserMessage","text":"go"}"#);
    }
}
