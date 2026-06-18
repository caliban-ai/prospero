//! Durable history for fleet events.
//!
//! Caliban exposes only live state, so Prospero persists a normalized event log
//! to satisfy "observe = live + history". The [`Store`] trait abstracts the
//! backend; [`JsonlStore`] is the first-stab append-only implementation (a
//! sqlite-backed `Store` can drop in later without touching callers).

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use async_trait::async_trait;

use crate::error::{CoreError, Result};
use crate::event::FleetEvent;

/// Map a sqlx error from an event `append` into a [`CoreError`]. A unique
/// constraint violation on `(stream_key, seq)` means a concurrent writer
/// (another replica) took this seq, surfaced as [`CoreError::SeqConflict`] so
/// the emitter can re-seed from the durable high-water and retry instead of
/// dropping the event. Shared by the sqlite and postgres backends. (#49)
pub(crate) fn map_append_error(e: sqlx::Error) -> CoreError {
    if e.as_database_error()
        .is_some_and(|d| d.is_unique_violation())
    {
        CoreError::SeqConflict
    } else {
        CoreError::Store(format!("append: {e}"))
    }
}

/// A durable, append-only event log keyed by stream.
#[async_trait]
pub trait Store: Send + Sync {
    /// Append one event to durable storage.
    async fn append(&self, event: &FleetEvent) -> Result<()>;

    /// Replay events for one stream with `seq >= from_seq`, in `seq` order.
    async fn replay(&self, stream_key: &str, from_seq: u64) -> Result<Vec<FleetEvent>>;

    /// The highest `seq` ever persisted for `stream_key` (0 if none). Used to
    /// resume that stream's sequence counter across daemon restarts.
    async fn high_water(&self, stream_key: &str) -> Result<u64>;

    /// Whether the backend can currently accept writes. A cheap, non-destructive
    /// probe used by the readiness endpoint.
    async fn writable(&self) -> bool;

    /// Delete events with `ts < before_ts` (RFC-3339, lexically ordered).
    /// Returns the number removed. Backs age-based retention (#4).
    async fn prune(&self, before_ts: &str) -> Result<u64>;
}

/// Append-only JSON-lines store. All events go to a single `events.jsonl`;
/// replay filters by stream key. Simple and debuggable for the first stab; rotation
/// and per-agent sharding are deferred.
pub struct JsonlStore {
    path: PathBuf,
    // Serialize writes so concurrent appends don't interleave partial lines.
    write_lock: Mutex<()>,
}

impl JsonlStore {
    /// Open (creating parent dirs) an append-only store at `dir/events.jsonl`.
    pub fn open(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        Ok(Self {
            path: dir.join("events.jsonl"),
            write_lock: Mutex::new(()),
        })
    }

    /// The backing file path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    fn read_all(&self) -> Result<Vec<FleetEvent>> {
        let file = match std::fs::File::open(&self.path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        };
        let reader = BufReader::new(file);
        let mut out = Vec::new();
        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            // Tolerate a corrupt/torn trailing line rather than failing replay.
            match serde_json::from_str::<FleetEvent>(&line) {
                Ok(ev) => out.push(ev),
                Err(_) => {
                    tracing::warn!(target: "prospero_store", "skipping unparseable event line");
                    continue;
                }
            }
        }
        Ok(out)
    }
}

#[async_trait]
impl Store for JsonlStore {
    async fn append(&self, event: &FleetEvent) -> Result<()> {
        let mut line = serde_json::to_string(event)?;
        line.push('\n');
        let _guard = self
            .write_lock
            .lock()
            .map_err(|_| CoreError::Store("event store write lock poisoned".into()))?;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        file.write_all(line.as_bytes())?;
        Ok(())
    }

    async fn replay(&self, stream_key: &str, from_seq: u64) -> Result<Vec<FleetEvent>> {
        let mut events: Vec<FleetEvent> = self
            .read_all()?
            .into_iter()
            .filter(|e| e.stream_key() == stream_key && e.seq >= from_seq)
            .collect();
        events.sort_by_key(|e| e.seq);
        Ok(events)
    }

    async fn high_water(&self, stream_key: &str) -> Result<u64> {
        Ok(self
            .read_all()?
            .iter()
            .filter(|e| e.stream_key() == stream_key)
            .map(|e| e.seq)
            .max()
            .unwrap_or(0))
    }

    async fn writable(&self) -> bool {
        // Non-destructive: opening for create+append touches no existing data,
        // and exercises the same path `append` takes.
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .is_ok()
    }

    async fn prune(&self, before_ts: &str) -> Result<u64> {
        let _guard = self
            .write_lock
            .lock()
            .map_err(|_| CoreError::Store("event store write lock poisoned".into()))?;
        let all = self.read_all()?;
        let before = all.len();
        let kept: Vec<FleetEvent> = all
            .into_iter()
            .filter(|e| e.ts.as_str() >= before_ts)
            .collect();
        let removed = (before - kept.len()) as u64;
        if removed == 0 {
            return Ok(0);
        }
        let mut body = String::new();
        for e in &kept {
            body.push_str(&serde_json::to_string(e)?);
            body.push('\n');
        }
        std::fs::write(&self.path, body)?;
        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{EventKind, OutputStream};

    fn ev(seq: u64, agent: &str, chunk: &str) -> FleetEvent {
        FleetEvent {
            seq,
            ts: "t".into(),
            repo: "r".into(),
            agent_id: agent.into(),
            kind: EventKind::Output {
                stream: OutputStream::Stdout,
                chunk: chunk.into(),
            },
        }
    }

    #[tokio::test]
    async fn append_and_replay_filters_by_agent_and_seq() {
        let dir = tempfile::tempdir().unwrap();
        let store = JsonlStore::open(dir.path()).unwrap();
        store.append(&ev(1, "a", "one")).await.unwrap();
        store.append(&ev(2, "b", "two")).await.unwrap();
        store.append(&ev(3, "a", "three")).await.unwrap();

        let a_events = store.replay("a", 0).await.unwrap();
        assert_eq!(a_events.len(), 2);
        assert_eq!(a_events[0].seq, 1);
        assert_eq!(a_events[1].seq, 3);

        let from2 = store.replay("a", 3).await.unwrap();
        assert_eq!(from2.len(), 1);
        assert_eq!(from2[0].seq, 3);
    }

    #[tokio::test]
    async fn high_water_recovers_max_seq_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let store = JsonlStore::open(dir.path()).unwrap();
            store.append(&ev(5, "a", "x")).await.unwrap();
            store.append(&ev(9, "a", "y")).await.unwrap();
        }
        let reopened = JsonlStore::open(dir.path()).unwrap();
        assert_eq!(reopened.high_water("a").await.unwrap(), 9);
    }

    #[tokio::test]
    async fn high_water_is_zero_when_empty() {
        let dir = tempfile::tempdir().unwrap();
        let store = JsonlStore::open(dir.path()).unwrap();
        assert_eq!(store.high_water("a").await.unwrap(), 0);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn writable_reflects_store_permissions() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let store = JsonlStore::open(dir.path()).unwrap();
        // First probe creates the (empty) events file.
        assert!(store.writable().await, "a fresh store is writable");

        // Make the events file read-only so an append open fails.
        std::fs::set_permissions(store.path(), std::fs::Permissions::from_mode(0o444)).unwrap();
        let observed = store.writable().await;
        // Restore perms so the tempdir can be cleaned up.
        std::fs::set_permissions(store.path(), std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(
            !observed,
            "a read-only events file must report not writable"
        );
    }

    #[tokio::test]
    async fn high_water_is_scoped_per_stream() {
        let dir = tempfile::tempdir().unwrap();
        let store = JsonlStore::open(dir.path()).unwrap();
        store.append(&ev(1, "a", "one")).await.unwrap();
        store.append(&ev(1, "b", "one")).await.unwrap();
        store.append(&ev(2, "a", "two")).await.unwrap();
        assert_eq!(store.high_water("a").await.unwrap(), 2);
        assert_eq!(store.high_water("b").await.unwrap(), 1);
        assert_eq!(store.high_water("missing").await.unwrap(), 0);
    }

    #[tokio::test]
    async fn corrupt_trailing_line_is_tolerated() {
        let dir = tempfile::tempdir().unwrap();
        let store = JsonlStore::open(dir.path()).unwrap();
        store.append(&ev(1, "a", "good")).await.unwrap();
        // Simulate a torn write.
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(store.path())
            .unwrap();
        f.write_all(b"{not valid json\n").unwrap();
        drop(f);
        let events = store.replay("a", 0).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(store.high_water("a").await.unwrap(), 1);
    }

    #[tokio::test]
    async fn jsonl_store_satisfies_conformance() {
        let dir = tempfile::tempdir().unwrap();
        let store = JsonlStore::open(dir.path()).unwrap();
        crate::testkit::store_conformance(&store).await;
    }

    #[tokio::test]
    async fn jsonl_store_prunes_by_age() {
        let dir = tempfile::tempdir().unwrap();
        let store = JsonlStore::open(dir.path()).unwrap();
        crate::testkit::store_prune_conformance(&store).await;
    }
}
