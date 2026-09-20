//! Server-Sent Events: replay an agent's history from the store, then tail the
//! live event bus — joined on the monotonic `seq` with no gap or dup.
//!
//! The stream closes right after the agent's terminal `AgentFinished` event, so
//! `prospero follow` behaves like `tail` of a finite run and the dashboard
//! shows the finished run then closes cleanly (rather than hanging forever).

mod tail;

use std::convert::Infallible;

use async_stream::stream;
use axum::extract::{Path, Query, State};
use axum::response::sse::{Event, KeepAlive, Sse};
use prospero_core::FleetEvent;
use prospero_core::event::EventKind;
use tokio_stream::{Stream, StreamExt};

use crate::AppState;
use crate::dto::{FleetFrom, FromSeq};
use tail::{Frame, GapSignal, Step, Tailer};

/// `GET /api/fleet/stream` — replay-then-tail SSE of **every** stream's events
/// (#219).
///
/// `?from=<cursor>` resumes after a cursor this endpoint issued (as the SSE
/// `id:` of each event); `?from=now` skips history and tails only what happens
/// next — the case a notifier needs so a restart does not re-announce the past
/// (caliban-ai/ariel#19). Omitted means from the beginning.
///
/// **The bus is only a doorbell here.** Every event is read from the durable
/// store, in cursor order, and the bus payload is discarded. The emitter
/// appends before it publishes, so a doorbell always finds its event, and
/// reading from the store is what makes the stream exactly-once across replicas
/// (each replica publishes its own events; all of them are in the one store)
/// and immune to the slow-consumer lag that the per-agent stream has to
/// self-heal from (#28). A missed doorbell costs latency, never an event: the
/// next one drains everything after the cursor.
pub async fn fleet_stream(
    State(st): State<AppState>,
    Query(q): Query<FleetFrom>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    // Subscribe before reading the cursor so nothing can slip between the two.
    let mut doorbell = st.bus.subscribe_all();
    let mut cursor = match q.from.as_deref() {
        Some("now") => st.store.latest_fleet_cursor().await.unwrap_or(0),
        Some(n) => n.parse().unwrap_or(0),
        None => 0,
    };

    let body = stream! {
        loop {
            // Drain everything after the cursor, in bounded batches, before
            // waiting again — a burst arrives as one doorbell, not one each.
            loop {
                let batch = st.store.replay_fleet(cursor, FLEET_BATCH).await.unwrap_or_default();
                if batch.is_empty() {
                    break;
                }
                for c in batch {
                    cursor = c.cursor;
                    yield Ok(to_cursored_event(c.cursor, &c.event));
                }
            }
            if doorbell.next().await.is_none() {
                break; // bus gone: the daemon is shutting down.
            }
        }
    };

    Sse::new(body).keep_alive(KeepAlive::default())
}

/// How many events one fleet-stream store read may return. Bounds both the
/// query and the burst written to a slow client before yielding.
const FLEET_BATCH: usize = 256;

/// One fleet event as SSE, with its cursor as the event `id:` — what a client
/// hands back as `?from=` to resume.
fn to_cursored_event(cursor: u64, ev: &FleetEvent) -> Event {
    Event::default()
        .id(cursor.to_string())
        .json_data(ev)
        .unwrap_or_else(|_| Event::default().data("{}"))
}

/// `GET /api/agents/{id}/stream` — replay-then-tail SSE of `FleetEvent`s.
pub async fn agent_stream(
    State(st): State<AppState>,
    Path(id): Path<String>,
    Query(q): Query<FromSeq>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    // Subscribe BEFORE reading history so no live event is missed in the gap:
    // InProcessBus registers its receiver here (eagerly); DistributedBus replays
    // from seq 0 on its first doorbell. Either way the live tail covers every
    // event after this point, and the `seq` dedup below drops the history overlap.
    let mut sub = st.bus.subscribe(&id);
    let history = st.store.replay(&id, q.from).await.unwrap_or_default();

    let body = stream! {
        // 1) Replay persisted history, stopping if it already contains the
        //    terminal event. Track the last seq delivered as the dedup
        //    high-water mark for the live tail. Seed it from the client's
        //    `from` floor so a later self-heal replay never re-sends events
        //    below what the client asked for (seq is monotonic per stream, so an
        //    agent can legitimately have no events at or above `from` yet).
        let mut last_delivered = q.from.saturating_sub(1);
        for ev in history {
            let terminal = is_terminal(&ev);
            last_delivered = ev.seq;
            yield Ok(to_event(&ev));
            if terminal {
                return;
            }
        }

        // 2) Tail live events, self-healing across a slow-consumer `Lagged`.
        //    The per-subscriber broadcast buffer is the lag tolerance
        //    (`FleetConfig::event_buffer`, default 1024). Exceed it and the
        //    `Tailer` emits a `gap` signal plus replays the missed events from
        //    the durable store, rather than silently skipping them.
        let mut tailer = Tailer::new(id, last_delivered, st.store.clone());
        loop {
            match tailer.on_recv(sub.next().await).await {
                Step::Emit(frames) => {
                    for f in frames {
                        yield Ok(frame_to_event(&f));
                    }
                }
                Step::EmitAndClose(frames) => {
                    for f in frames {
                        yield Ok(frame_to_event(&f));
                    }
                    break;
                }
                Step::Skip => continue,
                Step::Close => break,
            }
        }
    };

    Sse::new(body).keep_alive(KeepAlive::default())
}

fn is_terminal(ev: &FleetEvent) -> bool {
    matches!(ev.kind, EventKind::AgentFinished { .. })
}

fn to_event(ev: &FleetEvent) -> Event {
    // json_data only fails if serialization fails, which FleetEvent never does.
    Event::default()
        .json_data(ev)
        .unwrap_or_else(|_| Event::default().data("{}"))
}

fn frame_to_event(frame: &Frame) -> Event {
    match frame {
        Frame::Event(ev) => to_event(ev),
        Frame::Gap { skipped, last_seq } => Event::default()
            .event("gap")
            .json_data(GapSignal {
                skipped: *skipped,
                last_seq: *last_seq,
            })
            .unwrap_or_else(|_| Event::default().event("gap").data("{}")),
    }
}
