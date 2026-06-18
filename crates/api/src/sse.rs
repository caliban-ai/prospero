//! Server-Sent Events: replay an agent's history from the store, then tail the
//! live broadcast bus — joined on the monotonic `seq` with no gap or dup.
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
use tokio_stream::Stream;

use crate::AppState;
use crate::dto::FromSeq;
use tail::{Frame, GapSignal, Step, Tailer};

/// `GET /api/agents/{id}/stream` — replay-then-tail SSE of `FleetEvent`s.
pub async fn agent_stream(
    State(st): State<AppState>,
    Path(id): Path<String>,
    Query(q): Query<FromSeq>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    // Subscribe BEFORE reading history so no live event is missed in the gap.
    let mut rx = st.manager.subscribe();
    let history = st.manager.history(&id, q.from).await.unwrap_or_default();

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
        let mut tailer = Tailer::new(id, last_delivered, st.manager.clone());
        loop {
            match tailer.on_recv(rx.recv().await).await {
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
