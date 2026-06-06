//! Server-Sent Events: replay an agent's history from the store, then tail the
//! live broadcast bus — joined on the monotonic `seq` with no gap or dup.
//!
//! The stream closes right after the agent's terminal `AgentFinished` event, so
//! `prospero follow` behaves like `tail` of a finite run and the dashboard
//! shows the finished run then closes cleanly (rather than hanging forever).

use std::convert::Infallible;

use async_stream::stream;
use axum::extract::{Path, Query, State};
use axum::response::sse::{Event, KeepAlive, Sse};
use prospero_core::FleetEvent;
use prospero_core::event::EventKind;
use tokio::sync::broadcast::error::RecvError;
use tokio_stream::Stream;

use crate::AppState;
use crate::dto::FromSeq;

/// `GET /api/agents/{id}/stream` — replay-then-tail SSE of `FleetEvent`s.
pub async fn agent_stream(
    State(st): State<AppState>,
    Path(id): Path<String>,
    Query(q): Query<FromSeq>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    // Subscribe BEFORE reading history so no live event is missed in the gap.
    let mut rx = st.manager.subscribe();
    let history = st.manager.history(&id, q.from).unwrap_or_default();
    let last_seq = history.last().map(|e| e.seq).unwrap_or(0);

    let body = stream! {
        // 1) Replay persisted history, stopping if it already contains the
        //    terminal event.
        for ev in history {
            let terminal = is_terminal(&ev);
            yield Ok(to_event(&ev));
            if terminal {
                return;
            }
        }
        // 2) Tail live events for this agent until it finishes.
        loop {
            match rx.recv().await {
                Ok(ev) if ev.agent_id == id && ev.seq > last_seq => {
                    let terminal = is_terminal(&ev);
                    yield Ok(to_event(&ev));
                    if terminal {
                        break;
                    }
                }
                Ok(_) => continue,                       // other agents / replayed seqs
                Err(RecvError::Lagged(_)) => continue,   // slow consumer: skip the gap
                Err(RecvError::Closed) => break,         // daemon shutting down
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
