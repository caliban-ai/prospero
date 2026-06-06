//! Server-Sent Events: replay an agent's history from the store, then tail the
//! live broadcast bus — joined on the monotonic `seq` with no gap or dup.

use std::convert::Infallible;

use axum::extract::{Path, Query, State};
use axum::response::sse::{Event, KeepAlive, Sse};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::{Stream, StreamExt};

use crate::AppState;
use crate::dto::FromSeq;

/// `GET /api/agents/{id}/stream` — replay-then-tail SSE of `FleetEvent`s.
pub async fn agent_stream(
    State(st): State<AppState>,
    Path(id): Path<String>,
    Query(q): Query<FromSeq>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    // Subscribe BEFORE reading history so no live event is missed in the gap.
    let rx = st.manager.subscribe();
    let history = st.manager.history(&id, q.from).unwrap_or_default();
    let last_seq = history.last().map(|e| e.seq).unwrap_or(0);

    let replay = tokio_stream::iter(history).map(|ev| Ok(to_event(&ev)));

    let id_for_live = id.clone();
    let live = BroadcastStream::new(rx).filter_map(move |res| match res {
        Ok(ev) if ev.agent_id == id_for_live && ev.seq > last_seq => Some(Ok(to_event(&ev))),
        _ => None, // lagged errors and non-matching events are dropped
    });

    Sse::new(replay.chain(live)).keep_alive(KeepAlive::default())
}

fn to_event(ev: &prospero_core::FleetEvent) -> Event {
    // json_data only fails if serialization fails, which FleetEvent never does.
    Event::default()
        .json_data(ev)
        .unwrap_or_else(|_| Event::default().data("{}"))
}
