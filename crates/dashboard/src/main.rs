//! Prospero Dashboard v2 — a Dioxus/WASM single-page app (#97, epic #95).
//!
//! Renders live fleet data fetched from `/api/fleet` and deserialised into
//! [`prospero_types::FleetSnapshot`] — the exact serde type `prospero-api`
//! serialises, so there is no client/server DTO drift.
//!
//! Layers: [`api`] fetches, [`view_model`] derives (host-testable, no Dioxus),
//! [`ui`] renders.

use std::time::Duration;

use dioxus::prelude::*;
use prospero_types::FleetSnapshot;

mod api;
mod ui;
mod view_model;

use ui::{ErrorState, Freshness, LoadingState, Overview, Shell};

/// How often to re-poll `/api/fleet`. Per-agent SSE arrives with the stream
/// viewer in a follow-on ticket; the overview only needs coarse freshness.
const POLL_INTERVAL: Duration = Duration::from_secs(5);

fn main() {
    // Turn wasm panics into a readable console trace instead of a bare
    // `unreachable executed`.
    console_error_panic_hook::set_once();
    dioxus::launch(App);
}

/// What the app currently knows about the fleet.
#[derive(Clone, PartialEq)]
enum Load {
    /// First fetch in flight — nothing has ever arrived.
    Loading,
    /// A snapshot is on screen. `error` is set when the most recent *refresh*
    /// failed, in which case the snapshot is the last good one.
    Ready {
        snapshot: Box<FleetSnapshot>,
        error: Option<String>,
    },
    /// The first fetch failed; there is nothing to show but the reason.
    Failed(String),
}

#[component]
fn App() -> Element {
    let mut load = use_signal(|| Load::Loading);

    // Fetch on mount, then poll. A failed refresh must never blank a working
    // screen — v1 kept showing data through a hiccup and losing that would be a
    // regression — so an error after a good load is folded into `Ready`.
    use_future(move || async move {
        loop {
            match api::fetch_fleet().await {
                Ok(snapshot) => {
                    load.set(Load::Ready {
                        snapshot: Box::new(snapshot),
                        error: None,
                    });
                }
                Err(e) => {
                    let next = match load.peek().clone() {
                        Load::Ready { snapshot, .. } => Load::Ready {
                            snapshot,
                            error: Some(e),
                        },
                        _ => Load::Failed(e),
                    };
                    load.set(next);
                }
            }
            gloo_timers::future::sleep(POLL_INTERVAL).await;
        }
    });

    let current = load.read().clone();
    match current {
        Load::Loading => rsx! {
            Shell { host: "connecting…".to_string(), freshness: Freshness::Live,
                LoadingState {}
            }
        },
        Load::Failed(message) => rsx! {
            Shell { host: "unavailable".to_string(),
                    freshness: Freshness::Stale(message.clone()),
                ErrorState {
                    message,
                    on_retry: move |_| load.set(Load::Loading),
                }
            }
        },
        Load::Ready { snapshot, error } => {
            let freshness = match error {
                None => Freshness::Live,
                Some(e) => Freshness::Stale(format!("last refresh failed: {e}")),
            };
            rsx! {
                Shell { host: snapshot.host.clone(), freshness,
                    Overview { snapshot: *snapshot }
                }
            }
        }
    }
}
