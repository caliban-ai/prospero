//! Prospero Dashboard v2 — a Dioxus/WASM single-page app (#97, epic #95).
//!
//! Renders live fleet data fetched from `/api/fleet` and deserialised into
//! [`prospero_types::FleetSnapshot`] — the exact serde type `prospero-api`
//! serialises, so there is no client/server DTO drift. Control operations use
//! the same shared request/response types (#172).
//!
//! Layers: [`api`] talks HTTP, [`actions`] models the mutating operations,
//! [`view_model`] derives (host-testable, no Dioxus), [`ui`] renders.

use std::time::Duration;

use dioxus::prelude::*;
use prospero_types::{Capabilities, FleetSnapshot};

mod actions;
mod api;
mod ui;
mod view_model;

use ui::{Banner, ErrorState, Freshness, LoadingState, Modal, ModalHost, Overview, Shell, Ui};

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

    // Shared UI state, provided once so any control can raise a dialog, report
    // a failure, or ask for a refresh without prop-drilling through every
    // intermediate component.
    let ui = use_context_provider(|| Ui {
        modal: Signal::new(Modal::Closed),
        banner: Signal::new(None),
        note: Signal::new(None),
        // Assume no admin plane until /api/capabilities says otherwise, so a
        // slow or failed probe hides the registry controls rather than
        // offering operations the backend would answer with 405.
        caps: Signal::new(Capabilities {
            admin: false,
            async_workspace_ops: false,
        }),
        refresh: Signal::new(0),
        now_ms: Signal::new(now_ms()),
    });

    // Capabilities are fixed for the process lifetime — fetch once. A failure
    // is deliberately not surfaced: the conservative default already hides the
    // controls, and a banner here would be noise on a page that otherwise works.
    use_future(move || {
        let mut ui = ui;
        async move {
            if let Ok(caps) = api::fetch_capabilities().await {
                ui.caps.set(caps);
            }
        }
    });

    // Fetch on mount, then poll. A failed refresh must never blank a working
    // screen — v1 kept showing data through a hiccup and losing that would be a
    // regression — so an error after a good load is folded into `Ready`.
    use_future(move || {
        let mut ui = ui;
        async move {
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
                // Re-sample the clock each pass so agent ages advance.
                ui.now_ms.set(now_ms());
                wait_for_tick(ui.refresh).await;
            }
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
            let snapshot = *snapshot;
            rsx! {
                Shell { host: snapshot.host.clone(), freshness,
                    Banner {}
                    Overview { snapshot: snapshot.clone() }
                }
                ModalHost { snapshot }
            }
        }
    }
}

/// Sleep until the next poll, or wake early when a mutation asks for a refresh.
///
/// A control that just killed an agent should not leave the stale row on screen
/// for up to five seconds, so a bump to `refresh` short-circuits the wait.
async fn wait_for_tick(refresh: Signal<u32>) {
    // Step through the interval rather than sleeping through it in one go:
    // there is no `select!` over "a signal changed" and "a timer fired" here,
    // and a short poll is cheap next to an HTTP round-trip.
    const STEP: Duration = Duration::from_millis(150);
    let start = *refresh.peek();
    let steps = POLL_INTERVAL.as_millis() / STEP.as_millis();
    for _ in 0..steps {
        gloo_timers::future::sleep(STEP).await;
        if *refresh.peek() != start {
            return;
        }
    }
}

/// Browser wall clock in epoch milliseconds.
fn now_ms() -> f64 {
    js_sys::Date::now()
}
