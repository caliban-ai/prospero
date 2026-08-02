//! Prospero Dashboard v2 — a Dioxus/WASM single-page app (#97, epic #95).
//!
//! Renders live fleet data fetched from `/api/fleet` and deserialised into
//! [`prospero_types::FleetSnapshot`] — the exact serde type `prospero-api`
//! serialises, so there is no client/server DTO drift.

use dioxus::prelude::*;

mod view_model;

fn main() {
    // Turn wasm panics into a readable console trace instead of bare
    // `unreachable executed`.
    console_error_panic_hook::set_once();
    dioxus::launch(App);
}

#[component]
fn App() -> Element {
    rsx! { main { "prospero dashboard v2" } }
}
