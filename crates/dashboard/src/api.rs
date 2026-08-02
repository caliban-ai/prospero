//! The API seam: one call, and the shared-DTO proof that motivates the whole
//! Rust/WASM re-platform.
//!
//! `GET /api/fleet` deserialises straight into [`FleetSnapshot`] — the exact
//! serde type `prospero-api` serialises on the way out. There is no
//! hand-written client model to drift from the server's.

use gloo_net::http::Request;
use prospero_types::FleetSnapshot;

/// Fetch the current fleet snapshot.
///
/// Errors are returned as display-ready strings: this is a UI, and an operator
/// needs to read what went wrong, not match on it.
pub async fn fetch_fleet() -> Result<FleetSnapshot, String> {
    let response = Request::get("/api/fleet")
        .send()
        .await
        .map_err(|e| format!("could not reach prosperod: {e}"))?;

    if !response.ok() {
        return Err(format!(
            "GET /api/fleet returned {} {}",
            response.status(),
            response.status_text()
        ));
    }

    response
        .json::<FleetSnapshot>()
        .await
        .map_err(|e| format!("could not parse the fleet snapshot: {e}"))
}
