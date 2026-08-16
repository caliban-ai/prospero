//! Serve the embedded **deprecated** v1 dashboard (no Node toolchain — assets
//! are compiled in).
//!
//! Superseded by [`crate::dashboard_v2`], which serves `/` since #191. This one
//! moved to `/v1` and renders a deprecation notice pointing home. It is kept
//! rather than deleted so an operator who hits a v2 regression has somewhere to
//! land; removing it is a follow-up once v2 has a release of real-world use.
//!
//! Note it carries known defects v2 was built to fix — most visibly #106, where
//! a tool call whose finish frame has a blank name stays "running" forever.

use axum::http::header;
use axum::response::{Html, IntoResponse, Response};

const INDEX_HTML: &str = include_str!("../dashboard/index.html");
const APP_JS: &str = include_str!("../dashboard/app.js");

/// `GET /v1` — the deprecated dashboard page.
pub async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

/// `GET /v1/app.js` — the deprecated dashboard's script.
pub async fn app_js() -> Response {
    (
        [(
            header::CONTENT_TYPE,
            "application/javascript; charset=utf-8",
        )],
        APP_JS,
    )
        .into_response()
}
