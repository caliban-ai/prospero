//! Serve the embedded dashboard (no Node toolchain — assets are compiled in).

use axum::http::header;
use axum::response::{Html, IntoResponse, Response};

const INDEX_HTML: &str = include_str!("../dashboard/index.html");
const APP_JS: &str = include_str!("../dashboard/app.js");

/// `GET /` — the dashboard page.
pub async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

/// `GET /app.js` — the dashboard script.
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
