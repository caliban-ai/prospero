//! Print the OpenAPI document to stdout (#222).
//!
//! `GET /api/openapi.json` serves the same bytes from a running prosperod, but
//! client code generation usually happens in CI, where no fleet is up. This
//! example needs neither a daemon nor a workspace:
//!
//! ```sh
//! cargo run -p prospero-api --example openapi > openapi.json
//! ```

fn main() {
    let doc = prospero_api::openapi::document();
    println!(
        "{}",
        serde_json::to_string_pretty(&doc).expect("the document is plain JSON")
    );
}
