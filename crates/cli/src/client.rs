//! A thin blocking HTTP client for talking to `prosperod`.
//!
//! The CLI uses the same HTTP API as the dashboard — one control surface, not a
//! second protocol.

use std::io::{BufRead, BufReader};

use anyhow::{Context, Result, anyhow};

/// A client bound to a prosperod base URL (e.g. `http://127.0.0.1:7878`).
pub struct DaemonClient {
    base: String,
}

impl DaemonClient {
    /// Create a client for `base` (trailing slash trimmed).
    pub fn new(base: impl Into<String>) -> Self {
        let mut base = base.into();
        while base.ends_with('/') {
            base.pop();
        }
        Self { base }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base, path)
    }

    /// GET `path`, returning the parsed JSON body.
    pub fn get_json(&self, path: &str) -> Result<serde_json::Value> {
        let resp = ureq::get(&self.url(path)).call().map_err(map_err)?;
        resp.into_json().with_context(|| "parsing JSON response")
    }

    /// POST `path` with a JSON body, returning the parsed JSON body (or Null).
    pub fn post_json(&self, path: &str, body: serde_json::Value) -> Result<serde_json::Value> {
        let resp = ureq::post(&self.url(path))
            .send_json(body)
            .map_err(map_err)?;
        // Some endpoints reply with an empty body (201/202/204).
        let text = resp
            .into_string()
            .with_context(|| "reading response body")?;
        if text.trim().is_empty() {
            return Ok(serde_json::Value::Null);
        }
        Ok(serde_json::from_str(&text).unwrap_or(serde_json::Value::Null))
    }

    /// PUT `path` with a JSON body (response body ignored; endpoint replies 204).
    pub fn put_json(&self, path: &str, body: serde_json::Value) -> Result<()> {
        ureq::put(&self.url(path))
            .send_json(body)
            .map_err(map_err)?;
        Ok(())
    }

    /// DELETE `path`.
    pub fn delete(&self, path: &str) -> Result<()> {
        ureq::delete(&self.url(path)).call().map_err(map_err)?;
        Ok(())
    }

    /// Open an SSE stream and invoke `on_event(event_name, payload)` for each
    /// `data:` payload. `event_name` is the preceding `event:` field, or `""`
    /// for the default (unnamed) `message` event. Blocks until the stream
    /// closes.
    pub fn stream_events(
        &self,
        path: &str,
        mut on_event: impl FnMut(&str, serde_json::Value),
    ) -> Result<()> {
        let resp = ureq::get(&self.url(path)).call().map_err(map_err)?;
        let reader = BufReader::new(resp.into_reader());
        let mut event_name = String::new();
        for line in reader.lines() {
            let line = line.with_context(|| "reading SSE stream")?;
            if let Some(name) = line.strip_prefix("event:") {
                event_name = name.trim().to_string();
            } else if let Some(payload) = line.strip_prefix("data:") {
                let payload = payload.trim();
                if payload.is_empty() {
                    continue;
                }
                if let Ok(value) = serde_json::from_str::<serde_json::Value>(payload) {
                    on_event(&event_name, value);
                }
            } else if line.is_empty() {
                // Blank line ends an event; reset to the default for the next.
                event_name.clear();
            }
        }
        Ok(())
    }
}

/// Map a ureq error into a readable anyhow error, surfacing the server's JSON
/// error body when present.
fn map_err(err: ureq::Error) -> anyhow::Error {
    match err {
        ureq::Error::Status(code, resp) => {
            let body = resp.into_string().unwrap_or_default();
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&body)
                && let Some(msg) = v.get("error").and_then(|m| m.as_str())
            {
                return anyhow!("server returned {code}: {msg}");
            }
            anyhow!("server returned {code}: {body}")
        }
        ureq::Error::Transport(t) => {
            anyhow!("cannot reach prosperod: {t} (is the daemon running?)")
        }
    }
}
