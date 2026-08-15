//! The API seam — and the shared-DTO proof that motivates the whole Rust/WASM
//! re-platform.
//!
//! Every request body and response here is a `prospero_types` type: the exact
//! definitions `prospero-api` uses (#98 for the read model, #172 for the write
//! path). There is no hand-written client model to drift from the server's.
//!
//! Errors come back as display-ready strings. This is a UI — an operator needs
//! to read what went wrong, not match on it — and the server already sends a
//! human-readable `{"error": …}` body, which [`failure`] surfaces in preference
//! to a bare status code.

use gloo_net::http::{Request, Response};
use prospero_types::{
    AddWorkspaceBody, AgentInputBody, Capabilities, FleetSnapshot, SetConfigBody, SpawnBody,
    SpawnedResponse, WorkspaceConfig, WorkspaceSummary,
};
use serde::Serialize;

/// Turn a non-2xx response into the best message available.
///
/// The API's `ApiError` renders `{"error": "…", "kind": "…"}`; that sentence is
/// far more useful to an operator than "409 Conflict", so prefer it and fall
/// back to the status line only when the body is missing or unparseable.
async fn failure(what: &str, response: Response) -> String {
    let status = response.status();
    let status_text = response.status_text();
    match response.text().await {
        Ok(body) if !body.is_empty() => match serde_json::from_str::<serde_json::Value>(&body) {
            Ok(v) => match v.get("error").and_then(|e| e.as_str()) {
                Some(msg) => format!("{what}: {msg}"),
                None => format!("{what}: {status} {status_text}"),
            },
            Err(_) => format!("{what}: {status} {status_text}"),
        },
        _ => format!("{what}: {status} {status_text}"),
    }
}

/// `GET` a JSON resource into `T`.
async fn get_json<T: serde::de::DeserializeOwned>(path: &str, what: &str) -> Result<T, String> {
    let response = Request::get(path)
        .send()
        .await
        .map_err(|e| format!("could not reach prosperod: {e}"))?;
    if !response.ok() {
        return Err(failure(what, response).await);
    }
    response
        .json::<T>()
        .await
        .map_err(|e| format!("could not parse the {what} response: {e}"))
}

/// Send a mutating request that returns no body worth reading.
async fn mutate(method: Method, path: &str, what: &str) -> Result<(), String> {
    let response = method
        .build(path)
        .send()
        .await
        .map_err(|e| format!("could not reach prosperod: {e}"))?;
    if !response.ok() {
        return Err(failure(what, response).await);
    }
    Ok(())
}

/// Send a JSON body and decode the response into `T`.
async fn post_json<B: Serialize, T: serde::de::DeserializeOwned>(
    path: &str,
    body: &B,
    what: &str,
) -> Result<T, String> {
    let response = Request::post(path)
        .json(body)
        .map_err(|e| format!("could not encode the {what} request: {e}"))?
        .send()
        .await
        .map_err(|e| format!("could not reach prosperod: {e}"))?;
    if !response.ok() {
        return Err(failure(what, response).await);
    }
    response
        .json::<T>()
        .await
        .map_err(|e| format!("could not parse the {what} response: {e}"))
}

/// Send a JSON body where the response body is not needed.
async fn post_json_no_reply<B: Serialize>(path: &str, body: &B, what: &str) -> Result<(), String> {
    let response = Request::post(path)
        .json(body)
        .map_err(|e| format!("could not encode the {what} request: {e}"))?
        .send()
        .await
        .map_err(|e| format!("could not reach prosperod: {e}"))?;
    if !response.ok() {
        return Err(failure(what, response).await);
    }
    Ok(())
}

/// The HTTP verbs this client issues without a body.
enum Method {
    Post,
    Delete,
}

impl Method {
    fn build(&self, path: &str) -> gloo_net::http::RequestBuilder {
        match self {
            Method::Post => Request::post(path),
            Method::Delete => Request::delete(path),
        }
    }
}

/// Percent-encode a path segment.
///
/// Agent ids and workspace names go into the URL path, and a workspace name is
/// operator-chosen — it can contain a space or a slash. Interpolating one raw
/// would silently address the wrong route.
pub fn encode_segment(segment: &str) -> String {
    let mut out = String::with_capacity(segment.len());
    for byte in segment.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char);
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

// --- Read -------------------------------------------------------------------

/// `GET /api/fleet` — the whole fleet snapshot.
pub async fn fetch_fleet() -> Result<FleetSnapshot, String> {
    get_json("/api/fleet", "fleet").await
}

/// `GET /api/capabilities` — what the active backend supports.
///
/// Fetched once and used to gate the admin controls, so the UI never offers an
/// operation the backend can't serve.
pub async fn fetch_capabilities() -> Result<Capabilities, String> {
    get_json("/api/capabilities", "capabilities").await
}

/// `GET /api/workspaces` — summaries carrying the k8s reconciliation status and
/// the named providers an agent can bind, neither of which `FleetSnapshot` has.
pub async fn fetch_workspaces() -> Result<Vec<WorkspaceSummary>, String> {
    get_json("/api/workspaces", "workspaces").await
}

// --- Agent control ----------------------------------------------------------

/// `POST /api/agents/{id}/kill`.
pub async fn kill_agent(id: &str) -> Result<(), String> {
    let path = format!("/api/agents/{}/kill", encode_segment(id));
    mutate(Method::Post, &path, "kill").await
}

/// `DELETE /api/agents/{id}`.
pub async fn remove_agent(id: &str) -> Result<(), String> {
    let path = format!("/api/agents/{}", encode_segment(id));
    mutate(Method::Delete, &path, "remove").await
}

/// `POST /api/agents/{id}/respawn` — returns the **new** agent's id.
pub async fn respawn_agent(id: &str) -> Result<String, String> {
    let path = format!("/api/agents/{}/respawn", encode_segment(id));
    let response = Request::post(&path)
        .send()
        .await
        .map_err(|e| format!("could not reach prosperod: {e}"))?;
    if !response.ok() {
        return Err(failure("respawn", response).await);
    }
    response
        .json::<prospero_types::RespawnedResponse>()
        .await
        .map(|r| r.agent_id)
        .map_err(|e| format!("could not parse the respawn response: {e}"))
}

/// `POST /api/agents/{id}/input` — inject a message into an interactive agent.
pub async fn send_input(id: &str, text: &str) -> Result<(), String> {
    let path = format!("/api/agents/{}/input", encode_segment(id));
    let body = AgentInputBody { text: text.into() };
    post_json_no_reply(&path, &body, "send input").await
}

/// `POST /api/agents/{id}/end-input` — close an interactive agent's input.
pub async fn end_input(id: &str) -> Result<(), String> {
    let path = format!("/api/agents/{}/end-input", encode_segment(id));
    mutate(Method::Post, &path, "end input").await
}

// --- Spawn ------------------------------------------------------------------

/// `POST /api/workspaces/{workspace}/agents` — launch an agent.
pub async fn spawn_agent(workspace: &str, body: &SpawnBody) -> Result<SpawnedResponse, String> {
    let path = format!("/api/workspaces/{}/agents", encode_segment(workspace));
    post_json(&path, body, "spawn").await
}

// --- Workspace registry -----------------------------------------------------

/// `POST /api/workspaces` — register a workspace.
pub async fn add_workspace(body: &AddWorkspaceBody) -> Result<(), String> {
    post_json_no_reply("/api/workspaces", body, "add workspace").await
}

/// `PUT /api/workspaces/{name}/config` — replace a workspace's configuration.
pub async fn set_workspace_config(name: &str, config: &WorkspaceConfig) -> Result<(), String> {
    let path = format!("/api/workspaces/{}/config", encode_segment(name));
    let body = SetConfigBody(config.clone());
    let response = Request::put(&path)
        .json(&body)
        .map_err(|e| format!("could not encode the save-config request: {e}"))?
        .send()
        .await
        .map_err(|e| format!("could not reach prosperod: {e}"))?;
    if !response.ok() {
        return Err(failure("save config", response).await);
    }
    Ok(())
}

/// `DELETE /api/workspaces/{name}` — deregister a workspace.
pub async fn remove_workspace(name: &str) -> Result<(), String> {
    let path = format!("/api/workspaces/{}", encode_segment(name));
    mutate(Method::Delete, &path, "remove workspace").await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_segments_are_percent_encoded() {
        // A workspace name is operator-chosen; a raw slash would address a
        // different route entirely.
        assert_eq!(encode_segment("my ws"), "my%20ws");
        assert_eq!(encode_segment("a/b"), "a%2Fb");
        assert_eq!(encode_segment("../etc"), "..%2Fetc");
        assert_eq!(encode_segment("q?x=1&y=2"), "q%3Fx%3D1%26y%3D2");
        assert_eq!(encode_segment("a#frag"), "a%23frag");
    }

    #[test]
    fn unreserved_characters_pass_through_unchanged() {
        let unreserved = "abcXYZ0189-_.~";
        assert_eq!(encode_segment(unreserved), unreserved);
    }

    #[test]
    fn non_ascii_is_encoded_per_utf8_byte() {
        // Must encode each UTF-8 byte, never slice the char.
        assert_eq!(encode_segment("é"), "%C3%A9");
    }
}
