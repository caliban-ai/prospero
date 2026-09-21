//! Inbound API authentication (#2, ADR-0010).
//!
//! A `route_layer` middleware resolves a [`Principal`] from the request's
//! credential, checks it against the route's [`required_access`], writes one
//! audit log line per mutation, and runs the handler inside
//! `prospero_core::actor::scope` so directly emitted events carry the token name.

use std::fmt;
use std::sync::Arc;

use axum::extract::{MatchedPath, Request, State};
use axum::http::{HeaderMap, Method, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use prospero_core::Scope;
use prospero_core::auth::TokenSet;
use rand::RngCore as _;

use crate::error::ApiError;

pub mod handlers;
pub mod session;

/// HMAC key for dashboard session cookies. Never logged.
pub struct SessionKey(Vec<u8>);

impl SessionKey {
    /// Minimum key length in bytes.
    pub const MIN_LEN: usize = 32;

    /// A process-local random key (standalone default; sessions reset on restart).
    pub fn random() -> SessionKey {
        let mut bytes = vec![0u8; Self::MIN_LEN];
        rand::thread_rng().fill_bytes(&mut bytes);
        SessionKey(bytes)
    }

    /// A key from configured bytes.
    pub fn from_bytes(bytes: Vec<u8>) -> Result<SessionKey, String> {
        if bytes.len() < Self::MIN_LEN {
            return Err(format!(
                "session key must be at least {} bytes (got {})",
                Self::MIN_LEN,
                bytes.len()
            ));
        }
        Ok(SessionKey(bytes))
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for SessionKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SessionKey(<redacted>)")
    }
}

/// Authentication configuration shared by the middleware and session handlers.
#[derive(Debug)]
pub struct AuthState {
    tokens: Option<TokenSet>,
    session_key: SessionKey,
    cookie_secure: bool,
}

impl AuthState {
    /// No tokens configured: every request is admitted (loopback-only or
    /// `--insecure-no-auth`, enforced by prosperod at startup).
    pub fn disabled() -> AuthState {
        AuthState {
            tokens: None,
            session_key: SessionKey::random(),
            cookie_secure: false,
        }
    }

    /// Tokens configured: every non-open route requires a credential.
    pub fn enabled(tokens: TokenSet, session_key: SessionKey, cookie_secure: bool) -> AuthState {
        AuthState {
            tokens: Some(tokens),
            session_key,
            cookie_secure,
        }
    }

    /// Whether tokens are configured.
    pub fn is_enabled(&self) -> bool {
        self.tokens.is_some()
    }

    pub(crate) fn tokens(&self) -> Option<&TokenSet> {
        self.tokens.as_ref()
    }

    pub(crate) fn session_key(&self) -> &SessionKey {
        &self.session_key
    }

    pub(crate) fn cookie_secure(&self) -> bool {
        self.cookie_secure
    }
}

/// How the principal authenticated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Via {
    /// `Authorization: Bearer`.
    Bearer,
    /// `prospero_session` cookie.
    Cookie,
}

/// The authenticated caller, inserted as a request extension.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Principal {
    /// Tokens-file name.
    pub token_name: String,
    /// Scope from the *current* tokens file.
    pub scope: Scope,
    /// Credential kind.
    pub via: Via,
    /// Cookie expiry (unix seconds); `None` for bearer.
    pub expires_unix: Option<i64>,
}

/// A route's requirement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Access {
    /// No credential needed.
    Open,
    /// At least this scope.
    Requires(Scope),
}

/// The scope table, keyed on axum's matched route pattern. Unlisted routes
/// fail closed to `admin`.
pub fn required_access(method: &Method, route: &str) -> Access {
    use Access::{Open, Requires};
    use Scope::{Admin, Operate, Read};
    match (method.as_str(), route) {
        // #222: the OpenAPI document describes the API's shape, not its data,
        // and a client needs it to know how to authenticate in the first place.
        // It is the same information the published guide already carries.
        (_, "/healthz" | "/readyz" | "/" | "/assets/{*path}" | "/api/session"
            | "/api/openapi.json") => Open,
        // #220: a webhook trigger authenticates with an HMAC signature over the
        // request body, checked against the one automation's own key. That
        // signature *is* the credential, and it authorizes firing exactly that
        // automation — so the route carries no scope. It is `Open` only in the
        // sense that this table does not gate it; the handler rejects any
        // request whose signature does not verify.
        ("POST", "/api/automations/{id}/trigger") => Open,
        (
            "GET" | "HEAD",
            "/api/metrics"
            | "/api/capabilities"
            | "/api/fleet"
            // #219: the fleet-wide event stream is a read of the same fleet.
            | "/api/fleet/stream"
            | "/api/usage"
            | "/api/workspaces"
            | "/api/workspaces/{workspace}/agents"
            | "/api/agents/{id}"
            | "/api/agents/{id}/events"
            | "/api/agents/{id}/stream"
            // #220: reading automations and their run history is a read of
            // fleet configuration; neither response carries a signing key.
            | "/api/automations"
            | "/api/automations/{id}/runs",
        ) => Requires(Read),
        (
            "POST",
            "/api/workspaces/{workspace}/agents"
            | "/api/agents/{id}/kill"
            | "/api/agents/{id}/respawn"
            | "/api/agents/{id}/input"
            | "/api/agents/{id}/end-input"
            // #220: firing an automation by hand spawns an agent, which is
            // exactly what `operate` covers.
            | "/api/automations/{id}/run",
        )
        | ("DELETE", "/api/agents/{id}") => Requires(Operate),
        // #218: MCP is a *driving* surface — its tools spawn, steer and kill —
        // so the whole endpoint sits at the scope those actions need rather
        // than a second, per-tool authorization model inside the handler.
        (_, "/mcp") => Requires(Operate),
        // #220: creating an automation mints a credential and can grant an
        // unattended permission posture; editing or deleting one changes what
        // the fleet does with nobody watching. All three are `admin`, the same
        // bar #238 set for choosing `Unattended` on a manual spawn.
        ("POST", "/api/automations")
        | ("DELETE", "/api/automations/{id}")
        | ("PUT", "/api/automations/{id}/enabled") => Requires(Admin),
        ("POST", "/api/workspaces")
        | ("DELETE", "/api/workspaces/{name}")
        | ("PUT", "/api/workspaces/{name}/config") => Requires(Admin),
        _ => Requires(Admin),
    }
}

/// Header by which a client says which person it is acting for (#251).
pub const ON_BEHALF_OF_HEADER: &str = "x-prospero-on-behalf-of";

/// Read and validate the claimed subject from a request's headers.
///
/// `Ok(None)` means the client asserted nothing, which is the overwhelmingly
/// common case and must behave exactly as before. `Err` is a client mistake
/// worth a `400`: silently dropping a malformed value would leave the caller
/// believing its audit trail records something it does not.
pub(crate) fn subject_header(headers: &HeaderMap) -> Result<Option<String>, String> {
    let Some(raw) = headers.get(ON_BEHALF_OF_HEADER) else {
        return Ok(None);
    };
    let text = raw
        .to_str()
        .map_err(|_| "on-behalf-of must be valid UTF-8 text".to_string())?;
    prospero_core::actor::validate_subject(text).map(|s| Some(s.to_string()))
}

/// The `Authorization: Bearer` token, if present.
pub(crate) fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
        .map(str::trim)
}

/// Resolve the caller. A present-but-invalid bearer token is a failure; it never
/// falls back to another credential.
pub(crate) fn resolve_principal(auth: &AuthState, headers: &HeaderMap) -> Option<Principal> {
    let tokens = auth.tokens()?;
    if let Some(token) = bearer_token(headers) {
        return tokens.authenticate(token).map(|e| Principal {
            token_name: e.name.clone(),
            scope: e.scope,
            via: Via::Bearer,
            expires_unix: None,
        });
    }
    let value = session::cookie_value(headers)?;
    let now = chrono::Utc::now().timestamp();
    session::verify(auth.session_key(), tokens, value, now).map(|(e, expires)| Principal {
        token_name: e.name.clone(),
        scope: e.scope,
        via: Via::Cookie,
        expires_unix: Some(expires),
    })
}

/// The auth middleware (installed with `route_layer`, so `MatchedPath` is set).
pub async fn middleware(
    State(auth): State<Arc<AuthState>>,
    mut req: Request,
    next: Next,
) -> Response {
    // Resolve the claimed subject before anything else: it is orthogonal to
    // authentication (it applies with auth disabled and on open routes), and a
    // malformed one is the client's mistake either way.
    let subject = match subject_header(req.headers()) {
        Ok(subject) => subject,
        Err(why) => {
            tracing::debug!(target: "prospero_auth", %why, "rejected: bad on-behalf-of header");
            return ApiError::BadRequest(why).into_response();
        }
    };
    if !auth.is_enabled() {
        return prospero_core::actor::scope_with_subject(None, subject, next.run(req)).await;
    }
    let route = req
        .extensions()
        .get::<MatchedPath>()
        .map(|m| m.as_str().to_owned())
        .unwrap_or_default();
    let need = match required_access(req.method(), &route) {
        Access::Open => {
            return prospero_core::actor::scope_with_subject(None, subject, next.run(req)).await;
        }
        Access::Requires(scope) => scope,
    };
    let Some(principal) = resolve_principal(&auth, req.headers()) else {
        tracing::debug!(target: "prospero_auth", %route, "rejected: missing or invalid credential");
        return ApiError::Unauthorized.into_response();
    };
    if principal.scope < need {
        tracing::debug!(
            target: "prospero_auth", %route, token = %principal.token_name,
            have = principal.scope.as_str(), need = need.as_str(), "rejected: insufficient scope"
        );
        return ApiError::Forbidden(format!("requires scope {}", need.as_str())).into_response();
    }
    if principal.via == Via::Cookie
        && !matches!(*req.method(), Method::GET | Method::HEAD)
        && !session::same_origin(req.headers())
    {
        tracing::debug!(target: "prospero_auth", %route, token = %principal.token_name, "rejected: cross-origin cookie mutation");
        return ApiError::Forbidden("cross-origin request".into()).into_response();
    }
    if !matches!(*req.method(), Method::GET | Method::HEAD) {
        // The audit line carries both identities, and names the claimed one as
        // claimed: the token is what prosperod authenticated, the subject is
        // only what that token asserted.
        tracing::info!(
            target: "prospero_audit", actor = %principal.token_name,
            asserted_on_behalf_of = subject.as_deref().unwrap_or("-"),
            method = %req.method(), %route, "api mutation"
        );
    }
    let actor = principal.token_name.clone();
    req.extensions_mut().insert(principal);
    prospero_core::actor::scope_with_subject(Some(actor), subject, next.run(req)).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_routes_fail_closed_to_admin() {
        assert_eq!(
            required_access(&Method::GET, "/api/something-new"),
            Access::Requires(Scope::Admin)
        );
        assert_eq!(required_access(&Method::GET, "/healthz"), Access::Open);
    }

    /// #220: the automation surface sits at three different scopes, and
    /// getting any of them wrong is a real privilege bug — creating an
    /// automation mints a credential, so it must not be reachable at
    /// `operate`, and the signed-webhook route must not demand a token it
    /// was never meant to need.
    #[test]
    fn automation_routes_sit_at_their_intended_scopes() {
        use Access::{Open, Requires};
        use Scope::{Admin, Operate, Read};

        assert_eq!(
            required_access(&Method::GET, "/api/automations"),
            Requires(Read)
        );
        assert_eq!(
            required_access(&Method::GET, "/api/automations/{id}/runs"),
            Requires(Read)
        );
        assert_eq!(
            required_access(&Method::POST, "/api/automations/{id}/run"),
            Requires(Operate)
        );
        assert_eq!(
            required_access(&Method::POST, "/api/automations"),
            Requires(Admin)
        );
        assert_eq!(
            required_access(&Method::DELETE, "/api/automations/{id}"),
            Requires(Admin)
        );
        assert_eq!(
            required_access(&Method::PUT, "/api/automations/{id}/enabled"),
            Requires(Admin)
        );
        // The HMAC signature is this route's credential.
        assert_eq!(
            required_access(&Method::POST, "/api/automations/{id}/trigger"),
            Open
        );
        // …and only for POST. Nothing else on that path is open.
        assert_eq!(
            required_access(&Method::GET, "/api/automations/{id}/trigger"),
            Requires(Admin)
        );
    }

    #[test]
    fn session_key_minimum_and_redaction() {
        assert!(SessionKey::from_bytes(vec![7; 31]).is_err());
        let k = SessionKey::from_bytes(vec![7; 32]).unwrap();
        assert_eq!(format!("{k:?}"), "SessionKey(<redacted>)");
    }
}
