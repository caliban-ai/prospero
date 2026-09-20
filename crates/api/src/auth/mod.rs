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
        (_, "/healthz" | "/readyz" | "/" | "/assets/{*path}" | "/api/session") => Open,
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
            | "/api/agents/{id}/stream",
        ) => Requires(Read),
        (
            "POST",
            "/api/workspaces/{workspace}/agents"
            | "/api/agents/{id}/kill"
            | "/api/agents/{id}/respawn"
            | "/api/agents/{id}/input"
            | "/api/agents/{id}/end-input",
        )
        | ("DELETE", "/api/agents/{id}") => Requires(Operate),
        ("POST", "/api/workspaces")
        | ("DELETE", "/api/workspaces/{name}")
        | ("PUT", "/api/workspaces/{name}/config") => Requires(Admin),
        _ => Requires(Admin),
    }
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
    if !auth.is_enabled() {
        return next.run(req).await;
    }
    let route = req
        .extensions()
        .get::<MatchedPath>()
        .map(|m| m.as_str().to_owned())
        .unwrap_or_default();
    let need = match required_access(req.method(), &route) {
        Access::Open => return next.run(req).await,
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
        tracing::info!(
            target: "prospero_audit", actor = %principal.token_name,
            method = %req.method(), %route, "api mutation"
        );
    }
    let actor = principal.token_name.clone();
    req.extensions_mut().insert(principal);
    prospero_core::actor::scope(Some(actor), next.run(req)).await
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

    #[test]
    fn session_key_minimum_and_redaction() {
        assert!(SessionKey::from_bytes(vec![7; 31]).is_err());
        let k = SessionKey::from_bytes(vec![7; 32]).unwrap();
        assert_eq!(format!("{k:?}"), "SessionKey(<redacted>)");
    }
}
