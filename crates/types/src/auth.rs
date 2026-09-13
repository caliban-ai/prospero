//! Authentication DTOs shared by prosperod and the WASM dashboard (#2).
//!
//! Serde-only so they compile to wasm32 alongside the rest of this crate.

use serde::{Deserialize, Serialize};

/// A token's permission level. Ordered: `Read < Operate < Admin`, so a route's
/// requirement is met when `principal.scope >= required`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Scope {
    /// Every GET (fleet, usage, events, SSE, metrics).
    Read,
    /// Read plus agent actions (spawn, kill, respawn, input, end-input, rm).
    Operate,
    /// Operate plus workspace add / remove / config.
    Admin,
}

impl Scope {
    /// The lowercase wire/config spelling.
    pub fn as_str(&self) -> &'static str {
        match self {
            Scope::Read => "read",
            Scope::Operate => "operate",
            Scope::Admin => "admin",
        }
    }

    /// Parse the lowercase spelling; anything else is `None`.
    pub fn parse(s: &str) -> Option<Scope> {
        match s {
            "read" => Some(Scope::Read),
            "operate" => Some(Scope::Operate),
            "admin" => Some(Scope::Admin),
            _ => None,
        }
    }
}

/// `GET /api/session` / `POST /api/session` response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "auth", rename_all = "snake_case")]
pub enum SessionInfo {
    /// No tokens configured: the dashboard skips sign-in and shows every control.
    Disabled,
    /// Authenticated with a named token.
    Token {
        /// The tokens-file name of the credential in use.
        token_name: String,
        /// Its scope.
        scope: Scope,
        /// RFC-3339 cookie expiry; `None` when authenticated by bearer header.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expires_at: Option<String>,
    },
}

/// `POST /api/session` body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignInBody {
    /// The raw `pspo_…` token pasted by the operator.
    pub token: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scopes_are_ordered() {
        assert!(Scope::Read < Scope::Operate);
        assert!(Scope::Operate < Scope::Admin);
    }

    #[test]
    fn scope_parse_round_trips_and_rejects_unknown() {
        for s in [Scope::Read, Scope::Operate, Scope::Admin] {
            assert_eq!(Scope::parse(s.as_str()), Some(s));
        }
        assert_eq!(Scope::parse("Admin"), None);
        assert_eq!(Scope::parse("write"), None);
    }

    #[test]
    fn session_info_wire_shapes() {
        assert_eq!(
            serde_json::to_value(SessionInfo::Disabled).unwrap(),
            serde_json::json!({"auth": "disabled"})
        );
        let t = SessionInfo::Token {
            token_name: "alice".into(),
            scope: Scope::Admin,
            expires_at: None,
        };
        assert_eq!(
            serde_json::to_value(&t).unwrap(),
            serde_json::json!({"auth": "token", "token_name": "alice", "scope": "admin"})
        );
        let back: SessionInfo = serde_json::from_value(serde_json::to_value(&t).unwrap()).unwrap();
        assert_eq!(back, t);
    }
}
