//! Stateless dashboard session cookies (#2).
//!
//! `prospero_session=v1.<name>.<expires>.<mac>`; the MAC binds the token name,
//! expiry and the first 16 bytes of the token's *current* hash, so removing or
//! rotating a token invalidates its sessions on every replica with no store.

use axum::http::{HeaderMap, HeaderValue, header};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use hmac::{Hmac, Mac};
use prospero_core::auth::{TokenEntry, TokenSet, constant_time_eq};
use sha2::Sha256;

use super::SessionKey;

/// Cookie name.
pub const COOKIE_NAME: &str = "prospero_session";
/// Fixed session lifetime (12 h).
pub const SESSION_TTL_SECS: i64 = 12 * 60 * 60;

fn mac(key: &SessionKey, name: &str, expires_unix: i64, hash: &[u8; 32]) -> String {
    let mut m =
        Hmac::<Sha256>::new_from_slice(key.as_bytes()).expect("HMAC accepts any key length");
    m.update(format!("v1\n{name}\n{expires_unix}\n").as_bytes());
    m.update(&hash[..16]);
    URL_SAFE_NO_PAD.encode(m.finalize().into_bytes())
}

/// The cookie value for `entry`, valid until `expires_unix`.
pub fn sign(key: &SessionKey, entry: &TokenEntry, expires_unix: i64) -> String {
    format!(
        "v1.{}.{expires_unix}.{}",
        entry.name,
        mac(key, &entry.name, expires_unix, &entry.hash)
    )
}

/// Verify a cookie value against the current tokens. Token names cannot contain
/// `.` and base64url has no `.`, so the value splits unambiguously.
pub fn verify<'a>(
    key: &SessionKey,
    tokens: &'a TokenSet,
    value: &str,
    now_unix: i64,
) -> Option<(&'a TokenEntry, i64)> {
    let mut parts = value.splitn(4, '.');
    let (version, name, expires, given) =
        (parts.next()?, parts.next()?, parts.next()?, parts.next()?);
    if version != "v1" {
        return None;
    }
    let expires: i64 = expires.parse().ok()?;
    if expires <= now_unix {
        return None;
    }
    let entry = tokens.by_name(name)?;
    let expected = mac(key, name, expires, &entry.hash);
    constant_time_eq(expected.as_bytes(), given.as_bytes()).then_some((entry, expires))
}

/// The `prospero_session` value from the request's `Cookie` headers.
pub fn cookie_value(headers: &HeaderMap) -> Option<&str> {
    headers
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(';'))
        .find_map(|pair| pair.trim().strip_prefix("prospero_session="))
}

/// Same-origin test for cookie-authenticated mutations.
pub fn same_origin(headers: &HeaderMap) -> bool {
    let get = |name: &str| headers.get(name).and_then(|v| v.to_str().ok());
    if let Some(site) = get("sec-fetch-site") {
        return site == "same-origin";
    }
    match (get("origin"), get("host")) {
        (Some(origin), Some(host)) => origin.split_once("://").map(|(_, h)| h) == Some(host),
        _ => false,
    }
}

/// Whether a TLS-terminating proxy reported HTTPS.
pub fn is_https(headers: &HeaderMap) -> bool {
    headers
        .get("x-forwarded-proto")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|p| p.eq_ignore_ascii_case("https"))
}

/// A `Set-Cookie` value.
pub fn set_cookie(value: &str, max_age_secs: i64, secure: bool) -> HeaderValue {
    let mut s =
        format!("{COOKIE_NAME}={value}; HttpOnly; SameSite=Strict; Path=/; Max-Age={max_age_secs}");
    if secure {
        s.push_str("; Secure");
    }
    HeaderValue::from_str(&s).expect("cookie attributes are ASCII")
}

/// A `Set-Cookie` value that deletes the session.
pub fn clear_cookie(secure: bool) -> HeaderValue {
    set_cookie("", 0, secure)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;
    use prospero_core::Scope;
    use prospero_core::auth::{generate_token, tokens_file_line};

    fn set_with(name: &str, token: &str) -> TokenSet {
        TokenSet::parse(&tokens_file_line(name, Scope::Operate, token)).unwrap()
    }

    fn key() -> SessionKey {
        SessionKey::from_bytes(vec![42; 32]).unwrap()
    }

    #[test]
    fn signed_cookie_verifies_until_expiry() {
        let tokens = set_with("ops", &generate_token());
        let entry = tokens.by_name("ops").unwrap();
        let value = sign(&key(), entry, 1_000);
        let (e, exp) = verify(&key(), &tokens, &value, 999).unwrap();
        assert_eq!((e.name.as_str(), exp), ("ops", 1_000));
        assert!(
            verify(&key(), &tokens, &value, 1_000).is_none(),
            "expired at the boundary"
        );
    }

    #[test]
    fn tampering_wrong_key_and_bad_version_are_rejected() {
        let tokens = set_with("ops", &generate_token());
        let value = sign(&key(), tokens.by_name("ops").unwrap(), 1_000);
        let tampered = value.replacen(".1000.", ".9999.", 1);
        assert!(verify(&key(), &tokens, &tampered, 0).is_none());
        let other = SessionKey::from_bytes(vec![1; 32]).unwrap();
        assert!(verify(&other, &tokens, &value, 0).is_none());
        assert!(verify(&key(), &tokens, &value.replacen("v1.", "v2.", 1), 0).is_none());
        assert!(verify(&key(), &tokens, "garbage", 0).is_none());
    }

    #[test]
    fn removing_or_rotating_the_token_invalidates_its_cookies() {
        let tokens = set_with("ops", &generate_token());
        let value = sign(&key(), tokens.by_name("ops").unwrap(), 1_000);
        let removed = set_with("someone-else", &generate_token());
        assert!(verify(&key(), &removed, &value, 0).is_none());
        let rotated = set_with("ops", &generate_token());
        assert!(verify(&key(), &rotated, &value, 0).is_none());
    }

    #[test]
    fn cookie_value_is_found_among_other_cookies() {
        let mut h = HeaderMap::new();
        h.insert(
            header::COOKIE,
            HeaderValue::from_static("theme=dark; prospero_session=v1.ops.5.abc; x=y"),
        );
        assert_eq!(cookie_value(&h), Some("v1.ops.5.abc"));
        assert_eq!(cookie_value(&HeaderMap::new()), None);
    }

    #[test]
    fn same_origin_rules() {
        let mk = |pairs: &[(&str, &str)]| {
            let mut h = HeaderMap::new();
            for (k, v) in pairs {
                h.insert(
                    axum::http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                    HeaderValue::from_str(v).unwrap(),
                );
            }
            h
        };
        assert!(same_origin(&mk(&[("sec-fetch-site", "same-origin")])));
        assert!(!same_origin(&mk(&[("sec-fetch-site", "cross-site")])));
        assert!(!same_origin(&mk(&[("sec-fetch-site", "same-site")])));
        assert!(same_origin(&mk(&[
            ("origin", "https://prospero.example"),
            ("host", "prospero.example")
        ])));
        assert!(same_origin(&mk(&[
            ("origin", "http://127.0.0.1:7878"),
            ("host", "127.0.0.1:7878")
        ])));
        assert!(!same_origin(&mk(&[
            ("origin", "https://evil.example"),
            ("host", "prospero.example")
        ])));
        assert!(!same_origin(&HeaderMap::new()));
    }

    #[test]
    fn set_cookie_attributes() {
        let c = set_cookie("v", 60, false);
        let s = c.to_str().unwrap();
        assert!(s.starts_with("prospero_session=v;"));
        for attr in ["HttpOnly", "SameSite=Strict", "Path=/", "Max-Age=60"] {
            assert!(s.contains(attr), "{s}");
        }
        assert!(!s.contains("Secure"));
        assert!(
            set_cookie("v", 60, true)
                .to_str()
                .unwrap()
                .contains("; Secure")
        );
        assert!(clear_cookie(false).to_str().unwrap().contains("Max-Age=0"));
    }
}
