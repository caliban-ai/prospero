//! API token model for inbound authentication (#2, ADR-0010).
//!
//! Tokens are `pspo_` + 32 random bytes (base64url, no padding). prosperod
//! stores only SHA-256 hashes, declared one per line in a tokens file:
//! `<name> <scope> sha256:<hex>`. Kept here (not in `prospero-api`) so the CLI's
//! offline `prospero token new` shares the exact format.

use std::path::Path;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use rand::RngCore as _;
use sha2::{Digest, Sha256};

pub use prospero_types::Scope;

/// Prefix on every generated token, so leaked tokens are greppable.
pub const TOKEN_PREFIX: &str = "pspo_";

/// A fresh random token.
pub fn generate_token() -> String {
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    format!("{TOKEN_PREFIX}{}", URL_SAFE_NO_PAD.encode(bytes))
}

/// SHA-256 over the full token string.
pub fn hash_token(token: &str) -> [u8; 32] {
    Sha256::digest(token.as_bytes()).into()
}

/// `sha256:<lowercase hex>`.
pub fn format_hash(hash: &[u8; 32]) -> String {
    format!("sha256:{}", hex::encode(hash))
}

/// `[a-z0-9][a-z0-9_-]{0,62}`.
pub fn validate_token_name(name: &str) -> Result<(), String> {
    let bytes = name.as_bytes();
    let ok_first = bytes
        .first()
        .is_some_and(|c| c.is_ascii_lowercase() || c.is_ascii_digit());
    let ok_rest = bytes
        .iter()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'_' || *c == b'-');
    if ok_first && ok_rest && bytes.len() <= 63 {
        Ok(())
    } else {
        Err(format!(
            "invalid token name {name:?} (expected [a-z0-9][a-z0-9_-]{{0,62}})"
        ))
    }
}

/// The tokens-file line for a freshly generated token.
pub fn tokens_file_line(name: &str, scope: Scope, token: &str) -> String {
    format!(
        "{name} {} {}",
        scope.as_str(),
        format_hash(&hash_token(token))
    )
}

/// Constant-time byte-string equality: early exit only on a length mismatch
/// (lengths aren't secret), then XOR-accumulate every byte. Shared by the
/// session-plane preamble check and inbound API auth.
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// One configured token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenEntry {
    /// Unique name (recorded as the actor).
    pub name: String,
    /// Permission level.
    pub scope: Scope,
    /// SHA-256 of the token.
    pub hash: [u8; 32],
}

/// A tokens-file problem. The message names the file/line, never a hash.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct TokensFileError(pub String);

/// The parsed tokens file.
#[derive(Debug, Clone)]
pub struct TokenSet {
    entries: Vec<TokenEntry>,
}

impl TokenSet {
    /// Parse tokens-file contents.
    pub fn parse(src: &str) -> Result<TokenSet, TokensFileError> {
        let mut entries: Vec<TokenEntry> = Vec::new();
        for (idx, raw) in src.lines().enumerate() {
            let line_no = idx + 1;
            let line = raw.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let err = |reason: String| TokensFileError(format!("line {line_no}: {reason}"));
            let fields: Vec<&str> = line.split_whitespace().collect();
            let [name, scope, hash] = fields[..] else {
                return Err(err("expected `<name> <scope> sha256:<hex>`".into()));
            };
            validate_token_name(name).map_err(err)?;
            let scope = Scope::parse(scope)
                .ok_or_else(|| err(format!("unknown scope {scope:?} (read|operate|admin)")))?;
            let hash = parse_hash(hash).ok_or_else(|| {
                err("hash must be sha256: followed by 64 lowercase hex characters".into())
            })?;
            if entries.iter().any(|e| e.name == name) {
                return Err(err(format!("duplicate token name {name:?}")));
            }
            entries.push(TokenEntry {
                name: name.to_string(),
                scope,
                hash,
            });
        }
        if entries.is_empty() {
            return Err(TokensFileError("tokens file contains no tokens".into()));
        }
        Ok(TokenSet { entries })
    }

    /// Read and parse a tokens file. Missing/unreadable is an error.
    pub fn load(path: &Path) -> Result<TokenSet, TokensFileError> {
        let src = std::fs::read_to_string(path)
            .map_err(|e| TokensFileError(format!("reading tokens file {}: {e}", path.display())))?;
        TokenSet::parse(&src)
            .map_err(|e| TokensFileError(format!("tokens file {}: {}", path.display(), e.0)))
    }

    /// The entry whose hash matches `token`. Compares against **every** entry
    /// (no early return on a match) so timing doesn't reveal which one matched.
    pub fn authenticate(&self, token: &str) -> Option<&TokenEntry> {
        let presented = hash_token(token);
        let mut found = None;
        for e in &self.entries {
            if constant_time_eq(&e.hash, &presented) && found.is_none() {
                found = Some(e);
            }
        }
        found
    }

    /// Look up an entry by name (cookie verification).
    pub fn by_name(&self, name: &str) -> Option<&TokenEntry> {
        self.entries.iter().find(|e| e.name == name)
    }

    /// Number of configured tokens.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Always false for a successfully parsed set (kept for clippy's `len_without_is_empty`).
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

fn parse_hash(s: &str) -> Option<[u8; 32]> {
    let hex_part = s.strip_prefix("sha256:")?;
    if hex_part.len() != 64 || hex_part.bytes().any(|c| c.is_ascii_uppercase()) {
        return None;
    }
    hex::decode(hex_part).ok()?.try_into().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    const H: &str = "sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";

    #[test]
    fn generated_tokens_have_prefix_and_entropy() {
        let a = generate_token();
        let b = generate_token();
        assert!(a.starts_with(TOKEN_PREFIX));
        assert_eq!(
            a.len(),
            TOKEN_PREFIX.len() + 43,
            "32 bytes base64url no pad"
        );
        assert_ne!(a, b);
    }

    #[test]
    fn hash_matches_sha256_of_full_token() {
        // sha256("test") — the fixture hash H above.
        assert_eq!(format_hash(&hash_token("test")), H);
    }

    #[test]
    fn tokens_file_line_parses_back() {
        let tok = generate_token();
        let set = TokenSet::parse(&tokens_file_line("ci-bot", Scope::Operate, &tok)).unwrap();
        let e = set.authenticate(&tok).expect("round-trip authenticates");
        assert_eq!((e.name.as_str(), e.scope), ("ci-bot", Scope::Operate));
    }

    #[test]
    fn parse_skips_comments_and_blank_lines() {
        let src = format!("# header\n\nalice admin {H}\n   \n");
        let set = TokenSet::parse(&src).unwrap();
        assert_eq!(set.len(), 1);
        assert_eq!(set.by_name("alice").unwrap().scope, Scope::Admin);
    }

    #[test]
    fn parse_rejects_bad_input_naming_the_line() {
        let cases = [
            (String::from("alice admin"), "line 1"),
            (format!("alice root {H}"), "unknown scope"),
            (format!("Alice admin {H}"), "invalid token name"),
            (String::from("alice admin sha256:abc"), "hash"),
            (format!("alice admin md5:{}", &H[7..]), "hash"),
            (format!("alice admin {H}\nalice read {H}"), "duplicate"),
            (String::from("# only comments\n"), "no tokens"),
        ];
        for (src, needle) in cases {
            let err = TokenSet::parse(&src)
                .err()
                .unwrap_or_else(|| panic!("accepted: {src}"));
            assert!(err.0.contains(needle), "{src:?} → {err}");
            assert!(
                !err.0.contains(&H[7..]),
                "error must not echo the hash: {err}"
            );
        }
    }

    #[test]
    fn authenticate_rejects_wrong_and_near_miss_tokens() {
        let tok = generate_token();
        let set = TokenSet::parse(&tokens_file_line("a", Scope::Read, &tok)).unwrap();
        assert!(set.authenticate("pspo_nope").is_none());
        let mut near = tok.clone();
        near.pop();
        near.push('A');
        assert!(set.authenticate(&near).is_none());
        assert!(set.authenticate("").is_none());
    }

    #[test]
    fn token_names_are_validated() {
        assert!(validate_token_name("ariel").is_ok());
        assert!(validate_token_name("ci_bot-2").is_ok());
        for bad in ["", "-x", "_x", "Bob", "a b", &"a".repeat(64)] {
            assert!(validate_token_name(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn constant_time_eq_semantics() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"abcd"));
    }

    #[test]
    fn load_reports_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let err = TokenSet::load(&dir.path().join("nope")).err().unwrap();
        assert!(err.0.contains("nope"), "{err}");
    }
}
