//! The authenticated actor for the current request (#2), and the person that
//! request says it is acting for (#251).
//!
//! `prospero-api`'s auth middleware runs each handler inside [`scope`]; the fleet
//! emitter reads [`current`] when it builds an event, so events a request emits
//! directly carry the token name without threading it through `FleetProvider`.
//! Tasks spawned with `tokio::spawn` (the poll loop, watch loops, attach tasks)
//! do not inherit it — their events stay unattributed, which is correct.
//!
//! [`current_subject`] is the second half, and the two are **not** the same kind
//! of fact. The actor is who prosperod *authenticated*. The subject is who the
//! client *says* it is acting for — a multi-user front end like Ariel holds one
//! service token but serves many people. Prosperod cannot authenticate someone
//! else's user and does not try; it records the claim beside the credential that
//! made it, so an auditor reads "token `ariel` asserted this was for `U123`",
//! never "prosperod verified `U123`".

use std::future::Future;

tokio::task_local! {
    static ACTOR: Option<String>;
    static SUBJECT: Option<String>;
}

/// Run `fut` with `actor` as the current actor.
pub async fn scope<F: Future>(actor: Option<String>, fut: F) -> F::Output {
    ACTOR.scope(actor, fut).await
}

/// The current actor, or `None` outside any [`scope`].
pub fn current() -> Option<String> {
    ACTOR.try_with(Clone::clone).ok().flatten()
}

/// Run `fut` with both the authenticated actor and the client's asserted
/// subject in scope.
pub async fn scope_with_subject<F: Future>(
    actor: Option<String>,
    subject: Option<String>,
    fut: F,
) -> F::Output {
    SUBJECT.scope(subject, ACTOR.scope(actor, fut)).await
}

/// Who the current request says it is acting for, or `None`.
///
/// Client-asserted and unverified — see the module docs.
pub fn current_subject() -> Option<String> {
    SUBJECT.try_with(Clone::clone).ok().flatten()
}

/// Longest accepted subject. Bounds what a client can push into every event it
/// causes; ids and handles are far shorter.
pub const MAX_SUBJECT: usize = 128;

/// Validate a client-asserted subject.
///
/// Rejects rather than sanitizes, so a client learns its value was wrong
/// instead of discovering later that the audit trail says something else.
/// Control characters are refused because this string is written into the
/// event log and structured logs, where a newline would let a caller forge
/// what looks like a second record.
pub fn validate_subject(subject: &str) -> std::result::Result<&str, String> {
    let trimmed = subject.trim();
    if trimmed.is_empty() {
        return Err("on-behalf-of must not be blank".to_string());
    }
    if trimmed.chars().count() > MAX_SUBJECT {
        return Err(format!(
            "on-behalf-of must be at most {MAX_SUBJECT} characters"
        ));
    }
    if let Some(bad) = trimmed.chars().find(|c| c.is_control()) {
        return Err(format!(
            "on-behalf-of must not contain control characters (found {bad:?})"
        ));
    }
    Ok(trimmed)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// #251: the subject reaches the event log and the structured logs, so a
    /// newline in it could forge what reads as a separate record. Rejected,
    /// not sanitized — a client that sent something wrong should be told.
    #[test]
    fn a_subject_that_could_forge_a_log_line_is_refused() {
        assert!(validate_subject("discord:U123").is_ok());
        assert_eq!(validate_subject("  U123  ").unwrap(), "U123", "trimmed");

        assert!(validate_subject("").is_err(), "blank");
        assert!(validate_subject("   ").is_err(), "whitespace only");
        assert!(
            validate_subject("alice\nactor=admin").is_err(),
            "a newline must not be accepted"
        );
        assert!(validate_subject("alice\r\nx").is_err());
        assert!(validate_subject("alice\tbob").is_err());
        assert!(validate_subject("alice\u{0}").is_err());
        assert!(
            validate_subject(&"x".repeat(MAX_SUBJECT + 1)).is_err(),
            "over-long must be refused"
        );
        assert!(validate_subject(&"x".repeat(MAX_SUBJECT)).is_ok());
        // Non-ASCII names are people's names, not an attack.
        assert!(validate_subject("Zoë Ω 日本").is_ok());
    }

    /// The two identities must be independently readable: an auditor needs to
    /// see which credential acted *and* who it claimed to act for.
    #[tokio::test]
    async fn the_asserted_subject_is_separate_from_the_authenticated_actor() {
        scope_with_subject(Some("ariel".into()), Some("discord:U123".into()), async {
            assert_eq!(current().as_deref(), Some("ariel"));
            assert_eq!(current_subject().as_deref(), Some("discord:U123"));
        })
        .await;

        // A request that asserts nothing behaves exactly as before.
        scope_with_subject(Some("ariel".into()), None, async {
            assert_eq!(current().as_deref(), Some("ariel"));
            assert_eq!(current_subject(), None);
        })
        .await;

        assert_eq!(current_subject(), None, "does not leak past the scope");
    }

    #[tokio::test]
    async fn a_spawned_task_inherits_neither_identity() {
        scope_with_subject(Some("ariel".into()), Some("discord:U123".into()), async {
            let seen = tokio::spawn(async { (current(), current_subject()) })
                .await
                .unwrap();
            assert_eq!(
                seen,
                (None, None),
                "background work must not be attributed to the request that started it"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn current_is_none_outside_a_scope() {
        assert_eq!(current(), None);
    }

    #[tokio::test]
    async fn scope_is_visible_across_awaits_and_not_in_spawned_tasks() {
        scope(Some("alice".into()), async {
            tokio::task::yield_now().await;
            assert_eq!(current().as_deref(), Some("alice"));
            let spawned = tokio::spawn(async { current() }).await.unwrap();
            assert_eq!(spawned, None, "spawned tasks must not inherit the actor");
        })
        .await;
        assert_eq!(current(), None);
    }
}
