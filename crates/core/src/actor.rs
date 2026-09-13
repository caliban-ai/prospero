//! The authenticated actor for the current request (#2).
//!
//! `prospero-api`'s auth middleware runs each handler inside [`scope`]; the fleet
//! emitter reads [`current`] when it builds an event, so events a request emits
//! directly carry the token name without threading it through `FleetProvider`.
//! Tasks spawned with `tokio::spawn` (the poll loop, watch loops, attach tasks)
//! do not inherit it — their events stay unattributed, which is correct.

use std::future::Future;

tokio::task_local! {
    static ACTOR: Option<String>;
}

/// Run `fut` with `actor` as the current actor.
pub async fn scope<F: Future>(actor: Option<String>, fut: F) -> F::Output {
    ACTOR.scope(actor, fut).await
}

/// The current actor, or `None` outside any [`scope`].
pub fn current() -> Option<String> {
    ACTOR.try_with(Clone::clone).ok().flatten()
}

#[cfg(test)]
mod tests {
    use super::*;

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
