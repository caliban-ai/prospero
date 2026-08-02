//! Mutating operations, modelled as **data** rather than closures.
//!
//! A confirmation dialog needs to describe an action before performing it, and
//! the app state that holds "which dialog is open" has to be `PartialEq` for
//! Dioxus to diff it. Boxed closures are neither inspectable nor comparable, so
//! an action is an enum the executor matches on. The pleasant side effect is
//! that the wording and the destructive-or-not judgement become plain values —
//! unit-testable without rendering anything.

use crate::api;

/// Something the operator can ask the fleet to do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Stop a running or idle agent.
    KillAgent { id: String, name: String },
    /// Drop a finished agent from the fleet.
    RemoveAgent { id: String, name: String },
    /// Re-run a finished agent with its original task.
    RespawnAgent { id: String, name: String },
    /// Deregister a workspace.
    RemoveWorkspace { name: String },
    /// Close an interactive agent's input stream.
    EndInput { id: String, name: String },
}

impl Action {
    /// Whether this action needs an explicit confirmation first.
    ///
    /// Kill, remove, and end-input destroy work or state that cannot be undone
    /// from the UI. Respawn creates something new and is cheap to undo (kill
    /// it), so it fires immediately — gating it behind a dialog would just be
    /// friction.
    pub fn needs_confirmation(&self) -> bool {
        !matches!(self, Action::RespawnAgent { .. })
    }

    /// Whether the confirm button should read as destructive.
    pub fn is_destructive(&self) -> bool {
        matches!(
            self,
            Action::KillAgent { .. } | Action::RemoveAgent { .. } | Action::RemoveWorkspace { .. }
        )
    }

    /// Title for the confirmation dialog.
    pub fn title(&self) -> String {
        match self {
            Action::KillAgent { .. } => "Kill this agent?".into(),
            Action::RemoveAgent { .. } => "Remove this agent?".into(),
            Action::RespawnAgent { .. } => "Respawn this agent?".into(),
            Action::RemoveWorkspace { .. } => "Remove this workspace?".into(),
            Action::EndInput { .. } => "End this agent's input?".into(),
        }
    }

    /// What the dialog says will happen. Names the subject explicitly so a
    /// mis-click on a dense fleet list is caught before it lands.
    pub fn detail(&self) -> String {
        match self {
            Action::KillAgent { name, .. } => {
                format!("{name} will stop immediately. Work in progress is lost.")
            }
            Action::RemoveAgent { name, .. } => {
                format!("{name} will be dropped from the fleet. Its event history is kept.")
            }
            Action::RespawnAgent { name, .. } => {
                format!("{name} will be re-run with its original task, as a new agent.")
            }
            Action::RemoveWorkspace { name } => format!(
                "{name} will be deregistered. Its checkout on disk is untouched, \
                 but its agents will no longer be managed."
            ),
            Action::EndInput { name, .. } => {
                format!("{name} will receive no further input and will finish its run.")
            }
        }
    }

    /// Label for the confirming button.
    pub fn confirm_label(&self) -> &'static str {
        match self {
            Action::KillAgent { .. } => "Kill",
            Action::RemoveAgent { .. } => "Remove",
            Action::RespawnAgent { .. } => "Respawn",
            Action::RemoveWorkspace { .. } => "Remove",
            Action::EndInput { .. } => "End input",
        }
    }

    /// Message shown when it succeeds, or `None` when the fleet view speaks for
    /// itself (a killed agent visibly changes state; a respawn does not, since
    /// the new agent is a different row).
    pub fn success_note(&self) -> Option<String> {
        match self {
            Action::RespawnAgent { name, .. } => Some(format!("{name} respawned.")),
            _ => None,
        }
    }

    /// Perform it.
    pub async fn run(&self) -> Result<(), String> {
        match self {
            Action::KillAgent { id, .. } => api::kill_agent(id).await,
            Action::RemoveAgent { id, .. } => api::remove_agent(id).await,
            Action::RespawnAgent { id, .. } => api::respawn_agent(id).await.map(|_| ()),
            Action::RemoveWorkspace { name } => api::remove_workspace(name).await,
            Action::EndInput { id, .. } => api::end_input(id).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all() -> Vec<Action> {
        vec![
            Action::KillAgent {
                id: "a".into(),
                name: "parser".into(),
            },
            Action::RemoveAgent {
                id: "a".into(),
                name: "parser".into(),
            },
            Action::RespawnAgent {
                id: "a".into(),
                name: "parser".into(),
            },
            Action::RemoveWorkspace {
                name: "prospero".into(),
            },
            Action::EndInput {
                id: "a".into(),
                name: "parser".into(),
            },
        ]
    }

    #[test]
    fn every_action_has_non_empty_copy() {
        for a in all() {
            assert!(!a.title().is_empty(), "{a:?} has no title");
            assert!(!a.detail().is_empty(), "{a:?} has no detail");
            assert!(!a.confirm_label().is_empty(), "{a:?} has no button label");
        }
    }

    /// A confirmation that doesn't name its subject is worthless on a dense
    /// fleet list — the whole point is catching a mis-click on the wrong row.
    #[test]
    fn confirmation_detail_names_its_subject() {
        for a in all() {
            let subject = match &a {
                Action::RemoveWorkspace { name } => name.clone(),
                Action::KillAgent { name, .. }
                | Action::RemoveAgent { name, .. }
                | Action::RespawnAgent { name, .. }
                | Action::EndInput { name, .. } => name.clone(),
            };
            assert!(
                a.detail().contains(&subject),
                "{a:?} detail omits its subject: {}",
                a.detail()
            );
        }
    }

    #[test]
    fn destructive_actions_are_confirmed_and_respawn_is_not() {
        for a in all() {
            match a {
                Action::RespawnAgent { .. } => {
                    assert!(!a.needs_confirmation(), "respawn should not be gated");
                    assert!(!a.is_destructive());
                }
                _ => assert!(a.needs_confirmation(), "{a:?} must be confirmed"),
            }
        }
    }

    #[test]
    fn ending_input_is_confirmed_but_not_styled_as_destructive() {
        // It is irreversible, so it asks — but it ends a run cleanly rather
        // than discarding work, so it should not read as red-alert.
        let end = Action::EndInput {
            id: "a".into(),
            name: "x".into(),
        };
        assert!(end.needs_confirmation());
        assert!(!end.is_destructive());
    }

    #[test]
    fn removing_a_workspace_says_the_checkout_survives() {
        let a = Action::RemoveWorkspace {
            name: "prospero".into(),
        };
        let detail = a.detail().to_lowercase();
        assert!(
            detail.contains("untouched") || detail.contains("disk"),
            "operators need to know the checkout is safe: {detail}"
        );
    }
}
