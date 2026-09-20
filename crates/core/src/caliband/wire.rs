//! caliband's control-plane wire types.
//!
//! These are **caliban's own definitions**, re-exported from the published
//! `caliban-contract` crate (#239): serde-only, no daemon internals, so it is
//! not the wide coupling [ADR-0003] rejected — that was `caliban-supervisor`,
//! the whole daemon. ADR-0003's "revisit if" clause called for exactly this
//! once hand-mirroring became a drift source, and it had: the mirror never
//! learned caliban's `AgentStatus::Drained` (caliban ADR 0057), so a single
//! drained agent failed the *entire* `Listed` reply.
//!
//! What stays here is what has no upstream home: [`AttachInbound`] (caliban's
//! per-agent attach protocol, not part of the control-plane contract) and the
//! boundary conversion to prospero's own domain [`crate::model::AgentStatus`],
//! which is prospero's public API vocabulary and deliberately not caliban's
//! type.
//!
//! [ADR-0003]: ../../../../docs/adr/0003-couple-to-caliban-via-ndjson-wire-format.md

use serde::{Deserialize, Serialize};

/// caliban's lifecycle enum, as it appears on the wire. Converted to
/// prospero's [`crate::model::AgentStatus`] at the boundary by
/// [`domain_status`] — the two are separate on purpose: prospero's is the
/// vocabulary its own API and dashboard speak.
pub use caliban_contract::wire::AgentStatus as WireAgentStatus;
/// caliban's permission posture, as it appears on the wire. Prospero's own
/// [`crate::model::PermissionPosture`] (#238) is what the API and CR use.
pub use caliban_contract::wire::PermissionPosture as WirePermissionPosture;
pub use caliban_contract::wire::{
    AgentRecord, CtlReply, CtlRequest, DaemonStatus, DrainedAgent, DriveProtocol, Endpoint,
    SpawnSpec, SupervisorError,
};

pub use crate::model::{AgentStatus, PermissionPosture};

/// Map caliban's wire status onto prospero's domain status.
///
/// `Drained` (caliban ADR 0057) has no prospero equivalent: prospero's model
/// has no "stopped but resumable" state, and inventing one here would change
/// the public API. It reads as [`AgentStatus::Done`] — terminal and not a
/// failure, which is what a drained agent is from the fleet's point of view.
/// The exhaustive match is the point: a future caliban state stops compiling
/// here instead of failing a poll at runtime.
#[must_use]
pub fn domain_status(status: WireAgentStatus) -> AgentStatus {
    match status {
        WireAgentStatus::Spawning => AgentStatus::Spawning,
        WireAgentStatus::Running => AgentStatus::Running,
        WireAgentStatus::Idle => AgentStatus::Idle,
        WireAgentStatus::Killed => AgentStatus::Killed,
        WireAgentStatus::Drained | WireAgentStatus::Done => AgentStatus::Done,
        WireAgentStatus::Failed => AgentStatus::Failed,
        WireAgentStatus::Crashed => AgentStatus::Crashed,
    }
}

/// Map prospero's domain status onto caliban's wire status.
///
/// Total in this direction — prospero's model has no state caliban lacks. Used
/// where prospero writes a status caliban would have sent (the fake caliband).
#[must_use]
pub fn wire_status(status: AgentStatus) -> WireAgentStatus {
    match status {
        AgentStatus::Spawning => WireAgentStatus::Spawning,
        AgentStatus::Running => WireAgentStatus::Running,
        AgentStatus::Idle => WireAgentStatus::Idle,
        AgentStatus::Killed => WireAgentStatus::Killed,
        AgentStatus::Done => WireAgentStatus::Done,
        AgentStatus::Failed => WireAgentStatus::Failed,
        AgentStatus::Crashed => WireAgentStatus::Crashed,
    }
}

/// Map prospero's domain posture onto caliban's wire posture (#238).
#[must_use]
pub fn wire_posture(posture: PermissionPosture) -> WirePermissionPosture {
    match posture {
        PermissionPosture::Supervised => WirePermissionPosture::Supervised,
        PermissionPosture::Unattended => WirePermissionPosture::Unattended,
    }
}

/// Prospero's accessor on caliban's [`Endpoint`].
///
/// The contract crate puts the equivalent on `AgentRecord`, but prospero asks
/// it of a bare endpoint (a transport decision, made before any record exists),
/// so it lives here as an extension rather than forcing the call sites to
/// re-match.
pub trait EndpointExt {
    /// The Unix socket path, when this endpoint is one.
    fn unix_socket_path(&self) -> Option<&std::path::Path>;
}

impl EndpointExt for Endpoint {
    fn unix_socket_path(&self) -> Option<&std::path::Path> {
        match self {
            Endpoint::Unix { path } => Some(path.as_path()),
            Endpoint::Tcp { .. } => None,
        }
    }
}

/// Inbound control frames written to an interactive agent's per-agent socket.
/// Mirrors caliban `AttachInbound` (`caliban/src/attach.rs`); the outbound
/// stream stays caliban stream-json, so the two never share a direction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum AttachInbound {
    /// Inject a user message and resume the run.
    UserMessage {
        /// Message text.
        text: String,
    },
    /// Signal end-of-input: the agent finishes after this.
    EndInput,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn endpoint_matches_caliban_wire_shape() {
        // Byte-for-byte parity with caliban's transport::Endpoint.
        let unix = Endpoint::Unix {
            path: "/tmp/a1.sock".into(),
        };
        assert_eq!(
            serde_json::to_string(&unix).unwrap(),
            r#"{"scheme":"unix","path":"/tmp/a1.sock"}"#
        );
        let tcp = Endpoint::Tcp {
            addr: "host.ns.svc:9443".into(),
        };
        assert_eq!(
            serde_json::to_string(&tcp).unwrap(),
            r#"{"scheme":"tcp","addr":"host.ns.svc:9443"}"#
        );
        // Round-trips both ways.
        for e in [unix, tcp] {
            let s = serde_json::to_string(&e).unwrap();
            assert_eq!(serde_json::from_str::<Endpoint>(&s).unwrap(), e);
        }
    }

    #[test]
    fn endpoint_unix_socket_path_accessor() {
        assert_eq!(
            Endpoint::Unix {
                path: "/x.sock".into()
            }
            .unix_socket_path(),
            Some(std::path::Path::new("/x.sock"))
        );
        assert_eq!(
            Endpoint::Tcp { addr: "h:1".into() }.unix_socket_path(),
            None
        );
    }

    #[test]
    fn ctl_request_list_is_tagged() {
        assert_eq!(
            serde_json::to_string(&CtlRequest::List).unwrap(),
            "{\"kind\":\"list\"}"
        );
    }

    /// Compared through serde rather than `==`: the contract's request/reply
    /// enums derive no `PartialEq`, and the serialized form is the contract
    /// anyway.
    #[test]
    fn ctl_request_rm_force_defaults_false() {
        let r: CtlRequest = serde_json::from_str("{\"kind\":\"rm\",\"id\":\"a1\"}").unwrap();
        assert_eq!(
            serde_json::to_value(&r).unwrap(),
            json!({"kind": "rm", "id": "a1", "force": false})
        );
    }

    #[test]
    fn spawn_spec_defaults_inherit_hooks_true() {
        let s: SpawnSpec = serde_json::from_str("{\"initial_prompt\":\"hi\"}").unwrap();
        assert!(s.inherit_hooks);
        assert!(!s.isolation_worktree);
        assert!(s.model.is_none());
        assert!(s.provider.is_none());
    }

    /// #239: caliban's `AgentStatus` grew a `drained` state (caliban ADR 0057,
    /// #650) that prospero's hand-mirror never learned. The mirror has no
    /// catch-all, so one drained agent fails the **whole** `Listed` reply —
    /// every agent on that caliband disappears from the poll, not just the
    /// drained one. Exactly the silent drift the contract crate ends.
    #[test]
    fn a_drained_agent_does_not_break_the_whole_list_reply() {
        let listed = json!({
            "kind": "listed",
            "agents": [{
                "id": "a1", "name": "one", "status": "running",
                "started_at": "2026-09-19T00:00:00Z", "session_dir": "/s/a1",
                "endpoint": {"scheme": "unix", "path": "/s/a1.sock"},
                "spec": {"initial_prompt": "hi"}
            }, {
                "id": "a2", "name": "two", "status": "drained",
                "started_at": "2026-09-19T00:00:00Z", "session_dir": "/s/a2",
                "endpoint": {"scheme": "unix", "path": "/s/a2.sock"},
                "spec": {"initial_prompt": "hi"}
            }]
        });

        let reply: CtlReply = serde_json::from_value(listed).expect("drained must parse");
        let CtlReply::Listed { agents } = reply else {
            panic!("expected Listed");
        };
        assert_eq!(agents.len(), 2, "a drained agent must not drop its peers");
    }

    /// #238 / #239: the posture rides caliban's own type now, so what prospero
    /// must get right is the *conversion*, not the serialization.
    #[test]
    fn posture_converts_to_calibans_wire_values() {
        assert_eq!(
            serde_json::to_value(wire_posture(PermissionPosture::Unattended)).unwrap(),
            json!("unattended")
        );
        assert_eq!(
            serde_json::to_value(wire_posture(PermissionPosture::Supervised)).unwrap(),
            json!("supervised")
        );
        // Absent on the wire ⇒ supervised, fail-closed (caliban ADR 0059).
        let s: SpawnSpec = serde_json::from_str(r#"{"initial_prompt":"hi"}"#).unwrap();
        assert_eq!(s.permission_posture, WirePermissionPosture::Supervised);
    }

    /// #239: every caliban lifecycle state maps onto one prospero shows. The
    /// match is exhaustive, so a new caliban state breaks the build here rather
    /// than a poll at runtime — which is what the hand-mirror got wrong.
    #[test]
    fn every_caliban_status_maps_to_a_domain_status() {
        use WireAgentStatus as W;
        for (wire, expected) in [
            (W::Spawning, AgentStatus::Spawning),
            (W::Running, AgentStatus::Running),
            (W::Idle, AgentStatus::Idle),
            (W::Killed, AgentStatus::Killed),
            (W::Done, AgentStatus::Done),
            (W::Failed, AgentStatus::Failed),
            (W::Crashed, AgentStatus::Crashed),
            // No prospero equivalent: drained is terminal and not a failure.
            (W::Drained, AgentStatus::Done),
        ] {
            assert_eq!(domain_status(wire), expected, "{wire:?}");
        }
    }

    /// The reverse direction is total and round-trips for every domain state.
    #[test]
    fn domain_status_round_trips_through_the_wire_status() {
        for s in [
            AgentStatus::Spawning,
            AgentStatus::Running,
            AgentStatus::Idle,
            AgentStatus::Killed,
            AgentStatus::Done,
            AgentStatus::Failed,
            AgentStatus::Crashed,
        ] {
            assert_eq!(domain_status(wire_status(s)), s);
        }
    }

    /// A caliband-shaped spawn spec still deserializes, including one written
    /// before a field existed. The old goldens here pinned prospero's *own*
    /// serialization of its mirror; with the contract crate that would only
    /// re-test caliban's serde, so what is worth asserting is that the payloads
    /// prospero actually sees still parse (#239).
    #[test]
    fn caliban_spawn_spec_payloads_still_parse() {
        let interactive = r#"{"label":null,"frontmatter_path":null,"initial_prompt":"hi","model":null,"provider":null,"tool_allowlist":null,"isolation_worktree":false,"inherit_hooks":true,"interactive":true}"#;
        let spec: SpawnSpec = serde_json::from_str(interactive).expect("caliban spec parses");
        assert!(spec.interactive);
        assert_eq!(spec.permission_posture, WirePermissionPosture::Supervised);

        // #93: the provider round-trips.
        let with_provider = r#"{"initial_prompt":"hi","provider":"openai"}"#;
        let spec: SpawnSpec = serde_json::from_str(with_provider).unwrap();
        assert_eq!(spec.provider.as_deref(), Some("openai"));
        assert_eq!(
            serde_json::to_value(&spec).unwrap()["provider"],
            json!("openai")
        );
    }

    #[test]
    fn spawn_spec_without_provider_defaults_none() {
        // Back-compat: a pre-provider spec (field absent) still deserializes.
        let old = r#"{"initial_prompt":"hi"}"#;
        let spec: SpawnSpec = serde_json::from_str(old).unwrap();
        assert!(spec.provider.is_none());
    }

    #[test]
    fn spawn_spec_without_interactive_defaults_false() {
        // Back-compat: a pre-interactive spec (field absent) still deserializes.
        let old = r#"{"initial_prompt":"hi"}"#;
        let spec: SpawnSpec = serde_json::from_str(old).unwrap();
        assert!(!spec.interactive);
    }

    #[test]
    fn attach_inbound_user_message_serializes() {
        let j = serde_json::to_string(&AttachInbound::UserMessage {
            text: "hi there".into(),
        })
        .unwrap();
        assert_eq!(j, r#"{"type":"UserMessage","text":"hi there"}"#);
    }

    #[test]
    fn attach_inbound_end_input_serializes() {
        let j = serde_json::to_string(&AttachInbound::EndInput).unwrap();
        assert_eq!(j, r#"{"type":"EndInput"}"#);
    }

    #[test]
    fn attach_inbound_round_trips() {
        // Symmetric drift guard: the tagged shape must survive a serialize →
        // deserialize round-trip for both variants.
        for frame in [
            AttachInbound::UserMessage { text: "hi".into() },
            AttachInbound::EndInput,
        ] {
            let s = serde_json::to_string(&frame).unwrap();
            let back: AttachInbound = serde_json::from_str(&s).unwrap();
            assert_eq!(frame, back);
        }
    }

    #[test]
    fn ctl_reply_error_round_trips() {
        let reply = CtlReply::Error {
            error: SupervisorError::NotFound { id: "x".into() },
        };
        let s = serde_json::to_string(&reply).unwrap();
        let back: CtlReply = serde_json::from_str(&s).unwrap();
        assert_eq!(
            serde_json::to_string(&back).unwrap(),
            s,
            "round-trip must preserve the wire form"
        );
    }

    #[test]
    fn spawned_reply_parses() {
        let json =
            r#"{"kind":"spawned","id":"a1","endpoint":{"scheme":"unix","path":"/tmp/a1.sock"}}"#;
        let r: CtlReply = serde_json::from_str(json).unwrap();
        let CtlReply::Spawned { id, endpoint } = r else {
            panic!("expected Spawned");
        };
        assert_eq!(id, "a1");
        assert_eq!(
            endpoint,
            Endpoint::Unix {
                path: "/tmp/a1.sock".into()
            }
        );
    }

    #[test]
    fn spawned_reply_parses_tcp_endpoint() {
        let json =
            r#"{"kind":"spawned","id":"a1","endpoint":{"scheme":"tcp","addr":"pod.ns.svc:9443"}}"#;
        let r: CtlReply = serde_json::from_str(json).unwrap();
        let CtlReply::Spawned { id, endpoint } = r else {
            panic!("expected Spawned");
        };
        assert_eq!(id, "a1");
        assert_eq!(
            endpoint,
            Endpoint::Tcp {
                addr: "pod.ns.svc:9443".into()
            }
        );
    }
}
