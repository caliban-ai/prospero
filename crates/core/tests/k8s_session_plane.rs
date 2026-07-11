//! Task B4 integration proof: `K8sFleet`'s network session-plane bridge lands
//! a Running agent's live output in the SAME event bus + `Store` the API's
//! SSE/history reads (ADR 0008 §3) — so `/stream` works for a k8s-backed agent
//! unchanged.
//!
//! This drives the exact path a real k8s deployment would use: dial a pod
//! caliband's **control** endpoint over #71/#75's TCP+TLS+bearer-token
//! transport, `attach` to the agent's per-agent stream, normalize its frames,
//! and append them through `crate::fleet::attach_loop`'s shared `Emitter` —
//! reusing `FakeCaliband::start_tcp_tls` (already generalized for this by
//! #75) rather than a bespoke harness, since it already proves the control
//! plane over the network; only the per-agent stream itself needs no new
//! test infrastructure to exercise here.

#![cfg(all(feature = "k8s", feature = "testkit"))]

use std::sync::Arc;
use std::time::Duration;

use prospero_core::bus::InProcessBus;
use prospero_core::caliband::transport::tls_client_from_pem;
use prospero_core::caliband::wire::{AgentStatus as WireAgentStatus, Endpoint};
use prospero_core::event::{EventKind, stream_key_for};
use prospero_core::k8s::fake::FakeK8s;
use prospero_core::k8s::fleet::K8sFleet;
use prospero_core::store::{JsonlStore, Store};
use prospero_core::testkit::{FakeCaliband, test_record};

/// Bounded poll for `frames land in the store` — avoids a fixed sleep either
/// racing (too short) or padding every run (too long).
async fn wait_for_history(
    store: &JsonlStore,
    stream_key: &str,
    min_events: usize,
    timeout: Duration,
) -> Vec<prospero_core::FleetEvent> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let history = store.replay(stream_key, 0).await.unwrap();
        if history.len() >= min_events {
            return history;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {min_events} event(s) under {stream_key}; \
             got {history:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn k8s_fleet_streams_a_network_agent_into_the_store() {
    // 1. A fake caliband control plane over real TCP + TLS + bearer token
    //    (ADR 0051) — the same network transport prospero #71/#75 landed and
    //    a production `K8sFleet` dials against the pod-caliband Sandbox DNS.
    let token = "b4-test-token";
    let (mut fake, tls_fixture) = FakeCaliband::start_tcp_tls(token)
        .await
        .expect("start fake caliband over tcp+tls");

    // 2. Pre-register the agent this test attaches to. Its id doubles as the
    //    k8s-level `AgentId` (the id `ensure_agent` would hand back) AND the
    //    id sent in the `Attach` request — the documented MVP simplification
    //    in `K8sFleet::start_agent_stream`'s doc comment.
    let repo = "repo-a";
    let agent_id = "ct-fake-k8s-agent";
    let agent_dir = tempfile::tempdir().unwrap();
    let record = test_record(agent_id, agent_dir.path(), WireAgentStatus::Running, false);
    let script = vec![
        serde_json::json!({
            "type": "TurnStart", "turn_index": 0, "message_id": "m1", "model": "test-model",
        }),
        serde_json::json!({
            "type": "AssistantTextDelta", "turn_index": 0, "content_block_index": 0,
            "text": "hello from a k8s-backed agent",
        }),
        serde_json::json!({
            "type": "RunEnd", "final_messages": [], "total_usage": {}, "turn_count": 1,
            "stopped_for": "EndOfTurn",
        }),
    ];
    fake.add_agent(record, script).await;

    // 3. Real seams: a JsonlStore (durable history) + InProcessBus (live
    //    tail) — the SAME kind of bus/store the API's `/stream` endpoint
    //    reads for a `LocalFleet` agent (ADR 0004).
    let data_dir = tempfile::tempdir().unwrap();
    let store = Arc::new(JsonlStore::open(data_dir.path()).unwrap());
    let bus = Arc::new(InProcessBus::new(64));

    // 4. Build the client-side TLS trust material a K8sFleet would carry
    //    from operator-injected config (env/Secret), and a K8sFleet wired to
    //    use it. `FakeK8s` stands in for the `CalibanTaskApi` seam —
    //    unexercised here; only `start_agent_stream` is under test.
    let tls = tls_client_from_pem(&tls_fixture.ca_pem, "localhost").expect("client tls");
    let fleet = K8sFleet::new(FakeK8s::new(), bus, store.clone())
        .with_network(Some(tls), Some(token.to_string()));

    // 5. The seam under test: dial the pod caliband's control endpoint over
    //    the network, attach, normalize, and land frames in the store.
    fleet
        .start_agent_stream(
            repo,
            agent_id,
            &Endpoint::Tcp {
                addr: tls_fixture.addr,
            },
        )
        .await;

    // 6. Assert the frames actually reached the store, keyed the same way
    //    `/stream` would look them up — `stream_key_for(repo, agent_id)`,
    //    i.e. bare `agent_id` per `stream_key_for`'s contract.
    let key = stream_key_for(repo, agent_id);
    let history = wait_for_history(&store, &key, 2, Duration::from_secs(5)).await;

    assert!(
        history.iter().any(|e| matches!(
            &e.kind,
            EventKind::Output { chunk, .. } if chunk == "hello from a k8s-backed agent"
        )),
        "expected the AssistantTextDelta frame to land as an Output event; got {history:?}"
    );
    assert!(
        history
            .iter()
            .any(|e| matches!(&e.kind, EventKind::AgentFinished { turns: 1, .. })),
        "expected the terminal RunEnd frame to land as AgentFinished; got {history:?}"
    );

    // seq is per-stream monotonic, matching how `FleetManager`'s own attach
    // loop stamps events (Emitter::next_seq) — the k8s bridge reuses the same
    // Emitter, so this must hold here too.
    let seqs: Vec<u64> = history.iter().map(|e| e.seq).collect();
    let mut sorted = seqs.clone();
    sorted.sort_unstable();
    assert_eq!(seqs, sorted, "events must be persisted in seq order");
}
