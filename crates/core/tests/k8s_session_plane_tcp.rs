//! #77 I1: prove the K8sFleet **per-agent stream leg** is network-routable —
//! not just the control connection. The prior `k8s_session_plane.rs` networks
//! only control; the per-agent stream endpoint from `attach` is a same-process
//! Unix socket. Here `FakeCaliband::add_agent_tcp` serves the per-agent stream
//! over **TCP + TLS** and advertises an `Endpoint::Tcp` in `AttachAck`, so the
//! whole chain — control dial, `attach`, then `open_stream` on the returned
//! endpoint — rides the network, exactly as a real pod caliband must.

#![cfg(all(feature = "k8s", feature = "testkit"))]

use std::sync::Arc;
use std::time::Duration;

use prospero_core::bus::InProcessBus;
use prospero_core::caliband::transport::tls_client_from_pem;
use prospero_core::caliband::wire::Endpoint;
use prospero_core::event::{EventKind, stream_key_for};
use prospero_core::k8s::fake::FakeK8s;
use prospero_core::k8s::fleet::K8sFleet;
use prospero_core::store::{JsonlStore, Store};
use prospero_core::testkit::FakeCaliband;

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
            "timed out waiting for {min_events} event(s) under {stream_key}; got {history:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn k8s_fleet_streams_a_network_agent_stream_over_tcp() {
    // Control plane over TCP+TLS+token (as in the sibling test)...
    let token = "i1-test-token";
    let (mut fake, tls_fixture) = FakeCaliband::start_tcp_tls(token)
        .await
        .expect("start fake caliband over tcp+tls");

    // ...and — the point of #77 I1 — the PER-AGENT STREAM also served over
    // TCP+TLS (reusing the fake's cert, so the client's `ca_pem` trusts it).
    let repo = "repo-a";
    let agent_id = "ct-fake-k8s-agent";
    let script = vec![
        serde_json::json!({
            "type": "TurnStart", "turn_index": 0, "message_id": "m1", "model": "test-model",
        }),
        serde_json::json!({
            "type": "AssistantTextDelta", "turn_index": 0, "content_block_index": 0,
            "text": "hello over a tcp stream",
        }),
        serde_json::json!({
            "type": "RunEnd", "final_messages": [], "total_usage": {}, "turn_count": 1,
            "stopped_for": "EndOfTurn",
        }),
    ];
    fake.add_agent_tcp(agent_id, script).await;

    // Real seams + a K8sFleet carrying the client-side TLS trust.
    let data_dir = tempfile::tempdir().unwrap();
    let store = Arc::new(JsonlStore::open(data_dir.path()).unwrap());
    let bus = Arc::new(InProcessBus::new(64));
    let tls = tls_client_from_pem(&tls_fixture.ca_pem, "localhost").expect("client tls");
    let fleet = K8sFleet::new(FakeK8s::new(), bus, store.clone())
        .with_network(Some(tls), Some(token.to_string()));

    // Dial control over the network; `attach` returns a TCP stream endpoint;
    // `open_stream` dials THAT over TCP+TLS; frames land in the store.
    fleet.start_agent_stream(
        repo,
        agent_id,
        &Endpoint::Tcp {
            addr: tls_fixture.addr,
        },
    );

    let key = stream_key_for(repo, agent_id);
    let history = wait_for_history(&store, &key, 2, Duration::from_secs(5)).await;
    assert!(
        history.iter().any(|e| matches!(
            &e.kind,
            EventKind::Output { chunk, .. } if chunk == "hello over a tcp stream"
        )),
        "the AssistantTextDelta must land as an Output event over a TCP stream leg; got {history:?}"
    );
    assert!(
        history
            .iter()
            .any(|e| matches!(&e.kind, EventKind::AgentFinished { turns: 1, .. })),
        "the terminal RunEnd must land as AgentFinished; got {history:?}"
    );
}
