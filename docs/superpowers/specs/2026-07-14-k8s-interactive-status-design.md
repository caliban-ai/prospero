# Design: source interactive + awaiting-input status from the pod caliband (#130)

**Ticket:** caliban-ai/prospero#130 — `fix(k8s): source interactive + awaiting-input status from the pod caliband, not the CR (dashboard reply box)`

**Status:** approved, implementing.

## Problem

Under `PROSPERO_FLEET=k8s` the dashboard's interactive reply box never appears. The
box renders only when `agent.interactive && agent.status === "idle"`
(`crates/api/dashboard/app.js:783`), and for k8s agents neither is ever true.

Two independent defects sit behind that one symptom:

- **Read path (projection).** `K8sFleet::snapshot()` builds each `Agent` solely from the
  `CalibanTask` CR via `agent_from_task`, which hardcodes `interactive: false`
  (`crates/core/src/k8s/fleet.rs:231`) and derives status from the coarse CR phase
  (`phase_to_status`, `fleet.rs:228`, `175-191`). The only way a k8s agent reports `Idle`
  is CR phase `Draining` (teardown) — never "awaiting input."
- **Write path (delivery).** `K8sFleet::send_input` (`fleet.rs:1206`) hands the pod's
  **control** endpoint (`handle.endpoint`, from `status.calibandEndpoint`) straight to
  `CalibandClient::send_inbound`. But `send_inbound` is documented to require a **per-agent**
  endpoint obtained from `client.attach(id)` — which is exactly what the streaming path
  (`attach_once`, `fleet.rs:1520-1521`) and `LocalFleet::send_agent_input` both do first.
  So the "already implemented" reply path almost certainly delivers to the wrong endpoint
  and has never been exercised end-to-end.

The pod caliband's control protocol already carries both bits: `CtlRequest::List` →
`Vec<AgentRecord>`, where `AgentRecord.status` includes `Idle` ("awaiting input; no compute
pending") and `AgentRecord.spec.interactive` is the interactive flag. This is precisely how
`LocalFleet` sources them today (`crates/core/src/fleet.rs:1169-1179`). Prospero already dials
the pod over TCP+TLS+token for attach/stream; it simply never asks it for status.

## Goals

- Under k8s, an interactive agent awaiting input reports `interactive: true` + `Idle`,
  **sourced from the pod caliband** (not the CR phase), so the dashboard reply box appears.
- Sending a reply actually resumes the agent's turn (fix `send_input`).
- No caliban / CRD / operator change. The CR remains the source of membership/lifecycle.

## Non-goals

- Dashboard JS changes — `app.js:783` is already correct.
- Background status caching (see Approach B below) — a follow-up only if per-poll round-trip
  cost proves to matter.
- Deriving awaiting-input from the attach stream (Approach C) — no stream-derived state
  exists today; most new machinery, most protocol-guessing.

## Approach (A — synchronous overlay in `snapshot()`)

Chosen over (B) a background-refreshed status cache and (C) stream-derived state, because A
is the smallest change, reuses the proven `LocalFleet` pattern, is stateless, and is trivially
testable with `FakeCaliband`. Trade-off: one control `List` round-trip per pod per fleet poll —
acceptable at dashboard cadence. B is the escape hatch if that ever bites.

### Read path — status overlay

In `K8sFleet::snapshot()`, after building `agents` from `api.list()` (unchanged for
membership/lifecycle):

1. Select agents whose CR phase is `Running` (only these can be attachable / awaiting-input)
   and that carry a valid `calibandEndpoint`.
2. Group them by distinct endpoint. For each endpoint, dial
   `CalibandClient::connect_tcp(endpoint, self.session.tls, self.session.token)` and call
   `.list()` → `Vec<AgentRecord>`, indexed by id.
3. For each matched agent, overlay:
   - `agent.interactive ← record.spec.interactive`
   - `agent.status ← record.status` (this is where `Idle` / awaiting-input comes from)
4. This is a **read on any replica** — *not* leader-gated. Leader election (#108) governs only
   who attaches/writes the session plane; any replica may poll a pod's `List` for a read.

The CR (`api.list()`) continues to supply discovery, `Gone`, spawning, and lifecycle; the pod
`List` supplies live per-agent detail. This applies ADR 0004's hybrid-observability split to k8s.

### Write path — fix `send_input`

Change `send_input` to mirror `LocalFleet::send_agent_input` and `attach_once`: after resolving
the pod endpoint, resolve the per-agent endpoint and deliver to it.

```rust
let ep = client.attach(id.as_str()).await?;   // control endpoint -> per-agent endpoint
client.send_inbound(&ep, &input).await
```

instead of `client.send_inbound(&handle.endpoint, &input)`.

## Error handling

- **Overlay `list()` fails / pod unreachable:** log at debug/warn; keep the agent with its
  CR-phase status and `interactive: false`. A snapshot must never fail or drop an agent because
  one pod is unreachable — consistent with the resilient-list posture (#148).
- **`send_input` attach fails:** propagate as today (`CalibandUnreachable` / `AgentNotFound`);
  the API surfaces the error to the caller.

## Testing (via `FakeCaliband`, no real k8s)

`FakeCaliband::start_tcp_tls` + `add_agent_tcp` + `List` support let us drive the whole thing
over TCP/TLS against a fake pod.

- **Overlay:** register an agent on the fake with `status = Idle`, `spec.interactive = true`;
  point a `K8sFleet` (in-memory CR API with a `Running` task whose `calibandEndpoint` = the
  fake's addr) at it; call `snapshot()`; assert the projected agent is `interactive: true` +
  `status: Idle`.
- **Degradation:** a `calibandEndpoint` that doesn't answer → agent still present with CR-phase
  status; `snapshot()` succeeds.
- **Reply round-trip:** `send_input(UserMessage { .. })` → assert the `FakeCaliband` recorded the
  inbound frame on the per-agent endpoint (proves the `attach`-first fix).
- Existing `phase_to_status` tests unchanged; CR still drives membership/lifecycle.

## Affected code

- `crates/core/src/k8s/fleet.rs` — `snapshot()` (overlay), `agent_from_task` (no longer the sole
  source of `interactive`/status for `Running` agents), `send_input` (attach-first).
- `crates/core/src/caliband/client.rs` — `list()`, `attach()`, `send_inbound()` (used as-is).
- `crates/core/src/testkit.rs` — `FakeCaliband` (used as-is; extend only if a helper is missing).

## Acceptance

Under k8s, an interactive agent awaiting input reports `interactive: true` + `Idle` sourced from
the pod caliband; the dashboard reply box appears; sending input resumes its turn via `send_input`.
No caliban-side change. Verified end-to-end by a `FakeCaliband` integration test.
