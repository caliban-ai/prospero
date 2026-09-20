# ADR 0011 · ACP is a second **drive protocol**, not a second lifecycle

- **Status:** proposed
- **Date:** 2026-09-19
- **Source:** [#217](https://github.com/caliban-ai/prospero/issues/217). Amends
  [0003](0003-couple-to-caliban-via-ndjson-wire-format.md); builds on
  [0002](0002-control-plane-over-caliband.md) and
  [0008](0008-k8s-fleet-backend.md). Counterpart to caliban
  [ADR 0059](https://github.com/caliban-ai/caliban/blob/main/docs/adr/0059-acp-over-network-and-permission-posture.md).

## Context

Prospero drives caliban agents and nothing else. Per [0003](0003-couple-to-caliban-via-ndjson-wire-format.md)
the coupling is caliband's NDJSON wire: control requests over the daemon socket, and a
per-agent **attach** stream of caliban `stream-json` frames that
`caliband::stream::normalize_frame` turns into `EventKind`s. Every consumer —
the event stores, the SSE tail, the dashboard timeline, `/api/usage` — is fed
from that one path.

Two forces now push against that:

- **Heterogeneity.** Four of the five evaluated competitors drive several agent
  products (§F in each matrix): OpenHands, GitHub Agent HQ, Vibe Kanban and
  OpenClaw. The Agent Client Protocol is the common denominator — Claude Code,
  Codex and Gemini CLI all speak it — and it is the most widely shared gap in
  the evaluation tree.
- **Fidelity.** The NDJSON attach plane carries a tool call's *input* and
  ok/failed but was missing its result until caliban#391 (consumed in #236), and
  it has no way to ask a human for a permission decision. That second gap is why
  in-cluster agents were unusable before #238: an `Ask` with no human attached is
  simply denied, and the refusal reaches nobody. ACP models both first class —
  `tool_call_update` carries result content, and `session/request_permission`
  asks the driver.

caliban has since shipped the transport. In **v0.14.0** (ADR 0059, caliban#675) a
worker serves ACP over the *same* TLS + bearer-token per-agent listener prospero
already dials, selected per spawn by `SpawnSpec.drive_protocol` (`ndjson` |
`acp`). caliban#674 tracks the adapter's remaining data-parity gaps.

Four facts constrain what prospero can do with that:

1. **The worker's ACP mode is a fork, not an addition.** `caliban/src/worker.rs`
   returns into `run_acp_worker` before the NDJSON session plane is built: an ACP
   worker does **not** serve attach. Choosing ACP *replaces* prospero's
   observability path for that agent rather than supplementing it.
2. **ACP inverts the run model.** NDJSON is an autonomous agent with observers:
   caliband starts the run and prospero attaches, possibly late, possibly more
   than once, replaying from history. ACP is a passive server driven by exactly
   one client: the driver calls `session/new`, then each `session/prompt` blocks
   for one turn and returns a `stopReason`. Under ACP, prosperod is not an
   observer — it *is* the thing running the agent, and if it stops calling, the
   agent does nothing.
3. **Parity gaps are still open (caliban#674).** ACP's `tool_call` carries no
   input, `TurnEnd`/`RunEnd` are dropped (no usage, cost or turn counts), and
   `loadSession` is unwired. Adopting ACP for caliban agents today would regress
   the tool inspector (#236 ships input **and** result) and empty `/api/usage`.
4. **A non-caliban harness has no caliband.** No registry, no session dirs, no
   respawn, drain, idle-timeout or usage accounting. Those are caliband's, and
   [0002](0002-control-plane-over-caliband.md) is the decision not to
   re-implement them.

Three places an ACP client could live were weighed:

- **A. In prosperod, dialing the per-agent port** — for caliban agents, the same
  socket prospero already secures; caliband keeps the lifecycle.
- **B. In caliband, hosting non-caliban children** — caliband supervises a
  foreign harness as a first-class agent, so prospero sees one contract.
  caliban#296 opens this seam for *sub*-agents (the Agent tool's
  `ExternalHarness`), not yet for top-level supervised agents.
- **C. In prosperod, as a process supervisor** — a new `FleetProvider` that
  spawns `claude-code-acp`/`codex-acp`/`gemini --acp` itself and supervises them.

## Decision

**1. ACP is a second *drive protocol* for an agent's data path — never a second
lifecycle.** We add an `AgentDriver` seam in `prospero-core` with two
implementations: today's `NdjsonDriver` (attach stream + `AttachInbound`) and a
new `AcpDriver` (JSON-RPC over the per-agent TLS+token listener). Both produce
the same `EventKind` stream and accept the same steering calls, so the stores,
SSE tail, dashboard and usage path stay protocol-agnostic. `FleetProvider`
(`LocalFleet` / `K8sFleet`) is untouched: **caliband still spawns, registers,
kills and drains every agent** (option **A**).

**2. The protocol is chosen per spawn, defaults to NDJSON, and is not adopted
for caliban agents until caliban#674 lands.** Prospero sets
`SpawnSpec.drive_protocol` from an explicit request. Until the adapter carries
tool-call input and `TurnEnd`/`RunEnd` accounting, switching an agent to ACP is a
*regression* in exactly the surfaces #236 and #181 built, so ACP ships behind an
opt-in and NDJSON remains the default for caliban agents.

**3. ADR-0003 is amended, not superseded.** Its rule — *the wire format is the
only contract; prospero depends on no caliban crate* — still holds, now over two
wire formats. Two refinements:
   - ACP is an **open, third-party protocol**, not caliban's private wire. Its
     types are prospero's own mirrors, which is also what lets `AcpDriver` drive
     a non-caliban agent unchanged.
   - For the caliband control/NDJSON half, the hand-mirror is replaced by the
     published `caliban-contract` crate (#239) — the "revisit if" clause 0003
     itself wrote down, now triggered. Depending on a **serde-only contract
     crate** is not the wide coupling 0003 rejected (that was
     `caliban-supervisor`, the whole daemon).

**4. Heterogeneous harnesses: prospero does not become a process supervisor
(reject C for now).** Driving a foreign harness needs the lifecycle caliband
already owns; re-implementing registry, respawn, drain and usage inside prosperod
would contradict [0002](0002-control-plane-over-caliband.md) and would fork the
k8s story (a `CalibanTask` describes a caliban run, and a foreign process has no
Sandbox). The preferred route is **B**: caliband gains a top-level external-harness
backend — the natural extension of caliban#296's `ExternalHarness` seam from
sub-agents to supervised agents — so prospero keeps one lifecycle contract, one
CRD and one auth model, and gets heterogeneity through the seam this ADR already
builds (`AcpDriver` speaks to whatever is on the other end). If caliban declines
that, reopen C as its own ADR with the supervision cost stated explicitly.

**5. Permission prompts are the payoff and are in scope.** Under ACP a
supervised session surfaces `session/request_permission`; prospero answers it.
That means a pending-decision surface on the agent (API + dashboard approve/deny)
— the "supervised" half of #238's two postures, which today can only be honored
where a human is attached. The decision routes back as the ACP response;
`allow_always` (caliban#674) becomes a session-scoped allow.

**6. Event mapping (normative).** `AcpDriver` maps ACP `session/update`
notifications onto the existing `EventKind`s — no new variants, so every consumer
works unchanged:

| ACP | `EventKind` | Note |
|---|---|---|
| `agent_message_chunk` | `Output { stream: Stdout, chunk }` | |
| `agent_thought_chunk` | `Output { stream: Thinking, chunk }` | |
| `tool_call` | `ToolStarted { id, name, input }` | `input` is `Null` until caliban#674 |
| `tool_call_update` | `ToolFinished { id, ok, result, truncated }` | result from `content`, capped by `TOOL_RESULT_CAP` (#236) |
| `session/prompt` returns `stopReason` | `AgentFinished { outcome, cost_usd: 0.0, turns }` | usage/turns are 0 until caliban#674 |
| `session/request_permission` | *(new)* pending-decision state on the agent | answered over the same connection |
| `session/new` result | `AgentInit { session_id, model, tools }` | `tools` may be empty |

Steering maps as well: `send_input` becomes the next `session/prompt` on the
session (a better fit than `AttachInbound`, which exists only because the NDJSON
worker is autonomous), `end-input` ends the session, and a kill is
`session/cancel` followed by caliband's existing `Kill`.

**7. k8s expresses the protocol on the CR.** `CalibanTask.spec.task.driveProtocol`
(`ndjson` | `acp`, absent ⇒ `ndjson`), mirroring exactly how `permissionPosture`
landed in #238 / caliban-operator#80 — prospero writes it, the operator validates
it, prospero reads it back when building the `SpawnSpec`. That is a
caliban-operator change and must land before the k8s path can select ACP.

**8. Single-driver rule.** An ACP agent has exactly one driver: the prosperod
replica that owns it, under the existing ownership lease (#59/#108). A second
replica must not open a session on the same agent. Dashboard viewers remain
consumers of prospero's own SSE bus, never of ACP.

## Consequences

- **Positive.** Prospero gains the two things NDJSON cannot express — tool
  results with their input, and human permission decisions — on the connection it
  already secures, with no new endpoint, no second auth model and no new
  lifecycle. The `AgentDriver` seam is the same seam heterogeneity needs later, so
  the §F gap is approached without prospero becoming a supervisor. `FleetProvider`,
  the stores and the dashboard are untouched, and the fake-caliband harness
  ([0007](0007-fake-caliban-test-harness.md)) extends to a fake ACP peer the same
  way.
- **Negative.** Two data paths to keep working, and ACP's is the strictly more
  expensive one: prosperod must hold a session per agent and keep calling
  `session/prompt`, so an agent's progress now depends on its driver staying up —
  a failover mid-turn is a lost turn, not a re-attach. Replay/resume is weaker
  until `loadSession` exists (caliban#674): a restarted prosperod can re-read its
  own stored history but cannot rejoin the agent's session. Accounting is absent
  on ACP until the same ticket. Selecting ACP per spawn is a user-visible knob
  whose wrong setting silently degrades the timeline.
- **Revisit if.** caliban#674 stalls (then ACP stays opt-in indefinitely and this
  ADR is aspirational); or caliban declines a top-level external-harness backend
  (then option C — prosperod as supervisor — needs its own ADR with the
  supervision cost accepted); or a second non-caliban harness turns out to need
  per-harness lifecycle quirks in the driver, which would mean the `AgentDriver`
  seam is the wrong abstraction and belongs one layer up, at `FleetProvider`.
