# Live-cluster agent spawn: decouple spawn from reconcile + provision the session plane

**Date:** 2026-07-15
**Status:** Design (approved for planning)
**Scope:** cross-repo — `prospero`, `caliban-operator`, `helm-charts`

## Context

Two issues were observed spawning agents against the live k8s cluster:

1. **Spawn hangs the UI.** The spawn call blocks for a long time before the UI
   responds — it synchronously waits on the full `CalibanTask CR → operator
   reconcile → Sandbox → pod schedule → pod Running` chain.
2. **Agent fails to spawn** with:
   `caliband: --listen: a non-empty bearer token is required for network
   (--listen) mode; refusing to bind an unauthenticated listener`.

### Root causes

**Bug 1 (prospero).** `POST /api/workspaces/{ws}/agents` → `spawn_agent`
(`prospero/crates/api/src/handlers.rs:170-192`) awaits `ensure_agent` inline. In
the k8s backend, `K8sFleet::ensure_agent`
(`prospero/crates/core/src/k8s/fleet.rs:958-984`) applies the CR then **blocks in
a poll loop** until `handle_from` returns `Some`, which only happens once
`status.phase == "Running"` with a resolved `calibandEndpoint`
(`fleet.rs:118-127`). Deadline defaults to **30 s** (`PollConfig::default`,
`fleet.rs:423`). So the HTTP response is coupled to full reconcile latency.

**Bug 2 (operator + charts).** The error string is emitted by **caliband**, not
prospero: `caliban/crates/caliban-supervisor/src/transport.rs:204`
(`require_network_credentials`) fail-closes in `--listen` mode unless it is given
**both** a non-empty bearer token (checked first → the reported error) **and**
TLS cert/key. caliband accepts these via `--token` / `CALIBAN_DAEMON_TOKEN` and
`--tls-cert`/`--tls-key` (`caliban/crates/caliban-supervisor/src/bin/caliband.rs:59-108`).

The **caliban-operator** builds the caliband pod but supplies none of them.
`caliban-operator/src/resources.rs:223-228` sets args
`--workspace-root … --listen 0.0.0.0:<port>` only; `caliband_env`
(`resources.rs:93-116`) injects provider/model/router env but no token; and
`Settings` (`caliban-operator/src/config.rs:13-24`) has **no** TLS/token fields.
No chart wires session-plane credentials either. This is the operator/charts not
keeping up with caliban's #400/#288 fail-closed hardening; the prospero **dial**
side is already built (`prosperod` reads a mounted CA + token file —
`prospero/crates/daemon/src/main.rs:51-67`).

### The two bugs compound

Because Bug 2 makes the caliband pod crash-loop (exit code 2 on the missing
token), it never reaches `Running`, so Bug 1's poll runs the **full 30 s** and
then returns `timed out waiting for CalibanTask … to become Running`. That is the
observed "hung a long time, then failed." The fixes are independent: Bug 1 makes
the UI return immediately regardless of pod health; Bug 2 lets the pod actually
bind.

## Goals

- Spawn returns as soon as the CR is **admitted**, not when the pod is Running.
- caliband pods launched by the operator bind successfully (token + TLS present).
- `prosperod` trusts and authenticates to those pods with no code change (its
  dial path already exists).

## Non-goals

- Per-task / per-agent credentials. `prosperod` dials every agent with a
  **single** token file + single CA + fixed SNI (`caliband`)
  (`prospero/crates/daemon/src/main.rs:51-67`; `SessionPlane.token` is one value),
  so a **shared** session-plane credential is the only design that fits what is
  built. Per-agent rotation is out of scope.
- Reworking the fleet watch/attach loop (already correct — see below).
- Cross-namespace prosperod↔caliband dialing (see Risks).

## Constraints & findings (evidence gathered)

- **Single namespace.** prospero writes all CalibanTasks into one namespace,
  `PROSPERO_K8S_NAMESPACE` (default `default`)
  (`prospero/crates/daemon/src/main.rs:409`; `K8sFleet` API is
  `Api::namespaced`, `fleet.rs:298-304`). The operator creates the Sandbox in the
  task's namespace. So the shared Secret + cert-manager Certificate live in that
  one workload namespace — no per-namespace replication.
- **The watch loop already provides visibility and attach.**
  `spawn_watch_loop` (`fleet.rs:683-793`) lists every ~2 s, broadcasts
  `Discovered`/`StatusChanged`/`Gone` (dashboard SSE), and **#113-attaches every
  observed-Running agent** (ownership-gated). `ensure_agent`'s blocking poll is
  therefore redundant with the watch loop; removing it loses neither dashboard
  visibility nor session attach.
- **Admission is synchronous on apply.** `api.apply(&ct)` runs through the
  operator's admission webhook, so an invalid `workspaceRef` / empty
  providers still fails fast as 4xx (preserves #150's 400 behavior). We stop
  waiting on *reconcile*, not on *admission*.

## Design — Bug 1 (prospero only)

**`K8sFleet::ensure_agent` (`fleet.rs:958-984`):** after `self.api.apply(&ct)`
succeeds, return immediately with the agent identity built from the deterministic
`task_name` (already computed at `fleet.rs:959`, before apply). Delete the
`loop { get … until Running }` and the immediate `start_agent_stream` call — the
watch loop's #113 path attaches when the pod reaches Running, exactly as it does
for operator/peer-created agents.

**Contract change (the endpoint-shaped hole).** `AgentHandle.endpoint`
(`prospero/crates/core/src/model.rs:26-31`) is a **required** field, but at spawn
time the k8s endpoint is genuinely unknown (pod unscheduled). The only production
caller — `spawn_agent` — reads only `handle.id` and echoes `workspace` from the
path param, never `handle.endpoint`.

- **Decision:** change `FleetProvider::ensure_agent` to return
  `Result<AgentId>` instead of `Result<AgentHandle>`. This makes the spawn
  contract honest ("accepted; here is the identity") and removes the endpoint
  promise entirely rather than papering it with a placeholder. `LocalFleet`
  returns its `id`; `K8sFleet` returns `AgentId::from(task_name)`. The
  implementation plan sweeps all callers (handler, CLI, tests).
- **Alternative considered:** make `endpoint: Option<Endpoint>`. Rejected —
  weakens the type for the attach path (`handle_from`/`to_attach` always have a
  concrete endpoint) to serve one caller that ignores the field.

**Dead plumbing.** `PollConfig.deadline` and the "poll until Running" budget
become unused for spawn; remove the now-dead field/wiring (keep the
`watch_poll_interval` cadence — a separate concern).

**No other prospero changes.** The watch loop, session plane, ownership lease,
and dashboard SSE are unchanged.

## Design — Bug 2 (operator + charts; no prospero Rust change)

### Transport contract (recap)

caliband `--listen` mode requires token **and** TLS
(`caliban/.../transport.rs:204-220`). `prosperod` presents a CA + bearer token +
SNI `caliband` (`prospero/crates/daemon/src/main.rs:51-67`). The fix supplies the
serving side and hands the same material to `prosperod` via the charts.

### Shared credentials (cert-manager)

Provisioned once in the **workload namespace**, hosted in the `caliban-system`
parent chart and referenced by name from the operator and prospero subcharts via
values:

- **TLS (cert-manager):** self-signed `Issuer` → CA `Certificate` (`isCA: true`)
  → CA `Issuer` → serving `Certificate` `caliban-session-plane-tls` with
  `dnsNames: [caliban]` (matching `prosperod`'s default
  `PROSPERO_K8S_CALIBAND_SERVER_NAME=caliband`). The two-tier chain keeps
  `ca.crt` **stable** across serving-cert rotation, so prosperod's trust anchor
  does not churn. Resulting Secret carries `tls.crt` / `tls.key` / `ca.crt`.
- **Bearer token:** a Helm-generated `Secret` `caliban-session-plane-token`,
  key `token` = `randAlphaNum 48`, preserved across `helm upgrade` via `lookup`
  so it is not rotated on every upgrade.

### caliban-operator

- **`src/config.rs` (`Settings` + `from_env`):** add fields for the TLS secret
  name and the token secret name/key (env-driven, with defaults matching the
  chart names).
- **`src/resources.rs` (`build_sandbox`):**
  - Mount the TLS Secret as a read-only volume at `/etc/caliband/tls`.
  - Add args `--tls-cert /etc/caliband/tls/tls.crt --tls-key
    /etc/caliband/tls/tls.key`.
  - Add env `CALIBAN_DAEMON_TOKEN` via `secretKeyRef` → the token Secret.
  - No new RBAC: mounts are kubelet-level; the pod keeps its token-less SA and
    `automountServiceAccountToken: false`.

### prospero chart (`charts/prospero`)

- Mount `ca.crt` (from the TLS secret) and `token` (from the token secret) as
  files; set `PROSPERO_K8S_CALIBAND_CA_FILE`, `PROSPERO_K8S_CALIBAND_TOKEN_FILE`,
  and `PROSPERO_K8S_CALIBAND_SERVER_NAME=caliban`. The dial code already consumes
  these — no prospero Rust change for Bug 2.

## Testing

**Bug 1 (prospero):**
- Rewrite `ensure_agent_returns_handle_once_running` and
  `ensure_agent_times_out_if_never_running` (`fleet.rs:1429`, `:1471`) to the new
  contract: `ensure_agent` returns the identity immediately after apply without
  waiting for Running; a never-Running CR no longer errors.
- Add a test asserting spawn does not block on `status.phase` (e.g. a fake API
  whose `get` would never return Running still returns promptly).
- Sweep and fix all `ensure_agent` callers for the `AgentId` return type
  (handler, CLI, tests).

**Bug 2 (operator + charts):**
- Operator unit tests on `build_sandbox`: `--tls-cert`/`--tls-key` args present,
  `CALIBAN_DAEMON_TOKEN` `secretKeyRef` present, TLS volume + mount present.
- `helm template` golden assertions for the new cert-manager resources, token
  Secret, and the operator/prospero mounts + env.
- **End-to-end on the live cluster:** spawn an agent; confirm the caliband pod
  binds (no bearer error, reaches Running) and prosperod attaches the stream
  (`/stream` non-empty).

## Sequencing / rollout

- **Bug 1 ships independently first** — prospero-only; immediately fixes the UI
  hang and makes a still-crash-looping pod surface as Spawning/Failed on the
  dashboard instead of a 30 s freeze.
- **Bug 2 lands operator + charts together** — a half-wired token or cert
  crash-loops the pod, so the cert-manager chain, token Secret, operator
  injection, and prospero mounts must deploy as one unit.

## Risks & open items

- **NetworkPolicy is same-namespace.** The operator's default-deny NetworkPolicy
  ingress (`caliban-operator/src/resources.rs:64-70`) allows the caliband port
  from `podSelector: {}` with **no `namespaceSelector`** — only same-namespace
  pods. prosperod can dial only if co-located with the caliband pods (default
  namespace `default`). If prospero deploys to its own namespace, the dial is
  blocked even after credentials are correct. Verification must confirm
  co-location, or a follow-up adds a `namespaceSelector` for prospero's
  namespace. Orthogonal to the bearer crash (which precedes any dial) but gates
  end-to-end success.
- **SNI vs. dnsNames.** prosperod validates the cert against SNI `caliban`; the
  cert-manager `Certificate.dnsNames` must include exactly that. Kept aligned
  above; called out so a future SNI/name change updates both sides.
- **Token rotation** is a `helm upgrade` operation (all pods + prosperod remount;
  in-flight agents re-attach). Automatic token rotation is out of scope.
- **cert-manager dependency.** The stack now requires cert-manager installed;
  documented as a chart prerequisite.

## Files touched (per repo)

- **prospero:** `crates/core/src/k8s/fleet.rs` (ensure_agent, PollConfig,
  tests), `crates/core/src/fleet_provider.rs` (trait + LocalFleet return type),
  `crates/api/src/handlers.rs` (use returned `AgentId`), CLI + any other
  `ensure_agent` callers. `crates/core/src/model.rs` only if the alternative
  (Option endpoint) is chosen — not with the recommended `AgentId` return.
- **caliban-operator:** `src/config.rs`, `src/resources.rs` (+ tests).
- **helm-charts:** `charts/caliban-system` (cert-manager chain + token Secret),
  `charts/caliban-operator` (Settings env for secret names),
  `charts/prospero` (CA/token mounts + `PROSPERO_K8S_CALIBAND_*` env), values +
  README/prereq note.
