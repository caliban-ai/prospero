# k8s fleet: prospero must spawn the agent in caliband (#159) — design

**Goal:** Under `PROSPERO_FLEET=k8s`, spawning an agent must actually *start the
LLM run* inside the pod's caliband and stream its output to the dashboard — not
just create the `CalibanTask` CR and then attach to a non-existent agent.

## Root cause (confirmed)

caliband is a **passive supervisor**: it starts an agent only on
`CtlRequest::Spawn { spec }`, and **generates the agent id itself** on spawn (a
12-char opaque id — `SpawnSpec` carries no id field). `Attach { id }` only
streams an already-registered agent.

In the k8s topology nobody sent that `Spawn`:

- `K8sFleet::ensure_agent` server-side-applies the CR and returns (post-#157 it
  deliberately doesn't wait for `Running`).
- The shared poll loop, on observing a `Running` CR, calls
  `SessionPlane::attach` → `attach_loop` → `client.attach(<CR name>)`, which
  caliband 404s as `agent not found` because no agent was ever registered.
- `CalibandClient::spawn` is called only by the **local** backend
  (`fleet.rs` `spawn_agent_with_socket`). The k8s module had zero spawn calls.

The whole k8s session plane assumed "the pod caliband registers its agent under
the CR name" (see `start_agent_stream`'s doc comment). That assumption is
unsatisfiable without a caliban change, because caliban owns the id. The local
backend works precisely because it `spawn`s (getting caliband's id) *then*
attaches to that id.

## Approach (Option A — prospero issues the Spawn, no caliban/operator change)

Make the k8s path symmetric with the proven local path: **spawn (or resolve) the
pod's agent, then attach to caliban's assigned id — while keeping the CR name as
prospero's stream identity.**

### 1. Decouple the caliband attach-id from the prospero stream-key

`attach_loop` / `attach_once` today take a single `agent_id` used for **both**
the caliband `Attach { id }` request and the `Emitter` stream key
(`stream_key_for(repo, agent_id)`). Add a distinct `attach_id: &str`:

- `agent_id` stays the **stream key** — the CR name the dashboard subscribes to
  (`/stream?agent=<CR name>`), so events land where the API looks for them.
- `attach_id` is the **caliband-assigned id** used in the `Attach` request.

The local backend passes `attach_id == agent_id` (unchanged behavior).

### 2. Resolve the pod's caliband agent id (spawn-or-list), lease-gated

One CalibanTask ⇒ one pod ⇒ one caliband ⇒ (at most) one agent. Inside the
elected replica's attach task (so the poll loop stays non-blocking and the
existing ownership lease + `attached` dedup gate it):

```
ensure_pod_agent(client, spec):
    let agents = client.list().await?;
    if let Some(rec) = agents.into_iter().next() { return Ok(rec.id); } // already spawned
    match spec { Some(s) => Ok(client.spawn(s).await?.0), None => Err(..) }
```

- **Idempotent across poll cycles**: once attached, the `attached` map dedups
  further `attach` calls, so no re-spawn.
- **Idempotent across replicas**: the #108 ownership lease elects a single
  attacher; a failover replica finds the existing agent via `list()` (the pod
  persists) and attaches without re-spawning.
- Spawn lives in the watch-loop/attach path, **not** `ensure_agent` (which
  returns before the endpoint exists, post-#157).

### 3. Build the SpawnSpec from the CalibanTask

The poll loop already iterates each `Running` CR; it builds the spec there and
threads it into `to_attach`:

| SpawnSpec field       | Source                                                        |
|-----------------------|---------------------------------------------------------------|
| `initial_prompt`      | `spec.task.prompt`                                            |
| `label`               | the CR name (observability)                                  |
| `tool_allowlist`      | `spec.tools`                                                  |
| `provider`            | **None** — the operator resolves the workspace provider (ref→kind + creds) and injects `CALIBAN_PROVIDER`/base-url/key into the pod env |
| `model`               | None — the frozen CRD carries no per-task model              |
| `isolation_worktree`  | false — isolation defaults live on the Workspace, operator-resolved |
| `interactive`         | false — the CRD carries no interactive bit (MVP)            |
| `inherit_hooks`       | true (default)                                               |

`start_agent_stream` (public, used by tests / pure re-attach) keeps its
signature and passes `spec: None` — a pod with an existing agent still attaches
via the `list()` branch.

## Out of scope (documented follow-ups)

- `send_input` and `overlay_pod_status` still key caliband ops by the CR name.
  They are already unreachable pre-#159 (no agent exists) and remain in their
  current state after it (tests pre-register under the CR name). Making
  interactive reply + interactive/idle overlay resolve caliban's id for a
  prospero-spawned agent is a #130-adjacent follow-up, not required by the #159
  acceptance criteria (spawn + stream + idempotent + lease-gated).

## Acceptance criteria → coverage

- *Spawning under k8s registers a running agent (no `agent not found`)* →
  `ensure_pod_agent` spawns from the CR prompt; unit test asserts the fake
  received the spawn spec and the stream lands.
- *prosperod streams the agent's output to the dashboard* → decoupled
  `attach_loop` emits under the CR-name stream key; unit test replays the store
  under `stream_key_for(repo, <CR name>)` and finds the frames even though
  caliban assigned a different id.
- *Idempotent across poll cycles / replicas; lease-gated* → `attached` dedup +
  #108 lease + `list()`-before-spawn; covered by the ownership tests + a
  no-double-spawn assertion.
- *Verified live* → home-cluster smoke: dashboard spawn → agent runs → output
  streams.
