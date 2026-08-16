# #190 — k8s terminal outcomes, usage freshness, honest launch reporting

**Ticket:** caliban-ai/prospero#190 (epic #95)

Three defects found driving dashboard v2 against the live cluster at `sha-7665cc0`.
They are independent; each gets its own task and its own tests.

---

## Task 1 — the k8s arm never persists `StatusChanged`

### The defect

`Store::usage` counts terminal outcomes from `status_changed` events
(`crates/core/src/store.rs:80,105`). The **local** arm emits them:
`FleetManager::reconcile` diffs the poll snapshot and calls
`emitter.emit(repo, id, EventKind::StatusChanged { from, to })`
(`crates/core/src/fleet.rs:1193-1205`).

The **k8s** watch loop computes the *identical* diff — `Some(prev) if prev.status
!= agent.status` (`crates/core/src/k8s/fleet.rs:894`) — but only pushes a
`FleetChange::StatusChanged` onto the in-memory broadcast channel. Nothing is
written to the store. So on k8s the outcome facets are structurally zero.

### Why emit rather than derive

The ticket offered an alternative: derive outcomes from `AgentFinished`. Rejected
— `AgentFinished.outcome` is caliban's open-ended result subtype ("EndOfTurn",
"max_turns"), not a fixed set, which is exactly why `OutcomeCounts` was documented
as *deliberately not* sourced from it (`crates/types/src/api.rs:193-195`).
Deriving would mean mapping an unbounded string onto four buckets, and would leave
the per-agent timeline (which renders `StatusChanged`, `timeline.rs:220`) still
blind on k8s.

Emitting is also the smaller change and makes the two backends symmetric, so the
aggregate stays backend-agnostic — which is the property `store_usage_conformance`
already asserts.

### The multi-replica hazard

Every prospero replica polls the same `CalibanTask` CRs, so an ungated emit would
write N copies of each transition and multiply every outcome count by the replica
count. The local arm has the same hazard and solves it with a per-repo lifecycle
lease (`own_lifecycle`, `fleet.rs:1101`).

Reuse the existing `Ownership` trait (`crates/core/src/ownership.rs`) with a
dedicated observer key. `try_acquire` is idempotent for a lease this process
already holds, so calling it once per poll doubles as the renew; if the owning
replica dies its lease expires and a peer takes over. `SelfOwnsAll` always
acquires, so standalone and single-replica behaviour is unchanged.

Emission happens **after** the diff lock is released, alongside the existing
broadcast — `emit` is async and the lock is a `std::sync::Mutex`.

### Scope

Emit `StatusChanged` for every observed transition, matching local. `AgentDiscovered`
/ `AgentGone` are deliberately **not** added: they don't feed `usage`, and the
`FleetChange` broadcast already drives the live UI for them.

### Tests (RED first)

1. `k8s_watch_loop_persists_status_changed_to_the_store` — fake API, flip a CR to a
   terminal phase, assert a `StatusChanged { to: Done }` lands in the store.
2. `k8s_status_changed_is_gated_by_the_observer_lease` — an `Ownership` that denies
   the observer key emits nothing.
3. `k8s_terminal_agents_are_counted_by_usage` — end-to-end through `Store::usage`,
   proving the facet is non-zero. This is the ticket's acceptance criterion.

---

## Task 2 — the usage panel never refreshes

### The defect

`UsagePanel`'s `use_resource` closure reads only `window()`
(`crates/dashboard/src/ui.rs:1934`), so it refetches on a window change and never
again. Observed live: the API reported `turns: 1` while the panel showed `TURNS 0`.

Not riding the 5s fleet poll was deliberate (#181) — a 30-day store aggregate
should not re-run every five seconds — but never refetching is the wrong end of
that trade.

### The fix

Add `Ui.activity: Signal<u32>` — a revision counter meaning "something happened
that could change the aggregate". The fleet poll loop in `main.rs` bumps it when
either holds:

- the **activity key** of the new snapshot differs from the last one. The key is a
  cheap hash over each agent's `(id, status)`, so a spawn, a terminal transition,
  or a reap all change it; pure output does not.
- 60 seconds have elapsed. This is the safety net for activity this replica cannot
  observe — a peer replica writing to the shared store, where no local agent
  changes state at all.

`UsagePanel` reads `ui.activity` inside its `use_resource`, so Dioxus re-runs the
fetch when it moves. Cost is one aggregate per *change*, not one per poll.

Known and accepted gap: an agent that spawns, finishes, and is reaped entirely
between two 5s polls is invisible to the key. Its cost is still in the store and
lands on the next bump, and the 60s tick bounds the staleness regardless.

### Tests (RED first)

`activity_key` is pure and lives in `charts.rs` next to the other pure helpers, so
it is unit-testable on the host target:

1. identical snapshots hash equal;
2. a status transition changes it;
3. an added agent changes it;
4. a removed agent changes it;
5. agent ordering does **not** change it (the snapshot's order is not guaranteed);
6. output-only churn (nothing in the key) leaves it stable.

---

## Task 3 — a re-launch reports a launch that did not happen

### The defect

`ensure_agent` is idempotent by design. On k8s the `CalibanTask` name is derived
from the spec (`task_name(&spec)`), so an identical prompt applies over the
existing CR and returns the existing id. The UI reports `"Launched {id} in {ws}."`
(`crates/dashboard/src/ui.rs:925-929`) either way.

The idempotency is right; claiming a launch that did not occur is not.

### The fix

Thread the fact through the one seam that knows it:

- `AgentHandle` gains `created: bool` (`crates/core/src/model.rs:26`).
- Local always spawns a fresh agent → `true` (`fleet_provider.rs:77`).
- k8s does a `get(&name)` before `apply` — the trait already has it
  (`k8s/fleet.rs:382`) — and sets `created = existing.is_none()`. One extra API
  call on a cold path.
- `SpawnedResponse` gains `created: bool`, `#[serde(default = "…true")]` so an
  older client that omits it still deserializes as the pre-existing behaviour.
- The UI says `"Attached to existing run {id} in {ws}."` when `created` is false.

### Tests (RED first)

1. `k8s_ensure_agent_reports_created_for_a_new_task`;
2. `k8s_ensure_agent_reports_not_created_for_an_existing_task`;
3. a `dto`/handler test that `created` reaches `SpawnedResponse`;
4. a dashboard unit test on the message-selection helper (extract it as a pure
   `fn launch_note(created, id, workspace) -> String` so it is testable off-DOM).

---

## Verification

Local gate (`cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets
-D warnings`, `cargo build --workspace --all-targets`, `cargo test --workspace`),
plus the dashboard crate's own host-target tests (it is outside the workspace) and
`scripts/build-dashboard.sh` to refresh the committed bundle + `SOURCE_HASH`.
