# #194 — terminal outcomes must come from the pod, not the CR phase

**Ticket:** caliban-ai/prospero#194 (follow-up to #190, blocks epic #95)

## What #190 got wrong

#190 made the k8s watch loop persist the transitions it observes, so
`Store::usage` could count terminal outcomes. The emit path works — verified on
the live cluster, where a fresh agent produced a real `spawning -> running`
event, which never happened before.

But that is the only transition it ever writes, because the loop diffs
`agent_from_task(task)` — status derived purely from `CalibanTask.status.phase`.
On the real cluster the operator never advances that phase. Every CR sits at
`Running` indefinitely, including ones whose agents finished 20 hours earlier.

Meanwhile `snapshot()` calls `overlay_pod_status`, which dials each pod's
caliband and replaces the CR-phase status with the pod's real
`AgentRecord.status`. So the same page renders a workspace meter reading
"2 finished, 1 failed" directly above outcome facets reading all zeros.

**The defect is that two components disagree about what an agent's status is,
and the one that emits events is the one using the weaker source.**

## The fix

Give the watch loop the same notion of status `snapshot()` already has: overlay
live pod status onto the CR-derived agents *before* diffing.

That is a smaller change than it sounds — `overlay_pod_status` already exists,
is already tested (#130), and already handles the unreachable-pod degradation.
It moves from `K8sFleet` to `SessionPlane` (which owns the `tls`/`token` the
dial needs, and which the watch loop already holds), and `snapshot()` calls it
through the same seam.

Consequences beyond the facets: `FleetChange::StatusChanged` broadcasts become
correct too. They are wrong today for exactly the same reason — the live watch
stream reports CR-phase transitions while the dashboard's polled snapshot
reports pod truth.

### Bounding the cost

The overlay is one caliband `List` per pod. Doing it every ~2s poll for every CR
would be worse than today, because a finished agent's CR stays `Running`
forever — we would dial dead agents' pods indefinitely.

So the loop overlays only agents whose **last known status is non-terminal**.
Terminal is terminal; once observed there is nothing left to learn. Steady-state
cost is therefore proportional to *live* agents rather than to all agents ever
created — strictly better than `snapshot()`'s current behaviour, which re-dials
every Running-phase CR on every `/api/fleet` request.

Every replica still applies the overlay, so `known` and the broadcasts are
correct everywhere; only the observer-lease holder writes to the store, which is
the gate #190 already added.

### Known limit (shared with the local arm)

An agent that reaches a terminal state while prosperod is entirely down is first
*seen* as terminal, which is a `Discovered`, not a transition — so it is never
counted. The local arm has the same property. Not addressed here.

## The fixture was the real problem

#190's unit tests passed while the bug survived, because `MemTaskApi::set_phase`
flips the CR to `Completed` — something the real operator never does. The fake
was unfaithful, and mutation-testing the assertions could not reveal that.

New tests therefore model the cluster as observed: **the CR stays `Running`
forever, and the pod's caliband is the only thing that reports terminal.**
`FakeCaliband::start_tcp_tls` + `set_status` already supports exactly this shape
(it is how #130's overlay tests work).

### Tests (RED first)

1. `terminal_pod_status_is_persisted_when_the_cr_phase_never_advances` — CR
   pinned at `Running`, pod flips to `Done`; assert a persisted
   `StatusChanged { to: Done }`. This is the cluster bug in a test.
2. `terminal_pod_status_reaches_the_usage_aggregate` — same setup, asserted
   through `Store::usage`: `done == 1`. The acceptance criterion.
3. `a_failed_pod_agent_is_counted_as_failed` — proves the mapping is the pod's
   real status, not a hardcoded Done.
4. `the_overlay_is_skipped_for_agents_already_known_terminal` — the cost bound;
   assert no further dial happens once terminal is observed.
5. Keep #190's lease-gating test — the single-writer property must survive.

## Out of scope, but worth filing separately

The operator leaving `status.phase` at `Running` forever is wrong on its own
terms (caliban-operator). This fix makes prospero report honest numbers
regardless, which is the right dependency direction, but the CRD lying about
its own state should not stand.

## Verification

Full gate (workspace **and** the out-of-workspace `crates/dashboard`), then a
sha build driven on the live cluster — the same path that caught #190's gap.
A green unit suite is explicitly *not* sufficient evidence for this ticket.
