# ADR 0005 · Worktree isolation by default for agent spawns

- **Status:** accepted
- **Date:** 2026-06-05
- **Source:** [`docs/superpowers/specs/2026-06-05-prospero-framework-design.md`](../superpowers/specs/2026-06-05-prospero-framework-design.md) §1, §2, §5

## Context

A core use case is running **several parallel agents on the same codebase** at once —
multiple streams of work under one repo's caliband. If those agents share a single working
tree, their concurrent edits collide and corrupt each other's work. Caliban's `SpawnSpec`
exposes an `isolation_worktree` flag that gives each agent its own git worktree.

The question is the **default**: do agents share the tree unless told otherwise, or get an
isolated worktree unless told otherwise?

## Decision

**Worktree isolation is the default** for every spawn. Each agent gets its own git worktree
so concurrent edits on one codebase don't collide. Sharing the working tree is an explicit
opt-out via the `--shared-tree` flag (which sets `isolation_worktree: false`).

This default is enforced at the API boundary: `POST /api/repos/{repo}/agents` defaults
`isolation` to `worktree`, so the CLI, the dashboard, and any future client inherit the safe
behavior without each re-deciding it.

## Consequences

- **Positive:** the common case — parallel agents on one repo — is safe by default; a user
  has to go out of their way (`--shared-tree`) to opt into shared-tree behavior and its
  collision risk. Enforcing the default at the API boundary, not per client, keeps the policy
  in one place and makes "spawn defaults to a worktree" a testable invariant of the control
  plane.
- **Negative:** each isolated agent consumes a git worktree (disk + setup cost) — acceptable
  for the parallelism it buys, but real cost for many or short-lived agents.
- **Revisit if:** worktree setup cost dominates for high-churn or single-agent workloads, or
  a use case emerges where shared-tree is the safe common case — either would argue for a
  different default or a per-repo policy.
