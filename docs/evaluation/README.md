# Evaluation

Home for how we measure Prospero against comparable systems — chiefly other
**agent-orchestration control planes**.

This mirrors the convention established in the caliban repo
(`caliban/docs/evaluation/`), scoped to Prospero's layer: Prospero is a control
plane over fleets of coding agents, so its competitors are other control
planes / gateways that launch, route to, and observe agent workers — not the
terminal coding agents themselves (those live in caliban's evaluation tree).

## Layout

| Directory | Contents |
|-----------|----------|
| [`competitors/`](competitors/) | Per-competitor capability inventories and parity analysis. One subdirectory per competitor, each with a documented-capability inventory + a Prospero ↔ competitor parity gap matrix. Currently: [`openclaw/`](competitors/openclaw/) — OpenClaw, a multi-channel assistant gateway whose control-plane core is the same species as Prospero. |

## Conventions

- **Competitors** each get their own directory under `competitors/<name>/` with
  a static, dated `capability-inventory.md` (a snapshot of the competitor's
  documented surface) and a living `parity-gap-matrix.md` (Prospero ↔ competitor,
  ✅/🟡/🔴/n·a). Re-baseline the inventory manually before a parity-prioritization
  pass; tick matrix rows in the same PR that ships the feature.

## Relationship to the caliban evaluation tree

caliban's `docs/evaluation/competitors/` tracks **terminal coding agents**
(Claude Code, Codex, OpenCode, Grok Build) — things caliban competes with head
to head. Orchestration-layer competitors that *drive* such agents belong here
instead. OpenClaw is the first: it was originally captured in caliban's tree,
then re-homed here because its comparison is with a control plane, not a coding
engine. caliban keeps only the thin "caliban as a worker backend" note under its
own `competitors/openclaw/`.
