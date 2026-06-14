# 0001. Record architecture decisions

- **Status:** Accepted
- **Date:** 2026-06-13
- **Deciders:** Prospero maintainers

## Context

Prospero's architectural decisions — the coupling boundary to caliban, the
observability model, the crate layout, the testing strategy — have so far lived in PR
descriptions, commit messages, design docs under `docs/superpowers/`, and chat. Those
sources answer *what* the system does but scatter the *why*. Design docs cover whole
features at once and go stale; commit messages are hard to discover after the fact. As
the project grows and more people touch it, reconstructing the rationale behind a
decision means archaeology across several places.

We need a durable, discoverable, append-only record of significant decisions that
survives independently of any one feature's design doc.

## Decision

We will keep **Architecture Decision Records (ADRs)** in this repository under
`docs/adr/`, one decision per file, named `docs/adr/####-topic.md` (zero-padded,
monotonically increasing number + kebab-case slug).

Each ADR records the **context**, the **decision**, and its **consequences** in a short,
lightweight format (see [`template.md`](template.md)). ADRs are immutable once Accepted;
a decision is changed by writing a new ADR that supersedes the old one rather than by
editing history. The process is documented in [`README.md`](README.md).

We are seeding the directory with records for architectural decisions already made and
documented elsewhere (ADRs 0002–0007), so the practice starts with real content rather
than an empty convention.

## Consequences

- The rationale behind significant decisions has a single, version-controlled home that
  outlives individual design docs and PRs.
- Reviewers gain a lightweight place to record "why" during normal development; the cost
  is one short file per significant decision.
- The team must remember to write an ADR when a decision is architecturally significant.
  The [`README.md`](README.md) gives the "when to write one" bar to keep this from
  degrading into either noise or neglect.
- ADRs are additive and immutable, so the decision log only grows; superseded records
  stay in place with a pointer forward, preserving the full history.
