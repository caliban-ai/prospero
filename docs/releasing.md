# Releasing prospero

prospero ships a **multi-arch container image** (`prosperod`), not crates.io
crates. A release is a version bump + changelog roll-up + tag; pushing the tag
triggers the `release-image` workflow, which builds and pushes the image.

## Versioning

Semantic versioning. Pre-1.0, the **minor** version bumps for new features and
the **patch** version for fixes. All workspace crates share one version via
`[workspace.package].version`, and internal deps are path-only, so there are no
per-crate pins to move.

## The changelog

`CHANGELOG.md` follows [Keep a Changelog](https://keepachangelog.com/). Between
releases the `## [Unreleased]` section stays empty; changes are **rolled up at
release time** into a dated `## [X.Y.Z]` section — a short narrative summary plus
`Added` / `Changed` / `Fixed` subsections, each entry linking its issue and PR.
`docs/guide/sync-changelog.sh` ingests `CHANGELOG.md` into the mdBook guide at
docs-build time (the `Ingest changelog` step in `.github/workflows/docs.yml`).

## Cutting a release

1. On a release branch, bump `[workspace.package].version` in the root
   `Cargo.toml` to `X.Y.Z`.
2. Roll up `CHANGELOG.md`: move the accumulated changes since the last tag into a
   new `## [X.Y.Z] - YYYY-MM-DD` section (summary + Added/Changed/Fixed), and
   update the compare-link refs at the bottom — point `[Unreleased]` at
   `vX.Y.Z...HEAD` and add `[X.Y.Z]` → `vPREV...vX.Y.Z`. Derive the entries from
   `git log vPREV..main --oneline`.
3. Regenerate `Cargo.lock`: `cargo update --workspace` (bumps the five workspace
   crates in the lock; no dependency changes).
4. Open a PR titled `release: vX.Y.Z — bump workspace version + changelog`,
   let CI pass, and merge.
5. Tag the merge commit and push:

   ```sh
   git checkout main && git pull --ff-only
   git tag vX.Y.Z
   git push origin vX.Y.Z
   ```

6. The **`release-image`** workflow (triggered on `v*` tags) builds the
   `prosperod` image for `linux/amd64` + `linux/arm64` on native runners and
   pushes it by digest under the semver / `latest` / sha tags.

## Not published to crates.io

Unlike its siblings [caliban](https://github.com/caliban-ai/caliban) and
[gonzalo](https://github.com/caliban-ai/gonzalo) — which publish their library
crates and keep a `publish.yml` + `scripts/publish.sh` + a crates.io
`docs/releasing.md` — prospero is an application (a control-plane daemon + CLI),
not a library, and is distributed **only** as the `prosperod` container image.
There is intentionally no crates.io pipeline. If prospero ever needs to publish
its crates, mirror caliban's `docs/releasing.md` crates.io section (org
`CARGO_REGISTRY_TOKEN` secret, tag↔version guard, resumable rate-limit-aware
publisher).
