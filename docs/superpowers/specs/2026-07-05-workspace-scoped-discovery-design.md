# Design: workspace-scoped discovery — first-class Workspace model — prospero #72

- **Date:** 2026-07-05
- **Issue:** caliban-ai/prospero#72 · epic #274 (k8s), P1/P2 · counterpart to caliban **#281** (workspace-scoped caliband, ADR 0052)
- **Contract to mirror:** `caliban/crates/caliban-supervisor/src/runtime.rs` (`workspace_hash`, `workspace_socket_path`), `.../src/sources.rs` (`Source`, `discover_sources`), `caliband --workspace-root` (with `--repo-root` alias)

## Problem

Prospero's caliband discovery/identity is **per-repo**: `discovery.rs` derives
`hash16(repo_root)` from a single repo root and each registered repo gets its own
caliband. Caliban #281 (merged) generalized caliband to be **workspace-scoped** —
one daemon per **workspace root** supervising **1..N source checkouts**, keyed by
`hash(workspace_root)`. The identity rule is **duplicated** across the two repos
with no shared crate (caliban `runtime.rs` / prospero `discovery.rs`), so prospero
must move in lockstep or it will spawn one caliband *per repo* instead of driving
the single workspace-scoped daemon — mis-keying and failing to find it for any
workspace holding ≥2 sources.

**Already aligned:** prospero's `hash16(path)` = `hex(sha256(path))[..16]` is
**byte-identical** to caliban's `workspace_hash`. The hashing algorithm does **not**
change — only its *input* (repo root → workspace root) and the surrounding model.

## Scope (decided: first-class Workspace model)

**Ship:** a first-class `Workspace`/`Source` model in `prospero-core` —
registry, discovery (workspace-root keying + source enumeration), fleet
(one caliband per workspace, observe agents across sources), and DTO exposure —
end-to-end, with full back-compat.

**Deferred (tickets confirmed to exist):**
- Rich per-source **dashboard UI** → prospero **#5** (richer dashboard).
- Source-aware **spawn routing** (route a spawn to a named source) → caliban
  **#324** (`--source` flag / `SpawnSpec.source` machinery). `SpawnSpec` has no
  `source` field on the wire yet; prospero rides on #324 once it lands.

**Coordination:** #64 (K8sFleet, merged) added a separate `crates/core/src/k8s/`
module and only lightly touched `fleet.rs`/`testkit.rs`; it did **not** refactor
the registry/model, so this refactor is clear of it.

## Architecture

Units, each independently testable:

### 1. `Source` + `discover_sources` (`caliband/sources.rs`, new)

Mirror caliban's `sources.rs` so both sides agree on source identity:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Source {
    pub name: String,   // directory basename
    pub path: PathBuf,  // absolute source checkout path
}

/// Git checkouts that are immediate children of `workspace_root` (each a dir
/// containing `.git`); or, when `workspace_root` is itself a checkout, the root
/// as the single source. Names are directory basenames. Deterministic order.
pub fn discover_sources(workspace_root: &Path) -> Vec<Source>;
```

Matches caliban's rule exactly (immediate-child checkouts, or root-as-source for
single-repo). **Depends on:** nothing. **Consumers:** registry, fleet.

### 2. `Workspace` — registry unit (`model.rs` + `registry.rs`)

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Workspace {
    pub root: PathBuf,                 // canonical workspace root (discovery key)
    pub sources: Vec<Source>,          // 1..N discovered sources
    pub config: RepoProviderConfig,    // unchanged provider config
}
```

`Registry` is keyed by workspace **name** (as repos are today). **Persistence
back-compat:** the stored registry currently holds repo entries
(`{name, root, config}`). Deserialize a legacy entry as a **single-source
workspace** — `root` = the repo root, `sources = discover_sources(root)`
(which yields the root-as-source) — so existing on-disk registries load
unchanged, no migration step, no data loss. New entries serialize the workspace
shape. (Serde: accept both via `#[serde(default)]`/a small `From<legacy>` read
path; document the on-disk compatibility in `registry.rs`.)

### 3. Discovery keys on the workspace root (`discovery.rs`)

Rename `repo_root` → `workspace_root` throughout (`hash16` **unchanged**).
`control_socket_path(workspace_root, env)`, `resolve_socket(workspace_root, env)`,
`canonical_root` (the #45 symlink-canonicalization fix) all preserved verbatim,
just re-typed/renamed to "workspace". `ensure_caliband` spawns
`caliband --workspace-root <canonical>` (caliban accepts `--repo-root` as an
alias, so this is safe either way). Doc comment updated to name the
workspace-root rule and cite caliban #281 / ADR 0052.

### 4. Fleet: one caliband per workspace (`fleet.rs`)

- `add_workspace(name, root)` → canonicalize → `discover_sources` → register a
  `Workspace` + persist → trigger the immediate poll (as `add_repo` does today).
- `add_repo(name, root)` **kept as back-compat sugar**: registers a
  single-source workspace whose root is the repo root — byte-identical hash and
  behavior to today. Existing callers/tests are untouched.
- `client_for(workspace)` resolves/dials the single caliband keyed by
  `hash16(workspace.root)` (Unix today; the #71 network path stays available via
  the existing `caliband_network` seam).
- Poll / `list` / snapshot observe **all** agents the one caliband supervises;
  agents span the workspace's sources and all surface through the single control
  socket. `Agent.repo` (the registry key) becomes the **workspace** name.
  **Per-agent source attribution** (which of the N sources an agent runs in)
  would require adding caliban's `working_dir` to prospero's wire `AgentRecord`;
  that is **out of scope** here (a dashboard concern → #5) — #72 attributes
  agents to the *workspace*, and exposes the workspace's source list (§5). No new
  wire field. Routing a spawn *to* a source is #324.

### 5. API/DTO exposure (`api/dto.rs`, minimal)

The fleet-snapshot DTO gains the workspace's `sources` (name + path), so a
consumer can see the 1..N repos under a workspace. Existing endpoints and their
shapes keep working — a workspace name addresses it exactly as a repo name did.
No new endpoints; no dashboard UI change (that depth is #5).

## Data flow

`add_workspace(root)` → `discover_sources` → `Workspace` in `Registry` (persisted)
→ poll: `client_for(workspace)` dials `hash16(root)` caliband → `list` returns
agents across all sources → snapshot/DTO expose workspace + sources + agents.
Single-source workspace = today's path, unchanged.

## Error handling

- `discover_sources` on a non-existent / non-checkout root → empty `Vec`
  (caller surfaces "no sources"); never panics.
- Discovery/dial failures unchanged (`CoreError::Discovery` /
  `CalibandUnreachable`), now phrased in workspace terms.
- Legacy registry entry that fails the workspace read → surfaced as a registry
  load error, not a silent drop.

## Testing strategy (TDD)

1. **`discover_sources`** — 0 checkouts (empty), 1 (root-as-source), ≥2
   (immediate-child checkouts); deterministic order; matches caliban's rule on a
   shared fixture layout.
2. **Discovery parity** — `hash16(workspace_root)` unchanged; a single-source
   workspace root hashes identically to the old per-repo hash (pin the value,
   mirroring caliban's `workspace_hash_matches_legacy_repo_hash_for_same_path`).
3. **Registry migration** — a legacy `{name, root, config}` on-disk entry loads
   as a single-source workspace; a workspace entry round-trips.
4. **Fleet** — `add_workspace` with a ≥2-source fixture registers all sources;
   `add_repo` still yields a single-source workspace with identical behavior.
5. **Conformance / `FakeCaliband`** — a ≥2-source workspace driven through one
   control socket: agents from both sources observed via one `list`; back-compat
   suite stays green.

## Consequences

- **Positive:** prospero identity matches caliban #281 exactly (no mis-keying);
  one caliband per workspace across N repos; the model reads cleanly for the k8s
  "Pod = one workspace" orientation; single-source installs unchanged.
- **Negative:** touches the registry/model/DTO layer (blast radius), and the
  `Agent.repo`→workspace-name reinterpretation must be applied consistently.
  Persistence carries a legacy read path until old registries age out.
- **Revisit if:** prospero needs per-source addressing beyond observation (routing
  spawns to a source) — that arrives with caliban #324; or if a shared
  identity crate is introduced to end the caliban/prospero hash duplication.
