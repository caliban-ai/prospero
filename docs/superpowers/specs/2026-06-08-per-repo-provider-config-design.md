# Per-repo provider / environment config

**Date:** 2026-06-08
**Status:** Approved design, ready for implementation plan
**Branch:** lands on `dashboard-control-plane` (same PR as the dashboard control-plane controls — both features ship together)
**Scope:** Let an operator configure each managed repo's caliband environment (provider, base-URL/host, an API-key *reference*, and arbitrary env), with a prosperod-level global default underneath — so agents reach the intended model backend without editing shell environments or caliban config by hand.

## 1. Problem

caliban resolves its provider and provider settings from the **process environment** at the
daemon level (`CALIBAN_PROVIDER`, `OLLAMA_BASE_URL`, `ANTHROPIC_API_KEY`, …) — none of it is
part of the per-spawn `SpawnSpec`. Prospero autostarts caliband inheriting only prosperod's
bare environment (`discovery.rs:120`, `Command::spawn()` with no per-repo env), so every
repo's agents use whatever ambient environment prosperod happened to launch with. There is no
way to say "this repo's agents talk to the ollama server at `http://192.168.1.240:11434`."

This was discovered when an agent launched against a repo whose default model lived only on a
**remote** ollama; caliban defaulted to localhost, the model was absent, and the agent hung in
`spawning` forever. The fix had to be done by hand (relaunching prosperod with
`OLLAMA_BASE_URL` set), which is exactly the gap this feature closes.

## 2. Decisions (settled during brainstorming)

- **Granularity:** per-repo. The caliband daemon is per-repo; its environment is the natural
  home for provider/host config. (Model stays per-spawn via `SpawnSpec.model`.)
- **Env, not caliband args:** caliband's only args (`--repo-root`, `--socket-path`,
  `--data-base`) are Prospero-managed; exposing them would desync discovery. Config is
  expressed as environment variables only.
- **Config shape:** curated fields (provider, base-URL, API-key reference) **plus** a raw
  env-map escape hatch.
- **Secrets:** stored **as a reference** — `api_key_from_env` holds an env-var *name*,
  resolved from prosperod's own environment at spawn time. No literal secret values are
  written to `registry.json`.
- **Editability:** config is editable after registration; saving an edit **auto-restarts**
  that repo's caliband daemon to apply (UI guards with a confirm when agents are running).
- **Global default:** a prosperod-level default env is merged **under** each repo's config
  (repo keys win on conflict).

## 3. Data model (`crates/core/src/registry.rs`)

Each repo entry gains a `config`, defaulting empty (so existing `registry.json` files load
unchanged):

```rust
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepoProviderConfig {
    /// Selected provider → CALIBAN_PROVIDER.
    #[serde(default)]
    pub provider: Option<String>,
    /// Provider base URL / host → {PROVIDER}_BASE_URL.
    #[serde(default)]
    pub base_url: Option<String>,
    /// NAME of an env var in prosperod's environment whose value is injected
    /// as {PROVIDER}_API_KEY at spawn time. Never the literal secret.
    #[serde(default)]
    pub api_key_from_env: Option<String>,
    /// Raw escape-hatch env overrides (highest precedence within a repo).
    #[serde(default)]
    pub env: BTreeMap<String, String>,
}
```

The prosperod-level global default is a `BTreeMap<String,String>` carried on `FleetConfig`,
populated from a repeatable prosperod CLI flag `--default-env KEY=VAL` (parsed in the daemon's
`main.rs`). It is process-level config, not persisted to `registry.json`. **No secret values
are persisted** — only env-var names.

## 4. Env resolution — new pure module `crates/core/src/provider_env.rs`

The core logic, isolated and fully unit-testable. One pure function:

```rust
/// Resolve the environment overlay for a repo's caliband daemon.
/// `process_env` is prosperod's own environment (for api_key_from_env lookups).
pub fn resolve_env(
    default_env: &BTreeMap<String, String>,
    cfg: &RepoProviderConfig,
    process_env: &dyn Fn(&str) -> Option<String>,
) -> BTreeMap<String, String>
```

Layered lowest → highest:

1. **global default** (`default_env`)
2. **curated**, derived from `cfg`:
   - `provider` → `CALIBAN_PROVIDER = provider`
   - `base_url` → `{PROVIDER}_BASE_URL = base_url` (per the mapping table below)
   - `api_key_from_env` → look up `process_env(name)`; if `Some(v)` → `{PROVIDER}_API_KEY = v`;
     if `None`, skip and emit a `WARN` (the reference is dangling)
3. **raw `cfg.env`** (wins on conflict)

Repo config (curated + raw) always overrides the global default.

**Provider → env-var mapping table** (the one point of caliban coupling, kept in this module):

| provider  | base-URL var        | api-key var          |
|-----------|---------------------|----------------------|
| ollama    | `OLLAMA_BASE_URL`   | — (none)             |
| anthropic | `ANTHROPIC_BASE_URL`| `ANTHROPIC_API_KEY`  |
| openai    | `OPENAI_BASE_URL`   | `OPENAI_API_KEY`     |
| google    | `GEMINI_BASE_URL`   | `GEMINI_API_KEY`     |
| bedrock   | — (provider only)   | — (ambient AWS creds)|
| vertex    | — (provider only)   | — (ambient GCP creds)|

For a provider with no base-URL/api-key var, those curated fields are ignored (a `WARN` if
set); bedrock/vertex specifics go through the raw env map.

## 5. Spawn integration (`crates/core/src/discovery.rs`)

`EnsureConfig` gains `env: BTreeMap<String, String>` (the already-resolved overlay for this
repo). In `ensure_caliband`, the spawn becomes:

```rust
tokio::process::Command::new(&cfg.caliband_bin)
    .arg("--repo-root").arg(repo_root)
    .envs(&cfg.env)        // resolved overlay on top of the inherited process env
    .spawn()
```

`FleetManager` computes the overlay per repo via `resolve_env(...)` (passing
`std::env::var(name).ok()` as `process_env`) immediately before each `ensure_caliband` call.

## 6. Daemon-restart primitive

caliban's control protocol already has a graceful `Shutdown` request, and prosperod retains no
pid/handle for caliband — so restart is protocol-based, not signal-based.

New `FleetManager::restart_caliband(repo)`:
1. Resolve the repo's control socket; send `CtlRequest::Shutdown`.
2. Poll until the control socket is unreachable (bounded by `startup_timeout`).
3. Call `ensure_caliband` (autostart) — a fresh daemon comes up carrying the newly resolved env.

Restarting drops that repo's running agents (the accepted cost of auto-restart). The **UI**
guards the action with a confirm when the repo has running agents (§7); the backend primitive
itself just restarts.

## 7. API (`crates/api`)

- `AddRepoBody` gains an optional `config: RepoProviderConfig`. `add_repo` persists it; first
  autostart uses it.
- **New** `PUT /api/repos/{name}/config` with a `RepoProviderConfig` body → persist, then
  `restart_caliband(name)`; returns the updated repo summary.
- Fleet / repos responses include each repo's `config`. `api_key_from_env` is only a *name*, so
  it is safe to return; resolved secret values never leave prosperod's process and are never
  serialized.

## 8. UI (`crates/api/dashboard`, reusing the modal/confirm/`api()` helpers)

- **Add-repo modal** gains: a **provider** dropdown (caliban's providers), a **base-URL**
  field, an **API-key env-name** field (shown only for key-using providers), and an
  `▸ advanced` raw-env key/value editor.
- **Repo row** gains a `⚙` settings button → a **repo-settings modal** with the same fields
  prefilled from the repo's `config`. Save:
  - If the repo has running agents, `confirm("Restart caliban for <repo>? This stops N running
    agent(s).")` first.
  - `PUT /api/repos/{name}/config` → the daemon restarts; `refreshFleet()`.

## 9. Testing

- **`provider_env` unit tests** (the core): precedence (global < curated < raw), the
  provider→var mapping, `api_key_from_env` resolution including the dangling-reference WARN
  path, and provider-only providers ignoring base-URL/key.
- **Registry**: round-trip with `config`, and **backward-compat** — an old `registry.json`
  lacking `config` loads with `RepoProviderConfig::default()`.
- **Restart primitive**: a `FakeCaliband`-harness test that `Shutdown` → re-ensure yields a
  fresh daemon, and that `EnsureConfig.env` carried the resolved overlay.
- **API**: add-repo-with-config and `PUT …/config` against the harness.
- **UI**: manual (no JS test runner), per the dashboard-controls precedent.

## 10. Non-goals

- No literal secret storage (reference-only).
- No per-*agent* provider/host (caliban's `SpawnSpec` cannot carry it).
- bedrock/vertex curated to provider-selection only; their credentials remain ambient
  (AWS/GCP) or set via the raw env map.
- No change to caliban itself.
