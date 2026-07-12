# Spawn ergonomics: frontmatter / agent-template support — Design

**Ticket:** caliban-ai/prospero#6 (`kind/feature`). Design settled by inspection —
the caliban wire already carries the field; this is pure plumbing (no fork).

## Problem

Caliban's `SpawnSpec` has `frontmatter_path: Option<PathBuf>` (a path to an agent
template / frontmatter markdown), and prospero's wire type
(`crates/core/src/caliband/wire.rs::SpawnSpec`) already includes it. But prospero
**hardcodes `frontmatter_path: None`** everywhere it builds a `SpawnSpec`
(`fleet.rs::SpawnRequest::into_spec`, testkit, client tests) — there is no way for
an operator to select a template through the CLI `spawn` or the API.

## Change (thread the existing field through 3 layers)

1. **`SpawnRequest`** (`crates/core/src/fleet.rs`): add
   `frontmatter_path: Option<PathBuf>`; `None` in `SpawnRequest::new`; in
   `into_spec` pass `self.frontmatter_path` instead of the hardcoded `None`.
2. **`SpawnBody`** (`crates/api/src/dto.rs`): add
   `#[serde(default)] frontmatter_path: Option<String>`; `into_request` maps it to
   `Option<PathBuf>` (`.map(PathBuf::from)`).
3. **CLI `spawn`** (`crates/cli`): add `--frontmatter <PATH>` → set
   `SpawnBody.frontmatter_path`.

No behavior change when unset (`None` → identical to today). No new validation of
the path here — caliband owns resolving/validating the template (prospero just
forwards the operator's choice), matching how `model`/`tool_allowlist` flow.

## Testing

- **Core:** `SpawnRequest { frontmatter_path: Some("/t.md"), .. }.into_spec()`
  yields `SpawnSpec.frontmatter_path == Some("/t.md")` (unit test in fleet.rs).
- **API:** `SpawnBody` with `"frontmatter_path":"/t.md"` → `into_request()` carries
  it as a `PathBuf` (dto.rs test); default-absent stays `None`.
- **CLI:** `spawn --frontmatter /t.md` puts it in the request body (extend an
  existing CLI spawn test / e2e).
- Existing spawn wire golden tests (`wire.rs`) already assert the field's presence
  (currently `null`); unchanged.

## Acceptance

Operators can select an agent template via `spawn --frontmatter <path>` (CLI) or
`frontmatter_path` (API body); it reaches caliband's `SpawnSpec.frontmatter_path`.
Unset ⇒ unchanged behavior.
