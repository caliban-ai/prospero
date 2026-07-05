# k8s Session-Plane TLS/token + kubeconfig Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans (or subagent-driven-development) to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Thread session-plane TLS + bearer token and explicit kubeconfig selection into prosperod's `PROSPERO_FLEET=k8s` arm, via `K8sFleet::with_network`, with no behavior change when unset.

**Architecture:** Add four `Option` CLI args (with env fallbacks). Extract two pure helpers (`read_token_file`, `load_session_plane_tls`) that carry unit tests; `main`'s k8s arm resolves args → calls helpers → `with_network`. A third helper `build_kube_client` selects explicit-file vs infer.

**Tech Stack:** Rust 2024, clap, anyhow, kube, `prospero_core::caliband::transport::{TlsClient, tls_client_from_pem}`. Tests use `rcgen` + `tempfile`.

## Global Constraints

- Local gate before any push (CLAUDE.md): `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets $TESTKIT -- -D warnings`, `cargo build --workspace --all-targets $TESTKIT`, `cargo test --workspace $TESTKIT`.
- After Task 1, `$TESTKIT` = `--features prospero-core/testkit,prospero-core/k8s,prospero-api/k8s,prospero-daemon/k8s`.
- Backward compatibility: unset knobs ⇒ `with_network(None, None)`, identical to today.
- No mTLS, no hot-reload, no per-agent tokens (YAGNI per spec).
- `read_token_file` is NOT feature-gated; `load_session_plane_tls` + kube wiring ARE `#[cfg(feature = "k8s")]`.

---

### Task 1: CI + daemon deps carry the k8s daemon feature

**Files:**
- Modify: `.github/workflows/ci.yml:27`
- Modify: `crates/daemon/Cargo.toml` (add `[dev-dependencies]`)

**Interfaces:**
- Produces: CI now builds/tests `prospero-daemon/k8s`; daemon tests can use `rcgen` + `tempfile`.

- [ ] **Step 1: Add `prospero-daemon/k8s` to the CI feature set**

Edit `.github/workflows/ci.yml` line 27:

```yaml
  TESTKIT: "--features prospero-core/testkit,prospero-core/k8s,prospero-api/k8s,prospero-daemon/k8s"
```

- [ ] **Step 2: Add daemon dev-dependencies for the TLS-helper tests**

Append to `crates/daemon/Cargo.toml`:

```toml
[dev-dependencies]
tempfile.workspace = true
rcgen.workspace = true
```

- [ ] **Step 3: Verify the workspace still builds with the new feature**

Run: `cargo build --workspace --all-targets --features prospero-core/testkit,prospero-core/k8s,prospero-api/k8s,prospero-daemon/k8s`
Expected: PASS (daemon now compiled with `k8s`).

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/ci.yml crates/daemon/Cargo.toml
git commit -m "ci(daemon): build/test prospero-daemon/k8s; add rcgen/tempfile dev-deps (#82)"
```

---

### Task 2: `read_token_file` helper (not feature-gated)

**Files:**
- Modify: `crates/daemon/src/main.rs` (add fn + tests)

**Interfaces:**
- Produces: `fn read_token_file(path: &Path) -> anyhow::Result<String>` — reads a file, trims trailing whitespace/newline. Used by the k8s arm.

- [ ] **Step 1: Write the failing tests**

Add to the `#[cfg(test)] mod tests` in `crates/daemon/src/main.rs` (and `use super::read_token_file;`):

```rust
#[test]
fn read_token_file_trims_trailing_newline() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("token");
    std::fs::write(&p, "s3cr3t\n").unwrap();
    assert_eq!(read_token_file(&p).unwrap(), "s3cr3t");
}

#[test]
fn read_token_file_missing_is_err() {
    let dir = tempfile::tempdir().unwrap();
    assert!(read_token_file(&dir.path().join("nope")).is_err());
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p prospero-daemon read_token_file`
Expected: FAIL to compile (`read_token_file` not defined).

- [ ] **Step 3: Implement**

Add near the other free fns in `crates/daemon/src/main.rs` (add `use std::path::Path;` if not present — note `PathBuf` is already imported):

```rust
/// Read a bearer token from a mounted-Secret file, trimming the trailing
/// whitespace/newline that Secret files commonly carry. A missing or
/// unreadable path is fatal — a silently-empty token would defeat auth.
fn read_token_file(path: &Path) -> anyhow::Result<String> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("reading session-plane token file {}", path.display()))?;
    Ok(raw.trim_end().to_string())
}
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p prospero-daemon read_token_file`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add crates/daemon/src/main.rs
git commit -m "feat(daemon): read_token_file helper for session-plane token (#82)"
```

---

### Task 3: `load_session_plane_tls` helper (k8s-gated)

**Files:**
- Modify: `crates/daemon/src/main.rs` (add fn + tests)

**Interfaces:**
- Consumes: `prospero_core::caliband::transport::{TlsClient, tls_client_from_pem}`.
- Produces: `#[cfg(feature = "k8s")] fn load_session_plane_tls(ca_file: Option<&Path>, server_name: &str) -> anyhow::Result<Option<TlsClient>>`.

- [ ] **Step 1: Write the failing tests**

Add to `#[cfg(test)] mod tests`, in a k8s-gated inner block so the non-k8s build still compiles:

```rust
#[cfg(feature = "k8s")]
mod k8s_tls {
    use super::super::load_session_plane_tls;

    fn write_ca(dir: &std::path::Path) -> std::path::PathBuf {
        // A self-signed cert doubles as its own CA for trust-store loading.
        let cert = rcgen::generate_simple_self_signed(vec!["caliband".into()]).unwrap();
        let p = dir.join("ca.crt");
        std::fs::write(&p, cert.cert.pem()).unwrap();
        p
    }

    #[test]
    fn none_ca_means_tls_off() {
        assert!(load_session_plane_tls(None, "caliband").unwrap().is_none());
    }

    #[test]
    fn good_ca_builds_a_client() {
        let dir = tempfile::tempdir().unwrap();
        let ca = write_ca(dir.path());
        assert!(load_session_plane_tls(Some(&ca), "caliband").unwrap().is_some());
    }

    #[test]
    fn unparseable_pem_is_err() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("bad.crt");
        std::fs::write(&p, "not a pem").unwrap();
        assert!(load_session_plane_tls(Some(&p), "caliband").is_err());
    }

    #[test]
    fn missing_ca_file_is_err() {
        let dir = tempfile::tempdir().unwrap();
        assert!(load_session_plane_tls(Some(&dir.path().join("nope")), "caliband").is_err());
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p prospero-daemon --features k8s k8s_tls`
Expected: FAIL to compile (`load_session_plane_tls` not defined).

- [ ] **Step 3: Implement**

Add to `crates/daemon/src/main.rs`, gated:

```rust
/// Build client-side session-plane TLS from a CA file, when one is configured.
/// `None` ⇒ TLS stays off (plaintext, unchanged). A good PEM ⇒ `Some(client)`
/// trusting that CA and validating the server presents `server_name`. An
/// unreadable file or unparseable PEM is fatal (fail fast — no silent plaintext
/// fall-back).
#[cfg(feature = "k8s")]
fn load_session_plane_tls(
    ca_file: Option<&Path>,
    server_name: &str,
) -> anyhow::Result<Option<prospero_core::caliband::transport::TlsClient>> {
    let Some(ca_file) = ca_file else {
        return Ok(None);
    };
    let ca_pem = std::fs::read(ca_file)
        .with_context(|| format!("reading session-plane CA file {}", ca_file.display()))?;
    let client = prospero_core::caliband::transport::tls_client_from_pem(&ca_pem, server_name)
        .with_context(|| format!("building session-plane TLS from {}", ca_file.display()))?;
    Ok(Some(client))
}
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p prospero-daemon --features k8s k8s_tls`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add crates/daemon/src/main.rs
git commit -m "feat(daemon): load_session_plane_tls helper (CA file -> TlsClient) (#82)"
```

---

### Task 4: `build_kube_client` helper + CLI args + wire the k8s arm

**Files:**
- Modify: `crates/daemon/src/main.rs` (Args fields, k8s arm, `build_kube_client`)

**Interfaces:**
- Consumes: `read_token_file`, `load_session_plane_tls` (Tasks 2–3); `prospero_core::K8sFleet::with_network`.
- Produces: the k8s arm calls `with_network(tls, token)`; `build_kube_client` selects file-vs-infer.

- [ ] **Step 1: Add the four CLI args to `struct Args`**

Insert after the `fleet_backend` field (keep the k8s knobs together):

```rust
    /// k8s only: PEM CA bundle trusting caliband's session-plane serving cert.
    /// When set, per-agent dials use TLS; unset ⇒ plaintext (unchanged).
    #[arg(long, env = "PROSPERO_K8S_CALIBAND_CA_FILE")]
    k8s_caliband_ca_file: Option<PathBuf>,

    /// k8s only: file holding the session-plane bearer token (contents trimmed).
    /// When set, per-agent dials present the token; unset ⇒ no token.
    #[arg(long, env = "PROSPERO_K8S_CALIBAND_TOKEN_FILE")]
    k8s_caliband_token_file: Option<PathBuf>,

    /// k8s only: SNI / cert-validation name for the session-plane TLS check.
    #[arg(long, env = "PROSPERO_K8S_CALIBAND_SERVER_NAME", default_value = "caliband")]
    k8s_caliband_server_name: String,

    /// k8s only: explicit kubeconfig file. Unset ⇒ infer (in-cluster, then
    /// ambient kubeconfig).
    #[arg(long, env = "KUBECONFIG")]
    kubeconfig: Option<PathBuf>,
```

- [ ] **Step 2: Add `build_kube_client` (k8s-gated)**

Add near the other helpers:

```rust
/// Build a `kube::Client`: from an explicit kubeconfig file when `kubeconfig`
/// is set, else `try_default()` (infers in-cluster then ambient kubeconfig).
#[cfg(feature = "k8s")]
async fn build_kube_client(kubeconfig: Option<&Path>) -> anyhow::Result<kube::Client> {
    match kubeconfig {
        Some(path) => {
            let kc = kube::config::Kubeconfig::read_from(path)
                .with_context(|| format!("reading kubeconfig {}", path.display()))?;
            let cfg = kube::Config::from_custom_kubeconfig(kc, &kube::config::KubeConfigOptions::default())
                .await
                .with_context(|| format!("loading kubeconfig {}", path.display()))?;
            kube::Client::try_from(cfg).with_context(|| "building kube client from kubeconfig")
        }
        None => kube::Client::try_default()
            .await
            .with_context(|| "connecting to the Kubernetes API server"),
    }
}
```

- [ ] **Step 3: Rewire the k8s arm to use the helpers + `with_network`**

Replace the `FleetBackend::K8s` match arm body (main.rs ~244-252) with:

```rust
        #[cfg(feature = "k8s")]
        FleetBackend::K8s => {
            let client = build_kube_client(args.kubeconfig.as_deref()).await?;
            let ns =
                std::env::var("PROSPERO_K8S_NAMESPACE").unwrap_or_else(|_| "default".to_string());
            let api = prospero_core::KubeTaskApi::new(client, &ns);

            let tls = load_session_plane_tls(
                args.k8s_caliband_ca_file.as_deref(),
                &args.k8s_caliband_server_name,
            )?;
            let token = args
                .k8s_caliband_token_file
                .as_deref()
                .map(read_token_file)
                .transpose()?;

            let k8s = prospero_core::K8sFleet::new(api, manager.bus(), manager.store())
                .with_network(tls.clone(), token.clone());
            tracing::info!(
                target: "prosperod", backend = "k8s", namespace = %ns,
                session_tls = tls.is_some(), session_token = token.is_some(),
                "serving via K8sFleet"
            );
            (Arc::new(k8s), None)
        }
```

- [ ] **Step 4: Ensure `use std::path::Path;` is present**

`PathBuf` is already imported (main.rs:7). Add `Path` if the helpers need it — change the import to `use std::path::{Path, PathBuf};`.

- [ ] **Step 5: Run the full gate**

Run: `cargo fmt --all && cargo clippy --workspace --all-targets --features prospero-core/testkit,prospero-core/k8s,prospero-api/k8s,prospero-daemon/k8s -- -D warnings && cargo test --workspace --features prospero-core/testkit,prospero-core/k8s,prospero-api/k8s,prospero-daemon/k8s`
Expected: PASS (all helpers tested; k8s arm compiles).

- [ ] **Step 6: Sanity-check `--help` lists the new flags**

Run: `cargo run -p prospero-daemon --features k8s -- --help`
Expected: `--k8s-caliband-ca-file`, `--k8s-caliband-token-file`, `--k8s-caliband-server-name`, `--kubeconfig` present.

- [ ] **Step 7: Commit**

```bash
git add crates/daemon/src/main.rs
git commit -m "feat(daemon): wire session-plane TLS/token + kubeconfig into k8s arm (#82)"
```

---

### Task 5: Document the new knobs

**Files:**
- Modify: `docs/container.md` (Fleet backends / k8s section)

**Interfaces:** none (docs only).

- [ ] **Step 1: Document the four env/flags**

In `docs/container.md`, under the k8s fleet-backend section, add a short table
of the four knobs (env names + purpose) and a note that CA/token come from a
mounted Secret volume, TLS is off unless `--k8s-caliband-ca-file` is set, and
kubeconfig defaults to in-cluster/ambient infer. (Match the file's existing
heading style; if `docs/container.md` has no k8s section yet, add one titled
"k8s session-plane security".)

- [ ] **Step 2: Commit**

```bash
git add docs/container.md
git commit -m "docs(container): document k8s session-plane TLS/token + kubeconfig knobs (#82)"
```

---

## Self-Review

- **Spec coverage:** TLS on/off (Task 3 + 4), token trim (Task 2), kubeconfig select (Task 4), fail-fast errors (Tasks 2–3 tests), backward-compat `with_network(None,None)` (Task 4 arm), CI feature-gating (Task 1), docs (Task 5). All covered.
- **Placeholders:** none — every step has concrete code/commands.
- **Type consistency:** `read_token_file(&Path)->Result<String>`, `load_session_plane_tls(Option<&Path>,&str)->Result<Option<TlsClient>>`, `build_kube_client(Option<&Path>)->Result<kube::Client>` used consistently across tasks. `with_network(Option<TlsClient>, Option<String>)` matches `crates/core/src/k8s/fleet.rs:504`.
- **kube API note:** if `Kubeconfig::read_from` / `from_custom_kubeconfig` signatures differ in the pinned `kube` version, adjust to the available constructor (e.g. `Config::from_kubeconfig` with `KubeConfigOptions`) — the intent is "explicit file when given, else `try_default`". Verify against `cargo doc`/the lockfile during Task 4.
