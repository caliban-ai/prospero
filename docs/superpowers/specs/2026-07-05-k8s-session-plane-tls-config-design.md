# Wire k8s session-plane TLS/token + kubeconfig into prosperod — Design

**Ticket:** caliban-ai/prospero#82 (k8s epic #274). Relates #77; caliban #287/#288.

## Problem

`prosperod`'s `k8s` arm (added in #76, `crates/daemon/src/main.rs`) builds
`K8sFleet::new(api, bus, store)` — it never calls
`K8sFleet::with_network(tls, token)`. Every per-agent caliband session-plane
dial (`start_agent_stream`, `send_input`) therefore goes out **plaintext /
no-auth**, even though the transport already supports TCP + TLS + bearer-token
(ADR 0051) and `with_network` is wired and tested (#77). Kubeconfig selection
is also implicit (ambient `Client::try_default()` only) — no explicit
file-vs-in-cluster control.

This is a pure daemon **composition-edge** gap: the capability exists in
`prospero-core`; the daemon just doesn't thread the config into it.

## Goal

Under `PROSPERO_FLEET=k8s`, when configured, the agent session plane dials use
TLS + a bearer token, and the kubeconfig source is explicitly selectable — with
**no behavioral change** when the new knobs are unset (existing plaintext
deployments keep working).

## Config surface

Four new daemon CLI args, all `Option`, all with an env fallback (matching the
existing clap `#[arg(long, env = ...)]` convention). They are only consulted in
the k8s arm.

| Flag | Env | Purpose | Default |
|---|---|---|---|
| `--k8s-caliband-ca-file` | `PROSPERO_K8S_CALIBAND_CA_FILE` | PEM CA bundle to trust caliband's serving cert | unset ⇒ TLS off |
| `--k8s-caliband-token-file` | `PROSPERO_K8S_CALIBAND_TOKEN_FILE` | bearer-token file (contents trimmed) | unset ⇒ token off |
| `--k8s-caliband-server-name` | `PROSPERO_K8S_CALIBAND_SERVER_NAME` | SNI / cert-validation name | `caliband` |
| `--kubeconfig` | `KUBECONFIG` | explicit kubeconfig file | unset ⇒ `Client::try_default()` (infer) |

The CA/token arrive as **file paths** pointing at a mounted Kubernetes Secret
volume (the canonical pattern: keeps PEM out of process env listings, and a
rotation only needs the pod restart this class of config already implies).

## Behavior

- **TLS is on iff `--k8s-caliband-ca-file` is set.** When set: read the PEM,
  build a `TlsClient` via
  `prospero_core::caliband::transport::tls_client_from_pem(&ca_pem, &server_name)`.
  A missing/unreadable file or an unparseable PEM is a **fatal startup error**
  (fail fast — a silent fall-back to plaintext would defeat the point).
- **Token is on iff `--k8s-caliband-token-file` is set.** When set: read the
  file and trim trailing whitespace/newline (Secret files commonly end in `\n`).
  A missing/unreadable file is a fatal startup error.
- Both fold into `K8sFleet::new(api, bus, store).with_network(tls, token)`,
  where `tls: Option<TlsClient>` and `token: Option<String>`. Neither set ⇒
  `with_network(None, None)`, identical to today.
- **Kubeconfig:** if `--kubeconfig <path>` is set, build the `kube::Client`
  from that file; otherwise `Client::try_default()` (unchanged — infers
  in-cluster then ambient kubeconfig). The standard `KUBECONFIG` env is honored
  via the same arg.

## Decomposition (for testability)

`main`'s async k8s arm is not unit-testable (real kube client, real network).
Extract the pure, file-reading logic into small helpers in the daemon crate so
they carry their own tests:

```rust
/// Read a bearer token from a mounted-Secret file, trimming trailing
/// whitespace/newline. Fatal (`Err`) if the path can't be read.
fn read_token_file(path: &Path) -> anyhow::Result<String>;

/// Build client-side session-plane TLS from a CA file, if one is configured.
/// `None` when `ca_file` is `None`; `Ok(Some(_))` on a good PEM; `Err` on an
/// unreadable file or unparseable PEM.
#[cfg(feature = "k8s")]
fn load_session_plane_tls(
    ca_file: Option<&Path>,
    server_name: &str,
) -> anyhow::Result<Option<TlsClient>>;
```

`main`'s k8s arm becomes: resolve args → `load_session_plane_tls(...)?` →
`token = ca?/token_file.map(read_token_file).transpose()?` →
`K8sFleet::new(...).with_network(tls, token)`; and, for the client,
`build_kube_client(kubeconfig)` selecting file-vs-infer.

## Feature-gating (the #76 gotcha, do not repeat)

The daemon k8s arm — and thus `load_session_plane_tls` + its tests — is behind
`#[cfg(feature = "k8s")]`. CI's `TESTKIT`
(`.github/workflows/ci.yml:27`) is currently
`--features prospero-core/testkit,prospero-core/k8s,prospero-api/k8s` — it does
**not** include `prospero-daemon/k8s`, so the daemon k8s code is neither built
nor tested in CI today.

The plan **adds `prospero-daemon/k8s` to `TESTKIT`** so clippy/build/test all
exercise the new code, exactly as #76 did for `prospero-api/k8s`. The coverage
gate (`scripts/coverage.sh:59`, `FEATURES="prospero-core/testkit"`) stays
testkit-only; the k8s-gated daemon tests simply don't count toward coverage,
same as the api k8s tests — no coverage regression.

`read_token_file` is **not** feature-gated (plain file IO, always compiled and
tested) so its behavior is covered even in a non-k8s build.

## Scope (YAGNI)

- **No mTLS / client certs.** Server-auth TLS + bearer token only, matching
  ADR 0051's wire contract.
- **No hot-reload** of rotated secrets — read once at startup; rotation implies
  a pod restart (standard for mounted-Secret config of this class).
- **No per-agent distinct tokens** — one shared session-plane token.

## Testing

Unit tests (daemon crate, gated to match the code under test):

1. `read_token_file` trims a trailing newline: file `"tok\n"` → `"tok"`.
2. `read_token_file` on a missing path is `Err`.
3. `load_session_plane_tls(None, _)` → `Ok(None)` (TLS stays off).
4. `load_session_plane_tls(Some(good_ca), "caliband")` → `Ok(Some(_))`, using a
   self-signed CA generated with `rcgen` (added as a daemon dev-dependency) and
   written to a `tempfile`.
5. `load_session_plane_tls(Some(bad_pem_file), _)` → `Err` (fail fast).
6. `load_session_plane_tls(Some(missing_path), _)` → `Err`.

The end-to-end "dials use TLS" path is already proven by #77's
`k8s_session_plane_tcp.rs`; #82 only wires the daemon config into it, so the
daemon tests target the new pure helpers rather than re-proving the stream leg.

## Acceptance

- New flags/envs exist and are documented in `--help`.
- With `--k8s-caliband-ca-file` + `--k8s-caliband-token-file` set, the k8s arm
  builds `K8sFleet` via `with_network(Some(tls), Some(token))`.
- With none set, behavior is byte-for-byte today's (`with_network(None, None)`).
- `--kubeconfig` selects an explicit file; unset infers.
- CI builds and tests the daemon with `prospero-daemon/k8s`.
