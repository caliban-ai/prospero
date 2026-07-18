# Live-Cluster Spawn Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make live-cluster agent spawn work: return the HTTP response as soon as the CalibanTask is admitted (stop blocking on reconcile), and give the operator-launched caliband pod the bearer token + TLS it needs to bind.

**Architecture:** Two independent fixes. **Bug 1 (prospero):** `K8sFleet::ensure_agent` returns immediately after `apply`, handing back an `AgentHandle` with `endpoint: None`; the existing background watch loop surfaces the agent on the dashboard and attaches its session when it reaches Running. **Bug 2 (operator + helm-charts):** a shared session-plane bearer token (Helm Secret) + TLS serving cert (cert-manager) provisioned once in the workload namespace; the operator mounts them into the caliband pod; the prospero chart mounts the same token + CA for the dialer.

**Tech Stack:** Rust 2024 (tokio, kube 4.0, k8s-openapi v1_32, async-trait), Helm 3, cert-manager.

## Global Constraints

- **Repos:** `prospero` (`~/dev/caliban-ai/prospero`), `caliban-operator` (`~/dev/caliban-ai/caliban-operator`), `helm-charts` (`~/dev/caliban-ai/helm-charts`). Each has its own git history — commit within the repo you are editing.
- **prospero CI gate (run all four before pushing):** `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo build --workspace --all-targets`, `cargo test --workspace`. clippy denies warnings — no dead code.
- **operator CI gate:** same four commands in the `caliban-operator` workspace.
- **Shared names (must match across all three repos, verbatim):**
  - TLS Secret: `caliban-session-plane-tls` — keys `tls.crt`, `tls.key`, `ca.crt`.
  - Token Secret: `caliban-session-plane-token` — key `token`.
  - caliband TLS mount path: `/etc/caliband/tls` → files `tls.crt`, `tls.key`.
  - TLS cert SAN / prosperod SNI: **`caliband`** (prosperod's `PROSPERO_K8S_CALIBAND_SERVER_NAME` default).
- **Single namespace:** all CalibanTasks + caliband pods + both Secrets live in one workload namespace (prospero's `PROSPERO_K8S_NAMESPACE`, default `default`).
- **Sequencing:** Bug 1 (Phase A) ships alone. Bug 2 (Phases B + C) must deploy as one unit — a half-wired credential crash-loops the pod.

---

## Phase A — Bug 1: decouple spawn from reconcile (prospero)

### Task A1: Make `AgentHandle.endpoint` optional (mechanical, behavior-preserving)

**Files:**
- Modify: `crates/core/src/model.rs:26-31`
- Modify: `crates/core/src/fleet_provider.rs` (LocalFleet `ensure_agent` ~line 77-91; endpoint-assert test ~line 253-274)
- Modify: `crates/core/src/k8s/fleet.rs` (`handle_from` ~line 123-146; `to_attach` push ~line 733-740)

**Interfaces:**
- Produces: `AgentHandle { id: AgentId, workspace: String, endpoint: Option<crate::caliband::wire::Endpoint> }`. `handle_from` continues to return `Ok(Some(handle))` only for a Running CR, now with `endpoint: Some(ep)`.

- [ ] **Step 1: Change the field to optional**

In `crates/core/src/model.rs`, change the struct:

```rust
pub struct AgentHandle {
    pub id: AgentId,
    pub workspace: String,
    /// Endpoint the agent's per-agent socket is reachable at. `None` until the
    /// backend has resolved one — e.g. a k8s agent between spawn and Running.
    pub endpoint: Option<crate::caliband::wire::Endpoint>,
}
```

- [ ] **Step 2: Run the build to find every construction/read site**

Run: `cargo build --workspace --all-targets 2>&1 | rg -n "error\[|endpoint" | head -40`
Expected: FAIL — compile errors at each `AgentHandle { … endpoint: … }` construction and each `.endpoint` read. Use this list to drive the following steps.

- [ ] **Step 3: Fix `LocalFleet::ensure_agent` to wrap in `Some`**

In `crates/core/src/fleet_provider.rs`, in `LocalFleet::ensure_agent`:

```rust
        Ok(AgentHandle {
            id: AgentId::from(id),
            workspace: spec.workspace,
            endpoint: Some(endpoint),
        })
```

- [ ] **Step 4: Fix `handle_from` to wrap in `Some`**

In `crates/core/src/k8s/fleet.rs`, in `handle_from`, the `AgentHandle { … }` it returns inside `Ok(Some(…))` must set `endpoint: Some(ep)` (where `ep` is the parsed `Endpoint`). Keep every other field and all the Running/endpoint validation unchanged.

- [ ] **Step 5: Fix the watch loop `to_attach` extraction**

In `crates/core/src/k8s/fleet.rs`, the `Ok(Some(handle))` arm (~line 734) currently pushes `handle.endpoint` directly. `handle_from` guarantees `Some` here; extract it explicitly:

```rust
                        Ok(Some(handle)) => {
                            if let Some(endpoint) = handle.endpoint {
                                to_attach.push((
                                    agent.workspace.clone(),
                                    handle.id.as_str().to_string(),
                                    endpoint,
                                ));
                            }
                        }
```

- [ ] **Step 6: Fix the LocalFleet endpoint-assert test**

In `crates/core/src/fleet_provider.rs` (~line 274), the assertion becomes:

```rust
    assert_eq!(handle.endpoint, Some(expected));
```

- [ ] **Step 7: Build and test — behavior unchanged**

Run: `cargo build --workspace --all-targets && cargo test -p prospero-core`
Expected: PASS. No behavior changed — every real endpoint is now `Some(...)`.

- [ ] **Step 8: Commit**

```bash
git add crates/core/src/model.rs crates/core/src/fleet_provider.rs crates/core/src/k8s/fleet.rs
git commit -m "refactor(core): AgentHandle.endpoint is Option<Endpoint>

Prepares K8sFleet::ensure_agent to return before an endpoint exists.
Behavior-preserving: every current construction site wraps Some(...)."
```

---

### Task A2: `K8sFleet::ensure_agent` returns after apply; remove the poll budget

**Files:**
- Modify: `crates/core/src/k8s/fleet.rs` — `ensure_agent` (958-984), `PollConfig` (411-427), `new`/`with_poll_config` (801-848), `poll` field (~438-440), the two tests (1429-1489)

**Interfaces:**
- Consumes: `AgentHandle.endpoint: Option<Endpoint>` (Task A1), `task_name(&spec) -> String`, `build_calibantask(&spec, &name) -> CalibanTask`.
- Produces: `K8sFleet::ensure_agent` returns `Ok(AgentHandle { id, workspace, endpoint: None })` immediately after a successful `apply`. `K8sFleet::new(api, bus, store)` unchanged in signature; `with_poll_config` and `PollConfig` removed.

- [ ] **Step 1: Rewrite the two tests first (TDD — new contract)**

In `crates/core/src/k8s/fleet.rs`, replace `ensure_agent_returns_handle_once_running` (1429-1468) and `ensure_agent_times_out_if_never_running` (1470-1489) with these two:

```rust
    #[tokio::test]
    async fn ensure_agent_returns_immediately_without_waiting_for_running() {
        let api = MemTaskApi::new();
        let (bus, store) = test_seams();
        let fleet = K8sFleet::new(api, bus, store);

        let s = spec("repo-a", "task", None);
        let expected_name = task_name(&s);

        // The CR is never flipped to Running. ensure_agent must still return.
        let handle = fleet.ensure_agent(s).await.expect("ensure_agent");

        assert_eq!(handle.id, AgentId::from(expected_name.clone()));
        assert_eq!(handle.workspace, "repo-a");
        // No endpoint yet — the pod isn't Running.
        assert_eq!(handle.endpoint, None);
        // The CR was applied (admission happened synchronously).
        assert!(fleet.api.get(&expected_name).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn ensure_agent_does_not_block_on_a_never_running_cr() {
        let api = MemTaskApi::new();
        let (bus, store) = test_seams();
        let fleet = K8sFleet::new(api, bus, store);

        // A generous ceiling: the old code blocked ~30s here. The new code
        // returns in well under a second regardless of CR phase.
        let out = tokio::time::timeout(
            Duration::from_secs(2),
            fleet.ensure_agent(spec("repo-a", "task", None)),
        )
        .await;
        assert!(out.is_ok(), "ensure_agent must not block on Running");
        out.unwrap().expect("ensure_agent");
    }
```

- [ ] **Step 2: Run the tests to verify they fail to compile**

Run: `cargo test -p prospero-core k8s::fleet::tests::ensure_agent 2>&1 | rg -n "error|with_poll_config" | head`
Expected: FAIL — the tests reference the new contract; the old `with_poll_config`/poll loop is still present, and `ensure_agent` still blocks (the second test would hang → the 2s timeout guard makes it fail rather than hang forever).

- [ ] **Step 3: Rewrite `ensure_agent` to return after apply**

In `crates/core/src/k8s/fleet.rs`, replace the body (958-984) with:

```rust
    async fn ensure_agent(&self, spec: TaskSpec) -> Result<AgentHandle> {
        let name = task_name(&spec);
        let repo = spec.workspace.clone();
        let ct = build_calibantask(&spec, &name);
        // `apply` runs the operator's admission webhook synchronously, so an
        // invalid workspaceRef / empty providers still fails fast here (4xx).
        // We deliberately do NOT wait for status.phase == "Running": the shared
        // watch loop (spawn_watch_loop) surfaces the agent on the dashboard and
        // #113-attaches its session when the pod becomes Running. Blocking here
        // would couple the HTTP response to full reconcile latency (Bug 1).
        self.api.apply(&ct).await?;
        Ok(AgentHandle {
            id: AgentId::from(name),
            workspace: repo,
            endpoint: None,
        })
    }
```

- [ ] **Step 4: Remove the now-dead `PollConfig` and `poll` field**

In `crates/core/src/k8s/fleet.rs`:

1. Delete the `PollConfig` struct + its `Default` impl (411-427).
2. Delete the `poll: PollConfig` field from the `K8sFleet` struct (~438-440) and its doc comment.
3. Delete `with_poll_config` (806-848) and fold its body into `new`:

```rust
    #[must_use]
    pub fn new(api: A, bus: Arc<dyn EventBus>, store: Arc<dyn Store>) -> Self {
        let api = Arc::new(api);
        let known: KnownAgents = Arc::new(Mutex::new(HashMap::new()));
        let (changes, _) = tokio::sync::broadcast::channel::<FleetChange>(256);
        let session = SessionPlane {
            emitter: Emitter::new(bus, store),
            tls: None,
            token: None,
            attached: Arc::new(Mutex::new(HashMap::new())),
            ownership: Arc::new(SelfOwnsAll),
            generation: Arc::new(AtomicU64::new(0)),
        };
        let poll_task = spawn_watch_loop(
            Arc::clone(&api),
            Arc::clone(&known),
            changes.clone(),
            DEFAULT_WATCH_POLL_INTERVAL,
            session.clone(),
        );
        Self {
            api,
            watch_poll_interval: DEFAULT_WATCH_POLL_INTERVAL,
            session,
            known,
            changes,
            poll_task,
            workspaces: None,
        }
    }
```

- [ ] **Step 5: Remove `Duration` import if now unused; fix any leftover references**

Run: `cargo build --workspace --all-targets 2>&1 | rg -n "error|unused|PollConfig|with_poll_config|\.poll" | head`
Expected: iterate until clean. `Duration` is still used by the watch loop and other tests, so keep it if referenced; remove only genuinely-unused imports. There must be zero references to `PollConfig`, `with_poll_config`, or `self.poll`.

- [ ] **Step 6: Run the new tests**

Run: `cargo test -p prospero-core k8s::fleet::tests::ensure_agent -- --nocapture`
Expected: PASS — both new tests green.

- [ ] **Step 7: Full gate**

Run: `cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace`
Expected: PASS. In particular `fleet_provider_conformance` (run for K8sFleet, if wired) still passes — it converges via the watch loop, not synchronous attach.

- [ ] **Step 8: Commit**

```bash
git add crates/core/src/k8s/fleet.rs
git commit -m "fix(k8s): ensure_agent returns on CR apply, not on Running (#bug1)

Spawn no longer blocks the HTTP response on the full CR -> operator
reconcile -> pod Running chain (previously a ~30s poll budget). The
watch loop surfaces the agent and attaches its session when Running.
Removes the now-dead PollConfig / poll budget."
```

---

## Phase B — Bug 2: operator injects the session-plane credentials (caliban-operator)

Work in `~/dev/caliban-ai/caliban-operator`.

### Task B1: `Settings` carries the session-plane Secret names

**Files:**
- Modify: `src/config.rs` (`Settings` 13-24, `Default` 26-36, `from_env` 41-54, test `from_env_defaults_are_neutral` 141-149)

**Interfaces:**
- Produces: `Settings` gains `session_tls_secret: String`, `session_token_secret: String`, `session_token_key: String`. Defaults: `"caliban-session-plane-tls"`, `"caliban-session-plane-token"`, `"token"`. Env: `CALIBAN_SESSION_TLS_SECRET`, `CALIBAN_SESSION_TOKEN_SECRET`, `CALIBAN_SESSION_TOKEN_KEY`.

- [ ] **Step 1: Write the failing test**

Add to the `tests` module in `src/config.rs`:

```rust
    #[test]
    fn session_plane_defaults_match_the_shared_secret_names() {
        let s = Settings::default();
        assert_eq!(s.session_tls_secret, "caliban-session-plane-tls");
        assert_eq!(s.session_token_secret, "caliban-session-plane-token");
        assert_eq!(s.session_token_key, "token");
    }
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p caliban-operator session_plane_defaults 2>&1 | rg -n "error|no field" | head`
Expected: FAIL — `no field session_tls_secret on Settings`.

- [ ] **Step 3: Add the fields, defaults, and env reads**

In `src/config.rs`, add to the `Settings` struct:

```rust
    /// Name of the shared TLS serving-cert Secret (keys tls.crt/tls.key/ca.crt).
    pub session_tls_secret: String,
    /// Name of the shared bearer-token Secret.
    pub session_token_secret: String,
    /// Key within the token Secret.
    pub session_token_key: String,
```

Add to `Default::default()`:

```rust
            session_tls_secret: "caliban-session-plane-tls".to_string(),
            session_token_secret: "caliban-session-plane-token".to_string(),
            session_token_key: "token".to_string(),
```

Add to `from_env()` (extend the doc comment's env list too):

```rust
            session_tls_secret: std::env::var("CALIBAN_SESSION_TLS_SECRET")
                .unwrap_or(d.session_tls_secret),
            session_token_secret: std::env::var("CALIBAN_SESSION_TOKEN_SECRET")
                .unwrap_or(d.session_token_secret),
            session_token_key: std::env::var("CALIBAN_SESSION_TOKEN_KEY")
                .unwrap_or(d.session_token_key),
```

Note: `from_env` reads `d.session_tls_secret` etc. after the earlier fields already moved `d`'s String fields. `d` is consumed field-by-field; order the new reads after the existing ones (as written) and it compiles because each `d.field` is a distinct move. If the borrow checker complains, clone: `.unwrap_or_else(|_| d.session_tls_secret.clone())`.

- [ ] **Step 4: Run the test**

Run: `cargo test -p caliban-operator session_plane_defaults`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/config.rs
git commit -m "feat(config): Settings carries session-plane Secret names"
```

---

### Task B2: `build_sandbox` mounts TLS + injects token, satisfying caliband's fail-closed check

**Files:**
- Modify: `src/resources.rs` — `build_sandbox` (213-270); imports (7-10) add `Volume`, `SecretVolumeSource`; add a test

**Interfaces:**
- Consumes: `Settings.session_tls_secret`, `Settings.session_token_secret`, `Settings.session_token_key` (Task B1).
- Produces: the caliband container gets args `--tls-cert /etc/caliband/tls/tls.crt --tls-key /etc/caliband/tls/tls.key`, env `CALIBAN_DAEMON_TOKEN` via `secretKeyRef`, and a read-only volume mount at `/etc/caliband/tls`; the pod gets a matching `secret` volume.

- [ ] **Step 1: Write the failing test**

Add to the `tests` module in `src/resources.rs`:

```rust
    #[test]
    fn sandbox_wires_session_plane_tls_and_token() {
        let s = Settings::default();
        let sb = build_sandbox(&task(), &resolved(), &s);
        let pod = sb.spec.pod_template.spec.unwrap();
        let c = &pod.containers[0];

        // TLS args present and pointing at the mounted files.
        let args = c.args.as_ref().unwrap();
        let cert_idx = args.iter().position(|a| a == "--tls-cert").expect("--tls-cert");
        assert_eq!(args[cert_idx + 1], "/etc/caliband/tls/tls.crt");
        let key_idx = args.iter().position(|a| a == "--tls-key").expect("--tls-key");
        assert_eq!(args[key_idx + 1], "/etc/caliband/tls/tls.key");

        // Bearer token injected by reference (never inlined).
        let env = c.env.as_ref().unwrap();
        let tok = env.iter().find(|e| e.name == "CALIBAN_DAEMON_TOKEN").expect("token env");
        let sel = tok.value_from.as_ref().unwrap().secret_key_ref.as_ref().unwrap();
        assert_eq!(sel.name, "caliban-session-plane-token");
        assert_eq!(sel.key, "token");

        // TLS Secret mounted read-only at the expected path.
        let mount = c.volume_mounts.as_ref().unwrap()
            .iter().find(|m| m.mount_path == "/etc/caliband/tls").expect("tls mount");
        assert_eq!(mount.read_only, Some(true));
        let vol = pod.volumes.as_ref().unwrap()
            .iter().find(|v| v.name == mount.name).expect("tls volume");
        assert_eq!(vol.secret.as_ref().unwrap().secret_name.as_deref(), Some("caliban-session-plane-tls"));
    }
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p caliban-operator sandbox_wires_session_plane 2>&1 | rg -n "error|--tls-cert|panicked" | head`
Expected: FAIL — no `--tls-cert` arg / no token env / no tls mount.

- [ ] **Step 3: Extend imports**

In `src/resources.rs`, add `Volume` and `SecretVolumeSource` to the `k8s_openapi::api::core::v1` import list (it already imports `EnvVar, EnvVarSource, SecretKeySelector, VolumeMount, ...`):

```rust
use k8s_openapi::api::core::v1::{
    Container, ContainerPort, EnvVar, EnvVarSource, PersistentVolumeClaimSpec, PodSpec,
    PodTemplateSpec, SecretKeySelector, SecretVolumeSource, ServiceAccount, Volume, VolumeMount,
    VolumeResourceRequirements,
};
```

- [ ] **Step 4: Wire TLS + token into `build_sandbox`**

In `src/resources.rs`, in `build_sandbox`, before building the `Container`, add:

```rust
    const TLS_MOUNT: &str = "/etc/caliband/tls";
    const TLS_VOLUME: &str = "session-tls";
```

Extend the container `args` vec (append after the existing `--listen` pair):

```rust
        args: Some(vec![
            "--workspace-root".to_string(),
            s.workspace_root.clone(),
            "--listen".to_string(),
            format!("0.0.0.0:{}", s.caliband_port),
            "--tls-cert".to_string(),
            format!("{TLS_MOUNT}/tls.crt"),
            "--tls-key".to_string(),
            format!("{TLS_MOUNT}/tls.key"),
        ]),
```

Extend the container `volume_mounts` to add the TLS mount alongside the workspace mount:

```rust
        volume_mounts: Some(vec![
            VolumeMount {
                name: WORKSPACE_VOLUME.to_string(),
                mount_path: s.workspace_root.clone(),
                ..Default::default()
            },
            VolumeMount {
                name: TLS_VOLUME.to_string(),
                mount_path: TLS_MOUNT.to_string(),
                read_only: Some(true),
                ..Default::default()
            },
        ]),
```

In `caliband_env` (or inline where `env:` is set on the container), append the token env. Simplest: push it in `build_sandbox` after `caliband_env` returns. Change `env: Some(caliband_env(t, rw))` to:

```rust
        env: Some({
            let mut e = caliband_env(t, rw);
            e.push(EnvVar {
                name: "CALIBAN_DAEMON_TOKEN".to_string(),
                value: None,
                value_from: Some(EnvVarSource {
                    secret_key_ref: Some(SecretKeySelector {
                        name: s.session_token_secret.clone(),
                        key: s.session_token_key.clone(),
                        optional: Some(false),
                    }),
                    ..Default::default()
                }),
            });
            e
        }),
```

Add the `secret` volume to `pod_spec`. The current `PodSpec { containers, init_containers, runtime_class_name, service_account_name, automount_service_account_token, .. }` has no `volumes`; add:

```rust
        volumes: Some(vec![Volume {
            name: TLS_VOLUME.to_string(),
            secret: Some(SecretVolumeSource {
                secret_name: Some(s.session_tls_secret.clone()),
                ..Default::default()
            }),
            ..Default::default()
        }]),
```

Note on `SecretKeySelector.name`: in this `k8s-openapi` version `name` is `String` (matching the existing `provider_env` usage at `resources.rs:134`). Mirror that exact shape — do not wrap in `Some`.

- [ ] **Step 5: Run the new test + the existing sandbox test**

Run: `cargo test -p caliban-operator sandbox_`
Expected: PASS — `sandbox_wires_session_plane_tls_and_token` and the unchanged `sandbox_has_caliband_container_pvc_and_service` both green (the latter only checks `--workspace-root`/`--listen` are present, which still holds).

- [ ] **Step 6: Full gate**

Run: `cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/resources.rs
git commit -m "fix(sandbox): launch caliband with session-plane token + TLS

caliband --listen fail-closes without a bearer token and TLS. Mount the
shared TLS Secret at /etc/caliband/tls (--tls-cert/--tls-key) and inject
CALIBAN_DAEMON_TOKEN by secretKeyRef so the pod can bind. Fixes the
'a non-empty bearer token is required' crash loop."
```

---

## Phase C — Bug 2: provision the shared credentials + wire both deployments (helm-charts)

Work in `~/dev/caliban-ai/helm-charts`. cert-manager must be installed in the target cluster (document as a prerequisite).

### Task C1: umbrella-chart cert-manager chain + token Secret

**Files:**
- Create: `charts/caliban-system/templates/session-plane.yaml`
- Modify: `charts/caliban-system/values.yaml` (add a `sessionPlane` block)
- Modify: `charts/caliban-system/README.md` (cert-manager prerequisite note)

**Interfaces:**
- Produces, in the release namespace: Secret `caliban-session-plane-token` (key `token`); cert-manager `Certificate` `caliban-session-plane-tls` (dnsNames `[caliband]`) whose Secret carries `tls.crt`/`tls.key`/`ca.crt`; the self-signed Issuer chain backing it.

- [ ] **Step 1: Add the values block**

In `charts/caliban-system/values.yaml`, add:

```yaml
# Shared caliband session-plane credentials (bearer token + TLS serving cert).
# Consumed by caliban-operator (mounted into caliband pods) and prospero (dialer).
sessionPlane:
  enabled: true
  tlsSecret: caliban-session-plane-tls
  tokenSecret: caliban-session-plane-token
  tokenKey: token
  # DNS name the serving cert is issued for; MUST equal prosperod's
  # PROSPERO_K8S_CALIBAND_SERVER_NAME (default "caliband").
  dnsName: caliband
```

- [ ] **Step 2: Write the template**

Create `charts/caliban-system/templates/session-plane.yaml`:

```yaml
{{- if .Values.sessionPlane.enabled }}
# --- Bearer token (generated once, preserved across upgrades) ---
{{- $tokenSecretName := .Values.sessionPlane.tokenSecret }}
{{- $existing := lookup "v1" "Secret" .Release.Namespace $tokenSecretName }}
{{- $token := "" }}
{{- if and $existing $existing.data (index $existing.data .Values.sessionPlane.tokenKey) }}
{{- $token = index $existing.data .Values.sessionPlane.tokenKey | b64dec }}
{{- else }}
{{- $token = randAlphaNum 48 }}
{{- end }}
apiVersion: v1
kind: Secret
metadata:
  name: {{ $tokenSecretName }}
  namespace: {{ .Release.Namespace }}
type: Opaque
stringData:
  {{ .Values.sessionPlane.tokenKey }}: {{ $token | quote }}
---
# --- Self-signed root issuer ---
apiVersion: cert-manager.io/v1
kind: Issuer
metadata:
  name: caliban-session-plane-selfsign
  namespace: {{ .Release.Namespace }}
spec:
  selfSigned: {}
---
# --- CA cert (stable ca.crt across serving-cert rotations) ---
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: caliban-session-plane-ca
  namespace: {{ .Release.Namespace }}
spec:
  isCA: true
  commonName: caliban-session-plane-ca
  secretName: caliban-session-plane-ca
  privateKey:
    algorithm: ECDSA
    size: 256
  issuerRef:
    name: caliban-session-plane-selfsign
    kind: Issuer
---
# --- CA-backed issuer ---
apiVersion: cert-manager.io/v1
kind: Issuer
metadata:
  name: caliban-session-plane-ca
  namespace: {{ .Release.Namespace }}
spec:
  ca:
    secretName: caliban-session-plane-ca
---
# --- caliband serving cert (SAN must equal prosperod's SNI) ---
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: {{ .Values.sessionPlane.tlsSecret }}
  namespace: {{ .Release.Namespace }}
spec:
  secretName: {{ .Values.sessionPlane.tlsSecret }}
  dnsNames:
    - {{ .Values.sessionPlane.dnsName | quote }}
  privateKey:
    algorithm: ECDSA
    size: 256
  issuerRef:
    name: caliban-session-plane-ca
    kind: Issuer
{{- end }}
```

- [ ] **Step 3: Render and verify**

Run: `helm template t charts/caliban-system --show-only templates/session-plane.yaml | rg -n "kind:|secretName:|dnsNames|token:"`
Expected: renders the token Secret, self-signed Issuer, CA Certificate, CA Issuer, and serving Certificate; `dnsNames` shows `caliband`.

- [ ] **Step 4: Add the prerequisite note + commit**

Add to `charts/caliban-system/README.md` a line under prerequisites: "Requires cert-manager installed in the cluster (issues the caliband session-plane serving certificate)."

```bash
git add charts/caliban-system/templates/session-plane.yaml charts/caliban-system/values.yaml charts/caliban-system/README.md
git commit -m "feat(caliban-system): provision shared session-plane token + TLS (cert-manager)"
```

---

### Task C2: operator subchart references the Secret names

**Files:**
- Modify: `charts/caliban-operator/templates/deployment.yaml` (env block ~33-43)
- Modify: `charts/caliban-operator/values.yaml` (add `env.sessionTlsSecret` etc.)
- Modify: `charts/caliban-system/values.yaml` (pass-through under `caliban-operator:`)

**Interfaces:**
- Consumes: Task B1's env vars `CALIBAN_SESSION_TLS_SECRET`, `CALIBAN_SESSION_TOKEN_SECRET`, `CALIBAN_SESSION_TOKEN_KEY`.

- [ ] **Step 1: Add operator values**

In `charts/caliban-operator/values.yaml`, under the existing `env:` map, add:

```yaml
  sessionTlsSecret: caliban-session-plane-tls
  sessionTokenSecret: caliban-session-plane-token
  sessionTokenKey: token
```

- [ ] **Step 2: Add the env entries to the operator deployment**

In `charts/caliban-operator/templates/deployment.yaml`, after the `CALIBAN_WORKSPACE_STORAGE` env entry (line 43), add:

```yaml
            - name: CALIBAN_SESSION_TLS_SECRET
              value: {{ .Values.env.sessionTlsSecret | quote }}
            - name: CALIBAN_SESSION_TOKEN_SECRET
              value: {{ .Values.env.sessionTokenSecret | quote }}
            - name: CALIBAN_SESSION_TOKEN_KEY
              value: {{ .Values.env.sessionTokenKey | quote }}
```

- [ ] **Step 3: Render and verify**

Run: `helm template t charts/caliban-operator | rg -n "CALIBAN_SESSION_"`
Expected: three env entries with the shared Secret names.

- [ ] **Step 4: Commit**

```bash
git add charts/caliban-operator/templates/deployment.yaml charts/caliban-operator/values.yaml
git commit -m "feat(operator-chart): pass session-plane Secret names to the operator"
```

---

### Task C3: prospero subchart mounts CA + token for the dialer (both topologies)

**Files:**
- Modify: `charts/prospero/values.yaml` (add `sessionPlane` block)
- Modify: `charts/prospero/templates/statefulset.yaml` (standalone; k8s env 45-54, volumeMounts 70-72, add volumes)
- Modify: `charts/prospero/templates/deployment.yaml` (clustered; args 34-43, env 44-57, add volumeMounts + volumes)

**Interfaces:**
- Consumes: the TLS Secret (`ca.crt`) and token Secret (`token`) from Task C1.
- Produces: prosperod (in **both** the standalone StatefulSet and the clustered Deployment) runs with the k8s fleet backend, `PROSPERO_K8S_CALIBAND_CA_FILE`, `PROSPERO_K8S_CALIBAND_TOKEN_FILE`, `PROSPERO_K8S_CALIBAND_SERVER_NAME=caliband`, and the two Secrets mounted as files. All gated on the existing `.Values.fleetBackend == "k8s"` idiom (not a new toggle).

> **Why both templates:** the chart renders `statefulset.yaml` for
> `topology=standalone` and `deployment.yaml` for `topology=clustered`. The
> StatefulSet already wires k8s fleet (`--fleet-backend` arg +
> `PROSPERO_K8S_NAMESPACE` gated on `fleetBackend`); the Deployment does **not**,
> so it also gets that wiring here, in line with the StatefulSet. A live
> clustered cluster uses the Deployment — missing this is why the fix would
> otherwise not reach the dialer there.

- [ ] **Step 1: Add prospero values**

In `charts/prospero/values.yaml`, add (leave the existing `fleetBackend` value as-is; set it to `k8s` for k8s deployments):

```yaml
# k8s fleet session-plane dial credentials (mounted from the shared Secrets).
# Consumed only when fleetBackend == "k8s".
sessionPlane:
  tlsSecret: caliban-session-plane-tls
  tokenSecret: caliban-session-plane-token
  tokenKey: token
  serverName: caliband
```

- [ ] **Step 2: Wire the StatefulSet (standalone)**

In `charts/prospero/templates/statefulset.yaml`, extend the existing k8s
`env` block (45-50) so it reads — add the three session vars inside the same
`if`:

```yaml
          env:
            {{- if eq .Values.fleetBackend "k8s" }}
            # K8sFleet reads CalibanTask CRs in this namespace (RBAC below).
            - name: PROSPERO_K8S_NAMESPACE
              value: {{ .Release.Namespace | quote }}
            - name: PROSPERO_K8S_CALIBAND_CA_FILE
              value: /etc/prospero/session-tls/ca.crt
            - name: PROSPERO_K8S_CALIBAND_TOKEN_FILE
              value: /etc/prospero/session-token/{{ .Values.sessionPlane.tokenKey }}
            - name: PROSPERO_K8S_CALIBAND_SERVER_NAME
              value: {{ .Values.sessionPlane.serverName | quote }}
            {{- end }}
```

Extend the existing container `volumeMounts` (70-72) to add the two session
mounts under the same gate:

```yaml
          volumeMounts:
            - name: data
              mountPath: /data
            {{- if eq .Values.fleetBackend "k8s" }}
            - name: session-tls
              mountPath: /etc/prospero/session-tls
              readOnly: true
            - name: session-token
              mountPath: /etc/prospero/session-token
              readOnly: true
            {{- end }}
```

Add a pod-level `volumes:` block. The StatefulSet has no `volumes:` today (its
`data` mount is a `volumeClaimTemplate`), so add one inside `spec.template.spec`,
immediately after the `containers:` list ends and before the `{{- with
.Values.nodeSelector }}` block (line 75), at 6-space indent:

```yaml
      {{- if eq .Values.fleetBackend "k8s" }}
      volumes:
        - name: session-tls
          secret:
            secretName: {{ .Values.sessionPlane.tlsSecret }}
        - name: session-token
          secret:
            secretName: {{ .Values.sessionPlane.tokenSecret }}
      {{- end }}
```

- [ ] **Step 3: Wire the Deployment (clustered)**

In `charts/prospero/templates/deployment.yaml`, bring the k8s wiring in line with
the StatefulSet. In the `args` list (34-43), after the `--host`/`{{ .Values.host }}`
pair, add the backend selector:

```yaml
            - --fleet-backend
            - {{ .Values.fleetBackend | quote }}
```

In the `env:` list, replace the current start (lines 44-53, which begins with
`PROSPERO_REPLICA_ID` and `PROSPERO_DATABASE_URL`) by inserting the k8s block
right after `env:` and before `PROSPERO_REPLICA_ID`:

```yaml
          env:
            {{- if eq .Values.fleetBackend "k8s" }}
            - name: PROSPERO_K8S_NAMESPACE
              value: {{ .Release.Namespace | quote }}
            - name: PROSPERO_K8S_CALIBAND_CA_FILE
              value: /etc/prospero/session-tls/ca.crt
            - name: PROSPERO_K8S_CALIBAND_TOKEN_FILE
              value: /etc/prospero/session-token/{{ .Values.sessionPlane.tokenKey }}
            - name: PROSPERO_K8S_CALIBAND_SERVER_NAME
              value: {{ .Values.sessionPlane.serverName | quote }}
            {{- end }}
            - name: PROSPERO_REPLICA_ID
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
```

The Deployment has no container `volumeMounts` today. Add one after `resources:`
(line 73-74), at 10-space (container) indent:

```yaml
          {{- if eq .Values.fleetBackend "k8s" }}
          volumeMounts:
            - name: session-tls
              mountPath: /etc/prospero/session-tls
              readOnly: true
            - name: session-token
              mountPath: /etc/prospero/session-token
              readOnly: true
          {{- end }}
```

Add a pod-level `volumes:` block after the `containers:` list ends and before
`{{- with .Values.nodeSelector }}` (line 75), at 6-space indent:

```yaml
      {{- if eq .Values.fleetBackend "k8s" }}
      volumes:
        - name: session-tls
          secret:
            secretName: {{ .Values.sessionPlane.tlsSecret }}
        - name: session-token
          secret:
            secretName: {{ .Values.sessionPlane.tokenSecret }}
      {{- end }}
```

- [ ] **Step 4: Render and verify both topologies**

Run:
```bash
helm template t charts/prospero --set topology=standalone --set fleetBackend=k8s \
  | rg -n "PROSPERO_K8S_CALIBAND|session-tls|session-token|mountPath|secretName"
helm template t charts/prospero --set topology=clustered --set fleetBackend=k8s \
  --set database.url=postgres://x \
  | rg -n "fleet-backend|PROSPERO_K8S_CALIBAND|session-tls|session-token|secretName"
```
Expected: both render the three `PROSPERO_K8S_CALIBAND_*` env vars, two
volumeMounts, and two secret volumes; `SERVER_NAME` is `caliband`; the token file
path ends in `/token`; the clustered render also shows `--fleet-backend k8s`.

- [ ] **Step 5: Confirm non-k8s renders are unchanged**

Run: `helm template t charts/prospero --set topology=standalone --set fleetBackend=local | rg -n "session-tls|PROSPERO_K8S_CALIBAND" || echo "clean (no session-plane wiring when local)"`
Expected: `clean` — the session-plane block is fully gated on `fleetBackend=k8s`.

- [ ] **Step 6: Commit**

```bash
git add charts/prospero/templates/deployment.yaml charts/prospero/templates/statefulset.yaml charts/prospero/values.yaml
git commit -m "feat(prospero-chart): mount session-plane CA + token for the k8s dialer

Both topologies (standalone StatefulSet, clustered Deployment) gain the
CA + token mounts and PROSPERO_K8S_CALIBAND_* env under fleetBackend=k8s;
the clustered Deployment also gains the --fleet-backend arg + namespace
env it was previously missing."
```

---

### Task C4: umbrella lint end-to-end

**Files:** none (verification), optional subchart repackage.

- [ ] **Step 1: Repackage subcharts into the umbrella if needed**

The umbrella vendors subcharts as packaged `.tgz` under
`charts/caliban-system/charts/`. If subchart edits (Tasks C2, C3) don't appear in
the umbrella render, bump the changed subchart `version:` in their `Chart.yaml`,
update the dependency pins in `charts/caliban-system/Chart.yaml`, then run
`helm dependency update charts/caliban-system` (or `helm package` each changed
subchart into that directory).

- [ ] **Step 2: Render the umbrella and verify names line up**

Run: `helm template t charts/caliban-system --set prospero.fleetBackend=k8s | rg -n "caliban-session-plane|PROSPERO_K8S_CALIBAND|CALIBAN_DAEMON_TOKEN|CALIBAN_SESSION_" | head -40`
Expected: the shared Secret names (`caliban-session-plane-tls`,
`caliban-session-plane-token`) appear consistently across the session-plane
resources (Task C1), the operator env (Task C2), and the prospero env/mounts
(Task C3); `CALIBAN_DAEMON_TOKEN` is injected on the caliband container via the
operator's Sandbox (rendered by the operator at runtime, so it may not appear in
this static render — verify on-cluster in Phase D instead).

---

## Phase D — end-to-end verification (live cluster)

Not a code task — the acceptance gate. Do this after Phases A–C are deployed together.

- [ ] **Step 1: Deploy the updated stack**

Repackage/bump subchart versions as needed, `helm dependency update charts/caliban-system`, then `helm upgrade` the release. Confirm cert-manager issued `caliban-session-plane-tls` (`kubectl get certificate -n <ns>` → Ready) and the token Secret exists.

- [ ] **Step 2: Confirm co-location (NetworkPolicy risk)**

Verify prospero runs in the **same namespace** as the CalibanTasks (`PROSPERO_K8S_NAMESPACE`). The operator's NetworkPolicy ingress is same-namespace only (`caliban-operator/src/resources.rs:64-70`). If prospero is in a different namespace, the dial is blocked even with correct credentials — file a follow-up to add a `namespaceSelector`, or co-locate.

- [ ] **Step 3: Spawn an agent and observe**

From the dashboard, spawn an agent. Verify:
- The UI returns **immediately** (Bug 1) — the agent appears as Spawning, not a multi-second freeze.
- `kubectl logs <caliband-pod>` shows **no** `a non-empty bearer token is required` error, and the pod reaches Running (Bug 2).
- The agent transitions Spawning → Running on the dashboard, and its `/stream` carries live output (prosperod attached over TLS + token).

- [ ] **Step 4: Negative check (Bug 1 isolation)**

Confirm that even if a pod crash-loops (e.g. a bad image), the spawn call still returns immediately and the agent surfaces as Spawning/Failed on the dashboard rather than hanging the UI.

---

## Self-review notes (coverage)

- Spec "Design — Bug 1" → Tasks A1, A2. Endpoint `Option` decision → A1. PollConfig removal → A2. Watch-loop unchanged → verified in A2 Step 7.
- Spec "Design — Bug 2 / operator" → Tasks B1, B2. `Settings` fields → B1. `build_sandbox` args/env/mount → B2.
- Spec "Design — Bug 2 / shared credentials (cert-manager)" → Task C1.
- Spec "Design — Bug 2 / prospero chart" → Task C3 (both topologies). Operator chart env → C2. Umbrella lint → C4.
- Spec "Testing" → tests embedded in A1/A2/B1/B2 + `helm template` checks in C1–C4 + Phase D.
- Spec "Risks / NetworkPolicy same-namespace" → Phase D Step 2. "SNI vs dnsNames" → shared name `caliband` locked in Global Constraints, C1, C3.
