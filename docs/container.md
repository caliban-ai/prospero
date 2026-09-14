# Container image

`ghcr.io/caliban-ai/prospero` runs the `prosperod` control plane (REST + SSE +
embedded dashboard) on port `7878`. The image binds `0.0.0.0` and defaults to
**standalone** (sqlite under `/data`) with `--no-autostart` (no caliband in this
image).

## Run — standalone

    docker run --rm -p 7878:7878 -v prospero-data:/data \
      ghcr.io/caliban-ai/prospero
    # dashboard at http://localhost:7878/ , health at /healthz , /readyz

## Run — clustered (external Postgres)

    docker run --rm -p 7878:7878 \
      -e PROSPERO_DATABASE_URL='postgres://user:pass@host:5432/prospero' \
      -e PROSPERO_REPLICA_ID="$(hostname)" \
      ghcr.io/caliban-ai/prospero --no-autostart

## Environment

| Var | Purpose | Image default |
|-----|---------|---------------|
| `PROSPERO_ADDR` | bind address | `0.0.0.0:7878` |
| `PROSPERO_DATA_DIR` | sqlite dir (standalone) | `/data` |
| `PROSPERO_DATABASE_URL` | set ⇒ clustered (Postgres) | unset ⇒ standalone |
| `PROSPERO_REPLICA_ID` | unique per replica | `HOSTNAME` |
| `PROSPERO_HOST` | fleet identity | `local` |
| `RUST_LOG` | log filter | `info` |
| `PROSPERO_API_TOKENS_FILE` | tokens file (`<name> <scope> sha256:<hex>`); set ⇒ every non-probe request needs a token | unset |
| `PROSPERO_SESSION_KEY_FILE` | ≥ 32-byte dashboard session key; **required** when clustered with tokens | unset ⇒ random per process |
| `PROSPERO_INSECURE_NO_AUTH` | `1` ⇒ serve without auth on a non-loopback bind (logged every minute) | unset |
| `PROSPERO_COOKIE_SECURE` | `1` ⇒ always mark the session cookie `Secure` | unset (Secure when `X-Forwarded-Proto: https`) |

Schema is created in-process on boot (no migration step). prosperod handles
SIGTERM for graceful shutdown.

## Authentication

The image binds `0.0.0.0`, so prosperod **refuses to start** unless you either
mount a tokens file or pass `PROSPERO_INSECURE_NO_AUTH=1`:

    prospero token new admin --scope admin      # prints the token once + a tokens-file line
    docker run --rm -p 7878:7878 -v prospero-data:/data \
      -v "$PWD/tokens:/etc/prospero/tokens:ro" \
      -e PROSPERO_API_TOKENS_FILE=/etc/prospero/tokens \
      ghcr.io/caliban-ai/prospero

`/healthz` and `/readyz` stay open for probes. See ADR-0010 and the guide's
"Securing the API" page.

## Fleet backends

`--fleet-backend`/`PROSPERO_FLEET` selects which `FleetProvider` drives the
fleet (`local` by default):

| Value | Backend | Status |
|-------|---------|--------|
| `local` (default) | `LocalFleet` — caliband-over-Unix-sockets | Fully served; today's behavior, unchanged. |
| `k8s` | `K8sFleet` — `CalibanTask` CRs + a network session plane (ADR 0008) | Served via the `FleetProvider`/`FleetAdmin` seams (prospero #76). Requires a build with `--features k8s`. |

`PROSPERO_FLEET=k8s` serves the dashboard/API against a cluster of
`CalibanTask` agents (create/observe/kill/stream). The API's handlers route
through the backend-agnostic `FleetProvider` (control + snapshot/readiness/
metrics) and read observability (history/SSE) from the shared event store/bus,
so both backends serve the same request path (#76). The workspace-registry
plane (register/config/remove a workspace) is a `LocalFleet`-only concept —
those routes return **405** under `k8s`, where workspaces are `CalibanTask`/
namespace-driven rather than a prospero registry.

`PROSPERO_K8S_NAMESPACE` (default `default`) selects the namespace the
`CalibanTask` client is scoped to. The kube API-server connection defaults to
the ambient kubeconfig / in-cluster service account; pass `--kubeconfig <path>`
(or `KUBECONFIG`) to select an explicit kubeconfig file instead.

Under `k8s`, prosperod serves `K8sFleet` over the shared event store/bus and
runs **no** local `FleetManager`, poll loop, or lease heartbeat — those are
`local`-only machinery. Both backends still share the same storage topology
(sqlite standalone / Postgres clustered) for history and SSE.

### k8s session-plane security

The per-agent session plane (prosperod → each caliband pod: live output stream
and input) supports TLS + a bearer token (ADR 0051). Both are **off by
default** (plaintext), and each turns on only when its file is provided —
typically from a mounted Kubernetes `Secret` volume:

| Flag | Env | Purpose |
|------|-----|---------|
| `--k8s-caliband-ca-file` | `PROSPERO_K8S_CALIBAND_CA_FILE` | PEM CA bundle trusting caliband's serving cert. When set, dials use TLS. |
| `--k8s-caliband-token-file` | `PROSPERO_K8S_CALIBAND_TOKEN_FILE` | File holding the shared bearer token (contents trimmed). When set, dials present it. |
| `--k8s-caliband-server-name` | `PROSPERO_K8S_CALIBAND_SERVER_NAME` | SNI / cert-validation name (default `caliband`). |

An unreadable or malformed CA/token file is a **fatal startup error** — there
is no silent fall-back to plaintext. Secret rotation takes effect on the next
pod restart (files are read once at startup).

If prosperod wasn't built with the `k8s` cargo feature (which forwards to
`prospero-core/k8s`), selecting `k8s` fails at startup with a message pointing
at `cargo build -p prospero-daemon --features k8s`.
