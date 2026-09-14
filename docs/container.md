# Container image

`ghcr.io/caliban-ai/prospero` runs the `prosperod` control plane (REST, SSE and
the embedded dashboard) on port `7878`. The image:

- is built for `linux/amd64` and `linux/arm64`, with the `k8s` feature compiled
  in;
- binds `0.0.0.0`;
- defaults to **standalone** mode (sqlite under `/data`);
- defaults to `--no-autostart`, because the image contains no caliband.

## Run: standalone

    docker run --rm -p 7878:7878 -v prospero-data:/data \
      ghcr.io/caliban-ai/prospero
    # dashboard at http://localhost:7878/ , health at /healthz , /readyz

## Run: clustered (external Postgres)

    docker run --rm -p 7878:7878 \
      -e PROSPERO_DATABASE_URL='postgres://user:pass@host:5432/prospero' \
      -e PROSPERO_REPLICA_ID="$(hostname)" \
      ghcr.io/caliban-ai/prospero --no-autostart

Both examples need authentication configured (or explicitly disabled) before
prosperod will start; see [Authentication](#authentication).

## Environment

| Var | Purpose | Image default |
|-----|---------|---------------|
| `PROSPERO_ADDR` | Bind address | `0.0.0.0:7878` |
| `PROSPERO_DATA_DIR` | sqlite directory (standalone) | `/data` |
| `PROSPERO_DATABASE_URL` | When set, run clustered (Postgres) | unset (standalone) |
| `PROSPERO_REPLICA_ID` | Lease identity; must be unique per replica | `HOSTNAME` |
| `PROSPERO_HOST` | Fleet identity | `local` |
| `PROSPERO_FLEET` | Fleet backend: `local` or `k8s` | `local` |
| `RUST_LOG` | Log filter | `info` |
| `PROSPERO_API_TOKENS_FILE` | Tokens file (`<name> <scope> sha256:<hex>`); when set, every non-probe request needs a token | unset |
| `PROSPERO_SESSION_KEY_FILE` | Dashboard session key, at least 32 bytes; **required** when clustered with tokens | unset (random key per process) |
| `PROSPERO_INSECURE_NO_AUTH` | `1` serves without auth on a non-loopback bind, with a warning logged every minute | unset |
| `PROSPERO_COOKIE_SECURE` | `1` always marks the session cookie `Secure` | unset (`Secure` only when `X-Forwarded-Proto: https`) |

The schema is created in-process on boot; there is no migration step. prosperod
handles SIGTERM for graceful shutdown. For every flag, see the guide's
"Configuring prosperod" page.

## Authentication

Because the image binds `0.0.0.0`, prosperod **refuses to start** unless you
either mount a tokens file or pass `PROSPERO_INSECURE_NO_AUTH=1`:

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
| `local` (default) | `LocalFleet`: caliband over Unix sockets | Fully served. |
| `k8s` | `K8sFleet`: `CalibanTask` CRs plus a network session plane (ADR 0008) | Served through the `FleetProvider`/`FleetAdmin` seams (prospero #76). Requires a build with `--features k8s`. |

`PROSPERO_FLEET=k8s` serves the dashboard and API against a cluster of
`CalibanTask` agents (create, observe, kill, stream). The API's handlers go
through the backend-agnostic `FleetProvider` for control, snapshots, readiness
and metrics. They read history and SSE from the shared event store and bus, so
both backends serve the same request path (#76).

Workspace register, config and remove also work under `k8s` (#142). They write
`Workspace` custom resources that the operator reconciles, which has these
consequences:

- `POST /api/workspaces` and `PUT /api/workspaces/{name}/config` answer
  `202 Accepted` rather than `201`/`204`.
- A workspace's sources and named providers come from its config; `root` is
  ignored.
- `GET /api/capabilities` reports `"async_workspace_ops": true`.
- A spawn may pick a named provider with `provider_ref`; otherwise the
  workspace's default provider is used.

`PROSPERO_K8S_NAMESPACE` (default `default`) selects the namespace for
`CalibanTask` and `Workspace` resources. The kube API-server connection defaults
to the in-cluster service account, then the ambient kubeconfig; pass
`--kubeconfig <path>` (or set `KUBECONFIG`) to use a specific kubeconfig file.

`PROSPERO_INCLUDE_THINKING=1` records model reasoning as `thinking` output
events, which the dashboard shows as a collapsible segment. It is off by default
because of the volume and privacy cost.

Under `k8s`, prosperod serves `K8sFleet` over the shared event store and bus. It
runs **no** local `FleetManager` and no poll loop; those are `local`-only
machinery. When clustered, per-agent session-plane leases keep two replicas from
streaming the same agent. Both backends use the same storage topology (sqlite
standalone, Postgres clustered) for history and SSE.

### k8s session-plane security

The per-agent session plane is prosperod's link to each caliband pod: the live
output stream and input. It supports TLS and a bearer token, following caliban's
ADR 0051. Both are **off by default** (plaintext), and each turns on only when
its file is provided, typically from a mounted Kubernetes `Secret` volume:

| Flag | Env | Purpose |
|------|-----|---------|
| `--k8s-caliband-ca-file` | `PROSPERO_K8S_CALIBAND_CA_FILE` | PEM CA bundle trusting caliband's serving cert. When set, dials use TLS. |
| `--k8s-caliband-token-file` | `PROSPERO_K8S_CALIBAND_TOKEN_FILE` | File holding the shared bearer token (contents trimmed). When set, dials present it. |
| `--k8s-caliband-server-name` | `PROSPERO_K8S_CALIBAND_SERVER_NAME` | SNI / cert-validation name (default `caliband`). |

Configuring a token without the CA file is a **fatal startup error**, because
the token would otherwise travel in cleartext. An unreadable or malformed CA or
token file is also fatal; there is no silent fall-back to plaintext. Secret
rotation takes effect on the next pod restart, since the files are read once at
startup.

If prosperod wasn't built with the `k8s` cargo feature (which forwards to
`prospero-core/k8s`), selecting `k8s` fails at startup with a message pointing
at `cargo build -p prospero-daemon --features k8s`.
