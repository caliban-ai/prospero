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

Schema is created in-process on boot (no migration step). prosperod handles
SIGTERM for graceful shutdown.

## Fleet backends

`--fleet-backend`/`PROSPERO_FLEET` selects which `FleetProvider` drives the
fleet (`local` by default):

| Value | Backend | Status |
|-------|---------|--------|
| `local` (default) | `LocalFleet` — caliband-over-Unix-sockets | Fully served; today's behavior, unchanged. |
| `k8s` | `K8sFleet` — `CalibanTask` CRs + a network session plane (ADR 0008) | Backend library complete and conformance-tested in `crates/core` (feature `k8s`); **not yet servable by prosperod**. |

`PROSPERO_FLEET=k8s` fails fast at startup rather than serving partially.
The reason: prosperod's HTTP API calls `FleetManager` directly for most
operations (kill/respawn/steer/snapshot/stream) — only `spawn` goes through
the `FleetProvider` seam today — and `K8sFleet` does not produce a
`FleetManager`, so it cannot back those handlers as-is. Serving `K8sFleet`
requires rerouting the API layer's direct `FleetManager` calls through the
`FleetProvider` seam, deferred per [ADR 0008](adr/0008-k8s-fleet-backend.md)
§5 and tracked as a follow-up ("wire K8sFleet into prosperod").

If prosperod wasn't built with the `k8s` cargo feature (which forwards to
`prospero-core/k8s`), selecting `k8s` reports that distinctly from the
"not wired" case, e.g. `cargo build -p prospero-daemon --features k8s`.
