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
