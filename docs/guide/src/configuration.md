# Configuring prosperod

`prosperod` is configured with command-line flags. Most flags also read an
environment variable. Run `prosperod --help` for the authoritative list.

## Core flags

| Flag | Env | Default | Purpose |
|---|---|---|---|
| `--addr` | `PROSPERO_ADDR` | `127.0.0.1:7878` | Bind address for the API and dashboard |
| `--data-dir` | `PROSPERO_DATA_DIR` | `$XDG_DATA_HOME/prospero`, else `~/.local/share/prospero` | Standalone storage directory |
| `--host` | `PROSPERO_HOST` | `local` | Host identity reported in fleet snapshots |
| `--fleet-backend` | `PROSPERO_FLEET` | `local` | `local` (caliband over Unix sockets) or `k8s` |
| `--poll-interval-ms` | — | `2000` | How often the local backend polls each caliband |
| `--no-autostart` | — | off | Don't start caliband for a workspace that has none running |
| `--caliband-bin` | — | `caliband` | caliband binary used for autostart |
| `--default-env KEY=VALUE` | — | — | Env var applied under every workspace's config (repeatable) |
| `--retention-days` | — | `0` (off) | Hourly sweep deleting events older than N days |

Logging goes to stderr. The level is controlled by `RUST_LOG`, default `info`;
for example, `RUST_LOG=prosperod=debug,info`.

`prosperod` shuts down gracefully on Ctrl-C or SIGTERM.

## Authentication flags

| Flag | Env | Purpose |
|---|---|---|
| `--api-tokens-file` | `PROSPERO_API_TOKENS_FILE` | Tokens file; when set, every non-open route needs a token |
| `--session-key-file` | `PROSPERO_SESSION_KEY_FILE` | Dashboard session-cookie key, at least 32 bytes |
| `--insecure-no-auth` | `PROSPERO_INSECURE_NO_AUTH` | Serve unauthenticated on a non-loopback address |
| `--cookie-secure` | `PROSPERO_COOKIE_SECURE` | Always mark the session cookie `Secure` |

The startup rules are:

- Without tokens, `prosperod` refuses a non-loopback `--addr` unless given
  `--insecure-no-auth`.
- `--insecure-no-auth` cannot be combined with a tokens file.
- A clustered deployment with tokens requires a shared session key.

Details are in [Securing the API](./api-auth.md).

## Storage topology

Where history and configuration live depends on whether a Postgres URL is set:

| | Standalone (default) | Clustered |
|---|---|---|
| Selected by | no `--database-url` | `--database-url` / `PROSPERO_DATABASE_URL` |
| Event store + workspace registry | sqlite `events.db` in the data dir | Postgres |
| Live event bus | in-process | Postgres `LISTEN`/`NOTIFY` |
| Stream ownership | this process owns every stream | per-stream leases, so replicas fail over without double-writing |

The schema is created on startup; there is no separate migration step.

Clustered-only flags:

| Flag | Env | Default | Purpose |
|---|---|---|---|
| `--replica-id` | `PROSPERO_REPLICA_ID` | `$HOSTNAME` | Lease identity; **must be unique per replica** |
| `--lease-ttl-secs` | — | `30` | A stream's owner must renew within this window |
| `--heartbeat-interval-ms` | — | a third of the TTL | How often held leases are renewed |

`/readyz` reports `503` whenever the event store can't accept writes, so an
orchestrator can hold traffic back.

## Fleet backends

### `local`

The local backend drives one caliband per registered workspace. prosperod
finds each caliband's control socket the same way caliban does:
`$CALIBAN_DAEMON_RUNTIME_DIR`, else `$XDG_RUNTIME_DIR/caliban`, else
`$TMPDIR/caliban-daemon`, with the socket named after a hash of the workspace's
canonical root. If no daemon is reachable and autostart is on, prosperod runs
`<caliband-bin> --workspace-root <root>` and waits up to 10 seconds for the
socket to appear.

A workspace's provider config becomes environment variables on its caliband.
They are layered from lowest to highest precedence:

1. `--default-env` values.
2. The curated provider fields. `provider` becomes `CALIBAN_PROVIDER`. For
   `anthropic`, `openai` and `google`, `base_url` becomes
   `ANTHROPIC_BASE_URL`, `OPENAI_BASE_URL` or `GEMINI_BASE_URL`, and
   `api_key_from_env` fills `ANTHROPIC_API_KEY`, `OPENAI_API_KEY` or
   `GEMINI_API_KEY` from the named variable in prosperod's own environment.
3. The workspace's raw `env` map.

For a local OpenAI-compatible model server, use provider `openai` with a
`base_url` pointing at its `/v1` endpoint. A provider with an overridden
`base_url` may omit the API key. (Caliban's dedicated `ollama` provider was
removed in v0.7.0.)

### `k8s`

With `PROSPERO_FLEET=k8s`, agents are `CalibanTask` custom resources and
workspaces are `Workspace` custom resources. The caliban-operator reconciles
them. Under this backend, prosperod runs no poll loop and starts no caliband.
It needs a binary built with `--features k8s`; the published container image is.

| Flag | Env | Default | Purpose |
|---|---|---|---|
| `--kubeconfig` | `KUBECONFIG` | in-cluster, then ambient kubeconfig | API-server connection |
| — | `PROSPERO_K8S_NAMESPACE` | `default` | Namespace for `CalibanTask` and `Workspace` resources |
| `--k8s-caliband-ca-file` | `PROSPERO_K8S_CALIBAND_CA_FILE` | unset | Enables TLS to each agent pod's session plane |
| `--k8s-caliband-token-file` | `PROSPERO_K8S_CALIBAND_TOKEN_FILE` | unset | Bearer token for the session plane (requires the CA file) |
| `--k8s-caliband-server-name` | `PROSPERO_K8S_CALIBAND_SERVER_NAME` | `caliband` | Name checked against the pod's TLS certificate (SNI) |
| — | `PROSPERO_INCLUDE_THINKING` | off | `1` records model reasoning as `thinking` output |

Both storage topologies work under k8s. See
[Deploying the container](./deployment.md#fleet-backends) for the full k8s
notes.
