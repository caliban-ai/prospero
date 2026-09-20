# Securing the API

prosperod authenticates requests with **named API tokens** (ADR-0010).

## Scopes

| Scope | Allows |
|---|---|
| `read` | every GET: fleet, usage, workspaces, agent events and streams, metrics |
| `operate` | `read` + spawn, kill, respawn, input, end-input, remove agent |
| `admin` | `operate` + add/remove workspace, set workspace config, and spawn with `permission_posture: "unattended"` |

`/healthz`, `/readyz`, the dashboard shell and `/api/session` are always open.

## Creating tokens

    prospero token new ariel --scope operate

prints the token **once** and a line for the tokens file. prosperod stores only
the SHA-256 hash. Start it with `--api-tokens-file` / `PROSPERO_API_TOKENS_FILE`.

## Rules at startup

- Tokens configured ⇒ every request needs one, loopback included.
- No tokens and a non-loopback `--addr` ⇒ prosperod exits unless `--insecure-no-auth`.
- Clustered (`PROSPERO_DATABASE_URL`) with tokens ⇒ `--session-key-file` is required
  (`openssl rand -base64 48 > session.key`) so replicas share cookie signing.

## Clients

- CLI: `PROSPERO_TOKEN` / `--token`, or `PROSPERO_TOKEN_FILE` / `--token-file`; `prospero whoami`.
- Dashboard: sign in with a token; controls above your scope are hidden.
- Scripts: `Authorization: Bearer pspo_…`.

## Revoking

Delete the token's line and restart (in Kubernetes: update the Secret; Argo rolls
the pods). Its sessions stop working immediately after the restart. Streams that
were already open keep flowing until they reconnect.

## Audit

Every mutation logs `target=prospero_audit actor=<token name>`. On the local
fleet, `AgentSpawned` and `AgentGone` events from spawn and remove also carry
`actor`.
