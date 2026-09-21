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

## Acting for someone else

A client that serves many people — a chat bridge, an MCP front end, a bot —
holds **one** token, so `actor` alone cannot tell one person's agents from
another's. Such a client may name the person it is acting for:

```
X-Prospero-On-Behalf-Of: discord:U123
```

The value is recorded on the events that request emits, as `on_behalf_of`,
beside the token's own `actor`. Both appear in the audit line too:

```
target=prospero_audit actor=ariel asserted_on_behalf_of=discord:U123
```

**Prosperod does not verify this value.** It cannot: the credential it
authenticated is the token, and it has no way to check that the person named
really asked for anything. The header is the client's *assertion*, and it is
stored as one. The token remains the authenticated identity and is always
recorded, so a false claim is still attributable to the credential that made
it — which is why the header needs no extra permission, and why you should
trust `on_behalf_of` exactly as far as you trust the token beside it.

It applies to any request, not just spawns, so kills and respawns carry it too.
Send nothing and behaviour is unchanged; a daemon predating this simply ignores
the header. Values are at most 128 characters and may not contain control
characters — a malformed one is a `400` rather than a silent drop, so a client
never believes it recorded an attribution it did not.

- CLI: `prospero --on-behalf-of <who> spawn …`
