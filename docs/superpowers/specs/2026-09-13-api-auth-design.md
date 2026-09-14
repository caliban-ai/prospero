# Inbound API authentication & authorization — design

- **Ticket:** caliban-ai/prospero#2
- **Date:** 2026-09-13
- **Status:** implemented (plan docs/superpowers/plans/2026-09-13-api-auth.md; see "Amendments during implementation planning")
- **Decision record:** ADR-0010 (`docs/adr/0010-inbound-api-authentication.md`)

## Problem

prosperod's REST + SSE API has no authentication. The original framework spec
made that an explicit non-goal ("assume localhost / trusted operator",
`2026-06-05-prospero-framework-design.md` §9) and deferred "token/mTLS once the
API leaves localhost" (§10). The API has since left localhost:

- The container image binds `0.0.0.0:7878` (`Dockerfile`, `docs/container.md`).
- The homelab deployment (`johnford2002/helm-charts`, `caliban-system`) serves
  the dashboard and API at `prospero.hexadecimate.net` through Traefik, guarded
  only by a LAN `ClientIP` match.
- Anything that reaches the port can spawn, steer and kill agents and rewrite
  workspace provider configuration.

It is also a dependency for other work: Ariel's two-key command authorization
only means something if Ariel is the only path to prosperod's mutating routes
(caliban-ai/ariel#5), the fleet MCP server (#218) needs an auth model to reuse,
and caliban-ai/caliban#274 lists it for network-exposed control. It is the most
recurring gap in `docs/evaluation/` (🔴 against OpenClaw, OpenHands, Coder and
GitHub Agent HQ).

## Goals

- Every non-probe request to a network-reachable prosperod is authenticated.
- Distinct, individually revocable credentials for distinct clients (humans,
  Ariel, monitoring, CI), each limited to a scope.
- The browser dashboard, including its SSE streams, works with the same
  credentials.
- Works identically in standalone, clustered (Postgres) and k8s fleet modes with
  no new shared state.
- Local development keeps working with zero configuration.

## Non-goals

- Per-person identity, SSO/OIDC, or RBAC beyond three scopes. People, roles and
  channel grants belong to gonzalo records consumed by Ariel (caliban-ai/gonzalo#277).
- Runtime token management (create/revoke over the API). Tokens are declarative.
- Inbound TLS termination in prosperod. TLS stays at the ingress / proxy.
- mTLS client certificates.
- Rate limiting of sign-in attempts (tokens carry 256 bits of entropy).
- Cutting already-open SSE streams when a token is revoked (see Limits).

## Decisions

| # | Question | Decision |
|---|---|---|
| D1 | Scope of #2 | Named API tokens, each with one scope. No user model. |
| D2 | Token storage | A declarative tokens file (hashes only), mounted from a Kubernetes Secret in-cluster; no database tables. |
| D3 | Trust model | Tokens configured ⇒ every request needs one, loopback included. No tokens ⇒ loopback bind only, unless `--insecure-no-auth`. Probes always open. |
| D4 | Dashboard | Token sign-in → stateless HMAC-signed session cookie; same-origin check on cookie-authenticated mutations. |
| D5 | Scopes | Hierarchical `read` < `operate` < `admin`. |

D3 deliberately differs from caliban's drive-surface gate (caliban-ai/caliban#527),
which always trusts loopback peers. In a pod, `kubectl port-forward` and sidecar
traffic arrive as loopback, so loopback trust would let them bypass scopes.

## Design

### 1. Tokens

**Format.** `pspo_` followed by 32 random bytes, base64url without padding. The
prefix makes leaked tokens recognisable to secret scanners.

**Tokens file.** One token per line; blank lines and `#` comments ignored:

```text
# name       scope    hash
alice        admin    sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08
ariel        operate  sha256:...
grafana      read     sha256:...
```

- `name`: `[a-z0-9][a-z0-9_-]{0,62}`, unique.
- `scope`: `read`, `operate` or `admin`.
- `hash`: `sha256:` + 64 lowercase hex characters of SHA-256 over the full token string.

**Configuration.** `--api-tokens-file` / `PROSPERO_API_TOKENS_FILE`. Read once at
startup; changes take effect on restart (Argo rolls pods when the Secret
changes).

**Generation.** `prospero token new <name> --scope <scope>` runs offline (no
daemon call), prints the token once and the tokens-file line to paste.

### 2. Scopes and route table

Scopes are ordered: `admin` ⊇ `operate` ⊇ `read`. Every route declares the
minimum scope in one table, and a test fails if a registered route has no entry.

| Route | Minimum scope |
|---|---|
| `GET /healthz`, `GET /readyz` | none (always open) |
| `GET /`, `GET /assets/{*path}` | none (static dashboard shell; data calls are authenticated) |
| `POST /api/session`, `GET /api/session`, `DELETE /api/session` | none (session endpoints, below) |
| `GET /api/metrics`, `GET /api/capabilities`, `GET /api/fleet`, `GET /api/usage` | `read` |
| `GET /api/workspaces`, `GET /api/workspaces/{workspace}/agents` | `read` |
| `GET /api/agents/{id}`, `GET /api/agents/{id}/events`, `GET /api/agents/{id}/stream` | `read` |
| `POST /api/workspaces/{workspace}/agents` (spawn) | `operate` |
| `POST /api/agents/{id}/kill`, `/respawn`, `/input`, `/end-input`; `DELETE /api/agents/{id}` | `operate` |
| `POST /api/workspaces`, `DELETE /api/workspaces/{name}`, `PUT /api/workspaces/{name}/config` | `admin` |

### 3. Request authentication

A new `auth` module in `prospero-api` provides an axum middleware layer applied
to the router.

1. Resolve the credential: `Authorization: Bearer <token>` first, else the
   `prospero_session` cookie.
2. Bearer: SHA-256 the presented token and compare against **every** configured
   hash in constant time (no early exit on match). The existing constant-time
   compare in `crates/core/src/caliband/transport.rs` moves to a shared helper
   used by both paths.
3. Cookie: verify it (§4).
4. On success, insert `Principal { token_name, scope, via: Bearer | Cookie }` as a
   request extension.
5. Compare the principal's scope with the route's minimum scope.

**Trust model.**

- Tokens file configured ⇒ steps 1–5 apply to every request, whatever the peer
  address.
- No tokens file ⇒ the layer admits everything **and** startup refuses a
  non-loopback `--addr` unless `--insecure-no-auth` / `PROSPERO_INSECURE_NO_AUTH=1`
  is set. With that flag, prosperod logs a warning at startup and every 60 s.
- `/healthz` and `/readyz` are always open, so kubelet probes need no credentials.

Because the rule does not depend on the peer address, no `ConnectInfo` plumbing
is required, and existing `oneshot` tests are unaffected.

**Responses.** Through the existing `ApiError` JSON shape:

- `401 {"error":"unauthorized","kind":"unauthorized"}` with
  `WWW-Authenticate: Bearer realm="prospero"` for a missing, unknown, malformed,
  expired or revoked credential. The body never says which.
- `403 {"error":"requires scope operate","kind":"forbidden"}` when the scope is
  too low.
- `403 {"error":"cross-origin request","kind":"forbidden"}` for a failed
  same-origin check.

The precise denial reason is logged at `debug` with the token name when known.
Tokens, cookies and the session key are never logged: they are held in
`secrecy::SecretString` / redacted `Debug` implementations.

**Audit.** The `FleetEvent` envelope (`crates/types/src/event.rs`) gains
`actor: Option<String>` (`#[serde(default, skip_serializing_if =
"Option::is_none")]`) carrying the token name. It goes on the envelope, not
`EventKind`: `AgentSpawned` and `AgentGone` are unit variants of an internally
tagged enum, and `FleetEvent` does not deny unknown fields, so an optional
envelope field is additive for existing consumers, including Ariel's mirrored
types.

- Set on the events a mutation records directly, which exist only on the local
  fleet today: `AgentSpawned` for spawn and for respawn's new agent, `AgentGone`
  for rm and for respawn's old id (`crates/core/src/fleet.rs`). Absent under
  `--insecure-no-auth` and on poll- or watch-derived events.
- `K8sFleet` records only watch-derived `StatusChanged` events
  (`crates/core/src/k8s/fleet.rs`), so on that backend every mutation logs the
  actor at `info` instead. So do kill, input, end-input and workspace
  add/remove/config on both backends, which record no event of their own.
  Adding events for them, or annotating `CalibanTask` CRs with the actor, is out
  of scope.

### 4. Dashboard sessions

**Endpoints.**

- `POST /api/session` with `{"token": "pspo_..."}` validates the token, sets the
  cookie and returns `{"token_name", "scope", "expires_at"}`. An invalid token
  returns the generic 401.
- `GET /api/session` returns the same body for the current credential (bearer
  or cookie), or 401. The CLI's `whoami` uses it too.
- `DELETE /api/session` clears the cookie (`Max-Age=0`) and returns 204.
- With **no tokens configured** (`--insecure-no-auth` or a loopback bind),
  `GET /api/session` returns `200 {"auth": "disabled"}` so the dashboard skips
  sign-in and shows every control, and `POST /api/session` returns 404.

**Cookie.** `prospero_session=v1.<token_name>.<expires_unix>.<mac>` where
`mac = base64url(HMAC-SHA256(session_key, "v1" | token_name | expires_unix | hash_fingerprint))`
and `hash_fingerprint` is the first 16 bytes of the token's configured hash.
Verification rejects a bad MAC, a past expiry, an unknown token name, or a
fingerprint that no longer matches, so removing or rotating a token invalidates
its sessions on every replica with no session store. The cookie's scope is read
from the current tokens file, never from the cookie.

- Lifetime 12 hours, fixed (no sliding renewal).
- Attributes: `HttpOnly; SameSite=Strict; Path=/`, plus `Secure` when the request
  arrived over HTTPS (`X-Forwarded-Proto: https`) or `--cookie-secure` is set.

**Session key.** `--session-key-file` / `PROSPERO_SESSION_KEY_FILE`, at least 32
bytes. It is used only for cookie MACs, so a leaked tokens file (hashes) cannot
be turned into a session.

- Standalone without a key file: a random key is generated at startup; sessions
  do not survive a restart.
- Clustered (`PROSPERO_DATABASE_URL` set) with tokens configured and no key file:
  startup fails, because replicas would each sign differently.

**Same-origin check.** A cookie-authenticated request with a method other than
GET/HEAD must carry `Sec-Fetch-Site: same-origin`, or an `Origin` whose host
(and port) equals the request's `Host`. Bearer-authenticated requests are exempt:
browsers never attach bearer headers on their own. `SameSite=Strict` is a second
layer.

**Dashboard (WASM).**

- On load, call `GET /api/session`. On 401, render a sign-in form (a single
  token field) that posts to `/api/session`.
- Any later 401 (expired or revoked) returns to the sign-in form.
- The header shows the token name and scope, with a sign-out control.
- Controls above the principal's scope are hidden: `read` sees no spawn, kill,
  respawn, input or remove controls; only `admin` sees workspace add/remove and
  the config editor. The server still enforces scope; hiding is presentation.
- `EventSource` streams need no change: the cookie is sent automatically on
  same-origin requests. The CSP (`connect-src 'self'`) is unchanged.

### 5. Clients

**CLI.**

- `--token` / `PROSPERO_TOKEN`, and `--token-file` / `PROSPERO_TOKEN_FILE`
  (whitespace-trimmed, empty is an error).
- `DaemonClient` attaches `Authorization: Bearer` to every request, including
  the startup `/healthz` check and `follow` streaming.
- 401 prints "prosperod requires a token; set PROSPERO_TOKEN or pass --token".
  403 prints the server's message, which names the required scope.
- New commands: `prospero token new <name> --scope <scope>` (offline) and
  `prospero whoami`.

**Ariel.** Uses an `operate` token as a bearer header on its HTTP client and SSE
request. Its client has no auth support today
(`ariel/crates/core/src/prospero/client.rs`); an Ariel ticket tracks that,
linked from caliban-ai/ariel#5.

**Fleet MCP server (#218).** Reuses the same authenticator and `Principal`; its
stdio shim forwards `PROSPERO_TOKEN`.

### 6. Deployment and rollout

**`caliban-ai/helm-charts`, `prospero` chart.**

- New values: `apiAuth.enabled` (default `true`), `apiAuth.existingSecret`,
  `apiAuth.tokensKey` (default `tokens`), `apiAuth.sessionKeyKey` (default
  `sessionKey`).
- Enabled: both keys are mounted read-only as files and wired to
  `PROSPERO_API_TOKENS_FILE` and `PROSPERO_SESSION_KEY_FILE`, in both the
  Deployment (clustered) and StatefulSet (standalone) templates. The chart fails
  to render (`required`) when `existingSecret` is empty.
- `apiAuth.enabled: false` renders `--insecure-no-auth`, so an unauthenticated
  install is an explicit, reviewable choice.
- Probes unchanged.

**`johnford2002/helm-charts`, `caliban-system` umbrella.**

- New `templates/prospero-api-auth.sealed.yaml` (SealedSecret with `tokens` and
  `sessionKey`), following `caliban-session-plane-token.sealed.yaml`.
- `prospero.apiAuth.existingSecret` points at it.
- The Traefik route, TLS and `lanOnly` restriction stay as defence in depth.

**Order.** A new prosperod on a non-loopback bind without tokens refuses to
start, so:

1. Release prospero with this feature.
2. Release the `prospero` chart with `apiAuth`.
3. In a **single** `johnford2002/helm-charts` PR: add the SealedSecret, set
   `apiAuth.existingSecret`, and bump the chart/image pin. Argo then never runs
   the new image without its Secret.
4. Sign in to the dashboard with an `admin` token; give Ariel an `operate` token
   when it deploys.

Rollback: revert the pin PR; the previous image ignores the new env vars.

### 7. Errors at startup

prosperod exits non-zero with a message naming the file and line (never the
hash) when:

- the tokens file is missing, unreadable, or contains no tokens;
- a line is malformed, has an unknown scope, an invalid name, or a hash that is
  not `sha256:` + 64 hex characters;
- a token name is duplicated;
- the session key file is missing, unreadable, or shorter than 32 bytes;
- `--addr` is non-loopback, no tokens file is configured, and
  `--insecure-no-auth` is not set;
- the daemon is clustered, a tokens file is configured, and no session key file is.

### 8. Limits

- Revocation requires a restart (Secret change → rollout).
- An SSE stream opened before revocation keeps delivering until it disconnects;
  reconnects are then refused. Re-validating open streams is future work if it
  is ever needed.
- The `actor` on events is a token name, not a person.

## Testing

**Unit (`prospero-api` `auth`).**

- Tokens-file parsing: valid file, comments/blank lines, malformed line, unknown
  scope, bad name, bad hash, duplicate name, empty file.
- Scope ordering; route → scope table completeness against the router.
- Bearer matching against multiple hashes; wrong token; token with valid prefix
  but wrong bytes.
- Cookie sign/verify: round trip, tampered MAC, expired, unknown name, rotated
  hash (fingerprint mismatch), removed token.
- Same-origin rules: `Sec-Fetch-Site`, `Origin` host match and mismatch, missing
  headers, GET exemption, bearer exemption.
- `Debug` output of secrets is redacted.

**Integration (`crates/api/tests/api_integration.rs`).**

- A `router_with_auth(..., AuthConfig)` fixture. For every route: anonymous →
  401 (or open for probes/shell/session), insufficient scope → 403, sufficient
  → handler response.
- Session flow: `POST /api/session` → cookie → `GET /api/fleet` → SSE stream →
  `DELETE /api/session` → 401.
- Cross-origin cookie `POST` → 403; same request with bearer → allowed.
- On the local fleet, `AgentSpawned` / `AgentGone` from spawn, rm and respawn
  carry the authenticated token name as `actor`; poll-derived events and
  `--insecure-no-auth` requests carry none.
- On the k8s backend, the same mutations succeed and emit no `actor`-bearing
  event (the actor is logged instead).
- Existing tests keep using `router(...)` without auth and stay green.

**k8s backend (`crates/api/tests/k8s_backend.rs`).** Authentication and scope
enforcement apply identically over `K8sFleet`.

**Daemon.** Startup refusals: non-loopback bind without tokens; clustered with
tokens and no session key; malformed tokens file. `--insecure-no-auth` starts.

**CLI e2e (`crates/cli/tests/e2e_smoke.rs`).** Real binary against an
auth-enabled server: works with `PROSPERO_TOKEN`; without a token prints the
readable 401 message; `token new` output loads in the daemon; `whoami` reports
name and scope.

**Dashboard.** View-model tests that controls above the principal's scope are
hidden. Browser E2E coverage remains with #9.

**Gate.** Workspace `cargo fmt --all -- --check`, `cargo clippy --workspace
--all-targets -- -D warnings`, `cargo build --workspace --all-targets`,
`cargo test --workspace`, the line-coverage gate, and the out-of-workspace
`crates/dashboard` crate's own fmt, clippy and tests plus the wasm build.

## Documentation

- ADR-0010 "Inbound API authentication" records D1–D5 and supersedes the
  framework spec's "trusted operator" non-goal.
- `docs/container.md`: new env vars and the insecure opt-out.
- `README.md`: remove "API auth" from the deferred list; add a short "Securing
  the API" section.
- mdBook guide: an operator page on generating tokens, the Secret, and scopes.
- `docs/evaluation/competitors/*/parity-gap-matrix.md`: tick the control-plane
  auth rows (🟡 where the competitor row includes per-user identity).

## Follow-ups (not in this change)

- Ariel client bearer-token support (caliban-ai/ariel).
- `caliban-ai/helm-charts` `apiAuth` values and `johnford2002/helm-charts`
  SealedSecret + pin (rollout steps 2–3).
- #218 fleet MCP server consumes the `Principal`.

## Amendments during implementation planning

- **Actor on events:** only local-fleet spawn (`AgentSpawned`) and rm (`AgentGone`)
  emit an event inside the request; respawn's new agent is discovered by the poll,
  so respawn is attributed by the audit log line only. The actor reaches the
  emitter through a tokio task-local set by the middleware
  (`prospero_core::actor`), so no `FleetProvider` signature changed.
- **Route table:** unlisted routes fail closed to `admin`; the integration test
  walks the table against the live router.
- **`SessionInfo` shape:** internally tagged — `{"auth":"disabled"}` or
  `{"auth":"token","token_name":…,"scope":…,"expires_at":…}`.
