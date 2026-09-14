# ADR 0010 · Inbound API authentication with scoped, declarative tokens

- **Status:** accepted
- **Date:** 2026-09-13
- **Source:** [`docs/superpowers/specs/2026-09-13-api-auth-design.md`](../superpowers/specs/2026-09-13-api-auth-design.md)

## Context

prosperod's REST + SSE API was built for a trusted operator on localhost; the
framework spec made authentication a non-goal. That assumption no longer holds:
the container binds `0.0.0.0`, the homelab deployment serves the API through
Traefik at `prospero.hexadecimate.net` behind only a LAN IP match, and anything
that reaches the port can spawn and kill agents and rewrite workspace provider
configuration. Ariel's command authorization (caliban-ai/ariel#5) and the fleet
MCP server (#218) both need a credential model to rely on.

Options weighed:

- **Scope:** a single shared token (smallest, but no read-only access or
  per-client revocation); named tokens with scopes; full users + RBAC (overlaps
  gonzalo's people/grants records that Ariel owns); mTLS client certificates
  (awkward for the browser and CLI).
- **Storage:** a declarative file/Secret; runtime management through the API
  and config store (instant revocation, but migrations, cross-replica
  invalidation and a bootstrap problem); both.
- **Trust model:** caliban's drive-surface gate (caliban-ai/caliban#527) always
  trusts loopback peers. In a pod, `kubectl port-forward` and sidecars arrive as
  loopback, so that rule would bypass scopes.
- **Browser:** `EventSource` cannot send an `Authorization` header, so the
  dashboard needs a cookie, a token in the URL (leaks into history and logs), or
  an external auth proxy.

## Decision

We will authenticate every non-probe request to prosperod with **named API
tokens, each carrying one hierarchical scope** — `read` < `operate` < `admin` —
enforced by a middleware layer in `prospero-api` against a per-route scope table.

- Tokens (`pspo_` + 32 random bytes) are declared in a **tokens file of SHA-256
  hashes** (`--api-tokens-file`), mounted from a Kubernetes Secret in-cluster and
  generated offline with `prospero token new`. Revocation is a Secret change and
  rollout. There is no token database and no shared state across replicas.
- **When tokens are configured, every request needs one, loopback included.**
  With none configured, prosperod refuses a non-loopback bind unless
  `--insecure-no-auth` is set. `/healthz` and `/readyz` are always open.
- The **dashboard signs in with a token** and receives a stateless
  HMAC-signed, `HttpOnly; SameSite=Strict` session cookie, keyed by a separate
  session-key Secret and bound to the token's current hash. Cookie-authenticated
  mutations must be same-origin.
- Every mutation is attributed to the **token name as `actor`**: on the
  `FleetEvent` envelope where the mutation records an event (local-fleet spawn
  and remove), otherwise in an `info` log line.

Per-person identity, SSO and inbound TLS are out of scope.

## Consequences

- **Positive:** the API is safe to expose behind an ingress; Ariel, humans,
  monitoring and CI get separate, revocable, least-privilege credentials; the
  model works unchanged across standalone, clustered and k8s modes; the event
  log gains an audit trail; #218 and Ariel have one authenticator to reuse.
- **Negative:** every client must carry a token (CLI, dashboard sign-in, Ariel);
  revocation needs a restart and does not cut already-open SSE streams;
  deployments must ship a tokens Secret in the same rollout as the image, since
  a public bind without tokens now refuses to start; the trust model diverges
  from caliban's loopback-trusting gate.
- **Revisit if:** operators need runtime token creation or instant revocation;
  per-person accountability is required at the prosperod API rather than in
  Ariel/gonzalo; or prosperod starts terminating TLS or federating with an
  identity provider.
