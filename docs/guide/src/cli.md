# The `prospero` CLI

`prospero` is a thin client over `prosperod`'s [HTTP API](./api.md). Apart from
`token new`, every command sends an HTTP request to the daemon. Run
`prospero --help` or `prospero <command> --help` for the built-in help.

## Global options

| Option | Env | Default | Purpose |
|---|---|---|---|
| `--addr <URL>` | `PROSPERO_ADDR` | `http://127.0.0.1:7878` | Base URL of `prosperod` |
| `--token <TOKEN>` | `PROSPERO_TOKEN` | — | API token for an authenticated daemon |
| `--token-file <PATH>` | `PROSPERO_TOKEN_FILE` | — | File containing the token (trailing whitespace trimmed); wins over `--token` |

```admonish note
The CLI and `prosperod` both read `PROSPERO_ADDR`, but they expect different
values. The CLI wants a full base URL (`http://host:7878`). `prosperod` wants a
bind address (`0.0.0.0:7878`). Avoid exporting a daemon-style value in a shell
where you run the CLI.
```

## Workspaces

| Command | Purpose | Scope |
|---|---|---|
| `prospero workspace add <name> <root>` | Register a workspace by name and root path | admin |
| `prospero workspace list` | List workspaces with health, agent count, sources and provider | read |
| `prospero workspace config <name> [flags]` | Set the workspace's provider config and restart its caliband | admin |
| `prospero workspace rm <name>` | Unregister a workspace | admin |

`workspace config` flags:

| Flag | Meaning |
|---|---|
| `--provider <ID>` | Provider id, for example `anthropic`, `openai` or `google`. Leave it out to clear the provider |
| `--base-url <URL>` | Provider base URL |
| `--api-key-env <VAR>` | **Name** of an env var in prosperod's environment that holds the API key |
| `--env KEY=VALUE` | Raw env override (repeatable). Stored verbatim, so don't put secrets here |

Each call replaces the whole configuration. Flags you leave out are cleared.

## Agents

| Command | Purpose | Scope |
|---|---|---|
| `prospero spawn <workspace> <prompt> [flags]` | Launch an agent (worktree-isolated by default) | operate |
| `prospero ls` | List the fleet: every workspace and its agents | read |
| `prospero status` | Report whether the daemon is reachable, then list the fleet | read |
| `prospero follow <id> [--from N]` | Replay events from `seq` N (default 0), then stream live until the agent finishes | read |
| `prospero kill <id>` | Kill a running agent | operate |
| `prospero respawn <id>` | Kill and respawn with the same spec; prints the new id | operate |
| `prospero rm <id>` | Remove an agent from caliban's registry | operate |
| `prospero send <id> <text>` | Send a user message to an interactive agent | operate |
| `prospero end-input <id>` | Signal end-of-input to an interactive agent | operate |

`spawn` flags:

| Flag | Meaning |
|---|---|
| `--label <TEXT>` | Human-readable label |
| `--model <MODEL>` | Model override |
| `--shared-tree` | Run in the workspace's working tree instead of an isolated git worktree |
| `--interactive` | Wait for operator input after each run instead of finishing |
| `--tool-allowlist <TOOL>` | Restrict the agent to these tools; repeat once per tool |
| `--frontmatter <PATH>` | Agent-template / frontmatter markdown file |
| `--permission-posture <POSTURE>` | `supervised` (default) keeps the agent's permission gate; `unattended` runs every tool without asking. `unattended` needs an **admin** token, and in-cluster a workspace that allows it |

`follow` prints output text inline, one line per tool start and finish, and
short markers for `init`, `status` and `finished`. A `[gap]` line means the
stream fell behind and recovered the dropped events from history. A
`[persist-gap]` line means an event could not be written to durable storage.

## Tokens

| Command | Purpose |
|---|---|
| `prospero token new <name> --scope <read\|operate\|admin>` | Generate a token. Runs offline and never contacts `prosperod` |
| `prospero whoami` | Show the token name and scope in use, or report that auth is disabled |

`token new` prints the token once, together with the line to add to prosperod's
tokens file. Token names must match `[a-z0-9][a-z0-9_-]{0,62}`. See
[Securing the API](./api-auth.md).
