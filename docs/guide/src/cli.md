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
| `--timeout <SECONDS>` | Kill the agent after this much wall-clock time. Enforced by prosperod, so it survives a daemon restart; the log records why |
| `--permission-posture <POSTURE>` | `supervised` (default) keeps the agent's permission gate; `unattended` runs every tool without asking. `unattended` needs an **admin** token, and in-cluster a workspace that allows it |

`follow` prints output text inline, one line per tool start and finish, and
short markers for `init`, `status` and `finished`. A `[gap]` line means the
stream fell behind and recovered the dropped events from history. A
`[persist-gap]` line means an event could not be written to durable storage.

## Automations

An automation spawns an agent on a cron schedule or a signed webhook. See
[Automations](./api.md#automations) in the API guide for how triggers,
signing and run history work.

| Command | Purpose | Scope |
|---|---|---|
| `prospero automation add <id> <workspace> <task> [flags]` | Create an automation | admin |
| `prospero automation ls` | List automations: trigger, workspace, last firing | read |
| `prospero automation run <id>` | Fire one now, whatever its trigger | operate |
| `prospero automation runs <id> [--limit N]` | Recent runs, newest first (default 20) | read |
| `prospero automation disable <id>` | Stop it firing, without deleting it | admin |
| `prospero automation enable <id>` | Let a disabled automation fire again | admin |
| `prospero automation rm <id>` | Delete it and its run history | admin |

`add` needs exactly one trigger:

| Flag | Meaning |
|---|---|
| `--schedule <CRON>` | 5-field cron, in UTC, for example `"0 3 * * *"` |
| `--webhook` | Fire on a signed `POST`. Prints the signing key |
| `--label <TEXT>` | Label for the agents it spawns |
| `--model <MODEL>` | Model override |
| `--timeout-secs <SECONDS>` | Kill each spawned agent after this long |
| `--shared-tree` | Run in the workspace's working tree instead of an isolated worktree |
| `--disabled` | Create it without letting it fire yet |

With `--webhook`, `add` prints the trigger URL and the signing key.
**The key is shown once and can't be recovered.** Store it before you do
anything else. With `--webhook`, the task can use `{{ dotted.path }}`
placeholders filled from the request payload.

Automations created with the CLI always run in the `supervised` permission
posture. To create an `unattended` one, send `permission_posture` in the
template through the API.

```sh
prospero automation add nightly myproj "Sweep the logs" --schedule "0 3 * * *"
prospero automation add deploy-check myproj "Review the push to {{ ref }}" --webhook
```

## Usage

`prospero usage` reports cost, turns and outcomes per workspace over a window.
It reads `GET /api/usage`, the same report the dashboard draws, so the
numbers match what the dashboard shows for the same window. Needs **read**.

```text
$ prospero usage --since 2w
usage 2026-09-06 20:50 UTC → 2026-09-20 20:50 UTC

WORKSPACE        COST    TURNS    DONE  FAILED  KILLED  CRASHED
alpha         $1.2500       40       3       1       0        0
beta          $0.5000        2       1       1       0        0
TOTAL         $1.7500       42       4       2       0        0
```

| Flag | Meaning |
|---|---|
| `--since <WHEN>` | How far back to look. A day count (`7d`, `2w`), a date (`2026-09-01`, from UTC midnight) or an RFC-3339 timestamp. Defaults to the last 7 days |
| `--workspace <NAME>` | Only this workspace |
| `--json` | Print the server's report as JSON instead of the table |

Usage is recorded per UTC day, so `--since` looks back in whole days. It
refuses `24h` rather than implying a precision the report doesn't have. A day
count is resolved against the daemon's clock, the way the dashboard does it,
so a skewed laptop clock can't clip the window.

A workspace with no activity in the window doesn't appear in the report, so
`--workspace` can't tell an idle workspace from a misspelled one. Either way it
says that no usage was recorded. `killed` includes runs stopped by their
`--timeout`, and a note under the table says how many.

## Tokens

| Command | Purpose |
|---|---|
| `prospero token new <name> --scope <read\|operate\|admin>` | Generate a token. Runs offline and never contacts `prosperod` |
| `prospero whoami` | Show the token name and scope in use, or report that auth is disabled |

`token new` prints the token once, together with the line to add to prosperod's
tokens file. Token names must match `[a-z0-9][a-z0-9_-]{0,62}`. See
[Securing the API](./api-auth.md).
