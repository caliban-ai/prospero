# Getting started

This page runs `prosperod` locally against caliban, using the default `local`
backend, then launches and watches an agent. To run it in a container or on
Kubernetes instead, see [Deploying the container](./deployment.md).

## Prerequisites

- A recent stable Rust toolchain. The workspace uses edition 2024.
- caliban's `caliband` on your `PATH`. `prosperod` starts one per workspace
  when none is running. Use `--caliband-bin` to point it at a specific binary.
- Credentials for whichever model provider your agents will use.

## Build

```sh
git clone https://github.com/caliban-ai/prospero
cd prospero
cargo build --release -p prospero-daemon -p prospero-cli
# binaries: target/release/prosperod and target/release/prospero
```

The dashboard is compiled into `prosperod`, so the build needs no wasm
toolchain.

## Run the daemon

```sh
prosperod
```

By default it:

- listens on `127.0.0.1:7878` (the API, SSE and the dashboard at
  <http://127.0.0.1:7878/>);
- keeps its workspace registry and event history in sqlite (`events.db`) under
  `$XDG_DATA_HOME/prospero`, or `~/.local/share/prospero`;
- runs **without authentication**, which it allows only on a loopback address.

To expose it beyond loopback, set up tokens first; see
[Securing the API](./api-auth.md). Every flag is listed in
[Configuring prosperod](./configuration.md).

## Register a workspace

A workspace is a directory with a name. The directory is either a git checkout
itself or a folder whose immediate subdirectories are checkouts; each checkout
is one of the workspace's *sources*.

```sh
prospero workspace add myproj ~/src/myproj
prospero workspace list
```

Tell the workspace's caliband which provider to use. `--api-key-env` names a
variable in **prosperod's** environment; the secret itself is never stored.

```sh
prospero workspace config myproj --provider anthropic --api-key-env ANTHROPIC_API_KEY

# a local OpenAI-compatible server needs no key:
prospero workspace config myproj --provider openai --base-url http://127.0.0.1:8080/v1
```

Setting the config restarts that workspace's caliband.

## Launch and watch an agent

```sh
prospero spawn myproj "add tests for the parser"
# spawned agent <id> in workspace 'myproj' (worktree)

prospero ls                  # every workspace and its agents
prospero follow <id>         # replay the agent's events so far, then stream live
```

Each spawn gets its own git worktree by default, so parallel agents on one
codebase don't step on each other. Pass `--shared-tree` to run in the workspace's
own working tree.

`follow` exits when the agent finishes. The agent's history stays in the event
store afterwards, so you can follow it again later.

### Interactive agents

An interactive agent waits for input when its run finishes, instead of exiting:

```sh
prospero spawn myproj "let's plan the migration" --interactive
prospero send <id> "start with the schema"
prospero end-input <id>      # it finishes after the current run
```

### Other lifecycle commands

```sh
prospero kill <id>
prospero respawn <id>        # same spec, new agent id
prospero rm <id>
```

The full list is in [The `prospero` CLI](./cli.md).

## The dashboard

Open <http://127.0.0.1:7878/>. The dashboard uses the same API as the CLI and
offers:

- the fleet overview, with spend, turns and outcome charts over a 24h, 7d or 30d
  window;
- launching, killing, respawning and removing agents, and replying to
  interactive agents;
- registering, configuring and removing workspaces;
- a live stream and timeline for each agent, with a tool-call inspector;
- a System, Light or Dark theme setting.

Controls the active backend can't serve are hidden (`GET /api/capabilities`).
When authentication is on, the dashboard asks for a token first and hides
controls above that token's scope.
