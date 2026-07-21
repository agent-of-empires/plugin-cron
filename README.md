# Cron

Run Agent of Empires sessions on a cron schedule.

An [Agent of Empires](https://github.com/agent-of-empires/agent-of-empires)
plugin. You define scheduled jobs (a cron expression, an agent, and a prompt) in
the plugin's settings; a background worker fires each job on time, creating a
structured session and delivering the prompt as its first turn, with no human at
the keyboard.

## Install into Agent of Empires

```bash
# from a local checkout
aoe plugin install ./plugin-cron

# or from GitHub once published
aoe plugin install gh:agent-of-empires/plugin-cron
```

The host runs the manifest's `[[runtime.build]]` step at install time
(`cargo build --release` into `.aoe-build/`) and then launches the
plugin-relative worker. Manage the plugin from the TUI, the web dashboard, or
`aoe plugin {enable,disable,update}`. It needs `api_version` 9, so an Agent of
Empires host older than 1.13 refuses the install.

## Configure jobs

Open the plugin's settings (TUI or web dashboard). Each row of **Scheduled jobs**
is one job:

| Field | Meaning |
|---|---|
| Name | Label; also titles the spawned session. |
| Schedule | Cron expression, 5-field (`min hour dom mon dow`). |
| Agent | Which agent runs the session. |
| Model | Model override; empty for the agent default. |
| Approval mode | ACP approval mode. See the security note below. |
| Project | Project the session runs in. Required. |
| Prompt | Delivered to the session's first turn. |
| Group | Session group the scheduled sessions are filed under (default `Scheduled`). |
| Enabled | Whether the scheduler fires this job. |

The agent, model, mode, and project pickers are populated by the host from its
own catalogs. Scheduled sessions land in their own group so they do not mix with
interactive ones.

## Scheduling behavior

- **Timezone.** Cron expressions are interpreted in the daemon host's local
  time. Moving the daemon to a machine in another timezone changes when a job
  fires.
- **Missed runs (skip-missed).** If the worker was not running at a job's fire
  time (daemon down, machine asleep), that occurrence is skipped, not caught up.
  On startup every job is seeded to the current time, so past occurrences never
  fire retroactively and a restart never double-fires. Editing a job's schedule
  or enabled state reseeds it the same way, so an edit never fires the past.
- **Overlap.** The worker does not track whether a job's previous session is
  still running; back-to-back runs are bounded by the host's per-plugin limits
  (below). A dedicated overlap policy is future work.

## Security model

Session creation goes through the host, which enforces the boundary; the plugin
only proposes:

- **Approval modes are host-classified.** The plugin passes the mode id you
  picked; the host decides whether it is interactive, guarded, or unattended. An
  unattended mode (a bypass / auto-write mode, or any mode the host does not
  recognize) requires the `session.unattended` grant this plugin requests at
  install. Pick a read-only or plan mode for a safer unattended run.
- **Repository trust is always enforced.** A session against a repository whose
  hooks need approval is refused even with the unattended grant; the plugin
  cannot pre-approve trust.
- **Ownership.** The worker can only deliver turns to sessions it created.
- **Limits (per plugin).** 20 session creates per hour, 5 active sessions, 120
  turns per hour. Each fire uses an idempotency key (`job_id` + fire minute), so
  a retry after an ambiguous response replays the existing session instead of
  spawning a duplicate.

## Known limits

- **No sub-agent selection.** Picking a named sub-agent / persona with scoped
  permissions at spawn is not expressible through the current host API; it is
  tracked separately (plugin-cron#1).
- **No `aoe schedule` CLI.** Jobs are managed through the settings surfaces;
  plugins cannot yet contribute argument-taking CLI commands.

## Develop

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
cargo build --release
```

## Worker protocol

The host spawns the worker and exchanges newline-delimited JSON-RPC 2.0 messages
over stdio. The worker is primarily a client: it calls `config.get` to read its
jobs and `sessions.create` to launch them, and it reacts to the host's
`plugin.settings.changed` notification. It exits cleanly when stdin reaches EOF.
See the
[plugin authoring guide](https://agent-of-empires.com/docs/development/writing-plugins/)
and the [plugin API reference](https://agent-of-empires.com/docs/api/plugin-api/).

## Release

Push a `vX.Y.Z` tag to run the release workflow: it runs checks, generates the
changelog from conventional commits, and publishes a GitHub Release with a source
archive and its `sha256`.

## License

MIT
