# codex-bridge (cxb)

[English](README.md) | [中文](README.zh-CN.md)

Collaborate with [Codex](https://github.com/openai/codex) from [Claude Code](https://docs.anthropic.com/en/docs/claude-code): run tasks, review code, continue conversations, and handle Codex's approvals and questions along the way, with results coming straight back to your Claude session.

cxb has two parts:

- **The `cxb` CLI** (TypeScript, runs on Bun): 11 commands. Results go to stdout and progress to stderr, `--json` prints a parseable object, and exit codes are consistent across commands.
- **A per-workspace broker** (Rust, `cxb-broker`): connects to `codex app-server` over JSON-RPC and hosts runs, pending requests and local records. If `cxb run` disconnects, Codex is stopped right away; tasks started with `--detach` are hosted by the broker until they finish. It exits after 5 idle minutes.

**Currently Windows only.** Tested with codex-cli 0.160.0.

## Install

Requirements: Windows, PowerShell 7, [Bun](https://bun.sh/), [Rust](https://rustup.rs/) (cargo), and the [Codex CLI](https://github.com/openai/codex) (`npm install -g @openai/codex`, then `codex login`).

```powershell
git clone https://github.com/Dugumo2/codex-bridge.git
cd codex-bridge
pwsh -File install.ps1                       # installs to %USERPROFILE%\.cxb by default
pwsh -File install.ps1 -Prefix D:\tools\cxb  # or a directory of your choice
```

The installer builds the release broker, bundles the CLI, and creates the following in the install directory:

| Location | Contents |
|---|---|
| `<Prefix>\bin\cxb.cmd`, `cxb` | Command wrappers (CMD/PowerShell, Git Bash) |
| `<Prefix>\scripts\cxb.js`, `cxb-broker.exe` | The CLI and the broker |
| `<Prefix>\prompts\` | Built-in templates |
| `<Prefix>\skill\cxb\SKILL.md` | The generated Claude Code skill file (for review) |

The installer **does not modify PATH and does not register the skill by default**. It prints the command for adding cxb to PATH when it finishes. To register the skill with Claude Code:

```powershell
pwsh -File install.ps1 -SkillDir $env:USERPROFILE\.claude\skills   # installs to <SkillDir>\cxb\SKILL.md
```

Run the diagnostics once after installing:

```powershell
cxb doctor
```

## Quick start

```powershell
cxb run "What does this project do?" -s read-only    # run a task; the result goes to stdout
cxb run --approval auto -f prompt.md                 # use auto for complex tasks that need MCP or search
cxb review --mode uncommitted                        # review uncommitted changes (always read-only)
cxb run --resume <thread-id> "Keep checking the error handling"   # continue in the same thread
cxb run --detach "A long task..."; cxb wait <thread-id>           # let the broker host it, then wait for the result
cxb inbox; cxb reply <item-id> yes                   # handle approvals, questions, MCP forms and Guardian denials
cxb show <thread-id> --log                           # the full local log
```

See `cxb --help`, `cxb <command> --help`, and `SKILL.md` (written for Claude) for full usage.

## Configuration

```powershell
cxb config                       # show all settings and where they come from
cxb config approval auto         # set a default (explicit command-line flags take priority)
cxb config timeout --unset       # restore the built-in default
cxb config templates             # list built-in and user templates
```

Settings: `model`, `reasoning`, `sandbox`, `approval`, `timeout`, `memory`, `server` (`auto|shared|private`, whether the broker connects to a shared or private app-server), and `broker-idle-timeout`. `server` and `broker-idle-timeout` take effect the next time the broker starts.

Data directory: `%USERPROFILE%\.codex-bridge` (set `CXB_HOME` to use another absolute path). It holds `config.json`, user templates in `templates\`, and each workspace's threads, run records and logs.

## Uninstall

1. Wait for the broker to exit when idle (5 minutes by default), or run `cxb stop` first to stop running tasks.
2. Delete the install directory (`%USERPROFILE%\.cxb` by default). If you registered the skill, delete `<SkillDir>\cxb`. If you added cxb to PATH, remove `<Prefix>\bin` from your user PATH.
3. `%USERPROFILE%\.codex-bridge` holds your settings, templates and each workspace's thread records. Deleting it is up to you: local records cannot be recovered afterwards, while threads on the Codex server are not affected.

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md). All verification runs locally.

## License

MIT — see [LICENSE](LICENSE).

## References

- [Kevin7Qi/codex-collab](https://github.com/Kevin7Qi/codex-collab) (MIT): an early reference for this project. Some command names and a small amount of low-level code come from it; everything else was written from scratch.
