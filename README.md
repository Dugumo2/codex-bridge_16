# codex-bridge (cxb)

[English](README.md) | [中文](README.zh-CN.md)

> **The Chinese README ([README.zh-CN.md](README.zh-CN.md)) is the full documentation.** This page is a short summary.

Collaborate with [Codex](https://github.com/openai/codex) from [Claude Code](https://docs.anthropic.com/en/docs/claude-code): run tasks, review code, resume threads, and answer Codex's approvals and questions without leaving your Claude session.

cxb has two parts: the `cxb` CLI (TypeScript on Bun, 11 commands, unified exit codes, `--json` output) and a per-workspace broker (`cxb-broker`, Rust) that talks to `codex app-server` over JSON-RPC, hosts detached runs and pending requests, and exits after 5 idle minutes.

**Windows only.** Tested with codex-cli 0.160.0.

## Install

Requires Windows, PowerShell 7, Bun, Rust (cargo) and the Codex CLI.

```powershell
git clone https://github.com/Dugumo2/codex-bridge.git
cd codex-bridge
pwsh -File install.ps1                       # installs to %USERPROFILE%\.cxb
pwsh -File install.ps1 -Prefix D:\tools\cxb  # or a directory of your choice
```

The installer does not modify PATH and does not register the skill unless `-SkillDir` is given. Run `cxb doctor` afterwards.

## Acknowledgements

This project started from [Kevin7Qi/codex-collab](https://github.com/Kevin7Qi/codex-collab) (MIT).

- The broker is a complete rewrite in Rust; it contains no code from the original project.
- The command layer was rewritten with the original project as a reference. A few modules are carried over and adapted (`rpc`, `config`, `guardian`, `models`, `git`).
- [LICENSE](LICENSE) keeps the original copyright notice, as the MIT license requires.

This project is not affiliated with the original author, OpenAI or Anthropic.

## License

MIT — see [LICENSE](LICENSE).
