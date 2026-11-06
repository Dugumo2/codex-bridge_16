# codex-bridge（cxb）

[English](README.md) | [中文](README.zh-CN.md)

在 [Claude Code](https://docs.anthropic.com/en/docs/claude-code) 中与 [Codex](https://github.com/openai/codex) 协作：派发任务、审查代码、续聊、处理 Codex 中途的审批与提问，结果直接回到 Claude 会话。

cxb 由两部分组成：

- **`cxb` 命令行**（TypeScript，bun 运行）：11 个命令，结果写 stdout、过程写 stderr，`--json` 输出可解析的对象，退出码统一。
- **工作区 broker**（Rust，`cxb-broker`）：每个工作区一个，经 JSON-RPC 连接 `codex app-server`，托管运行、待回应事项和本地记录；`cxb run` 断开时立即让 Codex 停下，`--detach` 的任务由它托管到结束；空闲 5 分钟自动退出。

**目前只支持 Windows。** 已用 codex-cli 0.160.0 测试。

## 安装

前置条件：Windows、PowerShell 7、[Bun](https://bun.sh/)、[Rust](https://rustup.rs/)（cargo）、[Codex CLI](https://github.com/openai/codex)（`npm install -g @openai/codex`，并 `codex login`）。

```powershell
git clone https://github.com/Dugumo2/codex-bridge.git
cd codex-bridge
pwsh -File install.ps1                       # 默认装到 %USERPROFILE%\.cxb
pwsh -File install.ps1 -Prefix D:\tools\cxb  # 或指定目录
```

安装脚本会构建 broker 正式版、打包 CLI，并在安装目录生成：

| 位置 | 内容 |
|---|---|
| `<Prefix>\bin\cxb.cmd`、`cxb` | 命令包装（CMD/PowerShell、Git Bash） |
| `<Prefix>\scripts\cxb.js`、`cxb-broker.exe` | CLI 与 broker |
| `<Prefix>\prompts\` | 内置模板 |
| `<Prefix>\skill\cxb\SKILL.md` | 生成的 Claude Code 技能文件（供审阅） |

安装脚本**不修改 PATH、默认不注册技能**，结束时会打印加入 PATH 的命令。要把技能注册给 Claude Code：

```powershell
pwsh -File install.ps1 -SkillDir $env:USERPROFILE\.claude\skills   # 装到 <SkillDir>\cxb\SKILL.md
```

装好后先跑一次诊断：

```powershell
cxb doctor
```

## 快速开始

```powershell
cxb run "这个项目是做什么的？" -s read-only        # 派任务，结果在 stdout
cxb run --approval auto -f prompt.md              # 需要 MCP、搜索的复杂任务用 auto
cxb review --mode uncommitted                     # 审查未提交改动（永远只读）
cxb run --resume <线程ID> "继续检查错误处理"         # 同一线程续聊
cxb run --detach "长任务……"; cxb wait <线程ID>      # 交给 broker 托管，之后等结果
cxb inbox; cxb reply <事项ID> yes                  # 处理审批、提问、MCP 表单、Guardian 拒绝
cxb show <线程ID> --log                            # 完整本地日志
```

完整用法见 `cxb --help`、`cxb <命令> --help`，以及给 Claude 用的 `SKILL.md`。

## 配置

```powershell
cxb config                       # 查看全部配置与来源
cxb config approval auto         # 设置默认值（命令行显式参数优先）
cxb config timeout --unset       # 恢复内置默认
cxb config templates             # 列出内置与用户模板
```

可配置项：`model`、`reasoning`、`sandbox`、`approval`、`timeout`、`memory`、`server`（`auto|shared|private`，broker 连接共享还是私有的 app-server）、`broker-idle-timeout`。`server` 与 `broker-idle-timeout` 在 broker 下次启动时生效。

数据目录：`%USERPROFILE%\.codex-bridge`（可用 `CXB_HOME` 改为其他绝对路径），包含 `config.json`、用户模板 `templates\` 与各工作区的线程、运行记录和日志。

## 卸载

1. 等 broker 空闲退出（默认 5 分钟），或先 `cxb stop` 停掉运行中的任务；
2. 删除安装目录（默认 `%USERPROFILE%\.cxb`）；若注册过技能，删除 `<SkillDir>\cxb`；若加过 PATH，从用户 PATH 中去掉 `<Prefix>\bin`；
3. `%USERPROFILE%\.codex-bridge` 保存着配置、模板和各工作区的线程记录，是否删除由你决定（删除后本地记录无法恢复；Codex 服务器上的线程不受影响）。

## 开发

见 [CONTRIBUTING.md](CONTRIBUTING.md)。所有验证在本机进行。

## 许可

MIT，见 [LICENSE](LICENSE)。

## 参考

- [Kevin7Qi/codex-collab](https://github.com/Kevin7Qi/codex-collab)（MIT）：本项目的早期参考，部分命令命名和少量底层代码沿用自它，其余均为重新编写。
