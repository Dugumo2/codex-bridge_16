# codex-bridge（cxb）

Claude 与 Codex 协作的命令行工具：`cxb` CLI（TypeScript，bun 运行）经工作区 broker（Rust，`cxb-broker`）连接 `codex app-server` 的 JSON-RPC 协议。只支持 Windows。

## 开发

```powershell
bun install
bun run src/cxb/main.ts --help          # 源码直接运行 CLI
cargo build --manifest-path broker-rs\Cargo.toml   # 调试版 broker（CLI 源码运行时按 broker-rs\target\debug 查找，或设 CXB_BROKER_BIN）
bun run typecheck
bun test                                # TS 全量（含 Rust broker 的黑盒与扩展用例，需先编好调试版 broker）
cargo test --manifest-path broker-rs\Cargo.toml
pwsh -File install.ps1 -Prefix <目录>    # 打包安装到并行目录试用
```

所有验证在本机进行；编译目录、测试临时目录不要放 C 盘（`CARGO_TARGET_DIR`、`TEMP`、`TMP`）。

## 关键位置

| 位置 | 作用 |
|---|---|
| `src/cxb/main.ts` | CLI 入口：参数解析、工作区、连接 broker、命令分发（`HANDLERS`）、输出与清理 |
| `src/cxb/args.ts`、`help.ts` | 11 个命令的参数规格、帮助、已删除命令的提示 |
| `src/cxb/contracts.ts` | 共用类型、退出码、`CxbError` |
| `src/cxb/broker-conn.ts` | 拉起与连接 broker、版本握手 |
| `src/cxb/run.ts`、`review.ts` | 派任务与审查；`continue-prompt.ts` 续跑；`ask-instructions.ts` 提问开关与开发者指令 |
| `src/cxb/show.ts`、`ls.ts`、`rm.ts`、`inbox.ts`、`reply.ts`、`wait.ts`、`stop.ts` | 其余命令；`command-session.ts` 是它们共用的请求包装与退出码归类 |
| `src/cxb/server-requests.ts`、`preflight.ts` | 服务端请求（审批、提问、MCP、Guardian）的处理与交接；审批预检 |
| `src/cxb/ledger.ts`、`thread-ref.ts`、`wait-run.ts`、`stop-thread.ts` | 本地账本只读、线程 ID 解析、等待运行结果、停止 |
| `src/cxb/workspace.ts`、`config-schema.ts` | 工作区键与数据根、旧数据迁移、持久配置键表 |
| `src/config.ts`、`rpc.ts`、`types.ts`、`models.ts`、`questions.ts`、`guardian.ts`、`git.ts` | 复用的底层模块（常量与模板、JSON-RPC 端点、协议类型、模型选择、提问答案编码与终端清洗、Guardian 映射、git） |
| `broker-rs/` | Rust broker：认领与运行账本、inbox、停止、刹车、存储、app-server 接入 |
| `src/test-support/broker-harness.ts`、`src/cxb/test-fixtures/` | 测试夹具（拉起真实 broker、模拟 codex） |
| `src/prompts/*.md` | 内置模板 |
| `SKILL.md` | Claude Code 技能（安装时生成模板表） |

## 架构要点

- 数据根 `CXB_HOME`，默认 `%USERPROFILE%\.codex-bridge`：`config.json`（CLI 写、broker 只读 `server`）、`templates\`、`workspaces\<键>\`（broker 写的线程索引、运行记录、日志，CLI 只读）。
- 工作区根取 git 顶层目录；工作区键是规范路径的 sha256 前 16 位，管道名 `\\.\pipe\cxb-<键>`。
- broker 每个工作区一个，空闲 5 分钟退出；CLI 版本与 broker 不符时，在 broker 空闲时替换它。
- 退出码见 `src/cxb/contracts.ts` 的 `EXIT`；设计文档在 内部文档（命令面规格、详细设计）。
- 只用 bun 运行 TypeScript，不用 npm、yarn、pnpm。
