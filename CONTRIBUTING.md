# 参与开发 codex-bridge（cxb）

## 前置条件

- Windows（broker 依赖 Windows 命名管道与 Windows API）
- [Bun](https://bun.sh/) >= 1.0、[Rust](https://rustup.rs/)（cargo）、[Codex CLI](https://github.com/openai/codex)（带 `app-server`）
- PowerShell 7（`install.ps1` 需要）

## 开发环境

```powershell
bun install
cargo build --manifest-path broker-rs\Cargo.toml
bun run src/cxb/main.ts --help
```

源码运行时 CLI 在 `broker-rs\target\debug\cxb-broker.exe` 查找 broker；也可以用 `CXB_BROKER_BIN` 指定。编译目录可用 `CARGO_TARGET_DIR` 放到大容量盘。

## 验证（全部在本机进行）

```powershell
cargo fmt --check --manifest-path broker-rs\Cargo.toml
cargo clippy --all-targets --manifest-path broker-rs\Cargo.toml -- -D warnings
cargo test --manifest-path broker-rs\Cargo.toml
bun run typecheck
bun test                 # TS 全量，包含 Rust broker 的黑盒（src/broker-server.test.ts）与扩展用例（src/broker-ext*.test.ts）
bun run test:shards      # CLI 全量按文件分成 3 份并行；可加 --shards 2、--update-timings、--log-dir 路径、--deadline-ms 1800000
```

- 测试的临时目录跟随 `TEMP`、`TMP`，想放到大容量盘就先设好这两个变量；真实上游测试（`CXB_UPSTREAM_REAL=1`）从 `PATH` 找 Codex，也可用 `CXB_CODEX_CMD` 指定。
- `src/cxb/sa1b-ondemand.test.ts` 要从 git 历史里取旧版本做对比，源码压缩包和 `--depth 1` 的克隆跑不了这个文件；要跑全部测试请用普通 `git clone`，也可用 `SA1B_BASELINE_CLI` 指定对照版本。
- 互不依赖的项可以并行跑；黑盒与扩展用例使用调试版 broker，`cargo test` 链接时若可执行文件被占用，等测试结束再跑或改用独立的 `CARGO_TARGET_DIR`。
- 新增判定逻辑要做变异检查：故意改坏，确认测试在预期位置失败，再还原。
- 测试不得读写真实的 `%USERPROFILE%\.codex-bridge`、`.codex`、`.claude`：用 `CXB_HOME`、`USERPROFILE`、`CODEX_HOME` 指向临时目录，工作区也用临时目录。

## 安装试用

```powershell
pwsh -File install.ps1 -Prefix D:\cxb-try
D:\cxb-try\bin\cxb.cmd doctor
```

安装不改 PATH、不注册技能；详见 `README.zh-CN.md`。

## 提交

- 一次提交一个逻辑变更，提交信息用中文。
- 提交前上面的验证全部通过。

## 行为准则

见 [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)。
