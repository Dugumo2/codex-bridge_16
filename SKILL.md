---
name: cxb
description: 用户要求调用、委派或与 Codex 协作（实现、调研、审查、续跑）时使用；需要 Codex 的独立视角（代码、方案、架构、设计决策的第二意见）时也主动使用。Claude 先快速定位、写规划简报，Codex 读源码、实现、验证，Claude 核对结果。
---

# cxb

cxb 是 Claude 与 Codex 之间的桥：每个工作区（git 顶层目录或当前目录）一个 `cxb-broker`，连接 Codex 的 app-server，负责派任务、托管、待回应事项、停止与归档。命令只有 `cxb`。环境有问题先跑 `cxb doctor`。

## 1. 分工与授权

- **Claude 当架构师**：用会话里的检索工具（Grep、Glob、语义检索）几分钟内摸清结构，写规划简报；不逐个通读源码，不把实现代码写进提示词——那是 Codex 会重做的活。
- **Codex 动手**：自己读源码、实现、跑验证。
- **Claude 收口**：核对实际改动后再汇报（第 8 节）。派出去以后 Claude 继续做自己的事，不空等。
- 派单、续跑、停止、归档等会改变状态的动作，按用户的授权规矩先说方案。

## 2. 按场景选参数

默认：最新模型、`medium` 推理（`cxb config` 里的持久默认）、`workspace-write`、`--approval never`。命令行参数优先于持久默认。

| 场景 | 参数组合 | 要点 |
|---|---|---|
| 问答、读代码调研 | `cxb run -s read-only "…"` | 不改文件 |
| 要搜索、查资料、用 MCP（grok、sequential-thinking 等） | 加 `--approval auto` | never 下 MCP 会被服务端直接拒绝（退出 7） |
| 难题、方案判断、复杂实现 | 加 `-r high`（或 `xhigh`） | 简单活保持默认 |
| 小而确定的改动 | `cxb run -f brief.md` | |
| 跨文件实现 | `cxb run --approval auto -r high -f brief.md` | |
| 长任务、中途可能要拍板 | 加 `--template collab` | Codex 提问进收件箱，cxb 退出 5；短任务不要加 |
| 审实现计划 | `cxb run --template plan-review -f plan.md` | 模板自带只读 |
| 长时间运行、不能被断开打断 | 加 `--detach`，之后 `cxb wait <线程ID>` | broker 托管（第 4 节） |
| 有明确完成条件的多轮自主任务 | `--goal "完成条件" --budget <token> --template collab --detach` | 刹车或受阻退出 9 |
| 同一件事追加要求 | `cxb run --resume <线程ID> "…"` | 保留上下文；新事另开线程 |
| 被停下、刹车、超时后接着干 | `cxb run --resume <线程ID> --continue [补充]` | 见下 |
| 审查改动 | `cxb review`（默认 pr，可 `--base <分支>`）、`--mode uncommitted`、`--mode commit --ref <提交>` | 永远只读、never |
| 自定义角度审查 | `cxb review "关注……"` | custom 模式；`--resume <线程ID>` 可追问 |

- `--continue`：只能配 `--resume`；补充说明只能写在命令行，不能用 `-f` 或标准输入；不能与 `--goal`、`--template` 同用；**沙箱与审批不继承原运行**，需要的话重新给 `-s`、`--approval`；`--run <runId>` 指定接哪次运行。cxb 会生成续跑提示（原始要求、已完成步骤、观察到的改动、未确认操作、目标原文），让 Codex 先核验工作区再继续。
- goal：服务器会一轮接一轮自动续跑。broker 有两道刹车（连续 3 轮没有实际动作、单次运行满 50 轮）暂停目标并退出 9；`--budget` 是另一道按 token 的刹车。完成条件要写成可核验的状态。
- review 的 `-s`、`--approval` 只告警、被忽略；`--goal` 不可用。
- 普通 `cxb review` 在新建的独立线程上审查；`--resume <线程ID>` 在该线程上追加一轮审查。
- 模板：`cxb config templates` 列出内置和用户模板（用户模板放数据根目录下的 `templates\<名称>.md`，同名时优先于内置；数据根目录是 `CXB_HOME`，未设时为 `~\.codex-bridge`）；模板建议的沙箱只在没显式给 `-s` 时生效。

<!-- TEMPLATES -->

- 服务器档位 `server`（`cxb config server …` 或环境变量 `CXB_SERVER`）：`auto`（默认）在 Codex 桌面应用开着时接入它的服务器，任务在桌面应用里可见、可接手，但别的客户端可能占用同一线程（退出 8），归档、删除结果未知时可能一直待核实；`private` 自起服务器，进程由 broker 回收。批量、并行、无人值守的任务用 `private`。

## 3. 规划简报

- 简报里可以让 Codex 使用子代理：子代理的提问与审批同样进收件箱（第 5 节）；`cxb stop` 连带停止整棵子树；父线程不等子代理就结束时，cxb 停掉残留子代理并以退出 4 结束。v2 子线程不能直接 `--resume`（退出 1），从父线程继续。

每次 run 写清：要做什么；已定的设计决定；要改哪些文件、为什么；参考哪个既有实现（「照 `path/X` 里的 Y 写」）；约束与不许做的事；验证命令与完成标准。

- **给方向，不给代码**：写需求与参考位置，让 Codex 自己读、自己写。诊断类任务可以附必要的错误片段。
- **技术化措辞**：只写工程要求，不写伦理化或抽象理由，避免误拒。
- **指令无歧义**：不要让两条要求互相打架（例如「只执行第一步：追加一行」与「先核实文件存在」并存，Codex 会因文件不存在而停下）。需要 Codex 必须用某个工具或命令时，直接写出来。
- **能查证的别问**：写明「可自行查证的自行判断，按合理假设推进并在结果里说明」；只有真正要拍板的才让它提问（配 `--template collab`）。
- **一次一个逻辑变更**，后续用 `--resume` 接着做。
- **传提示词**：短的直接写在命令行；长的先用 Write 写成 UTF-8 文件，再 `cxb run -f <文件>`（由 cxb 按 UTF-8 读取，不经 shell）。命令行双引号里**不要写 `${...}`**，会被 shell 展开。
- **视觉任务**：让 Codex 用原生 `view_image` 读图；不要让它用 shell、PIL、ImageMagick 查尺寸，确实需要尺寸时写进简报。

坏例子（写代码）：「AppConfig.java 第 129 行加字段 `TargetSeat targetSeat`，新增 `record TargetSeat(boolean enabled, long deviceId)`……」
好例子（写简报）：「给座位扫描器加『指定座位』功能。配置：Scan 里加 target-seat（enabled、deviceId、可选 roomId），照 AppConfig.java 里 MultiAccount 的写法；解析照 AppConfigLoader 的 multiAccount；enabled 时 deviceId 必须为正整数。扫描：启用时只保留目标座位，roomId 缺省查全部房间。先读 SeatScanner.java 的 priorityDeviceIds 过滤逻辑。验证：`mvn compile -q` 通过，注释用简体中文。」

## 4. 在 Claude Code 里怎么跑

- 所有 cxb 命令加 `dangerouslyDisableSandbox=true`：数据在数据根目录（`CXB_HOME`，未设时为 `~\.codex-bridge`），不在沙箱允许范围内。
- `run`、`review`、`wait` 要几分钟：用 Bash 工具的 `run_in_background=true` 启动，告诉用户已在运行后结束本轮；完成会自动通知。期间**不轮询、不阻塞、不派代理盯着**；用户问进度时读后台输出或 `cxb show <线程ID> --tail`。
- **禁止 shell 的 `&` 后台**（`cxb run "…" &`）：命令立即返回，通知提前触发，结果丢失。
- **`cxb run` 一断开，Codex 就被叫停**（后台任务被停、意外断开都一样），进度保存，之后用 `--continue` 接着干。要不受断开影响地长时间运行，用 `--detach` 交给 broker 托管，再 `cxb wait <线程ID>`。`run_in_background` 是 Claude Code 的后台，`--detach` 是 broker 托管，两回事。
- 线程 ID 是 cxb 的线程短 ID（`cxb ls` 列出），**不是** Claude Code 的后台任务 ID。
- 可以同时派多个线程并行；遇到 429、`Concurrency limit` 等限流，停下向用户报告，不自动重试。
- 脚本化处理用 `--json`（stdout 只有一个 JSON 对象），按退出码和 `error.code` 分支，不解析自然语言；`-v` 把过程写到 stderr。

## 5. 待回应事项：怎么处理、谁拍板

Codex 需要人回应时，cxb 把线程交给 broker 托管、以退出码 5 结束并打印事项。先 `cxb inbox` 看（`--kind approval|question|mcp|guardian` 筛选），再 `cxb reply <事项ID> <内容>`，最后 `cxb wait <线程ID>`（后台）接上。

| 事项 | 何时出现 | 回复 | 期限 | 谁拍板 |
|---|---|---|---|---|
| `qu_` 提问 | 任何审批策略下 Codex 主动提问 | 文字；多题用 `{"问题ID":"答案"}` | 1 小时 | 能从仓库、文档、已有对话查证的技术细节，Claude 查证后直接答并写明依据；涉及需求取舍、范围增减、对外可见行为、费用或额度、破坏性操作、用户偏好的，转述给用户（带 Codex 给的上下文、选项、倾向），用户定了再答；拿不准就问用户 |
| `ap_` 审批 | `on-request`、`untrusted`、`on-failure` 下 | `yes` / `no` | 1 小时 | 已获准任务范围内、工作区内的常规工程动作（读写、编译、测试、只读查询）Claude 直接批准；项目外写入、删除或覆盖用户数据、联网外发、凭据、git push 或强推、改全局环境，拒绝并转述给用户 |
| `gd_` Guardian 拒绝 | `--approval auto` 下 | `yes` 推翻 / `no` 维持 | 24 小时 | 默认尊重拒绝；Claude 能具体说明是误判、且动作属上一行的常规范围时才 `yes`，汇报里写理由；否则转述给用户 |
| `mc_` MCP 表单 | MCP 服务器要求填写 | URL 型只能 `yes`/`no`；表单型按 schema 提交 JSON | 1 小时 | 按 schema 填；涉及授权、外部账号、付费、隐私的转述给用户 |

- 审批策略的实际行为：`never` 下命令与改文件审批自动通过、权限申请按会话授予、MCP 表单接受空内容，**提问仍进收件箱**；`auto` 是 on-request 加 Guardian 审核，被拒成 `gd_`；其余策略每个请求都进收件箱。不要把 `on-request` 当无人值守任务的默认值。
- **`gd_ yes` 不会让已结束的那一轮接着跑**：回复后用 `cxb run --resume <线程ID> "继续"` 开下一轮。
- 要等用户拍板时，告诉用户「Codex 在等，1 小时后过期按拒绝处理」。过期或已撤销的事项 reply 返回 2；broker 重启后旧事项全部过期。
- Codex 也可能不走提问工具，而是用文字写出问题并结束本轮：读结果，用 `cxb run --resume <线程ID> "回答……"` 继续。

## 6. 退出码怎么处理

| 码 | 含义 | 下一步 |
|---|---|---|
| 0 | 成功 | 核对结果（第 8 节） |
| 2 | 找不到、有歧义，或事项已过期/撤销 | `cxb ls` 找对 ID；必要时加 `-d <目录>` |
| 3 | 超时 | `cxb show <线程ID> --tail` 看进展；长任务改 `--detach` 或加大 `--timeout`；stop 未确认按第 7 节「停止未确认时」处理，不要循环重发 |
| 4 | 运行被中断 | 看输出的中断原因；`subagentsUnfinished` 表示父线程没等子代理就结束、残留子代理已被停下，按需 `--resume --continue` 让父线程重新派生或汇总 |
| 5 | 等待回应（不是失败） | 第 5 节 |
| 6 | broker 不可用 | `cxb doctor`；`cxb ls --local`、`cxb show <线程ID>` 读本地记录 |
| 7 | Codex 失败 | 读 stderr；MCP 被拒就改 `--approval auto` |
| 8 | 线程被占、停止中或待核实 | 按 stderr 提示做（`cxb stop`、`cxb stop <线程> --run <runId>`、重试 `cxb rm`），不要硬重试 |
| 9 | 目标受阻或空转刹车 | `cxb show <线程ID> --log` 看原因；继续用 `--continue [新指示]`，放弃用 `cxb stop <线程ID> --clear` |
| 1、10、12、130、143 | 用法错误、空闲、协议不匹配、信号 | 按 stderr 处理 |

## 7. 等待、查看与线程管理

- `cxb wait`：不带 ID 等本工作区出现新事项（没有活动也没有待答事项时返回 10）；带**线程 ID** 等整次运行结束（跨 goal 多轮）；带**事项 ID**（`ap_`/`qu_`/`mc_`/`gd_`）等它被回复、过期或撤销。
- `cxb show <线程ID>`：默认最近一轮结果；`--tail` 最近进展；`--log` 完整本地日志（权威记录）；`--follow` 实时跟随（尽力而为，`--json` 时每行一个 JSON）；`--server` 从服务器读。
- `cxb ls`：列线程（合并服务器信息）；`--local` 只读本地。
- 续聊还是新开：同一件事、同一项目接着做 → `--resume`；被停下或刹车 → `--resume --continue`；不相关的事或换项目 → 新线程；线程卡住 → 先 `cxb stop` 再决定。
- `cxb stop <线程ID>`：等 Codex 确认停下（最多约 40 秒）；有子代理时连带停整棵子树；确认还要求该线程（及子线程）由 Codex 管理的后台命令已清理。未确认退出 3，见下面「停止未确认时」。默认只暂停目标，`--clear` 放弃目标；`--run <runId>` 核实某次历史失联运行。
- `cxb rm <线程ID>`：归档（可恢复）；`--purge` 永久删除，不可恢复。有未结束的运行或仍有子代理子树时退出 8 并逐条给出下一步，按提示照做，不要自行重放。

**停止未确认时**（`stopOutcome:"unconfirmed"`、退出 3）：

- 先看报告：卡在哪个成员（`childrenStop.members`）、发现缺什么（`discovery.missing`）、当前步骤、后台命令清理状态（`backgroundCleanup.state` 与 `failureReason`）。
- 已结束运行的历史核验若返回 `cleanupState:"released"`（整树见 `childrenStop.cleanupState`），本次核验已结束，不占用线程，也不会后台重试；可以直接发起新工作，不必对单线程调用 `--abandon`。这仍是未确认，不能据此宣称进程已经退出。
- 临时故障（超时、断连）可等 broker 后台复核，隔一段时间 `cxb show <线程ID>` 看结论；不要在原线程循环 stop/resume。
- 子代理整树停止确认无法补齐证据时，执行一次 `cxb stop <线程> --run <运行> --abandon`，显式放弃本次树清理责任：仍退出 3，`actionApplied:true` 只表示放弃已应用，**不表示已经停止**。根轮次已有结束、goal 制动及所有权证据且请求已落定时，会将尚未提交的父运行以 interrupted、stopOutcome:unconfirmed、cleanupState:abandoned 结束并释放根认领，同线程可发起新运行（不等于 `--continue` 的停止核验通过）。回复 `childrenStop.rootRelease` 明确给出放弃时的 released 与 reason；根仍活动、证据不足或共享/外部认领冲突时保留占用并说明原因，其他运行的认领不动。残留子代理或命令仍可能运行，宜换新线程，必要时换隔离工作目录，避免互相覆盖。重复 stop 不会重启已放弃的清理；show/wait 保留放弃报告，不再提示先 stop 核实。`--abandon` 只作用于整树停止：单线程返回 `treeStopNotFound`。
- `backgroundCleanup.state` 为 `capabilityLimited`（本次后台方法不存在，或当前连接缺少其 `experimentalApi` 能力）时，重复 stop 补不了证据，线程会一直处于停止中直到 broker 重启（已知限制）：告诉用户，可改用私有档（`cxb config server private`）。判定仅接受匹配本次请求 ID 的 `-32601`，或 `-32600` 且顶层消息为 ``Invalid request: unknown variant `<本次方法>`, expected …`` / 精确为 `<本次方法> requires experimentalApi capability`；其他含 experimental、unsupported、not supported 的消息不据此判为能力受限。
- 停止确认的后台清理范围仅为「Codex 受管的后台命令」；空列表不保证操作系统没有残留后代进程。

已卸载线程若有当前 app-server 连接的关闭依据，可返回 `confirmed`；`backgroundCleanup.state` 仍为 `notLoaded`，`verificationBasis` 写明 `thread/closed` 或 `thread/read:notLoaded` 的来源和观察时间。stop 不会为了取得空列表而 resume；正常新派单的 resume 不受此限制。

## 8. 结果核对与汇报

- 逐个读 Codex 报告修改过的文件，确认实际改动；检查完整性、明显错误、导入、编译与测试。
- 只做了一部分或有错，用同一线程 `--resume` 指出具体位置让它补改。
- 汇报时分开写「Codex 说它做了什么」和「实际改动是什么」，不要只复述 stdout。

## 9. 命令速查

```text
cxb run [提示词] | - | -f <文件>   [--resume <ID> [--continue [--run <runId>]]] [--template <名称>] [--goal <完成条件> [--budget <token>]] [--detach] [--memory]
cxb review [说明] [--mode pr|uncommitted|commit|custom] [--ref <提交>] [--base <分支>] [--resume <ID>]
cxb show <ID> [--tail|--log|--follow] [--server] [--limit <行>] [--full]
cxb ls [--all|--limit <数量>] [--local]
cxb inbox [--kind <类型>] [--limit <数量>]
cxb reply <事项ID> <内容>|-
cxb wait [线程ID|事项ID] [--timeout <秒>]
cxb stop <ID> [--clear] [--run <runId> [--abandon]]
cxb rm <ID> [--purge]
cxb config [<键> [值]] [--unset] | cxb config templates
cxb doctor
```

全局选项（命令前后都可）：`-d <目录>`、`--json`、`-v`、`--timeout <秒>`（默认 3600）、`-m <模型>`、`-r <推理强度>`、`-s <沙箱>`、`--approval <策略>`。可用模型看 `cxb doctor`；`--memory` 允许 Codex 记忆新线程，默认关。

## 10. 安全边界

- 不把令牌、Cookie、完整环境变量写进提示词或交给 MCP。
- `cxb rm --purge` 不可恢复。
- 不逆向 Claude 的内部认证，不读取会话令牌。
