---
name: cxb
description: 用户要求调用、委派或与 Codex 协作完成任务时使用；需要来自 Codex 的独立视角（代码、方案、架构、设计决策的第二意见）时也主动使用。
---

# cxb

cxb 是 Claude 与 Codex 之间的桥：经工作区 broker（`cxb-broker`）连接 `codex app-server`，提供派任务、审查、续聊、待回应事项和结果查看。命令名只有 `cxb`。环境有问题先跑 `cxb doctor`。

分工：Claude 负责理解需求与架构、写规划简报、核对结果；Codex 负责读源码、实现和验证。

## 1. 按任务选择审批模式

- 默认 `--approval never`：适合短小、确定、不需要外部工具的任务。
- 复杂实现、跨文件改动、需要搜索或事实核验、要调用 grok、sequential-thinking 等 MCP 工具时，**必须** `--approval auto`。never 下 MCP 请求可能被 Codex 拒绝，run 开始前会在 stderr 打一行提醒；提醒是诊断，不是失败。
- auto 由 Codex 的 Guardian 自动审批，仍可能被拒：先 `cxb inbox`，对 `gd_` 事项用 `cxb reply <ID> yes` 推翻拒绝。
- 不要把 `on-request` 当无人值守后台任务的默认值；确需人工审批时才显式使用，并配合 `cxb inbox`、`cxb reply`。
- 持久默认值用 `cxb config <键> <值>` 设置（例如 `cxb config approval auto`），命令行显式参数优先。

## 2. 规划简报

每次 run 前写清：

- 要做什么、已定的设计决定；
- 要改哪些文件、为什么；可参考的既有模式；
- 约束（范围、不许做的事）与验证命令。

要点：

- 先用目录或语义检索定位，再让 Codex 自己读源码；不要把整个仓库贴进提示词。
- 只描述工程要求，不写预先实现好的代码；不要诱导 Codex 中途找人确认可以自行判断的事。
- 一次 run 保持一个逻辑变更，后续用 `--resume <线程ID>` 续聊。
- 长提示词走标准输入：`Get-Content prompt.md -Raw | cxb run - --approval auto`，或 `cxb run -f prompt.md`。
- **不要在 shell 双引号提示词里写 `${...}`**：会被 shell 展开。改用文字描述，或走标准输入、文件。

## 3. 三个常用流程

```text
cxb run -s workspace-write --approval auto "实现……"          # 派任务：结果在 stdout，过程加 -v 走 stderr
cxb review --mode uncommitted                               # 审查未提交改动；review 永远只读
cxb run --resume <线程ID> "继续：……"                         # 在同一线程续聊
```

- review 默认 `pr` 模式（对比默认分支）；`--mode commit --ref <提交>`、`--mode uncommitted`；直接给审查说明即为 `custom` 模式。`-s`、`--approval` 对 review 只告警、忽略。
- 长任务可 `cxb run --detach ...`：交给 broker 托管后立刻返回「线程ID 短ID 运行ID 运行中」，之后 `cxb wait <线程ID>` 拿结果，`cxb show <线程ID> --tail` 看进展。
- 运行被停下或刹车后接着干：`cxb run --resume <线程ID> --continue [补充说明]`。续跑的沙箱与审批不继承原运行，按本次参数（`-v` 显示实际生效值）。
- 模板：`cxb run --template <名称> "..."`，列表见 `cxb config templates`。

<!-- TEMPLATES -->

## 4. 结果核对

- 每次 run 完成后，逐个读 Codex 报告修改过的文件，确认实际 diff；检查完整性、明显错误、导入、编译与测试。需要修正时用同一线程 `--resume`。
- 汇报时同时引用 Codex 的说明和你自己的观察，不要只复述 stdout。
- `cxb show <线程ID>` 看最近一轮结果；`--tail` 看最近进展；`--log` 看完整本地日志（权威记录）。
- 线程 ID 是 cxb 的线程短 ID（`cxb ls` 列出；`-v` 时 stderr 有 `thread started：<短ID>`），**不是** Claude Code 后台任务 ID。

## 5. 待回应事项

- `cxb inbox` 列出待回应事项，按 ID 前缀区分类型：`ap_` 审批、`qu_` 提问、`mc_` MCP 表单、`gd_` Guardian 拒绝。`--kind approval|question|mcp|guardian` 筛选。
- 回复：`cxb reply ap_… yes|no`；`qu_…` 可直接写文字，多题用 `{"问题ID":"答案"}` JSON；`mc_…` 按表单 schema 写 JSON；`gd_… yes` 推翻拒绝（之后用 `cxb run --resume <线程ID> "继续"` 开下一轮）。重复回复同一事项是幂等的。
- broker 重启后旧事项显示为 expired，reply 返回 2；过期的不能再答。
- `cxb wait` 不带 ID：等本工作区出现新事项（没有活动运行也没有待答事项时返回 10）；带 ID：等这次运行结束。

## 6. 后台与 shell 规则（Claude Code）

- 所有 cxb 命令都加 `dangerouslyDisableSandbox=true`：数据写在 `~\.codex-bridge\`，在沙箱允许范围之外。
- `run`、`review`、`wait` 要几分钟：用 `run_in_background=true` 启动，告诉用户已在运行后结束本轮。运行期间**不要轮询、阻塞或派代理盯着**，完成时会自动通知。用户问进度时读后台输出或 `cxb show <线程ID> --tail`。
- **禁止 shell 的 `&` 后台**（`cxb run "..." &`）：调用方会误判已完成并丢掉通知。也不要用轮询脚本冒充完成通知。
- **`cxb run` 一断开，broker 就立即叫 Codex 停下**（主动停止或意外断开都一样），确认停下之前显示「停止中」。进度会保存，之后用 `cxb run --resume <线程ID> --continue` 接着干。想让 Codex 不受断开影响地一直干，就用 `--detach`，再 `cxb wait <线程ID>`。
- 停止 Codex：停掉 `cxb run` 的后台任务，或 `cxb stop <线程ID> [--run <runId>]`。`cxb stop` 会等到确认（最多约 40 秒），也能停 `--detach` 托管中的线程；没确认会以退出码 3 提示「已发出停止，Codex 尚未确认停下，状态为停止中」，可以再次重试。`--run` 用于核实指定的历史 `lost`/`stopping` 运行；`--clear` 同时放弃 goal（默认只暂停）。指定运行不属于该线程退出 2，线程属于其他客户端、正在判定归属或实际执行线程被占用退出 8，broker 不可用退出 6。
- **退出码 5 不是失败**：运行遇到需要人回答的事项时，cxb 把它交给 broker 托管、Codex 不中断，然后以 5 退出并打印事项与命令。先 `cxb inbox` 看事项、`cxb reply` 回答，再 `cxb wait <线程ID>`（后台）接上拿最终结果。
- **退出码 9（目标受阻或刹车）**：goal 被标为 blocked、达到预算，或 broker 的自动续跑刹车替你暂停了 goal（stderr 有「自动续跑刹车：连续无动作／轮数上限」）。先 `cxb show <线程ID> --log` 看原因；要继续就 `cxb run --resume <线程ID> --continue [新的指示]`，要放弃目标就 `cxb stop <线程ID> --clear`。
- `cxb show <线程ID> --follow` 只在确实需要实时流时用。加 `--json` 时逐行输出 JSON（最后一行是终态）。它是**尽力而为**的实时显示，个别进展可能少显示；完整进展以 `cxb show <线程ID> --log` 为准。

## 7. 视觉任务

- 让 Codex 用原生的 `view_image` 读图片。
- 不要让 Codex 用 shell、PIL、ImageMagick 查询尺寸；确实需要尺寸时，由 Claude 在规划简报里提供。

## 8. 命令速查与退出码

```text
cxb run [提示词] | - | -f <文件>   [--resume <ID> [--continue]] [--template <名称>] [--goal <目标> [--budget <令牌>]] [--detach] [--memory]
cxb review [说明] [--mode pr|uncommitted|commit|custom] [--ref <提交>] [--base <分支>] [--resume <ID>]
cxb show <ID> [--tail|--log|--follow] [--server] [--limit <行>] [--full]
cxb ls [--all|--limit <数量>] [--local]
cxb inbox [--kind <类型>] [--all] [--limit <数量>]
cxb reply <事项ID> <内容>|-
cxb wait [ID] [--timeout <秒>]
cxb stop <ID> [--clear] [--run <runId>]
cxb rm <ID> [--purge]
cxb config [<键> [值]] [--unset] | cxb config templates
cxb doctor
```

全局选项：`-d <目录>`、`--json`（stdout 只有一个 JSON 对象；例外：`show --follow --json` 每行一个 JSON，最后一行含 `terminal:true`，要逐行解析）、`-v`（过程写 stderr）、`--timeout`、`-m`、`-r`、`-s`、`--approval`，放在命令前后都可以。

退出码（脚本按码分支，不要解析自然语言）：0 成功；1 用法或本地错误；2 找不到或有歧义；3 超时；4 被中断；5 等待回应；6 broker 不可用；7 Codex 失败；8 线程被占用；9 目标受阻或刹车；10 空闲；12 协议不匹配；130、143 被信号中断。

出故障先 `cxb doctor`；broker 不可用时用 `cxb ls --local` 和 `cxb show <ID>`（读本地记录）恢复上下文。

## 9. 安全和边界

- 不要把令牌、Cookie、完整环境变量写进提示词或交给 MCP。
- `cxb rm <ID>` 通过 broker 归档；没有生命周期记录时可便利停止最近一次未结束运行，已有待核实记录时直接重试同 kind 的归档或删除核实。`cxb rm <ID> --purge` 通过 `broker/threads/purge{serverDelete:true}` 一次完成服务器线程、关联 review 子线程运行记录和日志的永久删除，不可恢复。
- `rm` 遇 `THREAD_HELD`（退出码 8）会逐条列出阻塞线程、`runId` 和下一步：运行中用 `cxb stop <线程>`，停止中或失联未确认用 `cxb stop <线程> --run <runId>`，gd_ 事项先用 `cxb inbox` 处理，生命周期待核实则重试对应的 `cxb rm <ID>` 或 `cxb rm <ID> --purge`；不会自动停止或重放。
- 归档请求结果未知退出 6，提示重试 `cxb rm <ID>`；上游明确拒绝退出 7，并显示脱敏原因。purge 回包中列出的 `outcomeUnknown` 失败项提示重试 `cxb rm <ID> --purge`，JSON 保留逐项失败原因。
- 不逆向 Claude 的内部认证，不读取会话令牌。
