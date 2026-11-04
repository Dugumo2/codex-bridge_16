//! 会话文件夹具：在真实临时目录里写 jsonl，按状态机发出的动作真实执行路径回包与文件读取。
//!
//! 没有安装夹具时（多数既有测试），路径请求按「app-server 不给 path」回答，模拟旧版或远端 app-server；
//! 这时状态机必须与原来的宽松回退完全一样。安装后，thread/read 回包给出登记的路径，
//! `ReadRollout` 用生产同一个 [`crate::rollout::read_chunk`] 读真实文件，读取次数与字节数都记账。
use crate::claim::{Action, Event, Machine, Operation, TimerKind};
use serde_json::{json, Value};
use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub(crate) struct Fixture {
    pub dir: PathBuf,
    /// 线程 → 会话文件路径（thread/read 回包里的 path）。
    pub paths: BTreeMap<String, String>,
    /// 线程 → 原样的 thread/read 回包（覆盖默认回包，测路径不合规或请求失败）。
    pub replies: BTreeMap<String, Result<Value, Value>>,
    pub path_reads: u64,
    pub reads: u64,
    pub bytes: u64,
    /// 每次读取的 (线程, 读出字节数)。
    pub log: Vec<(String, u64)>,
    /// 暂扣不执行的读取（测读取在途时换代、停止）。
    pub hold: bool,
    pub held: Vec<crate::rollout::RolloutReadRequest>,
}

thread_local! {
    static FIXTURE: RefCell<Option<Fixture>> = const { RefCell::new(None) };
}

/// 本测试线程的会话文件目录；每次调用新建一个，互不干扰。
pub(crate) fn install() -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "cxb-rollout-fixture-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(dir.join("sessions").join("2026").join("11").join("05")).unwrap();
    FIXTURE.with(|f| {
        *f.borrow_mut() = Some(Fixture {
            dir: dir.clone(),
            ..Default::default()
        })
    });
    dir
}

/// 卸下本测试线程的夹具（同一测试里先后跑「有夹具」与「无夹具」的情形时用）。
pub(crate) fn uninstall() {
    FIXTURE.with(|f| *f.borrow_mut() = None);
}

pub(crate) fn installed() -> bool {
    FIXTURE.with(|f| f.borrow().is_some())
}

pub(crate) fn with<R>(f: impl FnOnce(&mut Fixture) -> R) -> R {
    FIXTURE.with(|cell| f(cell.borrow_mut().as_mut().expect("已安装会话文件夹具")))
}

/// 为线程建会话文件（路径形如 `<目录>/sessions/2026/11/05/rollout-<时间>-<线程>.jsonl`），返回路径。
pub(crate) fn create(thread: &str) -> String {
    with(|f| {
        let path = f
            .dir
            .join("sessions")
            .join("2026")
            .join("11")
            .join("05")
            .join(format!("rollout-2026-11-05T01-00-00-{thread}.jsonl"));
        std::fs::File::create(&path).unwrap();
        let path = path.to_str().unwrap().to_owned();
        f.paths.insert(thread.into(), path.clone());
        path
    })
}

pub(crate) fn path(thread: &str) -> String {
    with(|f| f.paths[thread].clone())
}

/// 按上游 `RolloutLine` 的形状追加一行；时间戳取机器墙钟加偏移，与 raw 的观察时刻同一时钟。
pub(crate) fn append_line(thread: &str, wall_ms: u64, kind: &str, payload: Value) {
    let line = json!({"timestamp": timestamp(wall_ms), "type": kind, "payload": payload});
    append_raw(thread, &format!("{line}\n"));
}

pub(crate) fn append_raw(thread: &str, text: &str) {
    let path = path(thread);
    let mut file = std::fs::OpenOptions::new().append(true).open(path).unwrap();
    file.write_all(text.as_bytes()).unwrap();
}

pub(crate) fn timestamp(wall_ms: u64) -> String {
    let secs = wall_ms / 1000;
    let millis = wall_ms % 1000;
    let days = secs / 86_400;
    let rem = secs % 86_400;
    // days_from_civil 的逆变换。
    let z = days as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{millis:03}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

fn step(machine: &mut Machine, event: Event) -> Vec<Action> {
    let (next, actions) = crate::claim::step(std::mem::take(machine), event);
    *machine = next;
    actions
}

/// 执行动作里的路径请求与文件读取，把产生的动作并入 `actions`；有界。
pub(crate) fn service(machine: &mut Machine, actions: &mut Vec<Action>) {
    let mut queue: VecDeque<Action> = actions.iter().filter(|a| relevant(a)).cloned().collect();
    let mut rounds = 0;
    while let Some(action) = queue.pop_front() {
        rounds += 1;
        assert!(rounds < 256, "会话文件服务链必须有界");
        let next = match action {
            Action::Request(context) => {
                let Operation::RolloutPath { thread, .. } = &context.operation else {
                    continue;
                };
                let result = if installed() {
                    with(|f| {
                        f.path_reads += 1;
                        f.replies.get(thread).cloned().unwrap_or_else(|| {
                            Ok(match f.paths.get(thread) {
                                Some(path) => json!({"thread":{"id":thread,"path":path}}),
                                None => json!({"thread":{"id":thread,"path":null}}),
                            })
                        })
                    })
                } else {
                    // 旧版或远端 app-server：回包里没有可用的 path。
                    Ok(json!({"thread":{"id":thread}}))
                };
                step(machine, Event::Reply { context, result })
            }
            Action::ReadRollout(request) => {
                assert!(installed(), "没有会话文件夹具却发出了文件读取");
                if with(|f| f.hold) {
                    with(|f| f.held.push(request));
                    continue;
                }
                read(machine, request)
            }
            _ => continue,
        };
        queue.extend(next.iter().filter(|a| relevant(a)).cloned());
        actions.extend(next);
    }
}

fn read(machine: &mut Machine, request: crate::rollout::RolloutReadRequest) -> Vec<Action> {
    let result = crate::rollout::read_chunk(&request.path, request.cursor.as_ref());
    let bytes = result.as_ref().map_or(0, |c| c.bytes_read);
    with(|f| {
        f.reads += 1;
        f.bytes += bytes;
        f.log.push((request.thread.clone(), bytes));
    });
    step(
        machine,
        Event::RolloutRead {
            id: request.id,
            thread: request.thread,
            result,
        },
    )
}

/// 放行暂扣的读取（结果按原编号回来）。
pub(crate) fn release_held(machine: &mut Machine) -> Vec<Action> {
    let held = with(|f| {
        f.hold = false;
        std::mem::take(&mut f.held)
    });
    let mut out = vec![];
    for request in held {
        let mut next = read(machine, request);
        service(machine, &mut next);
        out.extend(next);
    }
    out
}

fn relevant(action: &Action) -> bool {
    matches!(action, Action::ReadRollout(_))
        || matches!(action, Action::Request(c) if matches!(c.operation, Operation::RolloutPath { .. }))
}

/// 推进一个轮询间隔并触发轮询计时（若已排）；返回产生的动作。
pub(crate) fn poll(machine: &mut Machine) -> Vec<Action> {
    let step_ms = crate::relations::rollout_pairing::ROLLOUT_POLL_MS;
    machine.clock.mono_ms += step_ms;
    machine.clock.now_ms += step_ms;
    let Some((_, generation)) = machine.relations.aux.rollout.timer else {
        return vec![];
    };
    let mut out = step(
        machine,
        Event::Timer {
            token: Machine::relation_token(),
            kind: TimerKind::RolloutPoll { generation },
        },
    );
    service(machine, &mut out);
    out
}

/// 动作里是否有任何会话文件相关的请求或读取。
pub(crate) fn touches_rollout(actions: &[Action]) -> bool {
    actions.iter().any(|a| {
        relevant(a)
            || matches!(
                a,
                Action::Schedule {
                    kind: TimerKind::RolloutPoll { .. },
                    ..
                }
            )
            || matches!(
                a,
                Action::Unschedule {
                    key: crate::claim::TimerKey::RolloutPoll
                }
            )
    })
}
