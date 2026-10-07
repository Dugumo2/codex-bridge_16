//! 生命周期的专用单次存储票据；终结失败后绝不重试同一版本。
use super::{disk::read_envelope_limited, disk::Disk, purge::RunHeader, StoreError};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Kind {
    Archive,
    Delete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Phase {
    LocalPurging,
    Forwarded,
    UpstreamDone,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Instance {
    pub pid: u32,
    pub created_at: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Attempt {
    pub attempt: u64,
    pub settled: bool,
    pub server_kind: String,
    pub instance: Option<Instance>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Record {
    pub operation_id: String,
    pub kind: Kind,
    pub thread_id: String,
    pub short_id: String,
    pub preview: Option<String>,
    pub phase: Phase,
    pub attempts: Vec<Attempt>,
    pub created_at: u64,
    pub updated_at: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct File {
    pub records: BTreeMap<String, Record>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResultValue {
    Written(u64),
    Scanned(Vec<RunHeader>),
    Run(Option<Box<crate::runs::RunRecord>>),
    Failed(String),
}

#[derive(Debug, Clone, Default)]
pub struct Ticket(Arc<Mutex<Option<ResultValue>>>);
impl PartialEq for Ticket {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Eq for Ticket {}
impl Ticket {
    pub fn result(&self) -> Option<ResultValue> {
        self.0.lock().unwrap().clone()
    }
    pub fn finish(&self, value: ResultValue) {
        let mut slot = self.0.lock().unwrap();
        if slot.is_none() {
            *slot = Some(value);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkKind {
    Write { version: u64, file: File },
    Scan { seeds: Vec<RunHeader> },
    ReadRun(String),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Work {
    pub kind: WorkKind,
    pub ticket: Ticket,
}

#[cfg(windows)]
pub(crate) fn instance_from_handle(
    pid: u32,
    handle: windows_sys::Win32::Foundation::HANDLE,
) -> Option<Instance> {
    use windows_sys::Win32::{Foundation::FILETIME, System::Threading::GetProcessTimes};
    let mut created = FILETIME {
        dwLowDateTime: 0,
        dwHighDateTime: 0,
    };
    let mut exited = created;
    let mut kernel = created;
    let mut user = created;
    // 句柄由调用方持有；所有输出指针均指向本栈帧。
    if unsafe { GetProcessTimes(handle, &mut created, &mut exited, &mut kernel, &mut user) } == 0 {
        return None;
    }
    Some(Instance {
        pid,
        created_at: (u64::from(created.dwHighDateTime) << 32) | u64::from(created.dwLowDateTime),
    })
}

pub(crate) fn instance_exited(instance: &Instance) -> bool {
    #[cfg(windows)]
    {
        use windows_sys::Win32::{
            Foundation::{CloseHandle, ERROR_INVALID_PARAMETER},
            System::Threading::{
                GetExitCodeProcess, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
            },
        };
        // 只查询进程身份，不终止进程；权限或查询失败都不能当作退出证据。
        let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, instance.pid) };
        if handle.is_null() {
            return std::io::Error::last_os_error().raw_os_error()
                == Some(ERROR_INVALID_PARAMETER as i32);
        }
        let observed = instance_from_handle(instance.pid, handle);
        let mut code = 259;
        let exited = unsafe { GetExitCodeProcess(handle, &mut code) } != 0 && code != 259;
        unsafe { CloseHandle(handle) };
        observed.is_some_and(|value| value.created_at != instance.created_at || exited)
    }
    #[cfg(not(windows))]
    {
        let _ = instance;
        false
    }
}

pub(super) fn recover_instances(file: &mut File) {
    for record in file.records.values_mut() {
        for attempt in &mut record.attempts {
            if attempt.server_kind == "private"
                && attempt.instance.as_ref().is_some_and(instance_exited)
            {
                attempt.settled = true;
            }
        }
    }
}

impl Disk {
    /// purge 与生命周期共用只读扫描，损坏和读取失败均向上传递。
    pub(super) fn scan_run_headers(&self) -> Result<BTreeMap<String, RunHeader>, StoreError> {
        let mut headers = BTreeMap::new();
        let mut bytes = 0;
        for entry in std::fs::read_dir(self.dir.join("runs"))? {
            self.gate.check()?;
            let entry = entry?;
            if entry.path().extension().is_none_or(|ext| ext != "json") {
                continue;
            }
            bytes += entry.metadata()?.len();
            if bytes > super::load::MAX_INITIALIZATION_BYTES {
                return Err(std::io::Error::other("运行头扫描超过字节预算").into());
            }
            if let Some(header) = read_envelope_limited::<RunHeader>(
                &entry.path(),
                super::load::MAX_INITIALIZATION_BYTES,
            )? {
                if header.epoch > self.epoch {
                    return Err(StoreError::Superseded {
                        file: entry.file_name().to_string_lossy().into(),
                    });
                }
                headers.insert(header.payload.run_id.clone(), header.payload);
            }
        }
        Ok(headers)
    }

    pub(super) fn lifecycle_work(&mut self, work: &Work) -> Result<ResultValue, StoreError> {
        match &work.kind {
            WorkKind::Write { version, file } => {
                if *version <= self.lifecycle_version {
                    return Err(std::io::Error::other("拒绝过期生命周期版本").into());
                }
                self.lifecycle_version = *version;
                self.write_json(
                    "lifecycle.json",
                    &serde_json::to_vec(file).expect("生命周期记录可序列化"),
                )?;
                Ok(ResultValue::Written(*version))
            }
            WorkKind::Scan { seeds } => {
                let mut headers = self.scan_run_headers()?;
                for seed in seeds {
                    headers.insert(seed.run_id.clone(), seed.clone());
                }
                Ok(ResultValue::Scanned(headers.into_values().collect()))
            }
            WorkKind::ReadRun(id) => {
                if !super::queue::valid_id(id) {
                    return Err(std::io::Error::other("运行编号无效").into());
                }
                let record = read_envelope_limited::<crate::runs::RunRecord>(
                    &self.dir.join(super::run_file(id)),
                    super::load::MAX_INITIALIZATION_BYTES,
                )?;
                if record.as_ref().is_some_and(|r| r.epoch > self.epoch) {
                    return Err(StoreError::Superseded {
                        file: super::run_file(id),
                    });
                }
                Ok(ResultValue::Run(record.map(|r| Box::new(r.payload))))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{
        disk::{Disk, Gate},
        tests::{read, run, Temp},
        StoreConfig,
    };
    use std::sync::atomic::AtomicBool;

    fn open(config: &mut StoreConfig) -> (Disk, crate::store::Loaded) {
        Disk::initialize(
            config,
            Gate {
                cancelled: Arc::new(AtomicBool::new(false)),
                initialization: None,
            },
        )
        .unwrap()
    }
    fn record(phase: Phase, private: bool) -> Record {
        Record {
            operation_id: "op-test".into(),
            kind: Kind::Delete,
            thread_id: "P".into(),
            short_id: "12345678".into(),
            preview: Some("待删除线程".into()),
            phase,
            attempts: if phase == Phase::LocalPurging {
                vec![]
            } else {
                vec![Attempt {
                    attempt: 1,
                    settled: false,
                    server_kind: if private { "private" } else { "shared" }.into(),
                    instance: private.then_some(Instance {
                        pid: u32::MAX,
                        created_at: 1,
                    }),
                }]
            },
            created_at: 1,
            updated_at: 2,
        }
    }

    #[test]
    fn 真实磁盘运行头扫描覆盖已淘汰失联运行且损坏不可忽略() {
        let temp = Temp::new();
        let mut disk = temp.disk();
        let mut old = run("old", "P", 1);
        old.state = crate::runs::RunState::Lost;
        old.stop_outcome = Some(crate::runs::StopOutcome::Unconfirmed);
        old.review_thread_id = Some("C".into());
        disk.write_json("runs/old.json", &serde_json::to_vec(&old).unwrap())
            .unwrap();
        let work = Work {
            kind: WorkKind::Scan { seeds: vec![] },
            ticket: Ticket::default(),
        };
        let ResultValue::Scanned(headers) = disk.lifecycle_work(&work).unwrap() else {
            panic!("应返回扫描头")
        };
        assert_eq!(headers.len(), 1);
        assert_eq!(headers[0], RunHeader::from(&old));
        std::fs::write(disk.dir.join("runs/broken.json"), b"{").unwrap();
        assert!(disk.lifecycle_work(&work).is_err());
    }

    #[test]
    fn 生命周期损坏或结构无效启动后保留失败而缺失为空() {
        let temp = Temp::new();
        let (disk, loaded) = open(&mut temp.config());
        assert!(loaded.lifecycle.unwrap().unwrap().records.is_empty());
        let path = disk.dir.join("lifecycle.json");
        drop(disk);
        for bytes in [
            b"{".as_slice(),
            br#"{"epoch":1,"revision":1,"payload":{}}"#,
            br#"{"epoch":1,"revision":1,"payload":{"records":{"P":{}}}}"#,
        ] {
            std::fs::write(&path, bytes).unwrap();
            let (_, loaded) = open(&mut temp.config());
            assert!(loaded.lifecycle.unwrap().is_err());
        }
    }

    #[test]
    fn 单次版本失败后不可再发布且新版本不被旧版本覆盖() {
        let temp = Temp::new();
        let mut disk = temp.disk();
        let path = disk.dir.join("lifecycle.json");
        std::fs::create_dir(&path).unwrap();
        let work = |version, file| Work {
            kind: WorkKind::Write { version, file },
            ticket: Ticket::default(),
        };
        let older = File {
            records: BTreeMap::from([("P".into(), record(Phase::Forwarded, false))]),
        };
        assert!(disk.lifecycle_work(&work(1, older.clone())).is_err());
        std::fs::remove_dir(&path).unwrap();
        assert_eq!(
            disk.lifecycle_work(&work(2, File::default())).unwrap(),
            ResultValue::Written(2)
        );
        assert!(disk.lifecycle_work(&work(1, older)).is_err());
        assert!(read::<File>(&path).payload.records.is_empty());
    }

    #[test]
    fn 三种持久阶段重启保留记录与实例终结证据且不重建索引() {
        for phase in [Phase::LocalPurging, Phase::Forwarded, Phase::UpstreamDone] {
            for private in [false, true] {
                let temp = Temp::new();
                let mut disk = temp.disk();
                let record = record(phase, private);
                disk.lifecycle_work(&Work {
                    kind: WorkKind::Write {
                        version: 1,
                        file: File {
                            records: BTreeMap::from([("P".into(), record.clone())]),
                        },
                    },
                    ticket: Ticket::default(),
                })
                .unwrap();
                let old = run("old", "P", 1);
                disk.write_json("runs/old.json", &serde_json::to_vec(&old).unwrap())
                    .unwrap();
                drop(disk);
                let (_, loaded) = open(&mut temp.config());
                let restored = &loaded.lifecycle.as_ref().unwrap().as_ref().unwrap().records["P"];
                assert_eq!(restored.phase, phase);
                assert_eq!(restored.short_id, record.short_id);
                assert_eq!(restored.preview, record.preview);
                assert!(loaded.index.threads.is_empty());
                if phase != Phase::LocalPurging {
                    assert_eq!(restored.attempts[0].settled, private && cfg!(windows));
                }
            }
        }
    }

    #[test]
    #[cfg(windows)]
    fn 私有实例查询区分仍在已退出和编号复用() {
        use windows_sys::Win32::System::Threading::GetCurrentProcess;
        let current =
            instance_from_handle(std::process::id(), unsafe { GetCurrentProcess() }).unwrap();
        assert!(!instance_exited(&current));
        assert!(instance_exited(&Instance {
            pid: current.pid,
            created_at: current.created_at + 1
        }));
        assert!(instance_exited(&Instance {
            pid: u32::MAX,
            created_at: 1
        }));
        let mut file = File {
            records: BTreeMap::from([("P".into(), record(Phase::Forwarded, true))]),
        };
        file.records.get_mut("P").unwrap().attempts[0].instance = None;
        recover_instances(&mut file);
        assert!(!file.records["P"].attempts[0].settled);
    }

    #[test]
    fn 子线程本地删除集合不包含祖先兄弟与共用日志() {
        use crate::store::purge::{PurgeTicket, PurgeWork};
        let temp = Temp::new();
        let mut disk = temp.disk();
        for (id, parent, child) in [("a", "P", "C"), ("b", "P", "S"), ("c", "C", "C2")] {
            let mut r = run(id, parent, 1);
            r.review_thread_id = Some(child.into());
            r.review_short_id = Some(format!("short-{child}"));
            disk.write_json(&format!("runs/{id}.json"), &serde_json::to_vec(&r).unwrap())
                .unwrap();
        }
        let plan = disk
            .prepare_purge(&PurgeWork {
                thread: "C".into(),
                plan: Default::default(),
                index: None,
                ticket: PurgeTicket::default(),
                seeds: vec![],
            })
            .unwrap();
        assert_eq!(
            plan.threads,
            std::collections::BTreeSet::from(["C".into(), "C2".into()])
        );
        assert!(!plan.files.contains("runs/short-P.log"));
        assert!(!plan.files.contains("runs/b.json"));
        assert!(!plan.files.contains("runs/short-S.log"));
    }
}
