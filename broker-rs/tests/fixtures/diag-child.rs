//! 由进程级测试单独编译的无依赖夹具；所有同步点有期限。

use std::ffi::c_void;
use std::io::{Read, Write};
use std::os::windows::io::AsRawHandle;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

#[link(name = "kernel32")]
extern "system" {
    fn GetCurrentProcess() -> *mut c_void;
    fn GetProcessTimes(
        handle: *mut c_void,
        created: *mut u64,
        exit: *mut u64,
        kernel: *mut u64,
        user: *mut u64,
    ) -> i32;
}

fn register(root: &Path, pid: u32, handle: *mut c_void) {
    let (mut created, mut exit, mut kernel, mut user) = (0, 0, 0, 0);
    assert_ne!(
        unsafe { GetProcessTimes(handle, &mut created, &mut exit, &mut kernel, &mut user) },
        0,
        "读取进程创建时间失败"
    );
    let pending = root.join(format!("{pid}.pending"));
    std::fs::write(&pending, format!("{pid} {created}")).expect("写入进程登记");
    std::fs::rename(pending, root.join(format!("{pid}.identity"))).expect("发布进程登记");
}

fn wait_file(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !path.exists() {
        assert!(Instant::now() < deadline, "等待测试控制点超时");
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn json_string(value: &str) -> String {
    let mut result = String::from("\"");
    for ch in value.chars() {
        match ch {
            '"' => result.push_str("\\\""),
            '\\' => result.push_str("\\\\"),
            ch if ch < ' ' => result.push_str(&format!("\\u{:04x}", ch as u32)),
            ch => result.push(ch),
        }
    }
    result.push('"');
    result
}

fn main() {
    let root =
        std::path::PathBuf::from(std::env::var_os("CXB_DIAG_FIXTURE_ROOT").expect("夹具目录"));
    let mode = std::env::var("CXB_DIAG_FIXTURE_MODE").expect("夹具模式");
    if mode != "leaf" {
        register(&root, std::process::id(), unsafe { GetCurrentProcess() });
    }
    if mode == "leaf" {
        std::fs::write(root.join("leaf.ready"), "已持有输出句柄").unwrap();
        wait_file(&root.join("leaf.release"));
        return;
    }
    if mode == "tree" {
        let child = Command::new(std::env::current_exe().unwrap())
            .env("CXB_DIAG_FIXTURE_MODE", "leaf")
            .stdin(Stdio::null())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("创建后代");
        // 使用创建时取得的句柄登记，不能仅保存可能复用的 PID。
        register(&root, child.id(), child.as_raw_handle());
        wait_file(&root.join("leaf.ready"));
        std::fs::write(root.join("tree.ready"), "父子均已登记").unwrap();
        wait_file(&root.join("parent.release"));
        return;
    }
    let args: Vec<_> = std::env::args().skip(1).map(|v| json_string(&v)).collect();
    std::fs::write(root.join("argv.json"), format!("[{}]", args.join(","))).unwrap();
    let mut stdin = Vec::new();
    std::io::stdin()
        .read_to_end(&mut stdin)
        .expect("读取空输入");
    assert!(stdin.is_empty(), "诊断标准输入必须为空");
    std::io::stdout().write_all(b"stdout\0\xff\r\n").unwrap();
    std::io::stdout().flush().unwrap();
    std::io::stderr().write_all(b"stderr\0\xfe\r\n").unwrap();
    std::io::stderr().flush().unwrap();
    let code: u32 = std::env::var("CXB_DIAG_FIXTURE_CODE")
        .unwrap_or_else(|_| "0".into())
        .parse()
        .unwrap();
    std::process::exit(code as i32);
}
