#!/usr/bin/env bash
# 本项目只支持 Windows（S4 拍板 F）：Rust broker 依赖 Windows 命名管道与 Windows API，
# 在 macOS、Linux 上无法编译。请在 Windows 上用 install.ps1 安装。
echo "不支持：codex-bridge 目前只支持 Windows，请在 Windows 上运行 install.ps1。" >&2
echo "Unsupported: codex-bridge currently supports Windows only; run install.ps1 on Windows." >&2
exit 1
