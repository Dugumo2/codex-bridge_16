#Requires -Version 7.0
<#
.SYNOPSIS
    在 Windows 上安装 cxb。
.DESCRIPTION
    构建 cxb-broker 正式版与打包的 CLI，安装到 -Prefix 指定目录：
      <Prefix>\scripts\cxb.js          打包后的 CLI（用 bun 运行）
      <Prefix>\scripts\cxb-broker.exe  broker（CLI 在同目录查找）
      <Prefix>\prompts\*.md            内置模板（CLI 按 scripts 的上一级查找）
      <Prefix>\skill\cxb\SKILL.md      生成的技能文件（供审阅；给出 -SkillDir 才注册）
      <Prefix>\bin\cxb.cmd、cxb        命令包装
    不修改用户 PATH，结束时打印加入 PATH 的命令。
.PARAMETER Prefix
    安装目录，默认 %USERPROFILE%\.cxb。
.PARAMETER SkillDir
    技能根目录（例如 %USERPROFILE%\.claude\skills）。给出时把 SKILL.md 装到 <SkillDir>\cxb\SKILL.md；默认不注册。
.PARAMETER CargoTargetDir
    cargo 编译目录，默认用环境变量 CARGO_TARGET_DIR，没设时用仓库里的 broker-rs\target。
#>
param(
    [string]$Prefix = (Join-Path $env:USERPROFILE ".cxb"),
    [string]$SkillDir = "",
    [string]$CargoTargetDir = "",
    [switch]$Help
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if ($Help) {
    Get-Help $MyInvocation.MyCommand.Path -Detailed
    exit 0
}

$RepoDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$Prefix = [System.IO.Path]::GetFullPath($Prefix)
$Utf8 = [System.Text.UTF8Encoding]::new($false)

function Fail([string]$Message) {
    Write-Host "安装失败：$Message" -ForegroundColor Red
    exit 1
}

function Invoke-Step([string]$Name, [scriptblock]$Block) {
    Write-Host "== $Name"
    & $Block
    if ($LASTEXITCODE -ne 0) { Fail "$Name（退出码 $LASTEXITCODE）" }
}

# 只取 frontmatter 的 name、description、sandbox（与 CLI 的模板解析口径一致：首行 ---，到下一个 --- 为止）。
function Read-TemplateMeta([string]$Path) {
    $lines = [System.IO.File]::ReadAllLines($Path, $Utf8)
    $meta = @{ name = [System.IO.Path]::GetFileNameWithoutExtension($Path); description = ""; sandbox = "" }
    if ($lines.Count -eq 0 -or $lines[0].Trim() -ne "---") { return $meta }
    for ($i = 1; $i -lt $lines.Count; $i++) {
        if ($lines[$i].Trim() -eq "---") { break }
        if ($lines[$i] -match '^\s*(name|description|sandbox)\s*:\s*(.*)$') { $meta[$Matches[1]] = $Matches[2].Trim() }
    }
    return $meta
}

function Write-SkillMd([string]$PromptsDir, [string]$OutPath) {
    $rows = @("| 模板 | 用途 | 建议沙箱 |", "|---|---|---|")
    foreach ($file in Get-ChildItem -LiteralPath $PromptsDir -Filter *.md | Sort-Object Name) {
        $meta = Read-TemplateMeta $file.FullName
        $sandbox = if ($meta.sandbox) { $meta.sandbox } else { "（按参数）" }
        $rows += "| ``$($meta.name)`` | $($meta.description -replace '\|', '\|') | $sandbox |"
    }
    $source = [System.IO.File]::ReadAllText((Join-Path $RepoDir "SKILL.md"), $Utf8)
    if (-not $source.Contains("<!-- TEMPLATES -->")) { Fail "SKILL.md 缺少模板表占位 <!-- TEMPLATES -->" }
    $text = $source.Replace("<!-- TEMPLATES -->", ($rows -join "`n"))
    New-Item -ItemType Directory -Path (Split-Path $OutPath) -Force | Out-Null
    [System.IO.File]::WriteAllText($OutPath, $text, $Utf8)
}

# 前置检查
$missing = @("bun", "codex", "cargo") | Where-Object { -not (Get-Command $_ -ErrorAction SilentlyContinue) }
if ($missing) { Fail "缺少 $($missing -join '、')（bun：https://bun.sh/；codex：npm install -g @openai/codex；cargo：https://rustup.rs/）" }

$scripts = Join-Path $Prefix "scripts"
$broker = Join-Path $scripts "cxb-broker.exe"
if (Test-Path -LiteralPath $broker) {
    try { [System.IO.File]::Open($broker, "Open", "ReadWrite", "None").Dispose() }
    catch { Fail "已安装的 cxb-broker 正在运行，无法覆盖：等它空闲退出（默认 5 分钟）或停止正在进行的任务后重试" }
}

Push-Location $RepoDir
try {
    Invoke-Step "安装依赖" { bun install --frozen-lockfile }
    if (-not $CargoTargetDir) {
        $CargoTargetDir = if ($env:CARGO_TARGET_DIR) { $env:CARGO_TARGET_DIR } else { Join-Path $RepoDir "broker-rs\target" }
    }
    $env:CARGO_TARGET_DIR = $CargoTargetDir
    Invoke-Step "构建 cxb-broker 正式版" { cargo build --release --manifest-path (Join-Path $RepoDir "broker-rs\Cargo.toml") }

    # 只清理本安装自己的子目录，Prefix 里其他内容不动。
    foreach ($sub in @("scripts", "prompts", "skill", "bin")) {
        $target = Join-Path $Prefix $sub
        if (Test-Path -LiteralPath $target) { Remove-Item -LiteralPath $target -Recurse -Force }
    }
    New-Item -ItemType Directory -Path $scripts -Force | Out-Null

    Invoke-Step "打包 CLI" { bun build (Join-Path $RepoDir "src\cxb\main.ts") --target bun --outfile (Join-Path $scripts "cxb.js") }
    Copy-Item -LiteralPath (Join-Path $CargoTargetDir "release\cxb-broker.exe") -Destination $broker
    Copy-Item -LiteralPath (Join-Path $RepoDir "src\prompts") -Destination (Join-Path $Prefix "prompts") -Recurse

    $skillOut = Join-Path $Prefix "skill\cxb\SKILL.md"
    Write-SkillMd (Join-Path $RepoDir "src\prompts") $skillOut
    Copy-Item -LiteralPath (Join-Path $RepoDir "LICENSE") -Destination (Join-Path (Split-Path $skillOut) "LICENSE.txt")
} finally {
    Pop-Location
}

$bin = Join-Path $Prefix "bin"
New-Item -ItemType Directory -Path $bin -Force | Out-Null
$entry = Join-Path $scripts "cxb.js"
[System.IO.File]::WriteAllText((Join-Path $bin "cxb.cmd"), "@bun `"$entry`" %*`r`n", $Utf8)
[System.IO.File]::WriteAllText((Join-Path $bin "cxb"), "#!/usr/bin/env bash`nexec bun `"$($entry -replace '\\', '/')`" `"`$@`"`n", $Utf8)

if ($SkillDir) {
    $skillTarget = Join-Path ([System.IO.Path]::GetFullPath($SkillDir)) "cxb"
    New-Item -ItemType Directory -Path $skillTarget -Force | Out-Null
    # -LiteralPath 不展开通配符，按文件逐个复制。
    foreach ($file in Get-ChildItem -LiteralPath (Join-Path $Prefix "skill\cxb") -File) {
        Copy-Item -LiteralPath $file.FullName -Destination (Join-Path $skillTarget $file.Name) -Force
    }
    Write-Host "已注册技能：$skillTarget\SKILL.md"
}

# 用安装包内的绝对路径验证，不依赖 PATH，也不受开发用的 broker 覆盖变量影响。
$cxb = Join-Path $bin "cxb.cmd"
$savedBroker = $env:CXB_BROKER_BIN
Remove-Item Env:CXB_BROKER_BIN -ErrorAction SilentlyContinue
try {
    Invoke-Step "检查版本" { & $cxb --version }
    Write-Host "== 环境诊断（只有 FAIL 视为安装失败）"
    & $cxb doctor
    if ($LASTEXITCODE -ne 0) { Fail "cxb doctor 报告 FAIL，见上方输出" }
} finally {
    if ($null -ne $savedBroker) { $env:CXB_BROKER_BIN = $savedBroker }
}

Write-Host ""
Write-Host "安装完成：$Prefix"
Write-Host "把 cxb 加入当前用户 PATH（需要时手动执行）："
Write-Host "  [Environment]::SetEnvironmentVariable('Path', '$bin;' + [Environment]::GetEnvironmentVariable('Path', 'User'), 'User')"
