// 诊断程序替身：向标准输出写第一个参数、标准错误写第二个参数，再以第三个参数为退出码退出。
const [stdout = "", stderr = "", code = "0"] = process.argv.slice(2);
process.stdout.write(stdout);
process.stderr.write(stderr);
process.exitCode = Number(code);
