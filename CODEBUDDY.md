# CODEBUDDY.md

This file provides guidance to CodeBuddy Code when working with code in this repository.

## 仓库概览

三套共存实现，核心目标是「按网速自动启用/禁用 NetLimiter 限速规则」：

- `cs/` — 官方 C# 示例（.NET Framework 4.6.2，VS 2017/2019，NuGet 包 `NetLimiter`），互相独立的控制台小样例，仅作 API 用法参考
- `py/` — Python 版自动限速控制器（多进程 supervisor + 3 个子模块），**当前主力实现**
- `ps/` — PowerShell 版（功能对等设计，但已落后于 py 版，见下文「py/ps 漂移」）

运行前提：Windows + 已安装 NetLimiter 4.1.1+（`nlsvc` 运行中）；API DLL 路径固定 `C:\Program Files\Locktime Software\NetLimiter\NetLimiter.dll`（pythonnet `clr.AddReference` / PowerShell `Add-Type -Path` / C# NuGet）；修改规则需管理员权限。

## 常用命令

### Python 控制器

```bash
pip install -r py/requirements.txt      # pythonnet / paramiko / cryptography / filelock / psutil
python py/main.py                       # 需管理员；拉起 speed_sampler + router_speed_sampler + rule_checker
python py/main.py --no-router           # 只跑本机链路，不启动 router_speed_sampler
python py/main.py --router              # 强制开启路由器链路（覆盖环境变量 NL_ROUTER_RULE_ENABLED）
```

单模块调试（各自独立可跑，不必经 supervisor）：

```bash
python py/speed_sampler.py
python py/rule_checker.py --no-router
python py/router_sampler.py
python py/demo_method1.py               # 观察「网卡上行 − LAN 应用层」修正后的本机上行
python py/demo_any_method.py            # 方案 B 演示（Any 过滤器）
```

本仓库**没有 pytest/unittest 测试套件**，事实上的校验手段：

```bash
python -m py_compile py/main.py py/rule_checker.py py/speed_sampler.py py/router_sampler.py py/common/*.py   # 提交前必做
```

- 改动 `rule_checker` 前先确认 NetLimiter 中目标规则 GUID 存在
- `ps/test_*.ps1` 是临时的 API 连接行为探针（多进程并发连接 / 隔离连接），非回归测试
- 需要实跑验证时：把 `os.environ["TEMP"]` 指向临时目录再导入模块，可在不干扰线上进程的前提下跑真实代码路径（心跳/数据文件闸门都挂在 `%TEMP%` 下）
- pythonnet 需系统级 Python（装有 `clr`）；managed 解释器若无 `clr`，跑探测脚本会 `ModuleNotFoundError`

### PowerShell 控制器

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
cd ps
.\supervisor.ps1                        # 前台运行，Ctrl+C 退出
.\install_service.ps1 -Install -ServiceAccount "USER" -ServicePassword "PWD"   # NSSM 安装为服务（凭据为必填）
.\install_service.ps1 -Uninstall
.\get_filter_id.ps1 / .\find_rule.ps1 / .\explore_api.ps1   # 只读探查工具
```

`install_service.ps1` 依赖 `ps/nssm.exe`，缺失时会尝试从 nssm.cc 下载；服务停止走 `AppStopMethodConsole`（Ctrl+C）→ supervisor 退出码 0 → `AppExit 0 Exit` 不重启。

### C# 示例

用 Visual Studio 打开 `cs/NLApiSamples.sln`，NuGet 还原 `NetLimiter` 包，以管理员身份运行目标项目。示例刻意省略 try/catch，保持最小可读。

## 架构

### 进程与数据流

```
supervisor (py/main.py 或 ps/supervisor.ps1)
  ├─ speed_sampler         → %TEMP%/qb_speed_data.json      （每 5s 采样，4 样本均值）
  ├─ router_speed_sampler  → %TEMP%/router_speed_data.json  （SSH 读路由器 + 本机对比）
  └─ rule_checker          ← 读上述两个 JSON，按阈值启用/禁用 NetLimiter 规则
```

**supervisor 的模块 key 必须与模块自己的 `HeartbeatManager(<name>)` 一致**，否则 fail-closed 心跳闸会读不到文件、把正常模块轮番杀掉（10 次后不再拉起）。历史上 `main.py` 用 `router_sampler` 而模块写的是 `router_speed_sampler.heartbeat.json`，就是这么翻车的。

- 跨进程通过 `%TEMP%` 下的 JSON + `filelock`（写侧「临时文件 + `os.replace`」原子替换）
- 输出用 `LastUpdate` 时间戳表达新鲜度；`rule_checker` 只接受新鲜数据（qb 5s / router 15s），陈旧一律跳过判定并告警，不用旧快照
- supervisor 通过 `SUPERVISOR_PID` 环境变量下发给子进程；子进程各自有 watchdog 线程，父进程消失即 `os._exit(2)`

### 双线程 + 三道存活闸（不要退化）

NetLimiter/SSH 调用会长时间阻塞，因此每个模块都是「主线程跑业务 + 独立 daemon 心跳线程（2s 写 `%TEMP%/nl_watchdog/<module>.heartbeat.json`）」。supervisor 12s 一轮，三道闸**全部 fail-closed**：

1. 进程存活（`Process.is_alive()`）
2. 心跳文件 `last_ok` 超过 10s 未更新；**文件缺失/损坏/解析失败也判异常**（`status=="STOPPED"` 例外放行）
3. 数据文件 mtime（qb / router 数据 60s 未更新视为主循环假死——抓「心跳线程活、主循环卡住」）

另有启动宽限 30s、连续 2 次异常才重启、单模块最多 10 次；心跳写入失败要计数并写入 `write_errors` 字段暴露给 supervisor，不能静默吞掉。历史上这三条都曾 fail-open 导致 22 小时无感（`plans/`、`MEMORY.md` 有事故记录）。

### 规则与阈值

阈值、Rule ID、采样间隔全部是 `py/rule_checker.py` 与 `py/router_sampler.py` 顶部的类常量（`LIMIT_RULE_ID` / `ROUTER_RULE_ID` / `THRESHOLD_KB` / `ROUTER_THRESHOLD_KB` / `*_INTERVAL*` / `COOLDOWN_*`）。冷却机制：任一规则变更后 60s 内检查间隔 ×3。

`router_sampler` 的比对值经过两层对齐，改动需谨慎：路由器侧取 `ppp0` 减去 BT 隧道封装开销（QUIC 流 − wg0 内层）；本机侧取「物理网卡上行（WMI 筛 `PCI\` 物理网卡 + psutil 计数）− LAN 应用层 × `LAN_PHY_COEFF`」再乘 `NIC_LINK_COEFF`；SSH 断开按指数退避重连。

### 日志

`py/common/logger.py` 输出 JSON 结构化日志到 `py/logs/<module>_YYYYMMDD.log`，告警另落 `alerts_YYYYMMDD.log`（`logger.alert()`）；5MB 轮转、保留 3 天、旧日志 gzip。PowerShell 版格式与之兼容，字段含 `ts/module/pid/event/reason`。

## 关键约束与坑

### 过滤器绝不能硬编码 InternalId

`InternalId` 会随过滤器增删被复用（实测 44 已从 priv 变成 IDM，导致采样恒 0、阈值永不触发且无日志）。稳定标识是 GUID 与名称：

- 发现顺序：GUID → 名称 → 失败即退出交 supervisor 重启（不静默采样）
- `Rule.FilterId` 是**过滤器 GUID（稳定）**；`FilterNode.FilterId` 是 **InternalId（会变）**，切勿混用
- 排查「阈值已达但规则未应用」时，第一步永远是确认采样源指向哪个过滤器，别相信代码里的常量：`%TEMP%/*.json` 的 mtime 判断采样进程是否假死；静态配置可解析 `C:\ProgramData\Locktime\NetLimiter\5\nl_settings.xml`；用只读 pythonnet 探针打印 `f.InternalId / f.Id / f.Name` 与两次 `nodeLoader.Load()` 的字节差

### py / ps 已经漂移，改一套必须同步另一套

`ps/` 长期未跟进 py 的修复，目前仍存在：`speed_sampler.ps1` 硬编码 `$privInternetInternalId = 44`（正是 py 已修复的失效模式）、`rule_checker.ps1` 用旧 Rule ID（`f4c3e3ac…` / `5b34aebb…`）与 400KB/s 阈值、无 `--no-router` / `NL_ROUTER_RULE_ENABLED` 开关。修改限速逻辑、阈值或规则行为时两边都要动，并在提交信息里说明对齐情况。

### 本地文件不入库

`.gitignore` 的 "My ignores" 段排除了 `ps/*.ps1`、`ps/*.lnk`、`ps/*.bat`、`py/logs/`、`plans/`、`MEMORY.md`、`.qwen/`、`.workbuddy/` —— 也就是说 **PowerShell 脚本与设计文档不会出现在 `git status`**，提交前留意别把只改 ps 的改动当成「无改动」。提交信息遵循 Conventional Commits，一个 PR 只做一件事。

### 其他

- 多进程启动必须走模块级函数（`_run_speed_sampler` 等），不能传实例方法，否则 Windows 下序列化失败
- `%TEMP%/nl_watchdog` 目录可能被系统清理，心跳写入前必须重建目录
- 关闭路由器规则链是**非侵入**设计：只跳过 router_speed_sampler 与判定，不强改现有规则状态（关闭时若规则已启用会持续限速），启动日志 `ROUTER_RULE_DISABLED_BY_CONFIG` 会带上当前规则状态
- 休眠唤醒后所有心跳与数据文件都会显得陈旧，Supervisor 会按预期重启三个模块
- 不提交个人规则 GUID、SSH 凭据与日志
