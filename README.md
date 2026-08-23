# NetLimiterAPISamples

NetLimiter API 的官方示例集合，并由社区扩展出 **Python / PowerShell 版「按网速自动限速」监控系统**。本仓库包含三套实现：

- **`cs/`** — 官方 C# 控制台示例（NetLimiter 4.1.1+ API 的基础用法）
- **`py/`** — Python 版本自动限速控制器（多进程 + 守护进程架构）
- **`ps/`** — PowerShell 版本自动限速控制器（与 Python 版逻辑对等的另一套实现）

---

## 项目简介

核心目标：当本机 qBittorrent 上传速度过高，或路由器侧上行速率明显高于本机时，自动启用 NetLimiter 限速规则；条件消失后自动禁用。

- `speed_sampler`：采样本机 qBittorrent（Private Internet 过滤器）上行速度，写入 `%TEMP%/qb_speed_data.json`
- `router_sampler`：通过 SSH 读取路由器实时上行速率，与本机速度对比，写入 `%TEMP%/router_speed_data.json`
- `rule_checker`：读取上面的共享文件，按阈值自动启用/禁用两条限速规则
- `supervisor`：以独立进程方式拉起上述三个模块，崩溃自动重启，并做心跳/孤儿进程看护

Python 与 PowerShell 两套实现功能对等，可任选其一运行。

---

## 仓库结构

```
NetLimiterApiSamples/
├── cs/                       # 官方 C# 示例（.NET Framework 4.6.2，Visual Studio 2017/2019）
│   ├── NLApiSamples.sln
│   ├── AllExceptFilter/      # 排除型过滤器
│   ├── BlockerRequests/      # 拦截请求
│   ├── EnterRegistrationData/# 写入注册信息
│   ├── FilterAndRuleUpdate/  # 过滤器与规则增改
│   ├── FilterNodeLoader/     # 加载过滤器节点
│   ├── LimitForApplication/  # 为应用限速
│   ├── LimitForComputer/     # 为整机限速
│   ├── LimitForInternetZone/# 为 Internet 区域限速
│   ├── Permissions/          # 权限设置
│   ├── QuotaWithRule/        # 配额 + 规则
│   └── ScheduledRule/        # 计划规则
├── py/                       # Python 自动限速控制器
│   ├── main.py               # 多进程 Supervisor 入口
│   ├── speed_sampler.py      # 本机速度采样器
│   ├── router_sampler.py     # 路由器速度采样器（SSH + NetLimiter 区域对比）
│   ├── rule_checker.py       # 规则检查器（自动启用/禁用限速规则）
│   ├── demo_method1.py       # 本机互联网上行修正方案 A 演示
│   ├── demo_any_method.py    # 本机互联网上行修正方案 B（Any 过滤器）演示
│   ├── common/               # 公共模块：logger.py / heartbeat.py
│   ├── requirements.txt      # Python 依赖
│   └── *.lnk                 # 快捷方式
├── ps/                       # PowerShell 自动限速控制器（与 py/ 对等）
│   ├── supervisor.ps1        # Job Object 守护进程，拉起并看护三个模块
│   ├── speed_sampler.ps1     # 本机速度采样器
│   ├── router_speed_sampler.ps1 # 路由器速度采样器
│   ├── rule_checker.ps1      # 规则检查器
│   ├── install_service.ps1   # 用 NSSM 安装/卸载为 Windows 服务
│   ├── get_filter_id.ps1     # 工具：查询过滤器 ID
│   ├── find_rule.ps1         # 工具：按 LimitSize 查找规则
│   ├── debug_filter.ps1      # 工具：过滤器调试
│   ├── explore_api.ps1       # 工具：API 探查
│   ├── permissions.ps1        # 工具：权限相关
│   ├── speed_monitor.ps1     # 工具：速度监控
│   ├── create_shortcut.ps1   # 工具：创建快捷方式
│   └── test_*.ps1            # 连接/并发测试脚本
├── plans/                    # 设计文档（架构/路由器校验/Supervisor-NSSM）
│   ├── python_version_architecture.md
│   ├── router_speed_check_plan.md
│   └── supervisor_nssm_plan.md
└── README.md
```

---

## 环境要求

| 项目 | 要求 |
|------|------|
| 操作系统 | Windows（NetLimiter 仅支持 Windows） |
| NetLimiter | 已安装 **NetLimiter 4.1.1 或更高版本**，且 `nlsvc` 服务正在运行 |
| .NET | C# 示例需 .NET Framework 4.6.2；Python 版通过 pythonnet 调用 .NET Framework |
| 权限 | 修改 NetLimiter 服务设置需 **管理员权限**（或关闭本机提权要求，见下文） |
| 路由器（仅 router_sampler） | 支持 SSH 登录，可读取实时上行速率 |

NetLimiter API 的 DLL 路径固定为：
```
C:\Program Files\Locktime Software\NetLimiter\NetLimiter.dll
```
C# 通过 NuGet 包 `NetLimiter` 引用；Python 通过 `pythonnet` 的 `clr.AddReference` 加载该 DLL；PowerShell 通过 `Add-Type -Path` 加载。

---

## 安装

### 1. C# 示例

1. 安装 **NetLimiter 4.1.1+**
2. 用 Visual Studio 2017/2019 打开 `cs/NLApiSamples.sln`
3. 通过 NuGet 恢复 `NetLimiter` 包（各项目已引用）
4. 以管理员身份构建并运行目标项目

> 样例均为 .NET Framework 控制台程序，刻意省略 try/catch 等结构以保持简洁。

### 2. Python 控制器

需要 Python 3.8+（推荐 3.10+）与 pip。

```bash
# 进入 Python 目录
cd py

# 创建虚拟环境（可选但推荐）
python -m venv .venv
.venv\Scripts\activate

# 安装依赖
pip install -r requirements.txt
```

依赖清单（`py/requirements.txt`）：

| 包 | 用途 |
|----|------|
| `pythonnet>=3.0.0` | 通过 CLR 调用 NetLimiter .NET API |
| `paramiko>=3.0.0` | SSH 连接路由器读取速率 |
| `cryptography>=41.0.0` | paramiko 的加密依赖 |
| `filelock>=3.0.0` | 进程间共享文件的互斥锁 |
| `psutil>=5.9.0` | 采集物理网卡上行速率 |

> 注意：`pythonnet` 在 Windows 上依赖 .NET Framework，且需能加载上述 NetLimiter DLL。若运行报 `无法加载 NetLimiter DLL`，请确认 NetLimiter 已安装且路径相符。

### 3. PowerShell 控制器

1. 系统自带 **PowerShell 5.1+**（脚本头部 `#Requires -Version 5.1`）
2. 确认 NetLimiter 已安装（脚本通过 `Add-Type -Path` 加载 DLL）
3. `install_service.ps1` 需要 **NSSM**（Non-Sucking Service Manager）放在 `ps/nssm.exe`

```powershell
# 以管理员身份运行 PowerShell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
cd ps
```

---

## 使用示例

### C# — 为某个应用限速

```csharp
using (NLClient client = new NLClient())
{
    client.Connect();

    // 创建匹配 msedge.exe 的过滤器
    Filter filter = new Filter("MyMSEdgeFilter");
    filter.Functions.Add(new FFAppIdEqual(
        new AppId(@"c:\program files (x86)\microsoft\edge dev\application\msedge.exe")));
    client.AddFilter(filter);

    // 入站限速 512 KB/s（512*1024 字节）
    client.AddRule(filter.Id, new LimitRule(RuleDir.In, 512 * 1024));

    Console.ReadKey();
    client.RemoveFilter(filter);   // 删除过滤器会同时移除其规则
}
```
（完整示例见 `cs/LimitForApplication/Program.cs`）

### Python — 启动自动限速监控

```bash
# 需管理员权限运行（脚本会检测 IsUserAnAdmin）
cd py
python main.py
```

启动后：
- 三个子进程（speed_sampler / router_sampler / rule_checker）独立运行
- Supervisor 每 12 秒巡检一次，崩溃自动重启（单模块最多 10 次）
- 日志与心跳文件位于 `%TEMP%/nl_watchdog/`
- 按 `Ctrl+C` 优雅关闭所有模块

单独调试某个模块：

```bash
python speed_sampler.py     # 仅采样本机速度
python rule_checker.py      # 仅运行规则检查
python demo_method1.py      # 观察本机互联网上行修正效果
```

### PowerShell — 启动 / 安装为服务

```powershell
# 直接运行（前台，Ctrl+C 退出）
cd ps
.\supervisor.ps1

# 安装为 Windows 服务（依赖 NSSM），开机自启、崩溃自愈
.\install_service.ps1 -Install

# 卸载服务
.\install_service.ps1 -Uninstall
```

### 工具脚本（PowerShell）

```powershell
.\get_filter_id.ps1   # 列出所有过滤器及其 ID
.\find_rule.ps1       # 按 LimitSize 查找规则（示例：≈1280KB）
.\explore_api.ps1     # 交互式探查 NetLimiter API 对象
```

---

## 架构说明

```
                        ┌─────────────────────────────┐
                        │        Supervisor           │
                        │  (main.py / supervisor.ps1)  │
                        │  - 拉起 3 个独立进程          │
                        │  - 崩溃自动重启 (≤10 次)      │
                        │  - 心跳 + 孤儿进程看护         │
                        └───────┬───────────┬──────────┘
            ┌───────────────────┘           └───────────────────┐
            ▼                                                   ▼
   ┌──────────────────┐                              ┌──────────────────┐
   │  speed_sampler   │  写 %TEMP%/qb_speed_data.json │  router_sampler  │
   │  (本机 qBittorrent│ ───────────────────────────► │  (SSH 读路由器 +  │
   │   PrivateInternet │                              │   NetLimiter 区域)│
   │   FilterId=44)   │                              │  写 router_*.json │
   └──────────────────┘                              └──────────────────┘
            │                                                   │
            └──────────────►  %TEMP%/*.json  ◄──────────────────┘
                                    │
                                    ▼
                          ┌──────────────────┐
                          │   rule_checker   │
                          │ 读共享 JSON，按阈值│
                          │ 启用/禁用限速规则  │
                          └──────────────────┘
```

进程间通过 `%TEMP%` 下的 JSON 文件 + `filelock` 共享速度数据；各模块用独立心跳线程写入 `%TEMP%/nl_watchdog/*.heartbeat.json`，Supervisor 据此判断模块是否"假死"。

---

## 配置说明

### 限速规则与阈值

| 规则 | Rule ID（示例） | 启用条件 | 禁用条件 | 检查间隔 |
|------|----------------|----------|----------|----------|
| qBittorrent 上传 | `f4c3e3ac-91d1-435b-af27-f9020b4eab4e` | 4 样本均值 ≥ **600 KB/s** | 连续低于阈值 **90 秒** | 20 秒 |
| 路由器上行 | `d36d9bf8-02f1-41d1-9d89-be65b2d4360a` | 路由器速度 − 本机均值 > **800 KB/s** 持续 **3 秒** | 连续低于阈值 **10 秒** | 2 秒 |

> 阈值、Rule ID、采样间隔均定义在 `py/rule_checker.py` 与 `ps/rule_checker.ps1` 顶部的常量中，可按需修改。请先在你的 NetLimiter 中创建对应规则并填入其真实 GUID。

**冷却机制**：任意规则启用/禁用后 60 秒内，检查间隔放大为 3 倍，避免抖动频繁切换。

### 过滤器 ID

| 名称 | InternalId / FilterId |
|------|------------------------|
| Private Internet（qBittorrent） | `44` |
| Internet 区域 | `2` |
| LocalNetwork（LAN） | `1` |
| Any | `3` |

### 关闭本机提权要求

默认情况下，修改 NetLimiter 服务设置需以管理员身份运行。若希望非提权客户端也能修改，在 NetLimiter 配置文件中将 `RequireElevationLocal` 设为 `false`：

```xml
<RequireElevationLocal>false</RequireElevationLocal>
```

详见 NetLimiter [XML 配置文件文档](https://netlimiter.com/docs/internals/xml-configuration-file)。

---

## 贡献指南

欢迎提交 Issue 与 PR。约定如下：

### 分支与提交

- 主分支为 `main`（或 `master`），新功能从 `main` 切出 `feature/xxx` 分支
- 提交信息建议遵循 Conventional Commits：`feat:` / `fix:` / `docs:` / `refactor:` / `chore:`
- 一个 PR 只做一件事，描述清楚「为什么改」

### 双语言一致性

`py/` 与 `ps/` 是**功能对等**的两套实现。修改其中一套的限速逻辑、阈值或规则行为时，**必须同步另一套**，并在 PR 中说明两边已对齐。设计讨论记录在 `plans/`。

### 代码风格

- **C#**：保持 Visual Studio 默认格式；示例保持简洁，可省略非必要的异常处理
- **Python**：PEP 8；模块级函数用于 `multiprocessing` 启动以避免序列化问题
- **PowerShell**：UTF-8 编码；脚本头部保留 `#Requires -Version 5.1`；关键脚本支持 `-ServiceMode` / `-Install` 等参数化开关

### 测试

- 改动 `rule_checker` 前，先确认 NetLimiter 中对应规则 GUID 存在
- 可用 `ps/test_*.ps1`（多进程并发连接、隔离连接）验证 API 连接行为
- 提交前请本地实跑一轮 `main.py` / `supervisor.ps1`，确认能正常采样、触发、恢复

### 安全与健壮性

- 涉及 API 阻塞的代码须有独立心跳/看门狗，避免成为孤儿进程
- 文件读写使用 `filelock` + 原子 rename，避免多进程写坏共享 JSON
- 不要在仓库中提交个人规则 GUID、SSH 凭据或日志

---

## About the NetLimiter API

The NetLimiter API is used to control machine which is running **NetLimiter 4.1.1** or later. All functionality available in NetLimiter is accessible via the API. It's possible to create filters, rules, monitor network activity etc. Our official **NetLimiter Client** (GUI) is whole built above the API too.
Currently, there is no documentation except this Readme file and the API samples. We will add more information on an ongoing basis.

## C# samples

- The samples were created using **Visual Studio 2019**. They work with earlier 2017 version too.
- Most of the samples are .NET framework console application.
- Samples are kept as simple as possible so the constructs like try/catch etc. are usually omitted.
- NetLimiter nuget package (the API) is added to each project.
- For security reasons, application must run elevated in order to modify NetLimiter service settings. It's possible to disable this requirement (see below).

## How to create project in Visual Studio

1. Create .NET framework project
2. Add ***NetLimiter*** nuget package
3. Use the API (check our samples)

## About the NetLimiter nuget package

https://www.nuget.org/packages/NetLimiter

- NetLimiter 4.1.2 API nuget package is no more dependent on NLog logging library.
- **NetLimiter 4.1.1** or later must be installed on the machine you are connecting to (usually local machine).

## How to allow non-elevated client to modify NetLimiter settings

In NetLimiter [configuration file](https://netlimiter.com/docs/internals/xml-configuration-file) set ***RequireElevationLocal*** to false:

```xml
<RequireElevationLocal>false</RequireElevationLocal>
```

For questions and comments: [support@netlimiter.com](mailto://support@netlimier.com)
