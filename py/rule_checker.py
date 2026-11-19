#!/usr/bin/env python3
"""
规则检查器 - Python 版本

使用独立心跳线程解决 NetLimiter API 阻塞导致心跳无法更新的问题。
通过 pythonnet 调用 NetLimiter API。
"""

import os
import sys
import json
import time
import signal
import argparse
import threading
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any

try:
    from filelock import FileLock, Timeout
except ImportError:
    print("需要安装 filelock: pip install filelock")
    sys.exit(1)

NETLIMITER_DLL_PATH = r"C:\Program Files\Locktime Software\NetLimiter\NetLimiter.dll"
try:
    import clr
    # 使用显式 DLL 路径加载（与 PowerShell 版本一致）
    clr.AddReference(NETLIMITER_DLL_PATH)  # type: ignore
    from NetLimiter.Service import NLClient  # type: ignore
except ImportError:
    print("需要安装 pythonnet: pip install pythonnet")
    sys.exit(1)
except Exception as e:
    print(f"无法加载 NetLimiter DLL: {e}")
    print(f"DLL 路径: {NETLIMITER_DLL_PATH}")
    print("请确保 NetLimiter 已安装在 C:\\Program Files\\Locktime Software\\NetLimiter\\")
    sys.exit(1)

# 添加当前目录到路径，以便导入 common 模块
sys.path.insert(0, str(Path(__file__).parent))

from common import ENV_ROUTER_RULE_ENABLED, env_flag
from common.heartbeat import HeartbeatManager
from common.logger import Logger

# 上传限速看门狗总开关（默认启用）
ENV_UPLOAD_GUARD_ENABLED = "NL_UPLOAD_GUARD_ENABLED"


class RuleChecker:
    """规则检查器"""
    
    # qBittorrent 限速规则配置
    # 规则归属过滤器: zzz_qbit_pub_internet (InternalId=69)
    # 2026-09-12 从组合过滤 2ad0b10c-379a-4a0e-ad5b-2dd863d8b312(InternalId=40) 搬运而来
    LIMIT_RULE_ID = "7c9220f2-74b4-4e22-aa1f-119271543f2a"
    THRESHOLD_KB = 450
    CHECK_INTERVAL_SECONDS = 20
    FIRST_CHECK_DELAY_SECONDS = 5
    RETRY_INTERVAL_SECONDS = 10
    MAX_RETRY_COUNT = 5
    
    # 路由器速度检查配置
    ROUTER_RULE_ID = "d36d9bf8-02f1-41d1-9d89-be65b2d4360a"
    ROUTER_THRESHOLD_KB = 1000  # 路由器规则阈值（与 router_sampler 一致）
    ROUTER_CONSECUTIVE_SECONDS = 3
    ROUTER_CHECK_INTERVAL_SECONDS = 2
    ROUTER_BELOW_THRESHOLD_SECONDS = 42  # 连续低于阈值 x 秒后恢复网速

    # 冷却机制：触发规则后间隔变为3倍，60秒后恢复
    COOLDOWN_MULTIPLIER = 3
    COOLDOWN_SECONDS = 60

    # 数据新鲜度上限
    QBIT_DATA_MAX_AGE_SECONDS = 5      # qb_speed_data.json（speed_sampler 每 5s 写一次）
    ROUTER_DATA_MAX_AGE_SECONDS = 15   # router_speed_data.json（router_sampler 主循环约 1s 写一次）

    # 采样源健康检查：qb 数据长期恒 0 时告警（不参与判定，仅暴露"采样读错过滤器"类静默失效）
    ZERO_ALERT_AFTER_SECONDS = 600    # 连续 10 分钟 AvgSpeedKB == 0 开始告警
    ZERO_ALERT_REPEAT_SECONDS = 3600  # 之后每 60 分钟重复一次

    # 路由器数据中断告警：数据陈旧即跳过判定（不再静默使用陈旧值），中断过久升级为 error
    ROUTER_STALE_ALERT_AFTER_SECONDS = 60     # 连续陈旧超过 60s 升级为 error 告警
    ROUTER_STALE_ALERT_REPEAT_SECONDS = 600   # 之后每 10 分钟重复一次
    STALE_LOG_FIRST_N = 3                     # 前 N 次陈旧只记 warn，避免刷屏

    # 连接错误自动恢复配置
    MAX_CONSECUTIVE_ERRORS = 10  # 连续错误最大次数，超过则退出

    # 数据文件路径
    DATA_FILE = Path(os.environ.get("TEMP", ".")) / "qb_speed_data.json"
    LOCK_FILE = Path(os.environ.get("TEMP", ".")) / "qb_speed_data.lock"
    ROUTER_DATA_FILE = Path(os.environ.get("TEMP", ".")) / "router_speed_data.json"
    ROUTER_LOCK_FILE = Path(os.environ.get("TEMP", ".")) / "router_speed_data.lock"

    # ------------------------------------------------------------------
    # 上传限速规则看门狗
    #
    # 目标：非本脚本控制的「Out 方向 + 限速 < 3MB/s」LimitRule。这类规则多由手工
    # 调试或第三方工具创建，一旦被打开就没人关，上传会被长期压在几百 KB/s。
    # 只对"连续启用超过 30 分钟"的规则动手，避免误伤临时手动限速。
    #
    # 注意：带 Action=Start 时间条件的规则整条跳过 —— 那是 NetLimiter 自身调度器
    # 在启用/禁用（如周末 01:00 自动提速），看门狗一禁，下次 Start 条件又开回来，
    # 会形成反复拉锯。
    # ------------------------------------------------------------------
    UPLOAD_GUARD_THRESHOLD_BYTES = 3 * 1024 * 1024   # 3 MiB/s（严格小于才命中）
    UPLOAD_GUARD_ENABLED_SECONDS = 1800              # 连续启用 30 分钟后自动禁用
    UPLOAD_GUARD_CHECK_INTERVAL_SECONDS = 60         # 看门狗轮询间隔
    UPLOAD_GUARD_STATE_FILE = Path(os.environ.get("TEMP", ".")) / "nl_upload_rule_guard.json"
    # 本脚本自己会动态开关的规则，看门狗必须回避（否则会跟 rule_checker 抢控制权）
    UPLOAD_GUARD_EXCLUDE_RULE_IDS = frozenset({
        LIMIT_RULE_ID.lower(),      # qBittorrent 限速规则
        ROUTER_RULE_ID.lower(),     # 路由器限速规则
    })
    
    def __init__(self, router_rule_enabled: Optional[bool] = None):
        self.logger = Logger("rule_checker")
        self.heartbeat = HeartbeatManager("rule_checker")

        # 路由器规则总开关：关闭后不读 router 采样数据、不做任何规则写入，
        # 规则状态保持现状（需要停用限速请手动在 NetLimiter 里关）
        self.router_rule_enabled = (
            env_flag(ENV_ROUTER_RULE_ENABLED, True) if router_rule_enabled is None else router_rule_enabled
        )

        # 确保数据文件目录存在
        self.DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
        
        # 状态变量
        self.loop_count = 0
        self.data_consecutive_expired = 0
        self.running = True

        # 冷却状态跟踪
        self._last_rule_change_time: Optional[float] = None
        self._last_router_rule_change_time: Optional[float] = None

        # 低于阈值连续时间跟踪（用于禁用判定）
        self._below_threshold_since: Optional[float] = None

        # 采样源零值看护状态
        self._zero_since: Optional[float] = None
        self._last_zero_alert: float = 0.0

        # 路由器数据陈旧看护状态
        self._router_stale_since: Optional[float] = None
        self._router_stale_count = 0
        self._last_router_stale_alert: float = 0.0

        # NetLimiter 客户端
        self.client: Optional[NLClient] = None
        self._data_file_lock = FileLock(str(self.LOCK_FILE), timeout=1)
        self._router_file_lock = FileLock(str(self.ROUTER_LOCK_FILE), timeout=1)

        # 规则缓存（避免每次线性搜索）
        self._rule_cache: Dict[str, Any] = {}

        # 错误计数器
        self._consecutive_errors = 0
        self._last_error_type = None

        # 上传限速看门狗状态
        self.upload_guard_enabled = env_flag(ENV_UPLOAD_GUARD_ENABLED, True)
        self._last_upload_guard_check: float = 0.0
        self._upload_guard_write_warned = False
        
        # 父进程检测
        self.supervisor_pid = os.environ.get("SUPERVISOR_PID")
        
        # 设置信号处理
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        # 启动 watchdog 线程（检测父进程退出，避免成为孤儿进程）
        self._start_watchdog()

        self.logger.info("初始化完成", event="INIT_OK")

    def _start_watchdog(self):
        """启动 watchdog 线程，检测父进程退出"""
        thread = threading.Thread(
            target=self._watchdog_loop,
            daemon=True,
            name="rule-checker-watchdog"
        )
        thread.start()

    def _watchdog_loop(self):
        """watchdog：独立线程检测父进程是否存活，避免主循环卡在 API 调用里时成为孤儿"""
        while self.running:
            time.sleep(5)
            if not self._check_parent_alive():
                self.logger.error(
                    "父进程(supervisor)已退出，watchdog 强制终止本进程",
                    event="WATCHDOG_PARENT_EXIT"
                )
                self.heartbeat.stop()
                os._exit(2)

    def _signal_handler(self, signum, frame):
        """信号处理器"""
        self.logger.info(f"收到信号 {signum}，准备退出", event="SIGNAL", reason=str(signum))
        self.running = False

    def _is_connection_error(self, error_msg: str) -> bool:
        """检测是否是连接错误（需要重连）"""
        connection_error_keywords = [
            "ServiceChannel",
            "无法用于通信",
            "Faulted",
            "通信对象",
            "通道",
            "Channel",
            "连接",
            "Connect",
            "未连接到服务器",
            "not connected",
        ]
        error_lower = error_msg.lower()
        return any(kw.lower() in error_lower for kw in connection_error_keywords)

    def _is_revision_error(self, error_msg: str) -> bool:
        """检测是否是 NetLimiter 规则版本(乐观锁)冲突

        典型报错：There is already newer revision of the rule <guid>.
        成因：规则对象被缓存后，期间有人在 NetLimiter UI 或别的进程改过该规则，
        revision 递增；再用旧对象 UpdateRule 就被服务拒绝。
        """
        revision_keywords = (
            "newer revision",
            "already newer",
            "revision of the rule",
            "较新的版本",
            "版本冲突",
        )
        error_lower = error_msg.lower()
        return any(kw.lower() in error_lower for kw in revision_keywords)

    def _set_rule_enabled_safe(self, rule_id: str, enabled: bool, context: str = "") -> bool:
        """安全地设置规则 IsEnabled，自动处理 NetLimiter revision 冲突。

        冲突时丢弃过时的缓存对象，从服务器重新拉取最新规则再重试（最多 3 次）。
        这是"手动改规则后脚本再也关不掉"的根治点：旧实现只在成功后清缓存，
        一旦 revision 冲突就永远用旧对象重试，形成死循环。

        Args:
            rule_id: 规则 GUID
            enabled: 目标启用状态
            context: 日志上下文前缀（如 "路由器规则" / "Qbit规则"）

        Returns:
            True  写入成功，或规则本就处于目标状态；
            False 规则不存在，或 revision 冲突重试耗尽（其余异常向上抛）。
        """
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            rule = self._get_cached_rule(rule_id)
            if rule is None:
                self.logger.error(
                    f"{context}未找到规则 {rule_id}，无法设置启用状态",
                    event="RULE_NOT_FOUND"
                )
                return False

            # 已是目标状态：仅在首次尝试时短路，避免用旧对象做无意义写入。
            # 注意：revision 冲突后的重试必须强制写入——前一次尝试已在本地对象上
            # 改过 IsEnabled，若此时短路会误判"已生效"，实际服务端并未写入成功。
            if attempt == 1:
                try:
                    if bool(rule.IsEnabled) == enabled:
                        return True
                except Exception:
                    pass

            try:
                rule.IsEnabled = enabled
                self.client.UpdateRule(rule)  # type: ignore
            except Exception as e:
                error_msg = str(e)
                if not self._is_revision_error(error_msg):
                    # 非版本冲突：交给调用方 _handle_api_error 处理（重连/计数/退出）
                    raise
                # 关键：清掉过时缓存，下一轮 _get_cached_rule 会从服务器重新拉取
                self._rule_cache.pop(rule_id, None)
                self.logger.warn(
                    f"{context}规则 {rule_id} revision 冲突（第 {attempt}/{max_attempts} 次），"
                    f"已刷新规则对象，稍后重试: {error_msg[:120]}",
                    event="RULE_REVISION_CONFLICT"
                )
                if attempt < max_attempts:
                    continue
                self.logger.error(
                    f"{context}规则 {rule_id} revision 冲突，重试 {max_attempts} 次仍失败，本轮放弃写入",
                    event="RULE_REVISION_RETRY_EXHAUSTED"
                )
                return False

            # 写入成功：清缓存，强制下次重新获取以验证实际状态
            self._rule_cache.pop(rule_id, None)
            if attempt > 1:
                self.logger.info(
                    f"{context}规则 {rule_id} 在第 {attempt} 次尝试写入成功",
                    event="RULE_REVISION_RETRY_OK"
                )
            return True

        return False

    def _cleanup_connection(self):
        """清理 NetLimiter 连接"""
        try:
            if self.client:
                self.client.Close()
        except Exception:
            pass
        self.client = None
        self._rule_cache.clear()

    def _handle_api_error(self, error_msg: str, context: str) -> bool:
        """
        处理 API 错误，如果是连接错误则尝试恢复

        Returns:
            True 表示已处理（重连成功或决定退出），False 表示未处理
        """
        # revision 冲突由 _set_rule_enabled_safe 内部重试处理；即使漏到这里，
        # 也是"规则在别处被改动"这种可恢复事件，不应计入连续错误误杀进程
        if self._is_revision_error(error_msg):
            self.logger.warn(
                f"{context}: 规则版本冲突（已在别处被修改），跳过本轮，不计入连续错误",
                event="RULE_REVISION_SKIPPED"
            )
            self._rule_cache.clear()
            return True

        if not self._is_connection_error(error_msg):
            # 非连接错误，只增加计数
            self._consecutive_errors += 1
            if self._consecutive_errors >= self.MAX_CONSECUTIVE_ERRORS:
                self.logger.error(
                    f"{context}: 连续错误 {self.MAX_CONSECUTIVE_ERRORS} 次，退出进程",
                    event="MAX_ERRORS_REACHED"
                )
                self.running = False
            return False

        # 连接错误
        self._consecutive_errors += 1
        self.logger.warn(
            f"{context}: 检测到连接错误，连续错误 {self._consecutive_errors}/{self.MAX_CONSECUTIVE_ERRORS}",
            event="CONNECTION_ERROR_DETECTED"
        )

        if self._consecutive_errors >= self.MAX_CONSECUTIVE_ERRORS:
            self.logger.error(
                f"{context}: 连续错误 {self.MAX_CONSECUTIVE_ERRORS} 次，退出进程",
                event="MAX_ERRORS_REACHED"
            )
            self.running = False
            return True

        # 清理并尝试重连
        self._cleanup_connection()

        if self._connect_nl_client():
            self.logger.info(f"{context}: 自动重连成功", event="AUTO_RECONNECT_SUCCESS")
            self._consecutive_errors = 0
        else:
            self.logger.error(f"{context}: 自动重连失败", event="AUTO_RECONNECT_FAILED")

        return True

    def _check_parent_alive(self) -> bool:
        """检查父进程是否存活"""
        if not self.supervisor_pid:
            return True
        
        try:
            os.kill(int(self.supervisor_pid), 0)
            return True
        except ProcessLookupError:
            # 进程不存在
            return False
        except PermissionError:
            # 进程存在但无权限（管理员子进程检查普通父进程），视为存活
            return True
        except OSError:
            # 其他 OS 错误，保守视为存活，避免误判
            return True
    
    def _is_admin(self) -> bool:
        """检查是否以管理员权限运行"""
        try:
            import ctypes
            return ctypes.windll.shell32.IsUserAnAdmin()
        except Exception:
            return False
    
    def _start_nl_service(self) -> bool:
        """启动 NetLimiter 服务"""
        try:
            import subprocess
            result = subprocess.run(
                ["sc", "query", "nlsvc"],
                capture_output=True,
                text=True,
                timeout=5
            )
            
            if "RUNNING" not in result.stdout:
                self.logger.info("正在启动 nlsvc 服务...", event="SERVICE_START")
                subprocess.run(
                    ["sc", "start", "nlsvc"],
                    capture_output=True,
                    text=True,
                    timeout=10
                )
                time.sleep(3)
                
                result = subprocess.run(
                    ["sc", "query", "nlsvc"],
                    capture_output=True,
                    text=True,
                    timeout=5
                )
                return "RUNNING" in result.stdout
            
            return True
        except Exception as e:
            self.logger.error(f"启动服务失败: {e}", event="SERVICE_START_ERROR")
            return False
    
    def _connect_nl_client(self) -> bool:
        """连接 NetLimiter 客户端"""
        retry_count = 0
        
        while retry_count < self.MAX_RETRY_COUNT and self.running:
            # 心跳由独立线程维护
            
            try:
                if self.client is not None:
                    try:
                        self.client.Close()
                    except Exception:
                        pass
                
                self.client = NLClient()
                self.client.Connect() # pyright: ignore[reportOptionalMemberAccess]
                self.logger.info("API 已连接", event="NL_CONNECTED")
                return True
                
            except Exception as e:
                self.logger.warn(f"连接失败: {e}", event="NL_CONNECT_FAILED")
                
                if self._start_nl_service():
                    try:
                        self.client = NLClient()
                        self.client.Connect()  # type: ignore
                        self.logger.info("API 已连接（服务启动后）", event="NL_CONNECTED")
                        return True
                    except Exception as e2:
                        self.logger.error(f"服务启动后连接仍失败: {e2}", event="NL_CONNECT_FAILED")
            
            retry_count += 1
            if retry_count < self.MAX_RETRY_COUNT:
                self.logger.info(f"第 {retry_count} 次重试失败，{self.RETRY_INTERVAL_SECONDS} 秒后重试...", event="RETRY")
                time.sleep(self.RETRY_INTERVAL_SECONDS)
        
        self.logger.error("连接失败，已达最大重试次数", event="NL_CONNECT_FAILED")
        return False

    def _get_effective_interval(self, base_interval: int, last_change_time: Optional[float]) -> int:
        """
        获取有效检查间隔

        规则变更后60秒内使用3倍间隔，之后恢复正常

        Args:
            base_interval: 基础间隔（秒）
            last_change_time: 上次规则变更时间戳，None表示从未变更

        Returns:
            实际使用的检查间隔（秒）
        """
        if last_change_time is None:
            return base_interval

        elapsed = time.time() - last_change_time
        if elapsed < self.COOLDOWN_SECONDS:
            # 冷却期内使用3倍间隔
            return base_interval * self.COOLDOWN_MULTIPLIER
        else:
            # 冷却期结束，恢复正常间隔
            return base_interval

    def _get_data(self) -> Optional[Dict[str, Any]]:
        """读取速度数据"""
        try:
            if not self.DATA_FILE.exists():
                return None
            
            with self._data_file_lock:
                data = json.loads(self.DATA_FILE.read_text(encoding="utf-8"))
            return data
            
        except Timeout:
            self.logger.warn("读取速度数据获取文件锁超时，跳过本轮", event="DATA_READ_LOCK_TIMEOUT")
            return None
        except Exception as e:
            self.logger.error(f"读取速度数据失败: {e}", event="DATA_READ_ERROR")
            return None
    
    def _get_router_data(self) -> Optional[Dict[str, Any]]:
        """读取路由器数据"""
        try:
            if not self.ROUTER_DATA_FILE.exists():
                return None
            
            with self._router_file_lock:
                data = json.loads(self.ROUTER_DATA_FILE.read_text(encoding="utf-8"))
            return data
            
        except Timeout:
            self.logger.warn("读取路由器数据获取文件锁超时，跳过本轮", event="ROUTER_DATA_READ_LOCK_TIMEOUT")
            return None
        except Exception as e:
            self.logger.error(f"读取路由器数据失败: {e}", event="ROUTER_DATA_READ_ERROR")
            return None
    
    def _data_age_seconds(self, data: Optional[Dict[str, Any]]) -> Optional[float]:
        """返回数据年龄（秒），无法解析时返回 None"""
        if not data or not data.get("LastUpdate"):
            return None

        try:
            last_update = datetime.fromisoformat(data["LastUpdate"])
            return (datetime.now() - last_update).total_seconds()
        except Exception:
            return None

    def _is_data_fresh(self, data: Optional[Dict[str, Any]]) -> bool:
        """检查 qb 数据是否新鲜"""
        age = self._data_age_seconds(data)
        return age is not None and age < self.QBIT_DATA_MAX_AGE_SECONDS

    def _is_router_data_fresh(self, data: Optional[Dict[str, Any]]) -> bool:
        """检查路由器数据是否新鲜

        与 qb 路径对齐：陈旧数据不得参与判定。router_sampler 假死时
        OverThresholdSeconds/BelowThresholdSeconds 会被冻结，规则状态随之卡死。
        """
        age = self._data_age_seconds(data)
        return age is not None and age < self.ROUTER_DATA_MAX_AGE_SECONDS
    
    def _is_client_connected(self) -> bool:
        """检查NetLimiter客户端是否已连接，兼容不同版本枚举表示"""
        if self.client is None:
            return False
        try:
            # 兼容不同版本的State枚举表示
            state_str = str(self.client.State)
            return "Connected" in state_str or state_str == "1"
        except:
            return False

    def _get_cached_rule(self, rule_id: str):
        """获取缓存的规则，如果未缓存则从服务器查找"""
        # 先检查缓存
        if rule_id in self._rule_cache:
            return self._rule_cache[rule_id]

        # 缓存未命中，从服务器查找
        if self.client:
            for r in self.client.Rules:  # type: ignore
                if str(r.Id) == rule_id:
                    self._rule_cache[rule_id] = r
                    return r

        return None

    def _get_fresh_rule(self, rule_id: str):
        """判定前强制从 NetLimiter 服务器拉取最新规则对象，并刷新缓存。

        与 _get_cached_rule 的区别：即使缓存命中也重新遍历 client.Rules，
        确保读到服务器当前的 IsEnabled / Revision。

        背景：用户在 NetLimiter UI 手动开关规则只改服务器端，进程内缓存不会自动失效。
        若判定读缓存旧值，会出现「手动开规则后 rule_enabled 仍是 False → 不进禁用分支
        → 永远不禁用」的静默失效。写入路径 _set_rule_enabled_safe 的 revision 重试
        只有在真正调用 UpdateRule 时才触发，判定必须在进入分支前就看到真实状态。

        性能：每次判定遍历一次规则集合（约百条），qbit 每 20s / router 每 2s，开销可忽略。
        """
        if not self.client:
            return None
        for r in self.client.Rules:  # type: ignore
            if str(r.Id) == rule_id:
                self._rule_cache[rule_id] = r
                return r
        # 服务器上已无此规则，清掉陈旧缓存避免后续误用
        self._rule_cache.pop(rule_id, None)
        return None

    def _check_rule(self, data: Dict[str, Any]):
        """检查并更新 qBittorrent 限速规则"""
        try:
            if not self._is_client_connected():
                if not self._connect_nl_client():
                    return
                # 连接重建后清除缓存
                self._rule_cache.clear()

            avg_speed_kb = data.get("AvgSpeedKB", 0.0)
            sample_count = data.get("SampleCount", 0)

            # 判定前强制拉取服务器最新规则，避免手动改规则后缓存旧值导致判定失效
            rule = self._get_fresh_rule(self.LIMIT_RULE_ID)

            if rule is None:
                self.logger.error(f"未找到规则 {self.LIMIT_RULE_ID}", event="RULE_NOT_FOUND")
                return

            rule_enabled = rule.IsEnabled

            if avg_speed_kb >= self.THRESHOLD_KB and not rule_enabled:
                if not self._set_rule_enabled_safe(self.LIMIT_RULE_ID, True, "Qbit规则"):
                    return
                # 记录规则变更时间，触发冷却
                self._last_rule_change_time = time.time()
                cooldown_end = self._last_rule_change_time + self.COOLDOWN_SECONDS
                self.logger.info(
                    f"Qbit规则 状态变更: 已启用 | 平均速度={avg_speed_kb} KB/s >= 阈值={self.THRESHOLD_KB} KB/s | "
                    f"冷却期至{datetime.fromtimestamp(cooldown_end).strftime('%H:%M:%S')}",
                    event="RULE_ENABLED"
                )
            elif avg_speed_kb < self.THRESHOLD_KB and rule_enabled:
                # 记录低于阈值的开始时间
                if self._below_threshold_since is None:
                    self._below_threshold_since = time.time()
                    elapsed = 0
                else:
                    elapsed = time.time() - self._below_threshold_since

                # 只有连续低于阈值 90 秒才禁用
                if elapsed >= 90:
                    if not self._set_rule_enabled_safe(self.LIMIT_RULE_ID, False, "Qbit规则"):
                        return
                    # 记录规则变更时间，触发冷却
                    self._last_rule_change_time = time.time()
                    cooldown_end = self._last_rule_change_time + self.COOLDOWN_SECONDS
                    # 重置低于阈值计时
                    self._below_threshold_since = None
                    self.logger.info(
                        f"Qbit规则 状态变更: 已禁用 | 平均速度={avg_speed_kb} KB/s < 阈值={self.THRESHOLD_KB} KB/s | "
                        f"连续低于阈值 {elapsed:.0f} 秒 | "
                        f"冷却期至{datetime.fromtimestamp(cooldown_end).strftime('%H:%M:%S')}",
                        event="RULE_DISABLED"
                    )
                else:
                    # 未满 90 秒，仅记录状态
                    self.logger.info(
                        f"Qbit规则 低于阈值累计 {elapsed:.0f}/90 秒 | 平均速度={avg_speed_kb} KB/s",
                        event="QBIT_BELOW_THRESHOLD"
                    )
            elif avg_speed_kb >= self.THRESHOLD_KB:
                # 速度恢复到阈值以上，重置低于阈值计时
                if self._below_threshold_since is not None:
                    self._below_threshold_since = None
                    self.logger.info("Qbit规则 低于阈值计时已重置", event="QBIT_THRESHOLD_RESET")

            # 成功执行，重置错误计数
            self._consecutive_errors = 0

        except Exception as e:
            error_msg = str(e)
            self.logger.error(f"Qbit规则检查错误: {e}", event="RULE_CHECK_ERROR", reason=error_msg)
            self._handle_api_error(error_msg, "Qbit规则检查")

    def _check_zero_data(self, data: Dict[str, Any]) -> None:
        """采样源零值看护：AvgSpeedKB 长期恒 0 时告警（不参与判定）

        典型场景：speed_sampler 过滤器选取错误 → 采到无流量过滤器 → 恒 0 →
        阈值永不达成且判定分支无日志（静默失效）。这里只负责把静默变成可见告警。
        """
        try:
            avg = data.get("AvgSpeedKB", 0.0) or 0.0
            now = time.time()

            if avg > 0:
                if self._zero_since is not None:
                    self.logger.info(
                        f"Qbit采样源恢复非零 (AvgSpeedKB={avg} KB/s)，零值看护复位",
                        event="QBIT_ZERO_WATCH_RESET"
                    )
                    self._zero_since = None
                return

            if self._zero_since is None:
                self._zero_since = now
                return

            elapsed = now - self._zero_since
            if elapsed < self.ZERO_ALERT_AFTER_SECONDS:
                return
            if now - self._last_zero_alert < self.ZERO_ALERT_REPEAT_SECONDS:
                return

            self._last_zero_alert = now
            self.logger.warn(
                f"Qbit采样源持续 {int(elapsed)} 秒恒为 0 (AvgSpeedKB=0)，"
                f"疑似过滤器选取错误或采样进程异常；阈值 {self.THRESHOLD_KB} KB/s 判定因此永不触发。"
                f"请核对 speed_sampler 的过滤器发现日志 (PRIV_FILTER_FOUND / PRIV_FILTER_NOT_FOUND)",
                event="QBIT_ZERO_DATA_SUSPECT"
            )
        except Exception as e:
            self.logger.error(f"零值看护异常: {e}", event="QBIT_ZERO_WATCH_ERROR")

    def _on_router_data_stale(self, age: Optional[float], read_failed: bool = False) -> None:
        """路由器数据陈旧/不可读时的看护：跳过判定并逐级告警

        Args:
            age: 数据年龄（秒），None 表示无法解析
            read_failed: 数据文件缺失/读取失败
        """
        now = time.time()
        if self._router_stale_since is None:
            self._router_stale_since = now
        self._router_stale_count += 1
        stale_for = now - self._router_stale_since

        if read_failed:
            detail = "数据文件缺失或不可读"
        elif age is None:
            detail = "LastUpdate 无法解析"
        else:
            detail = f"数据年龄={age:.1f}s > 上限={self.ROUTER_DATA_MAX_AGE_SECONDS}s"

        if self._router_stale_count <= self.STALE_LOG_FIRST_N:
            self.logger.warn(
                f"路由器数据已失效（{detail}），跳过本轮路由器规则检查 "
                f"(连续 {self._router_stale_count} 次)",
                event="ROUTER_DATA_EXPIRED"
            )
            return

        if stale_for < self.ROUTER_STALE_ALERT_AFTER_SECONDS:
            return
        if now - self._last_router_stale_alert < self.ROUTER_STALE_ALERT_REPEAT_SECONDS:
            return

        self._last_router_stale_alert = now
        self.logger.error(
            f"路由器采样数据已中断 {int(stale_for)} 秒（{detail}），router_sampler 可能已卡死；"
            f"路由器规则判定停摆，规则状态将冻结在当前值无法自动恢复。"
            f"请检查 router_sampler 进程存活与 router_speed_sampler 日志",
            event="ROUTER_DATA_STALE_ALERT"
        )

    def _get_local_avg_speed_kb(self) -> Optional[float]:
        """获取本机最近10秒平均速度"""
        try:
            data = self._get_data()
            if not data or not self._is_data_fresh(data):
                return None
            return data.get("AvgSpeedKB", 0.0)
        except Exception as e:
            self.logger.error(f"获取本机平均速度失败: {e}", event="LOCAL_AVG_ERROR")
            return None
    
    # ------------------------------------------------------------------
    # 上传限速规则看门狗
    # ------------------------------------------------------------------
    def _rule_has_start_condition(self, rule) -> bool:
        """规则是否带 Action=Start 的时间条件

        这类规则的启用/禁用由 NetLimiter 自身调度器负责（例如周末 01:00 自动开启）。
        看门狗一旦禁用，下次 Start 条件又会开回来，形成反复拉锯，因此整条规则跳过。
        条件读不出来时同样跳过（fail-safe：宁可不管，也不误伤计划任务）。
        """
        try:
            conds = list(rule.Conditions) if rule.Conditions else []
        except Exception:
            return True

        for c in conds:
            try:
                if c.GetType().Name != "TimeCondition":
                    continue
                if str(getattr(c, "Action", "")).strip().lower() == "start":
                    return True
            except Exception:
                return True
        return False

    def _is_upload_guard_target(self, rule) -> bool:
        """是否属于看门狗管辖：LimitRule + Out/Both 方向 + 限速 < 3MB/s + 非本脚本控制"""
        try:
            if rule.GetType().Name != "LimitRule":
                return False
            if str(rule.Id).lower() in self.UPLOAD_GUARD_EXCLUDE_RULE_IDS:
                return False
            # "Out" 与 "Both" 都会产生上传方向限速；注意 "both" 不含子串 "out"
            if str(rule.Dir).strip().lower() not in ("out", "both"):
                return False
            limit = int(rule.LimitSize)
            # limit == 0 表示不限速，不属于管辖范围
            return 0 < limit < self.UPLOAD_GUARD_THRESHOLD_BYTES
        except Exception:
            return False

    def _load_upload_guard_state(self) -> Dict[str, Any]:
        """读取计时状态（落盘，跨 rule_checker 重启保留）"""
        try:
            if not self.UPLOAD_GUARD_STATE_FILE.exists():
                return {}
            data = json.loads(self.UPLOAD_GUARD_STATE_FILE.read_text(encoding="utf-8"))
            rules = data.get("rules")
            return rules if isinstance(rules, dict) else {}
        except Exception as e:
            self.logger.warn(
                f"读取上传看门狗状态失败，按空状态处理: {e}",
                event="UPLOAD_GUARD_STATE_READ_ERROR"
            )
            return {}

    def _save_upload_guard_state(self, rules: Dict[str, Any]) -> None:
        """原子写入计时状态"""
        try:
            self.UPLOAD_GUARD_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
            tmp = str(self.UPLOAD_GUARD_STATE_FILE) + ".tmp"
            payload = {"updated_at": datetime.now().isoformat(), "rules": rules}
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False)
            os.replace(tmp, self.UPLOAD_GUARD_STATE_FILE)
            self._upload_guard_write_warned = False
        except Exception as e:
            # 写盘失败只告警一次，避免每 60 秒刷屏；计时退化为内存态（本次进程内仍有效）
            if not self._upload_guard_write_warned:
                self._upload_guard_write_warned = True
                self.logger.warn(
                    f"写入上传看门狗状态失败（计时将无法跨重启保留）: {e}",
                    event="UPLOAD_GUARD_STATE_WRITE_ERROR"
                )

    def _check_upload_rule_guard(self) -> None:
        """非脚本控制的 <3MB 上传限速规则兜底：连续启用 30 分钟即自动禁用

        只在规则"仍处于启用"时累计时长；规则一旦被关闭（人工或时间条件），
        计时条目随之丢弃，下次启用重新从 0 开始。
        """
        if not self._is_client_connected():
            if not self._connect_nl_client():
                return
            self._rule_cache.clear()

        try:
            rules = list(self.client.Rules)
        except Exception as e:
            self.logger.warn(f"枚举规则失败，跳过本轮上传看门狗: {e}", event="UPLOAD_GUARD_ENUM_ERROR")
            return

        now = time.time()
        old_state = self._load_upload_guard_state()
        new_state: Dict[str, Any] = {}

        for rule in rules:
            if not self._is_upload_guard_target(rule):
                continue

            rid = str(rule.Id)
            try:
                if not bool(rule.IsEnabled):
                    if rid in old_state:
                        self.logger.info(
                            f"上传限速规则 {rid} 已关闭，看门狗计时清除",
                            event="UPLOAD_GUARD_RESET"
                        )
                    continue
            except Exception:
                continue

            # 计划型规则由 NetLimiter 调度器管理，不干预也不计时
            if self._rule_has_start_condition(rule):
                if rid in old_state:
                    self.logger.info(
                        f"上传限速规则 {rid} 带时间计划(Start)，看门狗放弃干预并清除计时",
                        event="UPLOAD_GUARD_SKIP_SCHEDULED"
                    )
                continue

            limit_kb = int(rule.LimitSize) / 1024
            entry = old_state.get(rid)
            try:
                since = float(entry["since"]) if entry and "since" in entry else now
            except Exception:
                since = now
            enabled_for = now - since

            if enabled_for >= self.UPLOAD_GUARD_ENABLED_SECONDS:
                try:
                    disabled_ok = self._set_rule_enabled_safe(rid, False, "上传看门狗")
                except Exception as e:
                    # 非 revision 的回写异常：保留计时条目，下一轮继续尝试
                    disabled_ok = False
                    self.logger.error(
                        f"上传限速规则 {rid} 自动禁用失败: {e}",
                        event="UPLOAD_GUARD_DISABLE_ERROR",
                        reason=str(e)
                    )
                if not disabled_ok:
                    # 写入失败（规则消失 / revision 重试耗尽）：保留计时，下一轮再试
                    new_state[rid] = {
                        "since": since,
                        "limit_kb": round(limit_kb, 1),
                        "filter_id": str(getattr(rule, "FilterId", "")),
                    }
                    continue

                self.logger.info(
                    f"上传限速规则已自动禁用 | 过滤器={getattr(rule, 'FilterId', '?')} | "
                    f"限速={limit_kb:.1f} KB/s | 已连续启用 {enabled_for / 60:.0f} 分钟",
                    event="UPLOAD_GUARD_DISABLED"
                )
                self.logger.alert(
                    "UPLOAD_LIMIT_STUCK",
                    f"非脚本控制的上传限速规则连续启用 {enabled_for / 60:.0f} 分钟，已自动禁用",
                    {
                        "rule_id": rid,
                        "filter_id": str(getattr(rule, "FilterId", "")),
                        "limit_kb": round(limit_kb, 1),
                        "enabled_seconds": int(enabled_for),
                    }
                )
                # 禁用后不再保留计时条目
                continue

            if entry is None:
                self.logger.info(
                    f"检测到上传限速规则被启用 | 规则={rid} | "
                    f"限速={limit_kb:.1f} KB/s | "
                    f"{self.UPLOAD_GUARD_ENABLED_SECONDS // 60} 分钟后若仍未关闭将自动禁用",
                    event="UPLOAD_GUARD_ARMED"
                )

            new_state[rid] = {
                "since": since,
                "limit_kb": round(limit_kb, 1),
                "filter_id": str(getattr(rule, "FilterId", "")),
            }

        if new_state != old_state:
            self._save_upload_guard_state(new_state)

    def _check_router_rule(self, router_data: Dict[str, Any]):
        """检查并更新路由器限速规则"""
        try:
            if not self._is_client_connected():
                if not self._connect_nl_client():
                    return
                # 连接重建后清除缓存
                self._rule_cache.clear()

            router_speed_kb = router_data.get("RouterSpeedKB")
            over_threshold_seconds = router_data.get("OverThresholdSeconds", 0)
            below_threshold_seconds = router_data.get("BelowThresholdSeconds", 0)

            # Use the local Internet speed captured in the same router sampling loop.
            local_avg_speed_kb = router_data.get("LocalSpeedKB")
            if local_avg_speed_kb is None:
                self.logger.warn("路由器采样中的本机速度数据无效，跳过路由器规则检查", event="ROUTER_CHECK_SKIP")
                return

            # 差值计算：路由器速度 - 本机平均速度
            speed_diff = router_speed_kb - local_avg_speed_kb if router_speed_kb is not None else 0

            # 判定前强制拉取服务器最新规则：手动在 UI 改规则（如开启 1M 限速）只改
            # 服务器端，进程缓存不会失效；读旧缓存会误判 rule_enabled=False 而永不禁用
            rule = self._get_fresh_rule(self.ROUTER_RULE_ID)

            if rule is None:
                self.logger.error(f"未找到路由器规则 {self.ROUTER_RULE_ID}", event="ROUTER_RULE_NOT_FOUND")
                return

            rule_enabled = rule.IsEnabled

            # 连续超过阈值指定秒数，启用规则
            if speed_diff > self.ROUTER_THRESHOLD_KB and over_threshold_seconds >= self.ROUTER_CONSECUTIVE_SECONDS and not rule_enabled:
                if not self._set_rule_enabled_safe(self.ROUTER_RULE_ID, True, "路由器规则"):
                    return
                # 记录规则变更时间，触发冷却
                self._last_router_rule_change_time = time.time()
                cooldown_end = self._last_router_rule_change_time + self.COOLDOWN_SECONDS
                self.logger.info(
                    f"路由器规则 状态变更: 已启用 | 路由器速度={router_speed_kb} KB/s, 本机平均={local_avg_speed_kb} KB/s, "
                    f"差值={speed_diff:.2f} KB/s > 阈值={self.ROUTER_THRESHOLD_KB} KB/s, 连续超阈值={over_threshold_seconds}秒 | "
                    f"冷却期至{datetime.fromtimestamp(cooldown_end).strftime('%H:%M:%S')}",
                    event="ROUTER_RULE_ENABLED"
                )
            elif below_threshold_seconds >= self.ROUTER_BELOW_THRESHOLD_SECONDS and rule_enabled:
                # 连续低于阈值指定秒数，禁用规则
                if not self._set_rule_enabled_safe(self.ROUTER_RULE_ID, False, "路由器规则"):
                    return
                # 记录规则变更时间，触发冷却
                self._last_router_rule_change_time = time.time()
                cooldown_end = self._last_router_rule_change_time + self.COOLDOWN_SECONDS
                self.logger.info(
                    f"路由器规则 状态变更: 已禁用 | 路由器速度={router_speed_kb} KB/s, 本机平均={local_avg_speed_kb} KB/s, "
                    f"差值={speed_diff:.2f} KB/s | 连续低于阈值 {below_threshold_seconds} 秒 | "
                    f"冷却期至{datetime.fromtimestamp(cooldown_end).strftime('%H:%M:%S')}",
                    event="ROUTER_RULE_DISABLED"
                )

            # 成功执行，重置错误计数
            self._consecutive_errors = 0

        except Exception as e:
            error_msg = str(e)
            self.logger.error(f"路由器规则检查错误: {e}", event="ROUTER_RULE_CHECK_ERROR", reason=error_msg)
            self._handle_api_error(error_msg, "路由器规则检查")
    
    def _describe_router_rule_state(self) -> str:
        """读取路由器规则当前状态，仅用于开关关闭时的显式提示（不写入）"""
        try:
            if not self._is_client_connected() and not self._connect_nl_client():
                return "未知(未连接)"
            rule = self._get_cached_rule(self.ROUTER_RULE_ID)
            if rule is None:
                return "未找到规则"
            return "已启用(仍在限速，需手动关闭)" if rule.IsEnabled else "已禁用"
        except Exception as e:
            return f"未知({e})"

    def _wait_for_speed_sampler(self) -> bool:
        """等待速度采样器数据就绪"""
        self.logger.info("等待速度采样器数据...")
        wait_count = 0
        max_wait = 30
        
        while wait_count < max_wait and self.running:
            data = self._get_data()
            if data is not None and self._is_data_fresh(data):
                self.logger.info("速度采样器数据已就绪", event="SPEED_SAMPLER_READY")
                return True
            
            if wait_count % 5 == 0:
                self.logger.info(f"等待速度采样器数据: {wait_count}s", event="WAIT_SPEED_DATA")
            
            time.sleep(1)
            wait_count += 1
        
        self.logger.warn("等待速度采样器数据超时", event="SPEED_DATA_TIMEOUT")
        return False
    
    def _wait_for_router_sampler(self) -> bool:
        """等待路由器速度采样器数据就绪"""
        self.logger.info("等待路由器速度采样器数据...")
        wait_count = 0
        max_wait = 30
        
        while wait_count < max_wait and self.running:
            router_data = self._get_router_data()
            if self._is_router_data_fresh(router_data):
                self.logger.info("路由器速度采样器数据已就绪", event="ROUTER_SAMPLER_READY")
                return True
            
            if wait_count % 5 == 0:
                self.logger.info(f"等待路由器速度采样器数据: {wait_count}s", event="WAIT_ROUTER_DATA")
            
            time.sleep(1)
            wait_count += 1
        
        self.logger.warn("等待路由器速度采样器数据超时", event="ROUTER_DATA_TIMEOUT")
        return False
    
    def run(self):
        """主循环"""
        self.logger.info("开始运行", event="START")
        
        # 检查管理员权限
        if not self._is_admin():
            self.logger.warn("需要管理员权限才能正常工作", event="ADMIN_REQUIRED")
        
        # 初始连接
        if not self._connect_nl_client():
            self.logger.error("初始连接失败，退出", event="INIT_CONNECT_FAILED")
            sys.exit(1)

        # 上传限速看门狗状态提示
        if self.upload_guard_enabled:
            self.logger.info(
                f"上传限速看门狗已启用：非脚本控制的 Out 方向、限速 < "
                f"{self.UPLOAD_GUARD_THRESHOLD_BYTES / 1024 / 1024:.0f}MB/s 的规则，"
                f"连续启用 {self.UPLOAD_GUARD_ENABLED_SECONDS // 60} 分钟将被自动禁用"
                f"（带 Start 时间计划的规则跳过）",
                event="UPLOAD_GUARD_READY"
            )
        else:
            self.logger.warn(
                f"上传限速看门狗已关闭 ({ENV_UPLOAD_GUARD_ENABLED}=0)",
                event="UPLOAD_GUARD_DISABLED"
            )
        
        # 等待速度采样器数据就绪
        if not self._wait_for_speed_sampler():
            self.logger.error("速度采样器数据不可用，退出", event="SPEED_SAMPLER_UNAVAILABLE")
            sys.exit(1)
        
        # 等待路由器速度采样器数据就绪（路由器规则关闭时跳过）
        if self.router_rule_enabled:
            if not self._wait_for_router_sampler():
                self.logger.warn("路由器速度采样器数据不可用，继续运行", event="ROUTER_SAMPLER_UNAVAILABLE")
        else:
            self.logger.warn(
                f"路由器规则链已关闭 (--no-router / {ENV_ROUTER_RULE_ENABLED}=0)："
                f"不再读取 router 采样数据、不做规则变更；当前规则状态={self._describe_router_rule_state()}",
                event="ROUTER_RULE_DISABLED_BY_CONFIG"
            )
        
        # 首次检查延迟
        time.sleep(self.FIRST_CHECK_DELAY_SECONDS)
        
        last_qbit_check = time.time()
        last_router_check = time.time()

        while self.running:
            try:
                # 检查父进程
                if not self._check_parent_alive():
                    self.logger.info("父进程已退出，准备退出", event="PARENT_EXIT")
                    break
                
                self.loop_count += 1
                current_time = time.time()

                # 计算动态检查间隔（冷却期内使用3倍间隔）
                qbit_effective_interval = self._get_effective_interval(
                    self.CHECK_INTERVAL_SECONDS, self._last_rule_change_time
                )
                router_effective_interval = self._get_effective_interval(
                    self.ROUTER_CHECK_INTERVAL_SECONDS, self._last_router_rule_change_time
                )

                # Qbit规则检查（动态间隔，默认20秒，触发后60秒内60秒）
                if current_time - last_qbit_check >= qbit_effective_interval:
                    try:
                        # 读取速度数据
                        data = self._get_data()

                        if data is not None:
                            if self._is_data_fresh(data):
                                self.data_consecutive_expired = 0
                                self._check_rule(data)
                                self._check_zero_data(data)
                            else:
                                self.data_consecutive_expired += 1
                                if self.data_consecutive_expired <= 3:  # 只记录前3次过期
                                    self.logger.warn(
                                        f"Qbit数据已过期，跳过本轮检查 (连续 {self.data_consecutive_expired} 次)",
                                        event="QBIT_DATA_EXPIRED"
                                    )
                        else:
                            self.logger.warn("无Qbit速度数据，跳过本轮检查", event="QBIT_NO_DATA")
                    except Exception as e:
                        self.logger.error(f"Qbit检查链路异常: {e}", event="QBIT_CHECK_ERROR")
                    finally:
                        last_qbit_check = current_time
                
                # 路由器规则检查（动态间隔，默认2秒，触发后60秒内6秒）
                if self.router_rule_enabled and current_time - last_router_check >= router_effective_interval:
                    try:
                        router_data = self._get_router_data()
                        if router_data is None:
                            self._on_router_data_stale(None, read_failed=True)
                        elif self._is_router_data_fresh(router_data):
                            self._router_stale_since = None
                            self._router_stale_count = 0
                            self._check_router_rule(router_data)
                        else:
                            self._on_router_data_stale(self._data_age_seconds(router_data))
                    except Exception as e:
                        self.logger.error(f"路由器检查链路异常: {e}", event="ROUTER_CHECK_ERROR")
                    finally:
                        last_router_check = current_time
                
                # 上传限速规则看门狗（默认 60 秒一次，独立于上面两条判定链路）
                if (self.upload_guard_enabled
                        and current_time - self._last_upload_guard_check >= self.UPLOAD_GUARD_CHECK_INTERVAL_SECONDS):
                    try:
                        self._check_upload_rule_guard()
                    except Exception as e:
                        self.logger.error(f"上传限速看门狗异常: {e}", event="UPLOAD_GUARD_ERROR")
                    finally:
                        self._last_upload_guard_check = current_time
                
                # 主循环活性上报：心跳线程独立运行，只有 loop_count 持续推进才能
                # 区分「主循环在跑」与「心跳线程活着但主循环卡在 NetLimiter 调用里」
                self.heartbeat.increment_loop()
                self.heartbeat.update_status("OK")

                # 高精度短休眠，避免CPU占用过高
                time.sleep(0.5)
                
            except Exception as e:
                self.logger.error(f"主循环异常: {e}", event="MAIN_LOOP_ERROR")
                self.heartbeat.update_status("ERROR", str(e))
                time.sleep(1)
        
        # 清理
        if self.client is not None:
            try:
                self.client.Close()
            except Exception:
                pass
        
        self.heartbeat.stop()
        self.logger.info("退出", event="EXIT")


def main():
    parser = argparse.ArgumentParser(description="规则检查器")
    parser.add_argument("--service", action="store_true", help="服务模式运行")
    parser.add_argument("--no-router", action="store_true",
                        help=f"不检查/不变更路由器规则（等价于 {ENV_ROUTER_RULE_ENABLED}=0）")
    parser.add_argument("--router", action="store_true",
                        help="强制启用路由器规则检查（覆盖环境变量）")
    args = parser.parse_args()

    router_enabled = None
    if args.no_router:
        router_enabled = False
    elif args.router:
        router_enabled = True

    checker = RuleChecker(router_rule_enabled=router_enabled)
    checker.run()


if __name__ == "__main__":
    main()
