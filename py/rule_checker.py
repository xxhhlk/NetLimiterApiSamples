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
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any

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

from common.heartbeat import HeartbeatManager
from common.logger import Logger


class RuleChecker:
    """规则检查器"""
    
    # qBittorrent 限速规则配置
    LIMIT_RULE_ID = "f4c3e3ac-91d1-435b-af27-f9020b4eab4e"
    THRESHOLD_KB = 400
    CHECK_INTERVAL_SECONDS = 20
    FIRST_CHECK_DELAY_SECONDS = 5
    RETRY_INTERVAL_SECONDS = 10
    MAX_RETRY_COUNT = 5
    
    # 路由器速度检查配置
    ROUTER_RULE_ID = "5b34aebb-191d-439c-a3e4-33a918905ac6"
    ROUTER_CONSECUTIVE_SECONDS = 3
    ROUTER_CHECK_INTERVAL_SECONDS = 2

    # 冷却机制：触发规则后间隔变为3倍，60秒后恢复
    COOLDOWN_MULTIPLIER = 3
    COOLDOWN_SECONDS = 60

    # 连接错误自动恢复配置
    MAX_CONSECUTIVE_ERRORS = 10  # 连续错误最大次数，超过则退出

    # 数据文件路径
    DATA_FILE = Path(os.environ.get("TEMP", ".")) / "qb_speed_data.json"
    LOCK_FILE = Path(os.environ.get("TEMP", ".")) / "qb_speed_data.lock"
    ROUTER_DATA_FILE = Path(os.environ.get("TEMP", ".")) / "router_speed_data.json"
    ROUTER_LOCK_FILE = Path(os.environ.get("TEMP", ".")) / "router_speed_data.lock"
    
    def __init__(self):
        self.logger = Logger("rule_checker")
        self.heartbeat = HeartbeatManager("rule_checker")

        # 确保数据文件目录存在
        self.DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
        
        # 状态变量
        self.loop_count = 0
        self.data_consecutive_expired = 0
        self.running = True

        # 冷却状态跟踪
        self._last_rule_change_time: Optional[float] = None
        self._last_router_rule_change_time: Optional[float] = None

        # NetLimiter 客户端
        self.client: Optional[NLClient] = None

        # 规则缓存（避免每次线性搜索）
        self._rule_cache: Dict[str, Any] = {}

        # 错误计数器
        self._consecutive_errors = 0
        self._last_error_type = None
        
        # 父进程检测
        self.supervisor_pid = os.environ.get("SUPERVISOR_PID")
        
        # 设置信号处理
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        self.logger.info("初始化完成", event="INIT_OK")
    
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
        except (OSError, ProcessLookupError):
            return False
    
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
            # 检查锁文件
            if self.LOCK_FILE.exists():
                time.sleep(0.05)
            
            if not self.DATA_FILE.exists():
                return None
            
            data = json.loads(self.DATA_FILE.read_text(encoding="utf-8"))
            return data
            
        except Exception as e:
            self.logger.error(f"读取速度数据失败: {e}", event="DATA_READ_ERROR")
            return None
    
    def _get_router_data(self) -> Optional[Dict[str, Any]]:
        """读取路由器数据"""
        try:
            # 检查锁文件
            if self.ROUTER_LOCK_FILE.exists():
                time.sleep(0.05)
            
            if not self.ROUTER_DATA_FILE.exists():
                return None
            
            data = json.loads(self.ROUTER_DATA_FILE.read_text(encoding="utf-8"))
            return data
            
        except Exception as e:
            self.logger.error(f"读取路由器数据失败: {e}", event="ROUTER_DATA_READ_ERROR")
            return None
    
    def _is_data_fresh(self, data: Optional[Dict[str, Any]]) -> bool:
        """检查数据是否新鲜"""
        if not data or not data.get("LastUpdate"):
            return False
        
        try:
            last_update = datetime.fromisoformat(data["LastUpdate"])
            age = (datetime.now() - last_update).total_seconds()
            return age < 5
        except Exception:
            return False
    
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

            # 使用缓存获取规则
            rule = self._get_cached_rule(self.LIMIT_RULE_ID)

            if rule is None:
                self.logger.error(f"未找到规则 {self.LIMIT_RULE_ID}", event="RULE_NOT_FOUND")
                return

            rule_enabled = rule.IsEnabled

            if avg_speed_kb >= self.THRESHOLD_KB and not rule_enabled:
                rule.IsEnabled = True
                self.client.UpdateRule(rule)  # type: ignore
                # 清除缓存，强制下次重新获取以验证
                self._rule_cache.pop(self.LIMIT_RULE_ID, None)
                # 记录规则变更时间，触发冷却
                self._last_rule_change_time = time.time()
                cooldown_end = self._last_rule_change_time + self.COOLDOWN_SECONDS
                self.logger.info(
                    f"Qbit规则 状态变更: 已启用 | 平均速度={avg_speed_kb} KB/s >= 阈值={self.THRESHOLD_KB} KB/s | "
                    f"冷却期至{datetime.fromtimestamp(cooldown_end).strftime('%H:%M:%S')}",
                    event="RULE_ENABLED"
                )
            elif avg_speed_kb < self.THRESHOLD_KB and rule_enabled:
                rule.IsEnabled = False
                self.client.UpdateRule(rule)  # type: ignore
                # 清除缓存，强制下次重新获取以验证
                self._rule_cache.pop(self.LIMIT_RULE_ID, None)
                # 记录规则变更时间，触发冷却
                self._last_rule_change_time = time.time()
                cooldown_end = self._last_rule_change_time + self.COOLDOWN_SECONDS
                self.logger.info(
                    f"Qbit规则 状态变更: 已禁用 | 平均速度={avg_speed_kb} KB/s < 阈值={self.THRESHOLD_KB} KB/s | "
                    f"冷却期至{datetime.fromtimestamp(cooldown_end).strftime('%H:%M:%S')}",
                    event="RULE_DISABLED"
                )

            # 成功执行，重置错误计数
            self._consecutive_errors = 0

        except Exception as e:
            error_msg = str(e)
            self.logger.error(f"Qbit规则检查错误: {e}", event="RULE_CHECK_ERROR", reason=error_msg)
            self._handle_api_error(error_msg, "Qbit规则检查")

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

            # 获取本机最近10秒平均速度进行比较
            local_avg_speed_kb = self._get_local_avg_speed_kb()
            if local_avg_speed_kb is None:
                self.logger.warn("本机速度数据无效，跳过路由器规则检查", event="ROUTER_CHECK_SKIP")
                return

            # 差值计算：路由器速度 - 本机平均速度
            speed_diff = router_speed_kb - local_avg_speed_kb if router_speed_kb is not None else 0

            # 使用缓存获取规则
            rule = self._get_cached_rule(self.ROUTER_RULE_ID)

            if rule is None:
                self.logger.error(f"未找到路由器规则 {self.ROUTER_RULE_ID}", event="ROUTER_RULE_NOT_FOUND")
                return

            rule_enabled = rule.IsEnabled

            # 连续超过阈值指定秒数，启用规则
            if speed_diff > self.THRESHOLD_KB and over_threshold_seconds >= self.ROUTER_CONSECUTIVE_SECONDS and not rule_enabled:
                rule.IsEnabled = True
                self.client.UpdateRule(rule)  # type: ignore
                # 清除缓存，强制下次重新获取以验证
                self._rule_cache.pop(self.ROUTER_RULE_ID, None)
                # 记录规则变更时间，触发冷却
                self._last_router_rule_change_time = time.time()
                cooldown_end = self._last_router_rule_change_time + self.COOLDOWN_SECONDS
                self.logger.info(
                    f"路由器规则 状态变更: 已启用 | 路由器速度={router_speed_kb} KB/s, 本机平均={local_avg_speed_kb} KB/s, "
                    f"差值={speed_diff:.2f} KB/s > 阈值={self.THRESHOLD_KB} KB/s, 连续超阈值={over_threshold_seconds}秒 | "
                    f"冷却期至{datetime.fromtimestamp(cooldown_end).strftime('%H:%M:%S')}",
                    event="ROUTER_RULE_ENABLED"
                )
            elif (speed_diff <= self.THRESHOLD_KB or over_threshold_seconds < self.ROUTER_CONSECUTIVE_SECONDS) and rule_enabled:
                rule.IsEnabled = False
                self.client.UpdateRule(rule)  # type: ignore
                # 清除缓存，强制下次重新获取以验证
                self._rule_cache.pop(self.ROUTER_RULE_ID, None)
                # 记录规则变更时间，触发冷却
                self._last_router_rule_change_time = time.time()
                cooldown_end = self._last_router_rule_change_time + self.COOLDOWN_SECONDS
                self.logger.info(
                    f"路由器规则 状态变更: 已禁用 | 路由器速度={router_speed_kb} KB/s, 本机平均={local_avg_speed_kb} KB/s, "
                    f"差值={speed_diff:.2f} KB/s <= 阈值={self.THRESHOLD_KB} KB/s 或 连续超阈值={over_threshold_seconds}秒 < 要求={self.ROUTER_CONSECUTIVE_SECONDS}秒 | "
                    f"冷却期至{datetime.fromtimestamp(cooldown_end).strftime('%H:%M:%S')}",
                    event="ROUTER_RULE_DISABLED"
                )

            # 成功执行，重置错误计数
            self._consecutive_errors = 0

        except Exception as e:
            error_msg = str(e)
            self.logger.error(f"路由器规则检查错误: {e}", event="ROUTER_RULE_CHECK_ERROR", reason=error_msg)
            self._handle_api_error(error_msg, "路由器规则检查")
    
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
            if router_data is not None and router_data.get("LastUpdate"):
                try:
                    last_update = datetime.fromisoformat(router_data["LastUpdate"])
                    age = (datetime.now() - last_update).total_seconds()
                    if age < 5:
                        self.logger.info("路由器速度采样器数据已就绪", event="ROUTER_SAMPLER_READY")
                        return True
                except Exception:
                    pass
            
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
        
        # 等待速度采样器数据就绪
        if not self._wait_for_speed_sampler():
            self.logger.error("速度采样器数据不可用，退出", event="SPEED_SAMPLER_UNAVAILABLE")
            sys.exit(1)
        
        # 等待路由器速度采样器数据就绪
        if not self._wait_for_router_sampler():
            self.logger.warn("路由器速度采样器数据不可用，继续运行", event="ROUTER_SAMPLER_UNAVAILABLE")
        
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
                if current_time - last_router_check >= router_effective_interval:
                    try:
                        router_data = self._get_router_data()
                        if router_data is not None:
                            self._check_router_rule(router_data)
                    except Exception as e:
                        self.logger.error(f"路由器检查链路异常: {e}", event="ROUTER_CHECK_ERROR")
                    finally:
                        last_router_check = current_time
                
                # 高精度短休眠，避免CPU占用过高
                time.sleep(0.5)
                
            except Exception as e:
                self.logger.error(f"主循环异常: {e}", event="MAIN_LOOP_ERROR")
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
    args = parser.parse_args()
    
    checker = RuleChecker()
    checker.run()


if __name__ == "__main__":
    main()