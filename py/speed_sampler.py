"""
速度采样器 - Python 版本

核心改进：
- 使用独立心跳线程，即使 NetLimiter API 阻塞也不影响心跳更新
- 与 PowerShell 版本的心跳文件格式完全兼容
- 与 PowerShell 版本的共享数据文件格式完全兼容
"""

import sys
import os
import time
import json
import signal
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Optional

try:
    from filelock import FileLock, Timeout
except ImportError:
    print("需要安装 filelock: pip install filelock")
    sys.exit(1)

# 添加父目录到路径，以便导入 common 模块
sys.path.insert(0, str(Path(__file__).parent))

from common.heartbeat import HeartbeatManager
from common.logger import Logger

# 尝试导入 NetLimiter API
NETLIMITER_DLL_PATH = r"C:\Program Files\Locktime Software\NetLimiter\NetLimiter.dll"
try:
    import clr
    # 使用显式 DLL 路径加载（与 PowerShell 版本一致）
    clr.AddReference(NETLIMITER_DLL_PATH)  # type: ignore
    from NetLimiter.Service import NLClient  # type: ignore
    NETLIMITER_AVAILABLE = True
except Exception as e:
    print(f"警告: 无法加载 NetLimiter API: {e}")
    print(f"DLL 路径: {NETLIMITER_DLL_PATH}")
    NETLIMITER_AVAILABLE = False


class SpeedSampler:
    """速度采样器 - 使用独立心跳线程"""

    # 配置常量
    PRIV_INTERNET_FILTER_ID = 44  # Private Internet 过滤器 ID
    HISTORY_SECONDS = 4           # 历史窗口大小（样本数）
    SAMPLE_INTERVAL = 5.0         # 采样间隔（秒）
    MAX_RETRIES = 5               # 最大重试次数
    RETRY_INTERVAL = 10           # 重试间隔（秒）
    MAX_CONSECUTIVE_ERRORS = 10   # 连续错误最大次数，超过则退出

    def __init__(self):
        """初始化速度采样器"""
        self.heartbeat = HeartbeatManager("speed_sampler")
        self.logger = Logger("speed_sampler")

        # NetLimiter 客户端
        self.cli = None
        self.node_loader = None
        self.filter_node = None  # 缓存过滤器节点引用


        # 采样数据
        self.history = deque(maxlen=self.HISTORY_SECONDS)
        self.sample_count = 0
        self.avg_speed_kb = 0.0
        self.previous_out: Optional[int] = None
        self.previous_sample_ts: Optional[float] = None

        # 共享数据文件路径（与 PowerShell 版本兼容）
        self.data_file = Path(os.environ.get('TEMP', '.')) / "qb_speed_data.json"
        self.lock_file_path = Path(os.environ.get('TEMP', '.')) / "qb_speed_data.lock"
        self._file_lock = FileLock(str(self.lock_file_path), timeout=5)

        # 运行标志
        self.running = True

        # 错误计数器
        self._consecutive_errors = 0
        self._last_error_type = None

        # 设置信号处理
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """信号处理器"""
        self.logger.info("收到停止信号，正在关闭...", event="SIGNAL", reason=str(signum))
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
        ]
        error_lower = error_msg.lower()
        return any(kw.lower() in error_lower for kw in connection_error_keywords)

    def _cleanup_connection(self):
        """清理现有连接资源"""
        try:
            if self.cli:
                self.cli.Close()
        except Exception:
            pass
        self.cli = None
        self.node_loader = None
        self.filter_node = None

    def _reconnect(self) -> bool:
        """重新连接 NetLimiter API"""
        self.logger.info("正在尝试重新连接 NetLimiter...", event="RECONNECT_START")
        self.heartbeat.update_status("RECONNECTING", "reconnecting")

        # 清理旧连接
        self._cleanup_connection()

        # 等待 NetLimiter 服务恢复
        time.sleep(2)

        # 尝试重新连接
        retry_count = 0
        while retry_count < self.MAX_RETRIES and self.running:
            try:
                self.cli = NLClient()
                self.cli.Connect()
                self.node_loader = self.cli.CreateNodeLoader()  # type: ignore
                self.node_loader.Filters.SelectAll()  # type: ignore
                self.node_loader.Load()  # type: ignore

                # 重新查找过滤器
                self.filter_node = None
                for node in self.node_loader.Filters.Nodes:  # type: ignore
                    if node.FilterId == self.PRIV_INTERNET_FILTER_ID:
                        self.filter_node = node
                        break

                if self.filter_node:
                    # 重置基准值
                    self.previous_out = self.filter_node.Transferred.Out
                    self.previous_sample_ts = time.time()
                    self.logger.info(
                        f"重新连接成功，过滤器已缓存 (ID={self.PRIV_INTERNET_FILTER_ID})",
                        event="RECONNECT_SUCCESS"
                    )
                    self.heartbeat.update_status("OK")
                    # 重置错误计数
                    self._consecutive_errors = 0
                    return True
                else:
                    self.logger.warn("重新连接成功但未找到过滤器", event="RECONNECT_NO_FILTER")
                    return False

            except Exception as e:
                retry_count += 1
                self.logger.error(
                    f"重连失败 (尝试 {retry_count}/{self.MAX_RETRIES}): {e}",
                    event="RECONNECT_FAILED", reason=str(e)
                )
                if retry_count < self.MAX_RETRIES:
                    time.sleep(self.RETRY_INTERVAL)

        return False

    def connect(self) -> bool:
        """连接 NetLimiter API"""
        if not NETLIMITER_AVAILABLE:
            self.logger.error("NetLimiter API 不可用", event="API_UNAVAILABLE")
            return False
        
        self.heartbeat.update_status("STARTING", "connecting_nl_client")
        
        retry_count = 0
        while retry_count < self.MAX_RETRIES and self.running:
            try:
                # 创建客户端并连接
                self.cli = NLClient()
                self.cli.Connect()
                self.node_loader = self.cli.CreateNodeLoader()  # type: ignore
                self.node_loader.Filters.SelectAll()  # type: ignore

                # 预加载并缓存过滤器节点
                self.node_loader.Load()  # type: ignore
                for node in self.node_loader.Filters.Nodes:  # type: ignore
                    if node.FilterId == self.PRIV_INTERNET_FILTER_ID:
                        self.filter_node = node
                        break

                if self.filter_node:
                    self.logger.info(f"API 已连接，过滤器节点已缓存 (ID={self.PRIV_INTERNET_FILTER_ID})", event="API_CONNECTED")
                else:
                    self.logger.warn(f"API 已连接，但未找到过滤器 (ID={self.PRIV_INTERNET_FILTER_ID})", event="FILTER_NOT_FOUND")

                self.heartbeat.update_status("OK")
                return True
                
            except Exception as e:
                retry_count += 1
                self.logger.error(f"连接失败 (尝试 {retry_count}/{self.MAX_RETRIES}): {e}", 
                                  event="API_CONNECT_FAILED", reason=str(e))
                
                if retry_count < self.MAX_RETRIES:
                    self.heartbeat.update_status("RECONNECTING", f"retry_{retry_count}")
                    time.sleep(self.RETRY_INTERVAL)
        
        return False
    
    def sample(self) -> None:
        """执行一次采样，更新内部状态并保存数据"""
        if not self.node_loader or not self.filter_node:
            return None

        try:
            # 加载节点数据（这里可能阻塞，但心跳线程独立运行）
            self.node_loader.Load()  # type: ignore

            # 获取当前传输字节数（累计值）
            current_out = self.filter_node.Transferred.Out
            current_ts = time.time()

            if self.previous_out is not None:
                delta = current_out - self.previous_out
                # 计算实际时间间隔（秒）
                interval = current_ts - self.previous_sample_ts if self.previous_sample_ts else self.SAMPLE_INTERVAL
                if interval <= 0:
                    interval = self.SAMPLE_INTERVAL
                # 计算速度：字节数 / 时间间隔 / 1024 = KB/s
                speed_kb = round(delta / interval / 1024, 2)

                # 过滤异常数据：速度必须为非负且小于 1GB/s
                if delta >= 0 and speed_kb < 1048576:
                    self.history.append(speed_kb)
                    self.sample_count += 1
                    # 计算最近10秒平均速度
                    self.avg_speed_kb = 0.0
                    if len(self.history) > 0:
                        valid_values = [v for v in self.history if 0 <= v < 1048576]  # 1GB/s上限
                        if valid_values:
                            self.avg_speed_kb = round(sum(valid_values) / len(valid_values), 2)
                    # 计算与上次采样的间隔
                    interval_str = ""
                    if self.previous_sample_ts is not None:
                        interval = current_ts - self.previous_sample_ts
                        interval_str = f", 间隔={interval:.1f}s"
                    self.logger.info(f"采样 #{self.sample_count}: 速度={speed_kb}KB/s, 4样本平均={self.avg_speed_kb}KB/s, 队列={len(self.history)}{interval_str}")
                    self._save_data()
                else:
                    self.logger.warn(f"采样异常: 速度={speed_kb}KB/s (已忽略)", event="SAMPLE_ANOMALY")
            else:
                # 第一次采样，记录基准值，速度为0
                self.history.append(0)
                self.sample_count += 1
                self.logger.info("首次采样: 速度=0 KB/s (基准值初始化)", event="FIRST_SAMPLE")
                self._save_data()

            # 每次采样都更新基准
            self.previous_out = current_out
            self.previous_sample_ts = current_ts
            self.heartbeat.increment_loop()
            # 重置连续错误计数
            self._consecutive_errors = 0

        except Exception as e:
            error_msg = str(e)
            self.logger.error(f"采样错误: {e}", event="SAMPLE_ERROR", reason=error_msg)

            # 检测是否是连接错误
            if self._is_connection_error(error_msg):
                self._consecutive_errors += 1
                self.logger.warn(
                    f"检测到连接错误，连续错误 {self._consecutive_errors}/{self.MAX_CONSECUTIVE_ERRORS}",
                    event="CONNECTION_ERROR_DETECTED"
                )

                # 尝试自动重连
                if self._consecutive_errors < self.MAX_CONSECUTIVE_ERRORS:
                    if self._reconnect():
                        self.logger.info("自动重连成功，继续采样", event="AUTO_RECONNECT_SUCCESS")
                    else:
                        self.logger.error("自动重连失败，将在下次采样时重试", event="AUTO_RECONNECT_FAILED")
                else:
                    # 连续错误过多，退出让 supervisor 重启
                    self.logger.error(
                        f"连续错误 {self.MAX_CONSECUTIVE_ERRORS} 次，退出进程",
                        event="MAX_ERRORS_REACHED"
                    )
                    self.running = False
            else:
                # 非连接错误，增加计数但不触发重连
                self._consecutive_errors += 1
                if self._consecutive_errors >= self.MAX_CONSECUTIVE_ERRORS:
                    self.logger.error(
                        f"连续错误 {self.MAX_CONSECUTIVE_ERRORS} 次，退出进程",
                        event="MAX_ERRORS_REACHED"
                    )
                    self.running = False

        return None
    
    def _save_data(self):
        """保存数据到共享文件（使用 filelock 实现真正的进程间互斥）"""

        data = {
            "SampleCount": self.sample_count,
            "History": list(self.history),
            "AvgSpeedKB": self.avg_speed_kb,
            "LastUpdate": datetime.now().isoformat()
        }

        try:
            # 使用 filelock 获取跨进程锁
            with self._file_lock:
                # 原子写入
                tmp_file = str(self.data_file) + ".tmp"
                with open(tmp_file, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False)
                os.replace(tmp_file, self.data_file)

        except Timeout:
            self.logger.error("获取文件锁超时", event="SAVE_LOCK_TIMEOUT")
        except Exception as e:
            self.logger.error(f"保存数据失败: {e}", event="SAVE_ERROR", reason=str(e))
    
    def _is_admin(self) -> bool:
        """检查是否以管理员权限运行"""
        try:
            import ctypes
            return ctypes.windll.shell32.IsUserAnAdmin()
        except Exception:
            return False
    
    def _check_parent_alive(self) -> bool:
        """检查父进程是否存活"""
        supervisor_pid = os.environ.get("SUPERVISOR_PID")
        if not supervisor_pid:
            return True  # 非服务模式，不检测
        
        try:
            os.kill(int(supervisor_pid), 0)
            return True
        except (OSError, ProcessLookupError):
            return False
    
    def run(self):
        """主循环"""
        # 管理员权限检查
        if not self._is_admin():
            self.logger.error("需要管理员权限，退出", event="ADMIN_REQUIRED")
            sys.exit(1)
        
        self.logger.info("速度采样器启动")
        self.logger.info(f"采集间隔: {self.SAMPLE_INTERVAL}秒, 历史样本: {self.HISTORY_SECONDS}个")
        self.logger.info(f"数据文件: {self.data_file}")
        
        # 连接 NetLimiter
        if not self.connect():
            self.logger.error("连接失败，退出", event="STARTUP_FAILED")
            self.heartbeat.update_status("ERROR", "connection_failed")
            sys.exit(1)
        
        # 初始化首次采样
        try:
            self.node_loader.Load()  # type: ignore
            for node in self.node_loader.Filters.Nodes:  # type: ignore
                if node.FilterId == self.PRIV_INTERNET_FILTER_ID:
                    self.previous_out = node.Transferred.Out
                    self.previous_sample_ts = time.time()
                    self.logger.info(
                        f"首次采样，初始化基线: 过滤器={self.PRIV_INTERNET_FILTER_ID}, Out={self.previous_out}B",
                        event="INIT_BASELINE"
                    )
                    break
        except Exception as e:
            self.logger.error(f"初始化失败: {e}", event="INIT_FAILED")
        
        self.heartbeat.update_status("OK")
        
        # 主循环
        last_sample_time = time.time()
        
        while self.running:
            # 检测父进程是否存活
            if not self._check_parent_alive():
                self.logger.info("Supervisor 已退出，正在关闭...", event="PARENT_EXIT")
                break
            
            # 精确计时
            now = time.time()
            elapsed = now - last_sample_time
            sleep_time = max(0, self.SAMPLE_INTERVAL - elapsed)
            
            if sleep_time > 0:
                time.sleep(sleep_time)
            
            # 先更新时间点（基于固定间隔），再执行采样
            last_sample_time += self.SAMPLE_INTERVAL
            self.sample()
        
        # 清理
        self.logger.info("速度采样器停止")
        self.heartbeat.stop()


def main():
    """主入口"""
    sampler = SpeedSampler()
    sampler.run()


if __name__ == "__main__":
    main()
