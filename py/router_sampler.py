#!/usr/bin/env python3
"""
路由器速度采样器 - Python 版本

使用独立心跳线程解决 SSH 阻塞导致心跳无法更新的问题。
通过 paramiko 库进行 SSH 连接，比调用 ssh.exe 更可靠。
新增采集 NetLimiter Internet 区域（InternalId=2）速度，与路由器端对比，区域完全独立。
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
from collections import deque

try:
    import paramiko
except ImportError:
    print("需要安装 paramiko: pip install paramiko")
    sys.exit(1)

# NetLimiter API 支持
NETLIMITER_DLL_PATH = r"C:\Program Files\Locktime Software\NetLimiter\NetLimiter.dll"
try:
    import clr
    clr.AddReference(NETLIMITER_DLL_PATH)  # type: ignore
    from NetLimiter.Service import NLClient  # type: ignore
    NETLIMITER_AVAILABLE = True
except Exception as e:
    print(f"无法加载 NetLimiter API: {e}")
    print(f"DLL 路径: {NETLIMITER_DLL_PATH}")
    NETLIMITER_AVAILABLE = False

# 添加当前目录到路径，以便导入 common 模块
sys.path.insert(0, str(Path(__file__).parent))

from common.heartbeat import HeartbeatManager
from common.logger import Logger


class RouterSpeedSampler:
    """路由器速度采样器"""
    
    # 配置常量
    ROUTER_USER = "xxhhlk"
    ROUTER_HOST = "192.168.2.1"
    ROUTER_PORT = 14033
    ROUTER_SCRIPT = "/tmp/mnt/Test/entware/root/speed_num_only.sh"
    SSH_KEY_PATH = Path.home() / ".ssh" / "id_rsa"
    THRESHOLD_KB = 800
    CONSECUTIVE_SECONDS = 3
    INTERNET_FILTER_ID = 2  # Internet 区域 InternalId
    HISTORY_SECONDS = 10    # Internet 区域历史窗口大小（样本数）
    MAX_CONSECUTIVE_NL_ERRORS = 10  # NetLimiter 连续错误最大次数

    # 数据文件路径
    ROUTER_DATA_FILE = Path(os.environ.get("TEMP", ".")) / "router_speed_data.json"
    
    def __init__(self):
        self.logger = Logger("router_speed_sampler")
        self.heartbeat = HeartbeatManager("router_speed_sampler")
        
        # 状态变量
        self.over_threshold_count = 0
        self.sample_count = 0
        self.prev_router_speed_kb: Optional[float] = None
        self.last_sample_time: Optional[float] = None
        self.ssh_consecutive_failures = 0
        self.nl_consecutive_errors = 0
        self.running = True
        
        # SSH 客户端
        self.ssh_client: Optional[paramiko.SSHClient] = None
        self.ssh_channel: Optional[paramiko.Channel] = None

        # NetLimiter 客户端
        self.nl_client: Optional[NLClient] = None
        self.nl_node_loader = None
        self.internet_history = deque(maxlen=self.HISTORY_SECONDS)
        self.previous_internet_out: Optional[int] = None
        self.previous_nl_sample_ts: Optional[float] = None
        
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
        self._cleanup_ssh()
        self._cleanup_nl()
    
    def _cleanup_ssh(self):
        """清理 SSH 连接"""
        if self.ssh_channel:
            try:
                self.ssh_channel.close()
            except Exception:
                pass
            self.ssh_channel = None
        
        if self.ssh_client:
            try:
                self.ssh_client.close()
            except Exception:
                pass
            self.ssh_client = None
    
    def _cleanup_nl(self):
        """清理 NetLimiter 连接"""
        if self.nl_client:
            try:
                self.nl_client.Close()
            except Exception:
                pass
            self.nl_client = None
        self.nl_node_loader = None

    def _is_nl_connection_error(self, error_msg: str) -> bool:
        """检测是否是 NetLimiter 连接错误"""
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

    def _connect_nl(self) -> bool:
        """连接 NetLimiter API 采集 Internet 区域速度"""
        if not NETLIMITER_AVAILABLE:
            self.logger.error("NetLimiter API 不可用，无法采集 Internet 区域速度", event="NL_UNAVAILABLE")
            return False
        
        try:
            self._cleanup_nl()
            self.nl_client = NLClient()
            self.nl_client.Connect()  # type: ignore
            self.nl_node_loader = self.nl_client.CreateNodeLoader()  # type: ignore
            self.nl_node_loader.Filters.SelectAll()  # type: ignore
            
            # 初始化基线
            self.nl_node_loader.Load()  # type: ignore
            for node in self.nl_node_loader.Filters.Nodes:  # type: ignore
                if node.FilterId == self.INTERNET_FILTER_ID:
                    self.previous_internet_out = node.Transferred.Out
                    self.previous_nl_sample_ts = time.time()
                    self.logger.info(f"Internet 区域(InternalId={self.INTERNET_FILTER_ID}) 基线初始化完成，Out={self.previous_internet_out}B", event="NL_INIT_OK")
                    return True
            
            self.logger.error(f"未找到 Internet 区域过滤器 (InternalId={self.INTERNET_FILTER_ID})", event="NL_FILTER_NOT_FOUND")
            return False
            
        except Exception as e:
            self.logger.error(f"NetLimiter 连接失败: {e}", event="NL_CONNECT_FAILED", reason=str(e))
            self._cleanup_nl()
            return False
    
    def _sample_internet_speed(self) -> Optional[float]:
        """采集 Internet 区域最近10秒平均速度（KB/s）"""
        if not self.nl_node_loader or self.nl_client is None:
            if not self._connect_nl():
                return None
        
        try:
            self.nl_node_loader.Load()  # type: ignore
            filter_node = None
            for node in self.nl_node_loader.Filters.Nodes:  # type: ignore
                if node.FilterId == self.INTERNET_FILTER_ID:
                    filter_node = node
                    break
            
            if not filter_node:
                self.logger.warn("未找到 Internet 区域过滤器", event="NL_FILTER_NOT_FOUND")
                return None

            current_out = filter_node.Transferred.Out
            current_ts = time.time()

            if self.previous_internet_out is not None:
                delta = current_out - self.previous_internet_out
                speed_kb = round(delta / 1024, 2)

                if delta >= 0 and speed_kb < 1048576:
                    self.internet_history.append(delta)
                else:
                    self.logger.warn(f"Internet 区域采样异常: 速度={speed_kb}KB/s (已忽略)", event="NL_SAMPLE_ANOMALY")

            self.previous_internet_out = current_out
            self.previous_nl_sample_ts = current_ts

            # 计算最近10秒平均
            result = 0.0
            if len(self.internet_history) > 0:
                valid_values = [v for v in self.internet_history if 0 <= v < 1073741824]
                if valid_values:
                    result = round(sum(valid_values) / len(valid_values) / 1024, 2)

            # 成功执行，重置错误计数
            self.nl_consecutive_errors = 0
            return result

        except Exception as e:
            error_msg = str(e)
            self.logger.error(f"Internet 区域采样失败: {e}", event="NL_SAMPLE_ERROR", reason=error_msg)

            # 检测是否是连接错误
            if self._is_nl_connection_error(error_msg):
                self.nl_consecutive_errors += 1
                self.logger.warn(
                    f"检测到 NetLimiter 连接错误，连续错误 {self.nl_consecutive_errors}/{self.MAX_CONSECUTIVE_NL_ERRORS}",
                    event="NL_CONNECTION_ERROR_DETECTED"
                )

                if self.nl_consecutive_errors >= self.MAX_CONSECUTIVE_NL_ERRORS:
                    self.logger.error(
                        f"NetLimiter 连续错误 {self.MAX_CONSECUTIVE_NL_ERRORS} 次，退出进程",
                        event="NL_MAX_ERRORS_REACHED"
                    )
                    self.running = False
                    return None

                # 清理并尝试重连
                self._cleanup_nl()
                if self._connect_nl():
                    self.logger.info("NetLimiter 自动重连成功", event="NL_AUTO_RECONNECT_SUCCESS")
                    self.nl_consecutive_errors = 0
                else:
                    self.logger.error("NetLimiter 自动重连失败", event="NL_AUTO_RECONNECT_FAILED")
            else:
                # 非连接错误，增加计数
                self.nl_consecutive_errors += 1
                self._cleanup_nl()
                if self.nl_consecutive_errors >= self.MAX_CONSECUTIVE_NL_ERRORS:
                    self.logger.error(
                        f"NetLimiter 连续错误 {self.MAX_CONSECUTIVE_NL_ERRORS} 次，退出进程",
                        event="NL_MAX_ERRORS_REACHED"
                    )
                    self.running = False

            return None

    def _check_parent_alive(self) -> bool:
        """检查父进程是否存活"""
        if not self.supervisor_pid:
            return True
        
        try:
            os.kill(int(self.supervisor_pid), 0)
            return True
        except (OSError, ProcessLookupError):
            return False
    
    def _connect_ssh(self) -> bool:
        """建立 SSH 连接"""
        try:
            self._cleanup_ssh()
            
            self.logger.info(f"正在连接到路由器 {self.ROUTER_HOST}...", event="SSH_CONNECTING")
            
            # 创建 SSH 客户端
            self.ssh_client = paramiko.SSHClient()
            self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            # 加载私钥
            key = paramiko.RSAKey.from_private_key_file(str(self.SSH_KEY_PATH))
            
            # 连接
            self.ssh_client.connect(
                hostname=self.ROUTER_HOST,
                port=self.ROUTER_PORT,
                username=self.ROUTER_USER,
                pkey=key,
                timeout=10,
                banner_timeout=10,
                auth_timeout=10
            )
            
            # 启动长时间运行的脚本
            transport = self.ssh_client.get_transport()
            if transport is None:
                self.logger.error("获取 SSH transport 失败", event="SSH_TRANSPORT_ERROR")
                return False
            
            self.ssh_channel = transport.open_session()
            self.ssh_channel.exec_command(self.ROUTER_SCRIPT)
            self.ssh_channel.settimeout(5.0)  # 设置读取超时
            
            self.logger.info("SSH 连接成功", event="SSH_CONNECTED")
            self.ssh_consecutive_failures = 0
            return True
            
        except Exception as e:
            self.logger.error(f"SSH 连接失败: {e}", event="SSH_CONNECT_FAILED", reason=str(e))
            self.ssh_consecutive_failures += 1
            self._cleanup_ssh()
            return False
    
    def _read_router_speed(self) -> Optional[float]:
        """读取路由器速度（非阻塞）"""
        if not self.ssh_channel:
            return None
        
        try:
            # 非阻塞读取
            if self.ssh_channel.recv_ready():
                data = b""
                while self.ssh_channel.recv_ready():
                    chunk = self.ssh_channel.recv(1024)
                    if not chunk:
                        break
                    data += chunk
                
                # 解析速度值
                text = data.decode("utf-8", errors="ignore").strip()
                lines = text.strip().split("\n")
                
                for line in reversed(lines):  # 取最后一行
                    line = line.strip()
                    if line:
                        try:
                            # 脚本输出的是 KB/s
                            speed_kb = float(line)
                            return speed_kb
                        except ValueError:
                            continue
            
            return None
            
        except paramiko.SSHException as e:
            self.logger.error(f"SSH 读取错误: {e}", event="SSH_READ_ERROR", reason=str(e))
            return None
        except Exception as e:
            self.logger.error(f"读取路由器速度异常: {e}", event="READ_ERROR", reason=str(e))
            return None
    
    def _compare_speeds(
        self,
        router_speed: Optional[float],
        local_speed: Optional[float]
    ) -> Dict[str, Any]:
        """比较路由器速度与本机速度，返回对比结果"""

        if router_speed is None:
            return {"RouterSpeed": None, "LocalSpeed": None, "Diff": None, "Status": "无路由器数据"}

        if local_speed is None:
            return {"RouterSpeed": router_speed, "LocalSpeed": None, "Diff": None, "Status": "无本机速度数据"}

        speed_diff = router_speed - local_speed
        return {
            "RouterSpeed": router_speed,
            "LocalSpeed": local_speed,
            "Diff": speed_diff,
            "Status": "实时对比"
        }
    
    def _save_router_data(
        self, 
        router_speed_kb: float, 
        local_speed_kb: Optional[float], 
        over_threshold_seconds: int
    ):
        """保存路由器数据"""
        try:
            data = {
                "RouterSpeedKB": router_speed_kb,
                "LocalSpeedKB": local_speed_kb,
                "OverThresholdSeconds": over_threshold_seconds,
                "LastUpdate": datetime.now().isoformat()
            }
            
            # 原子写入
            tmp_file = self.ROUTER_DATA_FILE.with_suffix(".tmp")
            tmp_file.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
            tmp_file.replace(self.ROUTER_DATA_FILE)
            
        except Exception as e:
            self.logger.error(f"保存路由器数据失败: {e}", event="SAVE_ERROR", reason=str(e))
    
    def _wait_for_internet_data(self) -> bool:
        """等待 Internet 区域数据就绪"""
        wait_count = 0
        max_wait = 30
        
        self.logger.info("等待 Internet 区域采样数据...")
        
        while wait_count < max_wait and self.running:
            # 心跳由独立线程维护，无需手动更新
            speed = self._sample_internet_speed()
            if speed is not None:
                self.logger.info(f"Internet 区域数据已就绪: {speed} KB/s", event="NL_DATA_READY")
                return True
            
            time.sleep(1)
            wait_count += 1
        
        self.logger.warn("等待 Internet 区域数据超时，继续运行...", event="NL_DATA_TIMEOUT")
        return False
    
    def _is_admin(self) -> bool:
        """检查是否以管理员权限运行"""
        try:
            import ctypes
            return ctypes.windll.shell32.IsUserAnAdmin()
        except Exception:
            return False
    
    def run(self):
        """主循环"""
        # 管理员权限检查
        if not self._is_admin():
            self.logger.error("需要管理员权限，退出", event="ADMIN_REQUIRED")
            sys.exit(1)
        
        self.logger.info("开始运行", event="START")
        
        # 等待 Internet 区域数据就绪
        self._wait_for_internet_data()
        
        # 初始 SSH 连接
        if not self._connect_ssh():
            self.logger.warn("初始 SSH 连接失败，将在主循环中重试", event="SSH_INITIAL_FAILED")
        
        while self.running:
            try:
                # 检查父进程
                if not self._check_parent_alive():
                    self.logger.info("父进程已退出，准备退出", event="PARENT_EXIT")
                    break
                
                # 读取路由器速度
                router_speed = self._read_router_speed()
                current_time = time.time()
                
                if router_speed is not None:
                    self.sample_count += 1
                    
                    # 采集本机 Internet 区域速度
                    local_internet_speed = self._sample_internet_speed()
                    
                    # 使用当前路由器速度进行实时比较（不延迟）
                    comparison = self._compare_speeds(
                        router_speed,
                        local_internet_speed
                    )
                    
                    # 保存当前路由器速度
                    self.prev_router_speed_kb = router_speed
                    
                    # 检查阈值：差值 > 800KB/s
                    speed_diff = comparison.get("Diff")
                    if speed_diff is not None and speed_diff > self.THRESHOLD_KB:
                        self.over_threshold_count += 1
                    else:
                        self.over_threshold_count = 0
                    
                    # 保存数据
                    self._save_router_data(
                        router_speed,
                        comparison.get("LocalSpeed"),
                        self.over_threshold_count
                    )
                    
                    # 日志输出
                    local_speed_str = f"{comparison.get('LocalSpeed'):.1f}" if comparison.get("LocalSpeed") else "N/A"
                    diff_str = f"{speed_diff:.1f}" if speed_diff is not None else "N/A"
                    # 使用comparison中的RouterSpeed（上一秒的值）来保持一致性
                    displayed_router_speed = comparison.get("RouterSpeed")
                    router_speed_str = f"{displayed_router_speed:.1f}" if displayed_router_speed is not None else "N/A"
                    
                    # 计算与上次采样的间隔
                    interval_str = ""
                    if self.last_sample_time is not None:
                        interval = current_time - self.last_sample_time
                        interval_str = f", 间隔={interval:.1f}s"
                    
                    self.logger.info(
                        f"路由器: {router_speed_str} KB/s, "
                        f"本机: {local_speed_str} KB/s, "
                        f"差值: {diff_str} KB/s, "
                        f"超阈值: {self.over_threshold_count}s{interval_str}",
                        event="SAMPLE"
                    )
                    
                    # 更新上次采样时间
                    self.last_sample_time = current_time
                    
                    # 检查是否需要告警
                    if self.over_threshold_count >= self.CONSECUTIVE_SECONDS:
                        self.logger.alert(
                            "THRESHOLD_EXCEEDED",
                            f"速度差超过阈值 {self.THRESHOLD_KB} KB/s 已达 {self.over_threshold_count} 秒",
                            {"router_speed": router_speed, "local_speed": local_internet_speed, "diff": speed_diff, "threshold": self.THRESHOLD_KB}
                        )
                
                # 检查 SSH 连接状态
                if self.ssh_channel is None or self.ssh_channel.closed:
                    self.logger.warn("SSH 连接已断开，尝试重连...", event="SSH_RECONNECT")
                    self._connect_ssh()
                
            except Exception as e:
                self.logger.error(f"主循环异常: {e}", event="MAIN_LOOP_ERROR", reason=str(e))
                time.sleep(1)

        # 清理
        self._cleanup_ssh()
        self._cleanup_nl()
        self.heartbeat.stop()
        self.logger.info("退出", event="EXIT")


def main():
    parser = argparse.ArgumentParser(description="路由器速度采样器")
    parser.add_argument("--service", action="store_true", help="服务模式运行")
    args = parser.parse_args()

    sampler = RouterSpeedSampler()
    sampler.run()


if __name__ == "__main__":
    main()