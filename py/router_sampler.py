#!/usr/bin/env python3
"""
路由器速度采样器 - Python 版本

使用独立心跳线程解决 SSH 阻塞导致心跳无法更新的问题。
通过 paramiko 库进行 SSH 连接，比调用 ssh.exe 更可靠。
新增采集 NetLimiter Internet 区域（InternalId=2）速度，与路由器端对比，区域完全独立。

方法1补偿协议开销：
- 网卡物理上行速度（psutil）− NetLimiter LAN 区域应用层速度 = 真实互联网上行（含协议开销）
- 修正后 LocalSpeedKB = max(0, NicSpeedKB − LanSpeedKB)
- 原 Internet 应用层速度保留为 LocalAppSpeedKB 供对比
"""

import os
import sys
import json
import time
import socket
import signal
import argparse
import subprocess
import threading
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, List
from collections import deque

try:
    from filelock import FileLock, Timeout
except ImportError:
    print("需要安装 filelock: pip install filelock")
    sys.exit(1)

try:
    import paramiko
except ImportError:
    print("需要安装 paramiko: pip install paramiko")
    sys.exit(1)

try:
    import psutil
except ImportError:
    print("需要安装 psutil: pip install psutil")
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


# ---------------------------------------------------------------------------
# WMI 物理网卡识别
# ---------------------------------------------------------------------------
def discover_physical_nics() -> List[str]:
    r"""WMI 查询 Win32_NetworkAdapter，返回 PNPDeviceID 以 PCI\ 开头且 NetConnectionID 非空的网卡名列表。

    用于过滤 psutil 网卡，只采样真实物理网卡（排除 VirtualBox/Hyper-V/WSL 等虚拟网卡）。
    """
    ps_cmd = (
        "Get-CimInstance Win32_NetworkAdapter | "
        "Where-Object { $_.PhysicalAdapter -eq $true -and $_.NetConnectionID -ne $null -and $_.PNPDeviceID -like 'PCI*' } | "
        "Select-Object NetConnectionID | ConvertTo-Json -Depth 2"
    )
    try:
        result = subprocess.run(
            ["powershell", "-NoProfile", "-Command", ps_cmd],
            capture_output=True, text=True, timeout=15
        )
        if result.returncode != 0 or not result.stdout.strip():
            return []
        adapters = json.loads(result.stdout)
        if isinstance(adapters, dict):
            adapters = [adapters]
        names: List[str] = []
        for a in adapters:
            conn = a.get("NetConnectionID")
            if conn:
                names.append(conn)
        return names
    except Exception:
        return []


class RouterSpeedSampler:
    """路由器速度采样器"""
    
    # 配置常量
    ROUTER_USER = "xxhhlk"
    ROUTER_HOST = "192.168.2.1"
    ROUTER_PORT = 14033
    ROUTER_SCRIPT = "/opt/root/speed_num_only.sh"
    SSH_KEY_PATH = Path.home() / ".ssh" / "id_rsa"
    THRESHOLD_KB = 800
    CONSECUTIVE_SECONDS = 3
    INTERNET_FILTER_ID = 2  # Internet 区域 InternalId
    LAN_FILTER_NAME = "LocalNetwork"  # LAN 区域过滤器名称（自动发现用）
    HISTORY_SECONDS = 10    # 历史窗口大小（样本数，Internet/LAN/网卡共用）
    LAN_PHY_COEFF = 1.03     # LAN 物理层估算系数（LAN应用层 × 此系数 ≈ LAN物理层，扣除局域网协议开销）
    NIC_LINK_COEFF = 1    # 网卡链路层→IP层折算系数（本机网卡统计链路层含封装开销约9%，路由器ppp0统计IP层）
    MAX_CONSECUTIVE_NL_ERRORS = 10  # NetLimiter 连续错误最大次数
    SSH_RECONNECT_BASE_DELAY = 5  # SSH 重连基础延迟（秒）
    SSH_RECONNECT_MAX_DELAY = 60  # SSH 重连最大延迟（秒）
    # 网卡采样降级时排除的虚拟网卡关键字
    NIC_EXCLUDE_KEYWORDS = ("loopback", "vmware", "vethernet", "wsl", "docker",
                            "isatp", "teredo", "tunnel")

    # 数据文件路径
    ROUTER_DATA_FILE = Path(os.environ.get("TEMP", ".")) / "router_speed_data.json"
    ROUTER_LOCK_FILE = Path(os.environ.get("TEMP", ".")) / "router_speed_data.lock"
    MAIN_LOOP_STALL_SECONDS = 60
    
    def __init__(self):
        self.logger = Logger("router_speed_sampler")
        self.heartbeat = HeartbeatManager("router_speed_sampler")
        
        # 状态变量
        self.over_threshold_count = 0
        self.below_threshold_count = 0  # 低于阈值连续秒数（用于禁用判定）
        self.sample_count = 0
        self.prev_router_speed_kb: Optional[float] = None
        self.last_sample_time: Optional[float] = None
        self.ssh_consecutive_failures = 0
        self.nl_consecutive_errors = 0
        self.running = True
        self.last_ssh_reconnect_attempt: Optional[float] = None
        
        # SSH 客户端
        self.ssh_client: Optional[paramiko.SSHClient] = None
        self.ssh_channel: Optional[paramiko.Channel] = None

        # NetLimiter 客户端
        self.nl_client: Optional[NLClient] = None
        self.nl_node_loader = None
        self.internet_history = deque(maxlen=self.HISTORY_SECONDS)
        self.previous_internet_out: Optional[int] = None
        self.previous_nl_sample_ts: Optional[float] = None
        # LAN 区域采样（方法1：扣除局域网流量）
        self.lan_filter_id: Optional[int] = None
        self.lan_history = deque(maxlen=self.HISTORY_SECONDS)
        self.previous_lan_out: Optional[int] = None
        self.previous_lan_ts: Optional[float] = None
        # 网卡物理上行采样（方法1：含协议开销的真实上行）
        self.phys_nics: List[str] = []
        self.nic_history = deque(maxlen=self.HISTORY_SECONDS)
        self.previous_nic_sent: Optional[int] = None
        self.previous_nic_ts: Optional[float] = None
        self._file_lock = FileLock(str(self.ROUTER_LOCK_FILE), timeout=5)
        self._last_main_progress = time.monotonic()
        self._progress_lock = threading.Lock()
        
        # 父进程检测
        self.supervisor_pid = os.environ.get("SUPERVISOR_PID")
        
        # 设置信号处理
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        self.logger.info("初始化完成", event="INIT_OK")
        self._start_watchdog()

    def _touch_main_progress(self):
        """记录主循环仍在推进；watchdog 用它识别主线程卡死。"""
        with self._progress_lock:
            self._last_main_progress = time.monotonic()

    def _start_watchdog(self):
        """监控主循环停摆，避免独立心跳掩盖主线程卡死。"""
        thread = threading.Thread(
            target=self._watchdog_loop,
            daemon=True,
            name="router-main-loop-watchdog"
        )
        thread.start()

    def _watchdog_loop(self):
        while self.running:
            time.sleep(5)
            # 检测父进程是否已退出（独立线程，不受主循环阻塞影响）
            if not self._check_parent_alive():
                self.logger.error(
                    "父进程(supervisor)已退出，watchdog 强制终止本进程",
                    event="WATCHDOG_PARENT_EXIT"
                )
                self.heartbeat.stop()
                os._exit(2)

            with self._progress_lock:
                stalled_seconds = time.monotonic() - self._last_main_progress

            if stalled_seconds > self.MAIN_LOOP_STALL_SECONDS:
                self.logger.error(
                    f"主循环 {stalled_seconds:.1f} 秒无进展，强制退出等待 supervisor 重启",
                    event="WATCHDOG_STALLED",
                    reason=f"{stalled_seconds:.1f}s"
                )
                self.heartbeat.stop()
                os._exit(2)
    
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
        """连接 NetLimiter API 采集 Internet/LAN 区域速度"""
        if not NETLIMITER_AVAILABLE:
            self.logger.error("NetLimiter API 不可用，无法采集区域速度", event="NL_UNAVAILABLE")
            return False

        try:
            self._cleanup_nl()
            self.nl_client = NLClient()
            self.nl_client.Connect()  # type: ignore
            self.nl_node_loader = self.nl_client.CreateNodeLoader()  # type: ignore
            self.nl_node_loader.Filters.SelectAll()  # type: ignore

            # 自动发现 LAN 区域 FilterId（按名称 LocalNetwork）
            self.lan_filter_id = None
            try:
                for f in self.nl_client.Filters:  # type: ignore
                    try:
                        if f.Name and f.Name.lower() == self.LAN_FILTER_NAME.lower():
                            self.lan_filter_id = f.InternalId
                            break
                    except Exception:
                        continue
            except Exception as e:
                self.logger.warn(f"遍历 Filters 发现 LAN 区域失败: {e}", event="NL_LAN_DISCOVER_FAIL")

            if self.lan_filter_id is not None:
                self.logger.info(f"LAN 区域过滤器已发现: Name={self.LAN_FILTER_NAME}, FilterId={self.lan_filter_id}", event="NL_LAN_FOUND")
            else:
                self.logger.warn(f"未发现 LAN 区域过滤器(Name={self.LAN_FILTER_NAME})，LocalSpeedKB 将退回 Internet 应用层速度", event="NL_LAN_NOT_FOUND")

            # 不在此处初始化基线，让第一次采样时设基线（避免 dt 异常小导致首样本失真）
            self.previous_internet_out = None
            self.previous_nl_sample_ts = None
            self.previous_lan_out = None
            self.previous_lan_ts = None
            self.previous_nic_sent = None
            self.previous_nic_ts = None

            self.logger.info(f"NetLimiter 已连接，Internet FilterId={self.INTERNET_FILTER_ID}, LAN FilterId={self.lan_filter_id}", event="NL_INIT_OK")
            return True

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

            if self.previous_internet_out is not None and self.previous_nl_sample_ts is not None:
                delta = current_out - self.previous_internet_out
                # 计算实际时间间隔（秒），避免假设1秒导致误差
                interval = current_ts - self.previous_nl_sample_ts
                if interval <= 0:
                    interval = 1.0
                speed_kb = round(delta / interval / 1024, 2)

                if delta >= 0 and speed_kb < 1048576:
                    self.internet_history.append(speed_kb)
                else:
                    self.logger.warn(f"Internet 区域采样异常: 速度={speed_kb}KB/s (已忽略)", event="NL_SAMPLE_ANOMALY")

            self.previous_internet_out = current_out
            self.previous_nl_sample_ts = current_ts

            # 计算最近10秒平均（history 中已是 KB/s 速度值）
            result = 0.0
            if len(self.internet_history) > 0:
                valid_values = [v for v in self.internet_history if 0 <= v < 1048576]
                if valid_values:
                    result = round(sum(valid_values) / len(valid_values), 2)

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

    def _sample_lan_speed(self) -> Optional[float]:
        """采集 LAN 区域最近10秒平均速度（KB/s）。

        复用 _sample_internet_speed 已 Load 的 node_loader（同一轮调用，时间对齐）。
        平均方式与 _sample_internet_speed 一致：除以实际 dt 得 KB/s，append 速度值，返回 sum/len。
        若 LAN 过滤器未发现，返回 None（降级）。
        """
        if self.lan_filter_id is None:
            return None
        if not self.nl_node_loader or self.nl_client is None:
            return None

        try:
            # node_loader 已在 _sample_internet_speed 里 Load，这里直接读
            filter_node = None
            for node in self.nl_node_loader.Filters.Nodes:  # type: ignore
                if node.FilterId == self.lan_filter_id:
                    filter_node = node
                    break

            if not filter_node:
                self.logger.warn("未找到 LAN 区域过滤器节点", event="NL_LAN_NODE_NOT_FOUND")
                return None

            current_out = filter_node.Transferred.Out
            current_ts = time.time()

            if self.previous_lan_out is not None and self.previous_lan_ts is not None:
                delta = current_out - self.previous_lan_out
                # 计算实际时间间隔（秒）
                interval = current_ts - self.previous_lan_ts
                if interval <= 0:
                    interval = 1.0
                speed_kb = round(delta / interval / 1024, 2)

                if delta >= 0 and speed_kb < 1048576:
                    self.lan_history.append(speed_kb)
                else:
                    self.logger.warn(f"LAN 区域采样异常: 速度={speed_kb}KB/s (已忽略)", event="NL_LAN_SAMPLE_ANOMALY")

            self.previous_lan_out = current_out
            self.previous_lan_ts = current_ts

            # 计算最近10秒平均（history 中已是 KB/s 速度值）
            result = 0.0
            if len(self.lan_history) > 0:
                valid_values = [v for v in self.lan_history if 0 <= v < 1048576]
                if valid_values:
                    result = round(sum(valid_values) / len(valid_values), 2)
            return result

        except Exception as e:
            self.logger.warn(f"LAN 区域采样失败: {e}", event="NL_LAN_SAMPLE_ERROR", reason=str(e))
            return None

    def _sample_nic_speed(self) -> Optional[float]:
        """采集物理网卡总上行最近10秒平均速度（KB/s）。

        用 psutil 按缓存的物理网卡名过滤，求和 bytes_sent，做差得 delta。
        平均方式与 _sample_internet_speed 一致：除以实际 dt 得 KB/s，append 速度值，返回 sum/len。
        若物理网卡列表为空（WMI 失败），降级用名称关键字排除。
        """
        try:
            pernic = psutil.net_io_counters(pernic=True)
            total_sent = 0
            for name in self.phys_nics:
                if name in pernic:
                    total_sent += pernic[name].bytes_sent

            current_ts = time.time()

            if self.previous_nic_sent is not None and self.previous_nic_ts is not None:
                delta = total_sent - self.previous_nic_sent
                # 计算实际时间间隔（秒）
                interval = current_ts - self.previous_nic_ts
                if interval <= 0:
                    interval = 1.0
                speed_kb = round(delta / interval / 1024, 2)

                if delta >= 0 and speed_kb < 1048576:
                    self.nic_history.append(speed_kb)
                else:
                    self.logger.warn(f"网卡采样异常: 速度={speed_kb}KB/s (已忽略)", event="NIC_SAMPLE_ANOMALY")

            self.previous_nic_sent = total_sent
            self.previous_nic_ts = current_ts

            # 计算最近10秒平均（history 中已是 KB/s 速度值）
            result = 0.0
            if len(self.nic_history) > 0:
                valid_values = [v for v in self.nic_history if 0 <= v < 1048576]
                if valid_values:
                    result = round(sum(valid_values) / len(valid_values), 2)
            return result

        except Exception as e:
            self.logger.warn(f"网卡采样失败: {e}", event="NIC_SAMPLE_ERROR", reason=str(e))
            return None

    def _discover_phys_nics(self):
        """启动时识别物理网卡并缓存网卡名列表。WMI 失败时降级用名称关键字排除。"""
        self.phys_nics = discover_physical_nics()
        if self.phys_nics:
            self.logger.info(f"物理网卡识别成功(WMI): {self.phys_nics}", event="NIC_DISCOVER_OK")
        else:
            self.logger.warn("WMI 未找到物理网卡，降级用名称关键字排除", event="NIC_DISCOVER_FALLBACK")
            for name in psutil.net_io_counters(pernic=True):
                if not any(kw in name.lower() for kw in self.NIC_EXCLUDE_KEYWORDS):
                    self.phys_nics.append(name)
            self.logger.info(f"物理网卡(降级): {self.phys_nics}", event="NIC_DISCOVER_FALLBACK_OK")

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
    
    def _connect_ssh(self) -> bool:
        """建立 SSH 连接"""
        self.last_ssh_reconnect_attempt = time.time()

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
            self.ssh_channel.settimeout(1.0)  # 读取超时 1 秒，保证及时响应
            
            self.logger.info("SSH 连接成功", event="SSH_CONNECTED")
            self.ssh_consecutive_failures = 0
            return True
            
        except Exception as e:
            self.logger.error(f"SSH 连接失败: {e}", event="SSH_CONNECT_FAILED", reason=str(e))
            self.ssh_consecutive_failures += 1
            self._cleanup_ssh()
            return False
    
    def _read_router_speed(self) -> Optional[float]:
        """
        读取路由器速度（阻塞带超时）

        使用 channel 的超时设置（1秒），有数据立即返回，无数据阻塞等待。

        Returns:
            速度值（KB/s），无数据或超时返回 None
        """
        if not self.ssh_channel:
            return None

        try:
            data = b""

            # 阻塞等待数据，有数据立即返回
            try:
                chunk = self.ssh_channel.recv(1024)
                if not chunk:
                    # 连接已关闭
                    return None
                data += chunk

                # 如果还有数据待读，继续读取（非阻塞）
                while self.ssh_channel.recv_ready():
                    chunk = self.ssh_channel.recv(1024)
                    if not chunk:
                        break
                    data += chunk
            except socket.timeout:
                # 超时是正常的，表示没有新数据，继续检查是否有已读取的数据
                pass
            except Exception as e:
                # 其他错误可能是连接断开
                self.logger.warn(f"SSH 读取异常: {e}", event="SSH_READ_WARN")
                return None

            if not data:
                return None

            # 解析速度值
            text = data.decode("utf-8", errors="ignore").strip()
            lines = text.strip().split("\n")

            for line in reversed(lines):  # 取最后一行
                line = line.strip()
                if line:
                    try:
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
        local_app_speed_kb: Optional[float],
        nic_speed_kb: Optional[float],
        lan_speed_kb: Optional[float],
        over_threshold_seconds: int,
        below_threshold_seconds: int
    ):
        """保存路由器数据

        Args:
            router_speed_kb: 路由器实测上传速度
            local_speed_kb: 修正后本机互联网上行（网卡−LAN，含协议开销），供 rule_checker 使用
            local_app_speed_kb: 原 Internet 应用层速度（参考）
            nic_speed_kb: 网卡物理上行速度（参考）
            lan_speed_kb: LAN 应用层速度（参考）
        """
        try:
            data = {
                "RouterSpeedKB": router_speed_kb,
                "LocalSpeedKB": local_speed_kb,
                "LocalAppSpeedKB": local_app_speed_kb,
                "NicSpeedKB": nic_speed_kb,
                "LanSpeedKB": lan_speed_kb,
                "OverThresholdSeconds": over_threshold_seconds,
                "BelowThresholdSeconds": below_threshold_seconds,
                "LastUpdate": datetime.now().isoformat()
            }
            
            # 使用跨进程锁，避免 Windows 上 reader/writer 抢占导致 PermissionError。
            with self._file_lock:
                tmp_file = Path(f"{self.ROUTER_DATA_FILE}.{os.getpid()}.tmp")
                tmp_file.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
                os.replace(tmp_file, self.ROUTER_DATA_FILE)
            
        except Timeout:
            self.logger.error("获取路由器数据文件锁超时", event="SAVE_LOCK_TIMEOUT")
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
        
        # 识别物理网卡（方法1：网卡物理上行采样）
        self._discover_phys_nics()

        # 等待 Internet 区域数据就绪
        self._wait_for_internet_data()
        
        # 初始 SSH 连接
        if not self._connect_ssh():
            self.logger.warn("初始 SSH 连接失败，将在主循环中重试", event="SSH_INITIAL_FAILED")
        
        while self.running:
            try:
                self._touch_main_progress()
                self.heartbeat.increment_loop()

                # 检查父进程
                if not self._check_parent_alive():
                    self.logger.info("父进程已退出，准备退出", event="PARENT_EXIT")
                    break
                
                # 读取路由器速度
                router_speed = self._read_router_speed()
                if not self.running:
                    break
                self._touch_main_progress()

                current_time = time.time()
                
                if router_speed is not None:
                    self.sample_count += 1

                    # 方法1：依次连续采样网卡→Internet→LAN，保证时间对齐
                    # 网卡物理上行（含协议开销）
                    nic_speed = self._sample_nic_speed()
                    self._touch_main_progress()
                    # Internet 区域应用层速度（原 LocalSpeedKB，现作为参考 LocalAppSpeedKB）
                    local_app_speed = self._sample_internet_speed()
                    self._touch_main_progress()
                    # LAN 区域应用层速度（需扣除的局域网流量）
                    lan_speed = self._sample_lan_speed()
                    self._touch_main_progress()

                    # 计算修正后本机互联网上行速度
                    # 修正值 = max(0, 网卡物理上行 − LAN应用层)
                    # 若网卡或 LAN 采样失败，降级用 Internet 应用层速度
                    if nic_speed is not None and lan_speed is not None:
                        # 改进方案：减去 LAN 物理层估算（LAN应用层 × 系数），扣除局域网协议开销
                        lan_phy_est = lan_speed * self.LAN_PHY_COEFF
                        # 再乘链路层→IP层折算系数，扣除以太网/PPPoE封装开销约9%
                        local_speed = round(max(0.0, nic_speed - lan_phy_est) * self.NIC_LINK_COEFF, 2)
                    else:
                        local_speed = local_app_speed

                    # 使用当前路由器速度进行实时比较（不延迟）
                    comparison = self._compare_speeds(router_speed, local_speed)

                    # 保存当前路由器速度
                    self.prev_router_speed_kb = router_speed

                    # 检查阈值：差值 > THRESHOLD_KB
                    speed_diff = comparison.get("Diff")
                    if speed_diff is not None and speed_diff > self.THRESHOLD_KB:
                        self.over_threshold_count += 1
                        self.below_threshold_count = 0  # 超阈值时重置低于阈值计数
                    else:
                        self.over_threshold_count = 0
                        self.below_threshold_count += 1  # 低于阈值时累加

                    # 保存数据
                    self._save_router_data(
                        router_speed,
                        comparison.get("LocalSpeed"),
                        local_app_speed,
                        nic_speed,
                        lan_speed,
                        self.over_threshold_count,
                        self.below_threshold_count
                    )

                    # 日志输出
                    local_speed_str = f"{comparison.get('LocalSpeed'):.1f}" if comparison.get("LocalSpeed") else "N/A"
                    app_speed_str = f"{local_app_speed:.1f}" if local_app_speed is not None else "N/A"
                    nic_speed_str = f"{nic_speed:.1f}" if nic_speed is not None else "N/A"
                    lan_speed_str = f"{lan_speed:.1f}" if lan_speed is not None else "N/A"
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
                        f"本机(修正): {local_speed_str} KB/s, "
                        f"应用层: {app_speed_str} KB/s, "
                        f"网卡: {nic_speed_str} KB/s, "
                        f"LAN: {lan_speed_str} KB/s, "
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
                            {"router_speed": router_speed, "local_speed": local_speed, "diff": speed_diff, "threshold": self.THRESHOLD_KB}
                        )
                
                # 检查 SSH 连接状态
                if self.running and (self.ssh_channel is None or self.ssh_channel.closed):
                    # 计算重连延迟（指数退避）
                    delay = min(
                        self.SSH_RECONNECT_BASE_DELAY * (2 ** min(self.ssh_consecutive_failures, 4)),
                        self.SSH_RECONNECT_MAX_DELAY
                    )

                    # 检查是否需要等待
                    now = time.time()
                    if self.last_ssh_reconnect_attempt is not None:
                        elapsed = now - self.last_ssh_reconnect_attempt
                        if elapsed < delay:
                            remaining = delay - elapsed
                            self.logger.info(
                                f"等待 {remaining:.1f}s 后重试 SSH 连接 (失败 {self.ssh_consecutive_failures} 次)",
                                event="SSH_WAIT_RETRY"
                            )
                            time.sleep(remaining)

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
