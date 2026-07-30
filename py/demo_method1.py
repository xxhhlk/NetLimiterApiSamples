#!/usr/bin/env python3
"""
方法1 演示脚本：实时输出修正后的本机互联网上行速度

逻辑预览（与后续集成到 router_sampler.py 的逻辑一致）：
1. WMI 识别物理网卡(PNPDeviceID 以 PCI\\ 开头)
2. psutil 采样网卡 bytes_sent，求和计算物理上行速度
3. NetLimiter 采样 LAN 区域（LocalNetwork, FilterId=1）Transferred.Out，计算 LAN 应用层速度
4. NetLimiter 采样 Internet 区域（FilterId=2）Transferred.Out，作为应用层参考
5. 修正后本机互联网上行 = max(0, 网卡速度滑动平均 - LAN速度滑动平均)
6. 每秒输出一行，便于观察效果

运行（需管理员权限）：
    python py/demo_method1.py
"""
import sys
import time
import json
import subprocess
from collections import deque
from pathlib import Path

import psutil

NETLIMITER_DLL_PATH = r"C:\Program Files\Locktime Software\NetLimiter\NetLimiter.dll"
try:
    import clr  # type: ignore
    clr.AddReference(NETLIMITER_DLL_PATH)
    from NetLimiter.Service import NLClient  # type: ignore
except Exception as e:
    print(f"无法加载 NetLimiter API: {e}")
    sys.exit(1)

# ---------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------
LAN_FILTER_NAME = "LocalNetwork"   # 自动发现用名称
INTERNET_FILTER_ID = 2              # Internet 区域 InternalId
HISTORY_SIZE = 10                   # 滑动平均窗口
SAMPLE_INTERVAL = 1.0               # 采样间隔（秒）


# ---------------------------------------------------------------------------
# WMI 物理网卡识别
# ---------------------------------------------------------------------------
def discover_physical_nics() -> list:
    r"""WMI 查询 Win32_NetworkAdapter，返回 PNPDeviceID 以 PCI\ 开头且 NetConnectionID 非空的网卡名列表"""
    ps_cmd = """
Get-CimInstance Win32_NetworkAdapter |
  Where-Object { $_.PhysicalAdapter -eq $true -and $_.NetConnectionID -ne $null -and $_.PNPDeviceID -like 'PCI*' } |
  Select-Object NetConnectionID, Name, PNPDeviceID |
  ConvertTo-Json -Depth 2
"""
    try:
        result = subprocess.run(
            ["powershell", "-NoProfile", "-Command", ps_cmd],
            capture_output=True, text=True, timeout=15
        )
        if result.returncode != 0 or not result.stdout.strip():
            print(f"  [WMI] 查询失败: {result.stderr.strip()}")
            return []
        adapters = json.loads(result.stdout)
        if isinstance(adapters, dict):
            adapters = [adapters]
        names = []
        for a in adapters:
            conn = a.get("NetConnectionID")
            if conn:
                names.append(conn)
        return names
    except Exception as e:
        print(f"  [WMI] 异常: {e}")
        return []


# ---------------------------------------------------------------------------
# 采样器
# ---------------------------------------------------------------------------
class Method1Demo:
    def __init__(self):
        # 物理网卡名列表
        self.phys_nics: list = []
        # 网卡采样基线
        self.prev_nic_sent: int = 0
        self.prev_nic_ts: float = 0.0
        self.nic_history = deque(maxlen=HISTORY_SIZE)
        # LAN 采样基线
        self.prev_lan_out: int = 0
        self.prev_lan_ts: float = 0.0
        self.lan_history = deque(maxlen=HISTORY_SIZE)
        # Internet 采样基线（参考）
        self.prev_inet_out: int = 0
        self.prev_inet_ts: float = 0.0
        self.inet_history = deque(maxlen=HISTORY_SIZE)
        # NetLimiter
        self.client = None
        self.node_loader = None
        self.lan_filter_id = None

    # ---- NetLimiter 连接 + LAN FilterId 自动发现 ----
    def connect_nl(self) -> bool:
        try:
            self.client = NLClient()
            self.client.Connect()
            # 自动发现 LAN FilterId
            for f in self.client.Filters:
                try:
                    if f.Name and f.Name.lower() == LAN_FILTER_NAME.lower():
                        self.lan_filter_id = f.InternalId
                        break
                except Exception:
                    continue
            self.node_loader = self.client.CreateNodeLoader()
            self.node_loader.Filters.SelectAll()
            self.node_loader.Load()
            # 不在此处初始化基线，让第一次采样时设基线（避免 dt 异常小导致首样本失真）
            print(f"  [NL] 已连接，LAN FilterId={self.lan_filter_id}, Internet FilterId={INTERNET_FILTER_ID}")
            return True
        except Exception as e:
            print(f"  [NL] 连接失败: {e}")
            return False

    # ---- 网卡采样 ----
    def sample_nic(self) -> float:
        pernic = psutil.net_io_counters(pernic=True)
        total_sent = 0
        for name in self.phys_nics:
            if name in pernic:
                total_sent += pernic[name].bytes_sent
        now = time.time()
        if self.prev_nic_ts == 0:
            self.prev_nic_sent = total_sent
            self.prev_nic_ts = now
            return 0.0
        dt = now - self.prev_nic_ts
        if dt <= 0:
            return 0.0
        delta = total_sent - self.prev_nic_sent
        speed_kbs = delta / dt / 1024
        if delta >= 0 and speed_kbs < 1048576:
            self.nic_history.append(speed_kbs)
        self.prev_nic_sent = total_sent
        self.prev_nic_ts = now
        return speed_kbs

    # ---- LAN 采样 ----
    def sample_lan(self) -> float:
        if not self.node_loader or self.lan_filter_id is None:
            return 0.0
        try:
            self.node_loader.Load()
            for node in self.node_loader.Filters.Nodes:
                if node.FilterId == self.lan_filter_id:
                    cur = node.Transferred.Out
                    now = time.time()
                    if self.prev_lan_ts == 0:
                        # 第一次只设基线，返回 0
                        self.prev_lan_out = cur
                        self.prev_lan_ts = now
                        return 0.0
                    dt = now - self.prev_lan_ts
                    if dt > 0:
                        delta = cur - self.prev_lan_out
                        speed_kbs = delta / dt / 1024
                        if delta >= 0 and speed_kbs < 1048576:
                            self.lan_history.append(speed_kbs)
                    self.prev_lan_out = cur
                    self.prev_lan_ts = now
                    return self.lan_history[-1] if self.lan_history else 0.0
        except Exception as e:
            print(f"  [LAN] 采样失败: {e}")
        return 0.0

    # ---- Internet 采样（参考）----
    def sample_inet(self) -> float:
        if not self.node_loader:
            return 0.0
        try:
            # node_loader 已在 sample_lan 里 Load 过，这里直接读
            for node in self.node_loader.Filters.Nodes:
                if node.FilterId == INTERNET_FILTER_ID:
                    cur = node.Transferred.Out
                    now = time.time()
                    if self.prev_inet_ts == 0:
                        # 第一次只设基线，返回 0
                        self.prev_inet_out = cur
                        self.prev_inet_ts = now
                        return 0.0
                    dt = now - self.prev_inet_ts
                    if dt > 0:
                        delta = cur - self.prev_inet_out
                        speed_kbs = delta / dt / 1024
                        if delta >= 0 and speed_kbs < 1048576:
                            self.inet_history.append(speed_kbs)
                    self.prev_inet_out = cur
                    self.prev_inet_ts = now
                    return self.inet_history[-1] if self.inet_history else 0.0
        except Exception as e:
            print(f"  [INET] 采样失败: {e}")
        return 0.0

    @staticmethod
    def avg(history: deque) -> float:
        if not history:
            return 0.0
        return sum(history) / len(history)

    def run(self, rounds: int = 30):
        print("=" * 70)
        print("方法1 演示：修正后本机互联网上行速度")
        print("=" * 70)

        # 1. WMI 识别物理网卡
        print("\n[1] WMI 识别物理网卡...")
        self.phys_nics = discover_physical_nics()
        if not self.phys_nics:
            print("  ✗ 未找到物理网卡，降级用名称关键字排除")
            EXCLUDE = ("loopback", "vmware", "vethernet", "wsl", "docker", "isatp", "teredo", "tunnel")
            for name in psutil.net_io_counters(pernic=True):
                if not any(kw in name.lower() for kw in EXCLUDE):
                    self.phys_nics.append(name)
        print(f"  物理网卡: {self.phys_nics}")

        # 2. 连接 NetLimiter
        print("\n[2] 连接 NetLimiter...")
        if not self.connect_nl():
            print("  ✗ NetLimiter 连接失败，退出")
            return

        # 3. 主循环
        print(f"\n[3] 开始采样，间隔 {SAMPLE_INTERVAL}s，共 {rounds} 轮\n")
        print(f"{'轮次':>4} | {'网卡KB/s':>10} | {'LAN KB/s':>10} | {'应用层KB/s':>10} | "
              f"{'修正后KB/s':>10} | {'修正-应用层':>10}")
        print("-" * 80)

        for i in range(1, rounds + 1):
            # 时间对齐：依次连续采样
            nic_speed = self.sample_nic()
            lan_speed = self.sample_lan()
            inet_speed = self.sample_inet()

            nic_avg = self.avg(self.nic_history)
            lan_avg = self.avg(self.lan_history)
            inet_avg = self.avg(self.inet_history)

            # 修正后本机互联网上行
            corrected = max(0.0, nic_avg - lan_avg)
            # 与应用层（原 LocalSpeedKB）的差，看修正了多少
            diff_vs_app = corrected - inet_avg

            print(f"{i:>4} | {nic_avg:>10.2f} | {lan_avg:>10.2f} | {inet_avg:>10.2f} | "
                  f"{corrected:>10.2f} | {diff_vs_app:>+10.2f}")

            time.sleep(SAMPLE_INTERVAL)

        print("\n" + "=" * 70)
        print("演示完成。")
        print("说明：")
        print("  网卡KB/s    = 物理网卡总上行（含协议开销）")
        print("  LAN KB/s    = NetLimiter LAN 区域应用层上行")
        print("  应用层KB/s  = NetLimiter Internet 区域应用层上行（原 LocalSpeedKB）")
        print("  修正后KB/s  = max(0, 网卡 - LAN) = 真实互联网上行（含协议开销）")
        print("  修正-应用层 = 修正后 - 应用层，正值代表协议开销被补回的量")
        print("=" * 70)

        try:
            self.client.Close()
        except Exception:
            pass


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="方法1 演示脚本")
    parser.add_argument("-n", "--rounds", type=int, default=30, help="采样轮数（默认30）")
    args = parser.parse_args()
    demo = Method1Demo()
    demo.run(args.rounds)
