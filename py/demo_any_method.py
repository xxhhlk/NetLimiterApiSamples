#!/usr/bin/env python3
"""
演示脚本：用 NetLimiter Any 过滤器替代网卡物理层

对比两种方案：
  网卡方案：修正值 = 网卡物理上行 − LAN应用层
  Any 方案：修正值 = Any应用层 − LAN应用层

Any 过滤器(FilterId=3)统计所有应用层流量，Any − LAN = 互联网应用层。
与 Internet(FilterId=2) 应该接近，但 Any 可能包含 Internet 漏掉的部分。

同时从 router_speed_data.json 读取路由器速度，计算三种方案与路由器的差值。

运行（需管理员权限，且 router_sampler 需在运行以提供路由器速度）：
    python py/demo_any_method.py
"""
import sys
import time
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

# 配置
LAN_FILTER_NAME = "LocalNetwork"
INTERNET_FILTER_ID = 2
ANY_FILTER_ID = 3
HISTORY_SIZE = 10
SAMPLE_INTERVAL = 1.0

EXCLUDE_KEYWORDS = ("loopback", "vmware", "vethernet", "wsl", "docker",
                    "isatp", "teredo", "tunnel")


def discover_physical_nics():
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
        import json
        adapters = json.loads(result.stdout)
        if isinstance(adapters, dict):
            adapters = [adapters]
        return [a["NetConnectionID"] for a in adapters if a.get("NetConnectionID")]
    except Exception:
        return []


class AnyMethodDemo:
    def __init__(self):
        self.phys_nics = []
        # 网卡基线
        self.prev_nic_sent = 0
        self.prev_nic_ts = 0.0
        self.nic_history = deque(maxlen=HISTORY_SIZE)
        # Internet 基线
        self.prev_inet_out = 0
        self.prev_inet_ts = 0.0
        self.inet_history = deque(maxlen=HISTORY_SIZE)
        # LAN 基线
        self.prev_lan_out = 0
        self.prev_lan_ts = 0.0
        self.lan_history = deque(maxlen=HISTORY_SIZE)
        # Any 基线
        self.prev_any_out = 0
        self.prev_any_ts = 0.0
        self.any_history = deque(maxlen=HISTORY_SIZE)
        # NetLimiter
        self.client = None
        self.node_loader = None
        self.lan_filter_id = None

    def connect_nl(self):
        try:
            self.client = NLClient()
            self.client.Connect()
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
            print(f"  [NL] 已连接，LAN FilterId={self.lan_filter_id}, Internet={INTERNET_FILTER_ID}, Any={ANY_FILTER_ID}")
            return True
        except Exception as e:
            print(f"  [NL] 连接失败: {e}")
            return False

    def _avg(self, history):
        if not history:
            return 0.0
        return sum(history) / len(history)

    def sample_nic(self):
        pernic = psutil.net_io_counters(pernic=True)
        total = sum(pernic[n].bytes_sent for n in self.phys_nics if n in pernic)
        now = time.time()
        if self.prev_nic_ts == 0:
            self.prev_nic_sent = total
            self.prev_nic_ts = now
            return 0.0
        dt = now - self.prev_nic_ts
        if dt <= 0:
            return 0.0
        delta = total - self.prev_nic_sent
        speed = delta / dt / 1024
        if delta >= 0 and speed < 1048576:
            self.nic_history.append(speed)
        self.prev_nic_sent = total
        self.prev_nic_ts = now
        return self._avg(self.nic_history)

    def _sample_filter(self, filter_id, prev_out, prev_ts, history):
        """通用过滤器采样，返回 (speed_avg, new_prev_out, new_prev_ts)"""
        for node in self.node_loader.Filters.Nodes:
            if node.FilterId == filter_id:
                cur = node.Transferred.Out
                now = time.time()
                if prev_ts > 0:
                    dt = now - prev_ts
                    if dt > 0:
                        delta = cur - prev_out
                        speed = delta / dt / 1024
                        if delta >= 0 and speed < 1048576:
                            history.append(speed)
                return self._avg(history), cur, now
        return 0.0, prev_out, prev_ts

    def sample_internet(self):
        avg, self.prev_inet_out, self.prev_inet_ts = self._sample_filter(
            INTERNET_FILTER_ID, self.prev_inet_out, self.prev_inet_ts, self.inet_history)
        return avg

    def sample_lan(self):
        if self.lan_filter_id is None:
            return 0.0
        avg, self.prev_lan_out, self.prev_lan_ts = self._sample_filter(
            self.lan_filter_id, self.prev_lan_out, self.prev_lan_ts, self.lan_history)
        return avg

    def sample_any(self):
        avg, self.prev_any_out, self.prev_any_ts = self._sample_filter(
            ANY_FILTER_ID, self.prev_any_out, self.prev_any_ts, self.any_history)
        return avg

    def run(self, rounds=30):
        print("=" * 80)
        print("Any 方案 vs 网卡方案 对比演示")
        print("=" * 80)

        print("\n[1] WMI 识别物理网卡...")
        self.phys_nics = discover_physical_nics()
        if not self.phys_nics:
            for name in psutil.net_io_counters(pernic=True):
                if not any(kw in name.lower() for kw in EXCLUDE_KEYWORDS):
                    self.phys_nics.append(name)
        print(f"  物理网卡: {self.phys_nics}")

        print("\n[2] 连接 NetLimiter...")
        if not self.connect_nl():
            return

    def read_router_speed(self):
        """从 router_speed_data.json 读取路由器速度"""
        import json
        import os
        data_file = Path(os.environ.get('TEMP', '.')) / "router_speed_data.json"
        try:
            if data_file.exists():
                data = json.loads(data_file.read_text(encoding='utf-8'))
                # 检查数据是否新鲜（5秒内）
                from datetime import datetime
                last_update = datetime.fromisoformat(data.get("LastUpdate", ""))
                age = (datetime.now() - last_update).total_seconds()
                if age < 5:
                    return data.get("RouterSpeedKB")
        except Exception:
            pass
        return None

    def run(self, rounds=30):
        print("=" * 100)
        print("Any 方案 vs 网卡方案 对比演示（含路由器差值）")
        print("=" * 100)

        print("\n[1] WMI 识别物理网卡...")
        self.phys_nics = discover_physical_nics()
        if not self.phys_nics:
            for name in psutil.net_io_counters(pernic=True):
                if not any(kw in name.lower() for kw in EXCLUDE_KEYWORDS):
                    self.phys_nics.append(name)
        print(f"  物理网卡: {self.phys_nics}")

        print("\n[2] 连接 NetLimiter...")
        if not self.connect_nl():
            return

        # LAN 物理层估算系数（以太网开销约3%，可调整）
        LAN_PHY_COEFF = 1.03

        print(f"\n[3] 开始采样，间隔 {SAMPLE_INTERVAL}s，共 {rounds} 轮")
        print(f"    路由器速度从 router_speed_data.json 读取（需 router_sampler 在运行）")
        print(f"    LAN物理层估算系数 = {LAN_PHY_COEFF}\n")
        print(f"{'轮':>3} | {'路由器':>7} | {'网卡':>7} | {'LAN':>5} | "
              f"{'当前修':>7} | {'改进修':>7} | "
              f"{'当前diff':>8} | {'改进diff':>8}")
        print("-" * 80)

        for i in range(1, rounds + 1):
            self.node_loader.Load()
            nic = self.sample_nic()
            lan = self.sample_lan()
            router = self.read_router_speed()

            # 当前方案：网卡 - LAN应用层
            current_corrected = max(0.0, nic - lan) if lan > 0 else nic
            # 改进方案：网卡 - LAN物理层估算（LAN应用层 × 系数）
            lan_phy_est = lan * LAN_PHY_COEFF
            improved_corrected = max(0.0, nic - lan_phy_est) if lan > 0 else nic

            # 与路由器的差值（路由器 - 修正值）
            current_diff = (router - current_corrected) if router is not None else None
            improved_diff = (router - improved_corrected) if router is not None else None

            router_str = f"{router:>7.1f}" if router is not None else "    N/A"
            cur_diff_str = f"{current_diff:>+8.1f}" if current_diff is not None else "     N/A"
            imp_diff_str = f"{improved_diff:>+8.1f}" if improved_diff is not None else "     N/A"

            print(f"{i:>3} | {router_str} | {nic:>7.1f} | {lan:>5.1f} | "
                  f"{current_corrected:>7.1f} | {improved_corrected:>7.1f} | "
                  f"{cur_diff_str} | {imp_diff_str}")

            time.sleep(SAMPLE_INTERVAL)

        print("\n" + "=" * 80)
        print("说明：")
        print("  路由器   = router_speed_data.json 中的路由器速度")
        print("  网卡     = psutil 物理网卡总上行（链路层，含协议开销）")
        print("  LAN      = NetLimiter LAN 过滤器 应用层")
        print(f"  当前修   = max(0, 网卡 - LAN应用层)  [当前方案]")
        print(f"  改进修   = max(0, 网卡 - LAN应用层×{LAN_PHY_COEFF})  [改进方案，扣除LAN物理层估算]")
        print("  当前diff = 路由器 - 当前修（正值=路由器高，负值=本机高）")
        print("  改进diff = 路由器 - 改进修")
        print("=" * 80)

        try:
            self.client.Close()
        except Exception:
            pass


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Any 方案对比演示")
    parser.add_argument("-n", "--rounds", type=int, default=30, help="采样轮数")
    args = parser.parse_args()
    demo = AnyMethodDemo()
    demo.run(args.rounds)
