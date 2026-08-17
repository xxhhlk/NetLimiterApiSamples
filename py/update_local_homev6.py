#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
update_local_homev6.py — 同步 NetLimiter 区域过滤器 local-homev6 的 IPv6 地址范围

功能：
1. 自动识别当前"主上网"网卡（IPv6 默认路由所在接口；无 IPv6 默认路由时回退 IPv4 默认路由接口）。
2. 获取该网卡上的公网(Global) IPv6 地址，按前缀分组。
   宽带重拨后旧前缀地址残留时，通过以下规则挑出"新"前缀：
   a) 剔除 DAD 状态为 Deprecated(反对) 的过期地址；
   b) 若仍存在多个前缀，取"首选寿命(Preferred) 最大、其次有效寿命(Valid) 最大"的前缀
      —— 重拨后路由器只广播新前缀，新前缀地址的寿命会被 RA 刷新到满值，旧前缀地址只会倒计时。
3. 以该前缀的完整范围（network_address :: 至 broadcast_address，/64 即
   240e:xxxx:xxxx:xxxx:: - 240e:xxxx:xxxx:xxxx:ffff:ffff:ffff:ffff）更新区域过滤器
   local-homev6 的 远程地址范围(FFRemoteAddressInRange) 与 本地地址范围(FFLocalAddressInRange)。

用法（更新 NetLimiter 需要管理员权限）：
    python py/update_local_homev6.py              # 检测并更新
    python py/update_local_homev6.py --dry-run    # 仅检测并打印，不写 NetLimiter
    python py/update_local_homev6.py --iface "以太网 2"   # 指定网卡，跳过自动识别
    python py/update_local_homev6.py --force      # 范围未变化时也强制更新

依赖：pythonnet（读取/写入 NetLimiter API）；标准库 ipaddress / subprocess / base64 / json。
"""

import argparse
import base64
import ipaddress
import json
import subprocess
import sys

NETLIMITER_DLL_PATH = r"C:\Program Files\Locktime Software\NetLimiter\NetLimiter.dll"
ZONE_FILTER_NAME = "local-homev6"

# ---------------------------------------------------------------------------
# NetLimiter API 加载
# ---------------------------------------------------------------------------
NLClient = None
IPRangeFilterValue = None


def load_nl_api() -> bool:
    """加载 NetLimiter DLL，返回是否成功"""
    global NLClient, IPRangeFilterValue
    try:
        import clr  # type: ignore
        clr.AddReference(NETLIMITER_DLL_PATH)
        from NetLimiter.Service import NLClient as _NLClient  # type: ignore
        from NetLimiter.Service import IPRangeFilterValue as _IPRangeFilterValue  # type: ignore
        NLClient = _NLClient
        IPRangeFilterValue = _IPRangeFilterValue
        return True
    except ImportError:
        print("[错误] 需要 pythonnet: pip install pythonnet")
        return False
    except Exception as e:
        print(f"[错误] 无法加载 NetLimiter DLL: {e}")
        print(f"       请确认 NetLimiter 已安装且路径为 {NETLIMITER_DLL_PATH}")
        return False


# ---------------------------------------------------------------------------
# 网络状态检测（PowerShell）
# ---------------------------------------------------------------------------
_PS_DETECT = r"""
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$ErrorActionPreference = 'SilentlyContinue'
$r6 = Get-NetRoute -AddressFamily IPv6 | Select-Object InterfaceAlias, InterfaceIndex, DestinationPrefix, RouteMetric, NextHop
$r4 = Get-NetRoute -AddressFamily IPv4 -DestinationPrefix '0.0.0.0/0' | Sort-Object RouteMetric | Select-Object InterfaceAlias, InterfaceIndex, RouteMetric, NextHop
$a  = Get-NetIPAddress -AddressFamily IPv6 | Select-Object InterfaceAlias, InterfaceIndex, IPAddress, PrefixLength, PrefixOrigin, SuffixOrigin, AddressState, @{n='PreferredSec';e={$_.PreferredLifetime.TotalSeconds}}, @{n='ValidSec';e={$_.ValidLifetime.TotalSeconds}}
[PSCustomObject]@{ V6Routes = @($r6); V4Routes = @($r4); Addrs = @($a) } | ConvertTo-Json -Depth 3
"""

# MSFT_NetIPAddress.AddressState 枚举（ConvertTo-Json 会序列化为数字）
ADDRESS_STATE = {
    0: "Unknown", 1: "Tentative", 2: "Duplicate",
    3: "Deprecated", 4: "Preferred", 5: "Invalid",
}


def run_ps_detect(timeout: int = 15):
    """执行 PowerShell 检测脚本，返回解析后的 dict；失败返回 None"""
    b64 = base64.b64encode(_PS_DETECT.encode("utf-16-le")).decode("ascii")
    cmd = ["powershell.exe", "-NoProfile", "-NonInteractive", "-EncodedCommand", b64]
    try:
        p = subprocess.run(cmd, capture_output=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        print(f"[错误] PowerShell 检测超时（>{timeout}s）")
        return None
    except FileNotFoundError:
        print("[错误] 找不到 powershell.exe")
        return None
    if p.returncode != 0:
        print(f"[错误] PowerShell 执行失败 (rc={p.returncode}): "
              f"{p.stderr.decode('utf-8', 'replace').strip()}")
        return None
    out = p.stdout.decode("utf-8", "replace").strip()
    if not out:
        return None
    try:
        return json.loads(out)
    except json.JSONDecodeError as e:
        print(f"[错误] PowerShell 输出解析失败: {e}")
        return None


def detect_main_iface(data: dict):
    """返回 (InterfaceAlias, InterfaceIndex)；找不到返回 None"""
    # IPv6 默认路由优先；无则回退 IPv4 默认路由
    v6_defaults = [r for r in (data.get("V6Routes") or [])
                   if str(r.get("DestinationPrefix")) == "::/0"]
    for routes in (v6_defaults, data.get("V4Routes") or []):
        if routes:
            best = min(routes, key=lambda r: (int(r.get("RouteMetric") or 0),
                                              int(r.get("InterfaceIndex") or 0)))
            return best.get("InterfaceAlias"), int(best.get("InterfaceIndex") or 0)
    return None


def fmt_lifetime(sec) -> str:
    """秒 -> 可读字符串"""
    if sec is None:
        return "?"
    s = float(sec)
    if s >= 1e9:  # infinite 约为 2^32-1 秒
        return "infinite"
    return f"{s:.0f}s"


def get_onlink_prefixes(v6_routes, iface_index) -> list:
    """
    从路由表提取该接口上的前缀（排除默认路由 ::/0 和主机路由 /128），按前缀长度降序。
    用于把地址归组到正确的前缀 —— Get-NetIPAddress 的 PrefixLength 对临时地址
    常误报为 /128，不可直接用于分组。
    """
    nets = []
    for r in v6_routes or []:
        if int(r.get("InterfaceIndex") or -1) != iface_index:
            continue
        try:
            net = ipaddress.IPv6Network(str(r.get("DestinationPrefix")), strict=False)
        except ValueError:
            continue
        if net.prefixlen in (0, 128):
            continue
        nets.append(net)
    nets.sort(key=lambda n: n.prefixlen, reverse=True)
    return nets


def resolve_address_prefix(addr: ipaddress.IPv6Address, onlink: list,
                           default_prefix_len: int) -> ipaddress.IPv6Network:
    """地址 -> 所属前缀网络（最长匹配路由；无路由时按默认前缀长度兜底）"""
    for net in onlink:
        if addr in net:
            return net
    return ipaddress.IPv6Network(f"{addr}/{default_prefix_len}", strict=False)


def select_prefix(data: dict, iface_index=None, iface_alias=None, default_prefix_len=64):
    """
    从地址列表中选出"主上网"前缀。

    Returns:
        (chosen: ipaddress.IPv6Network | None, 全部公网地址列表)
    """
    addrs = data.get("Addrs") or []
    if not addrs:
        return None, []

    # 1. 过滤到指定网卡
    if iface_index is not None:
        cands = [a for a in addrs if int(a.get("InterfaceIndex") or -1) == iface_index]
        onlink = get_onlink_prefixes(data.get("V6Routes"), iface_index)
    elif iface_alias:
        cands = [a for a in addrs if str(a.get("InterfaceAlias") or "") == iface_alias]
        idx = int(cands[0].get("InterfaceIndex") or -1) if cands else -1
        onlink = get_onlink_prefixes(data.get("V6Routes"), idx)
    else:
        cands = list(addrs)
        onlink = []

    # 2. 只保留公网(Global)地址
    glob = []
    for a in cands:
        ip = str(a.get("IPAddress") or "").split("%")[0]
        try:
            addr = ipaddress.IPv6Address(ip)
        except ValueError:
            continue
        if addr.is_global:
            glob.append(a)

    if not glob:
        return None, cands

    # 3. 剔除 Deprecated（旧前缀/旧临时地址的典型状态），全部 Deprecated 时放宽
    active = [a for a in glob if int(a.get("AddressState") or 0) != 3]  # 3=Deprecated
    pool = active or glob

    # 4. 按前缀分组（路由最长匹配；兜底默认前缀长度）
    groups = {}
    for a in pool:
        try:
            addr = ipaddress.IPv6Address(str(a["IPAddress"]).split("%")[0])
        except ValueError:
            continue
        net = resolve_address_prefix(addr, onlink, default_prefix_len)
        groups.setdefault(net, []).append(a)

    if not groups:
        return None, glob

    # 5. 排名：首选寿命最大，其次有效寿命最大（新前缀被 RA 刷新到满值，旧前缀只会倒计时）
    def score(net):
        pref = max((float(a.get("PreferredSec") or 0) for a in groups[net]), default=0.0)
        valid = max((float(a.get("ValidSec") or 0) for a in groups[net]), default=0.0)
        return (pref, valid)

    chosen = max(groups, key=score)
    return chosen, glob


# ---------------------------------------------------------------------------
# NetLimiter 区域过滤器更新
# ---------------------------------------------------------------------------
def find_filter_by_name(client, name: str):
    for f in client.Filters:  # type: ignore
        try:
            if f.Name and str(f.Name).lower() == name.lower():
                return f
        except Exception:
            continue
    return None


def update_zone_filter(client, filt, start: str, end: str, force: bool) -> bool:
    """
    更新区域过滤器的远程/本地地址范围。
    返回是否发生了写入。
    """
    changed = False
    for fn in filt.Functions:  # type: ignore
        full = str(fn.GetType().FullName)
        if full not in ("NetLimiter.Service.FFRemoteAddressInRange",
                        "NetLimiter.Service.FFLocalAddressInRange"):
            continue

        cur_start = cur_end = None
        if fn.Values.Count > 0:  # type: ignore
            v = fn.Values[0]  # type: ignore
            cur_start = str(v.Range.Start)
            cur_end = str(v.Range.End)

        if not force and cur_start == start and cur_end == end:
            print(f"  - {full.rsplit('.', 1)[-1]}: 已是最新，跳过 ({start} - {end})")
            continue

        fn.Values.Clear()  # type: ignore
        fn.Values.Add(IPRangeFilterValue(start, end))  # type: ignore
        changed = True
        print(f"  + {full.rsplit('.', 1)[-1]}: {cur_start} - {cur_end}  ->  {start} - {end}")

    if changed:
        client.UpdateFilter(filt)  # type: ignore
        print("[更新] UpdateFilter 已提交")
        # 回读验证
        for f2 in client.Filters:  # type: ignore
            if str(f2.Id) != str(filt.Id):
                continue
            for fn in f2.Functions:  # type: ignore
                full = str(fn.GetType().FullName)
                if "AddressInRange" in full and fn.Values.Count > 0:  # type: ignore
                    v = fn.Values[0]  # type: ignore
                    print(f"  [验证] {full.rsplit('.', 1)[-1]}: {v.Range.Start} - {v.Range.End}")
            break
    else:
        print("[更新] 范围无变化，跳过写入")
    return changed


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="同步 NetLimiter 区域过滤器 local-homev6 的 IPv6 范围")
    ap.add_argument("--dry-run", action="store_true", help="仅检测并打印，不更新 NetLimiter")
    ap.add_argument("--force", action="store_true", help="范围未变化时也强制更新")
    ap.add_argument("--name", default=ZONE_FILTER_NAME, help=f"区域过滤器名称（默认 {ZONE_FILTER_NAME}）")
    ap.add_argument("--iface", default=None, help="指定网卡别名（如 以太网 2），跳过自动识别")
    ap.add_argument("--prefix-length", type=int, default=64, help="地址自带前缀长度缺失时的兜底（默认 64）")
    args = ap.parse_args()

    print("=" * 64)
    print("IPv6 前缀检测 + NetLimiter 区域过滤器更新")
    print("=" * 64)

    # ---- 1. 检测 ----
    data = run_ps_detect()
    if data is None:
        print("[失败] 无法获取网络状态（PowerShell 不可用或输出异常）")
        sys.exit(2)

    if args.iface:
        iface_alias, iface_index = args.iface, None
    else:
        det = detect_main_iface(data)
        if det is None:
            print("[失败] 未找到默认路由接口（IPv6/IPv4 均无默认路由）")
            sys.exit(2)
        iface_alias, iface_index = det
    print(f"[网卡] {iface_alias}" + (f" (Idx={iface_index})" if iface_index is not None else ""))

    chosen, glob = select_prefix(data, iface_index=iface_index,
                                 iface_alias=iface_alias, default_prefix_len=args.prefix_length)

    if glob:
        print(f"[地址] 该网卡上的公网 IPv6（共 {len(glob)} 个）：")
        for a in glob:
            ip = str(a.get("IPAddress"))
            state = ADDRESS_STATE.get(int(a.get("AddressState") or 0), str(a.get("AddressState")))
            mark = ""
            if chosen is not None and int(a.get("AddressState") or 0) != 3:  # 非 Deprecated 才标记
                try:
                    if ipaddress.IPv6Address(ip.split("%")[0]) in chosen:
                        mark = "  <-- 选用前缀"
                except ValueError:
                    pass
            print(f"       {ip:<46} {state:<10} pref={fmt_lifetime(a.get('PreferredSec')):<10} "
                  f"valid={fmt_lifetime(a.get('ValidSec'))}{mark}")
    else:
        print("[地址] 该网卡上没有公网 IPv6 地址")

    if chosen is None:
        print("[失败] 主上网网卡上没有可用的公网 IPv6 地址（IPv6 未连接或仅剩旧前缀残留）")
        sys.exit(2)

    start = str(chosen.network_address)
    end = str(chosen.broadcast_address)
    print(f"[前缀] 选用: {chosen}")
    print(f"[范围] {start} - {end}")

    if args.dry_run:
        print("[dry-run] 跳过 NetLimiter 更新")
        return

    # ---- 2. 更新 NetLimiter ----
    if not load_nl_api():
        sys.exit(2)

    client = NLClient()
    try:
        client.Connect()  # type: ignore
    except Exception as e:
        print(f"[错误] NetLimiter 连接失败: {e}")
        print("       请确认 nlsvc 服务已运行（可执行: sc start nlsvc）")
        sys.exit(2)

    try:
        filt = find_filter_by_name(client, args.name)
        if filt is None:
            print(f"[错误] 未找到区域过滤器 '{args.name}'")
            sys.exit(2)
        print(f"[过滤器] {filt.Name} (Id={filt.Id}, Type={filt.Type})")

        try:
            update_zone_filter(client, filt, start, end, args.force)
        except Exception as e:
            if "Admin" in str(e) or "权限" in str(e):
                print(f"[错误] 需要管理员权限才能修改 NetLimiter 设置: {e}")
                print("       请右键以管理员身份运行（或在提升的终端中执行）")
            else:
                print(f"[错误] 更新过滤器失败: {e}")
            sys.exit(1)
    finally:
        try:
            client.Close()  # type: ignore
        except Exception:
            pass

    print("=" * 64)
    print("完成")


if __name__ == "__main__":
    main()
