#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
update_local_homev4.py — 获取当前公网 IPv4，同步到 NetLimiter 区域过滤器 local-homev4

背景：路由器 PPPoE 拨号（PC 在内网，本机网卡没有公网 IPv4），出口 IP 只能通过
外部 API 查询。宽带重拨后出口 IPv4 可能变化，本脚本将其写入区域过滤器
local-homev4 的 远程地址范围(FFRemoteAddressInRange)，范围语义为单地址
（start == end == 公网 IPv4）。

注意：v4 过滤器只配置远程地址范围。本机在 NAT 后面只有私网地址（如 192.168.x.x），
公网 IPv4 永远不会作为本地源地址出现，所以不需要 FFLocalAddressInRange
（对比 v6：本机通过 SLAAC 直接持有公网前缀，才需要本地范围）。若过滤器中
残留了本地地址范围函数，脚本会将其移除。

检测：多源并发查询 + 一致性比对（ip.sb / ipify / 3322.net / ident.me），
所有源统一走 IPv4 直连（强制 A 记录解析 + 绕过环境代理），
任一源超时/失败不阻塞，多源结果不一致时明确报错。

用法（写入 NetLimiter 需要管理员权限）：
    python py/update_local_homev4.py              # 查询并更新（过滤器不存在则自动创建）
    python py/update_local_homev4.py --dry-run    # 仅查询并打印，不写 NetLimiter
    python py/update_local_homev4.py --force      # IP 未变化时也强制更新
    python py/update_local_homev4.py --name myv4  # 指定过滤器名称

依赖：标准库 socket / ssl / http.client / concurrent.futures；pythonnet（写入 NetLimiter）。
"""

import argparse
import ipaddress
import re
import socket
import ssl
import sys
import urllib.parse
from concurrent.futures import ThreadPoolExecutor

NETLIMITER_DLL_PATH = r"C:\Program Files\Locktime Software\NetLimiter\NetLimiter.dll"
ZONE_FILTER_NAME = "local-homev4"

# 出口 IP 查询源（并发）。所有源统一走 IPv4 直连（见 query_source），
# 因此不依赖源端点本身是否为强制 v4。两条硬约束：
# 1) 必须绕过环境代理强制直连——否则国外源被本地代理分流到代理出口（如阿里云香港），
#    拿不到家庭宽带真实 IP。
# 2) 客户端有 IPv6 时双栈源优先返回 IPv6，对 IPv4 检测无效——所以 DNS 只解析 A 记录。
IP_SOURCES = {
    "ip.sb": "https://api-ipv4.ip.sb/ip",
    "ipify": "https://api.ipify.org/",
    "3322.net": "https://ip.3322.net/",
    "ident.me": "https://ipv4.ident.me/",
}
DEFAULT_TIMEOUT = 8

# ---------------------------------------------------------------------------
# NetLimiter API 加载
# ---------------------------------------------------------------------------
NLClient = None
IPRangeFilterValue = None
Filter = None
FilterType = None
FFRemoteAddressInRange = None
FFLocalAddressInRange = None


def load_nl_api() -> bool:
    """加载 NetLimiter DLL，返回是否成功"""
    global NLClient, IPRangeFilterValue, Filter, FilterType
    global FFRemoteAddressInRange, FFLocalAddressInRange
    try:
        import clr  # type: ignore
        clr.AddReference(NETLIMITER_DLL_PATH)
        from NetLimiter.Service import (  # type: ignore
            NLClient as _NLClient,
            Filter as _Filter,
            FilterType as _FilterType,
            FFRemoteAddressInRange as _FFRemote,
            FFLocalAddressInRange as _FFLocal,
            IPRangeFilterValue as _IPRangeFilterValue,
        )
        NLClient, Filter, FilterType = _NLClient, _Filter, _FilterType
        FFRemoteAddressInRange, FFLocalAddressInRange = _FFRemote, _FFLocal
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
# 出口 IPv4 检测
# ---------------------------------------------------------------------------
def query_source(name: str, url: str, timeout: int):
    """
    查询单个源，返回 (name, ip 或 None)；异常/超时返回 None。

    统一强制 IPv4 直连：
    - DNS 只解析 A 记录（AF_INET），避免双栈客户端拿到源返回的 IPv6；
    - 直接与 IPv4 地址建连（不读环境代理），避免被本地代理分流到代理出口；
    - TLS 握手用 server_hostname 保留 SNI 与证书校验（http.client 无法注入
      已连接 socket 且其 SNI 取 self.host，因此用原始 socket 手动发请求）。
    """
    try:
        p = urllib.parse.urlparse(url)
        host = p.hostname
        port = p.port or 443
        path = p.path or "/"
        if p.query:
            path += "?" + p.query

        # 只取 IPv4 (A 记录)，按顺序逐个尝试
        infos = socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_STREAM)
        ctx = ssl.create_default_context()
        for _, _, _, _, addr in infos:
            try:
                raw = socket.create_connection((addr[0], port), timeout=timeout)
                with ctx.wrap_socket(raw, server_hostname=host) as ssock:
                    req = (f"GET {path} HTTP/1.1\r\n"
                           f"Host: {host}\r\n"
                           f"User-Agent: curl/8.4.0\r\n"
                           f"Connection: close\r\n\r\n")
                    ssock.sendall(req.encode())
                    buf = b""
                    while True:
                        chunk = ssock.recv(65536)
                        if not chunk:
                            break
                        buf += chunk
                text = buf.decode("utf-8", "replace")
                m = re.search(r"\b(\d{1,3}(?:\.\d{1,3}){3})\b", text)
                if not m:
                    return name, None
                addr_obj = ipaddress.ip_address(m.group(1))
                return name, str(addr_obj) if addr_obj.version == 4 else None
            except Exception:
                continue  # 换下一个 A 记录
        return name, None
    except Exception:
        return name, None


def detect_public_ipv4(timeout: int = DEFAULT_TIMEOUT):
    """
    多源并发查询出口 IPv4。

    Returns:
        (ip, source_detail) — source_detail 形如 "ip.sb,ipip.net(2源一致)" 或
        "ip.sb(单源)"；多源结果不一致或全部失败时 ip 为 None。
    """
    print(f"[查询] 并发请求 {len(IP_SOURCES)} 个源: {', '.join(IP_SOURCES)} (超时 {timeout}s)")
    with ThreadPoolExecutor(max_workers=len(IP_SOURCES)) as ex:
        futures = {ex.submit(query_source, n, u, timeout): n for n, u in IP_SOURCES.items()}
        results = {futures[f]: f.result()[1] for f in futures}

    ok = {n: ip for n, ip in results.items() if ip}
    for n in IP_SOURCES:
        if n in ok:
            print(f"  {n:<9} -> {ok[n]}")
        else:
            print(f"  {n:<9} -> [失败/无响应]")

    if not ok:
        print("[失败] 所有查询源均不可用（网络不通或被墙），请检查出口网络")
        return None, ""

    uniq = {}
    for ip in set(ok.values()):
        uniq[ip] = [n for n, v in ok.items() if v == ip]

    if len(uniq) > 1:
        print("[失败] 多源结果不一致，拒绝更新：")
        for ip, names in uniq.items():
            print(f"  {ip}  <- {','.join(names)}")
        return None, ""

    ip = next(iter(uniq))
    names = uniq[ip]
    detail = f"{','.join(names)}({len(names)}源一致)" if len(names) > 1 else f"{names[0]}(单源)"
    return ip, detail


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


# v4 过滤器只配置远程地址范围（NAT 后公网 IP 不可能是本地源地址）
REMOTE_FN = "NetLimiter.Service.FFRemoteAddressInRange"
LOCAL_FN = "NetLimiter.Service.FFLocalAddressInRange"


def update_zone_filter(client, filt, ip: str, force: bool) -> bool:
    """
    将远程地址范围更新为单地址 ip-ip；移除无意义的本地地址范围函数
    （本机在 NAT 后面只有私网地址，公网 IPv4 不会作为本地源地址出现）。
    返回是否发生写入。
    """
    changed = False
    found_remote = False
    to_remove = []

    for fn in filt.Functions:  # type: ignore
        full = str(fn.GetType().FullName)
        if full == REMOTE_FN:
            found_remote = True
            cur = None
            if fn.Values.Count > 0:  # type: ignore
                v = fn.Values[0]  # type: ignore
                cur = f"{v.Range.Start} - {v.Range.End}"
            if not force and cur == f"{ip} - {ip}":
                print(f"  - FFRemoteAddressInRange: 已是最新，跳过 ({ip} - {ip})")
                continue
            fn.Values.Clear()  # type: ignore
            fn.Values.Add(IPRangeFilterValue(ip, ip))  # type: ignore
            changed = True
            print(f"  + FFRemoteAddressInRange: {cur}  ->  {ip} - {ip}")
        elif full == LOCAL_FN:
            to_remove.append(fn)

    for fn in to_remove:
        filt.Functions.Remove(fn)  # type: ignore
        changed = True
        print("  - FFLocalAddressInRange: 已移除（NAT 下公网 IP 不可能是本地地址）")

    if not found_remote:
        filt.Functions.Add(FFRemoteAddressInRange(IPRangeFilterValue(ip, ip)))  # type: ignore
        changed = True
        print(f"  + FFRemoteAddressInRange: (缺失，已补全)  ->  {ip} - {ip}")

    return changed


def ensure_zone_filter(client, name: str, ip: str):
    """
    查找过滤器；不存在则创建 Zone 过滤器（只含远程地址范围函数）。
    返回 (filter, created: bool)
    """
    filt = find_filter_by_name(client, name)
    if filt is not None:
        return filt, False

    filt = Filter(FilterType.Zone, name)
    filt.Functions.Add(FFRemoteAddressInRange(IPRangeFilterValue(ip, ip)))  # type: ignore
    filt = client.AddFilter(filt)  # type: ignore
    print(f"[创建] 区域过滤器 '{name}' 已创建 (Id={filt.Id})")
    return filt, True


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="同步 NetLimiter 区域过滤器 local-homev4 的公网 IPv4")
    ap.add_argument("--dry-run", action="store_true", help="仅查询并打印，不更新 NetLimiter")
    ap.add_argument("--force", action="store_true", help="IP 未变化时也强制更新")
    ap.add_argument("--name", default=ZONE_FILTER_NAME, help=f"区域过滤器名称（默认 {ZONE_FILTER_NAME}）")
    ap.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT, help=f"单源查询超时秒数（默认 {DEFAULT_TIMEOUT}）")
    args = ap.parse_args()

    print("=" * 64)
    print("公网 IPv4 检测 + NetLimiter 区域过滤器更新")
    print("=" * 64)

    # ---- 1. 查询出口 IPv4 ----
    ip, detail = detect_public_ipv4(args.timeout)
    if ip is None:
        sys.exit(2)
    print(f"[出口] 公网 IPv4 = {ip}  [{detail}]")

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
        filt, created = ensure_zone_filter(client, args.name, ip)
        if not created:
            print(f"[过滤器] {filt.Name} (Id={filt.Id}, Type={filt.Type})")

        try:
            changed = update_zone_filter(client, filt, ip, args.force)
            if created or changed:
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
