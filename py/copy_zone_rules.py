#!/usr/bin/env python3
"""copy_zone_rules.py - 把一个 NetLimiter 过滤器(区域)下的规则复制到另一个过滤器。

典型用途：把 Internet 区域(InternalId=2)的规则搬到自定义过滤器 "True internet"。

设计要点
--------
- 默认 **dry-run**：只打印计划，不写 NetLimiter。加 ``--apply`` 才真正写入。
- 规则克隆按类型分派：LimitRule / PriorityRule / IgnoreRule / FwRule / QuotaRule。
- TimeCondition 一并克隆（含私有字段 ``Time``，用反射复制）。
- 目标已存在"同签名"规则时默认跳过；``--fix-conditions`` 可为已存在规则补齐缺失的时间条件。
- 启用状态由 ``--enable-mode`` 控制，默认 ``source``（与源规则一致，最安全）。

用法
----
    python copy_zone_rules.py                                  # dry-run，Internet -> True internet
    python copy_zone_rules.py --apply                          # 真正写入
    python copy_zone_rules.py --apply --enable-mode on         # 新规则一律启用
    python copy_zone_rules.py --apply --fix-conditions         # 补齐目标规则缺失的时间条件
    python copy_zone_rules.py --src "Internet" --dst "True internet"
"""

import argparse
import ctypes
import sys

NETLIMITER_DLL_PATH = r"C:\Program Files\Locktime Software\NetLimiter\NetLimiter.dll"

try:
    import clr

    clr.AddReference(NETLIMITER_DLL_PATH)
    from NetLimiter.Service import (  # type: ignore
        NLClient,
        LimitRule,
        PriorityRule,
        IgnoreRule,
        FwRule,
        QuotaRule,
        TimeCondition,
        RuleCondition,
    )
    from System import DateTime, DayOfWeek  # type: ignore
    from System.Collections.Generic import List  # type: ignore

    NETLIMITER_AVAILABLE = True
except Exception as e:  # pragma: no cover
    print(f"无法加载 NetLimiter API: {e}")
    print(f"DLL 路径: {NETLIMITER_DLL_PATH}")
    NETLIMITER_AVAILABLE = False


# ---------------------------------------------------------------------------
# 规则克隆
# ---------------------------------------------------------------------------
def clone_conditions(src_rule, warn):
    """克隆规则的 Conditions，返回 (list, skipped_count)。仅支持 TimeCondition。"""
    out = []
    skipped = 0
    try:
        conds = list(src_rule.Conditions) if src_rule.Conditions else []
    except Exception as e:
        warn(f"读取 Conditions 失败: {e}")
        return out, skipped

    for cd in conds:
        if cd.GetType().Name != "TimeCondition":
            skipped += 1
            warn(f"跳过未知条件类型: {cd.GetType().Name}")
            continue
        try:
            n = TimeCondition()
            n.IsEnabled = cd.IsEnabled
            n.TimeConditionType = cd.TimeConditionType
            n.DayOfMonth = cd.DayOfMonth
            n.Action = cd.Action
            try:
                days = list(cd.Days) if cd.Days else []
                if days:
                    lst = List[DayOfWeek]()
                    for d in days:
                        lst.Add(d)
                    n.Days = lst
            except Exception as e:
                warn(f"复制 Days 失败: {e}")
            tf = cd.GetType().GetField("Time")
            nf = n.GetType().GetField("Time")
            if tf is not None and nf is not None:
                nf.SetValue(n, tf.GetValue(cd))
        except Exception as e:
            skipped += 1
            warn(f"克隆 TimeCondition 失败: {e}")
            continue
        out.append(n)
    return out, skipped


def clone_rule(src, warn):
    """按类型构造同参数的新规则对象（不含 Id/FilterId/InternalId）。"""
    tname = src.GetType().Name

    if tname == "LimitRule":
        r = LimitRule(src.Dir, int(src.LimitSize))
    elif tname == "PriorityRule":
        r = PriorityRule(src.Priority)
        try:
            r.Dir = src.Dir
        except Exception as e:
            warn(f"设置 PriorityRule.Dir 失败: {e}")
    elif tname == "IgnoreRule":
        r = IgnoreRule(src.Dir)
        r.IgnoreData = src.IgnoreData
        r.IgnoreLimit = src.IgnoreLimit
    elif tname == "FwRule":
        r = FwRule(src.Dir, src.Action)
    elif tname == "QuotaRule":
        r = QuotaRule(src.Dir, int(src.Quota))
        for attr in ("ShowAlertWindow", "RunActions", "SendEmail", "EmeailRcps"):
            try:
                setattr(r, attr, getattr(src, attr))
            except Exception:
                pass
    else:
        raise NotImplementedError(f"不支持的规则类型: {tname}")

    try:
        r.Weight = src.Weight
    except Exception:
        pass

    conds, skipped = clone_conditions(src, warn)
    if conds:
        try:
            lst = List[RuleCondition]()
            for cd in conds:
                lst.Add(cd)
            r.Conditions = lst
        except Exception as e:
            warn(f"附加 Conditions 失败: {e}")
    return r, skipped


# ---------------------------------------------------------------------------
# 描述 / 指纹
# ---------------------------------------------------------------------------
def cond_fingerprint(conds):
    fps = []
    for cd in conds:
        tn = cd.GetType().Name
        if tn == "TimeCondition":
            f = cd.GetType().GetField("Time")
            tv = f.GetValue(cd) if f is not None else None
            try:
                tstr = tv.ToString("HH:mm:ss")
            except Exception:
                tstr = str(tv)
            days = ""
            try:
                days = ",".join(sorted(str(d) for d in (cd.Days or [])))
            except Exception:
                pass
            fps.append(f"Time/{cd.TimeConditionType}/{cd.Action}/{tstr}/{days}/d{cd.DayOfMonth}")
        else:
            fps.append(f"{tn}")
    return tuple(sorted(fps))


def describe(r):
    tname = r.GetType().Name
    if tname == "LimitRule":
        val = f"{r.LimitSize / 1024:.1f} KB/s"
    elif tname == "PriorityRule":
        val = str(r.Priority)
    elif tname == "IgnoreRule":
        val = f"data={r.IgnoreData},limit={r.IgnoreLimit}"
    elif tname == "FwRule":
        val = str(r.Action)
    elif tname == "QuotaRule":
        val = f"quota={r.Quota}"
    else:
        val = "?"
    conds = list(r.Conditions) if r.Conditions else []
    cstr = ""
    if conds:
        parts = []
        for cd in conds:
            if cd.GetType().Name == "TimeCondition":
                f = cd.GetType().GetField("Time")
                tv = f.GetValue(cd) if f is not None else None
                try:
                    tstr = tv.ToString("HH:mm:ss")
                except Exception:
                    tstr = str(tv)
                parts.append(f"{cd.Action}@{tstr}({cd.TimeConditionType})")
            else:
                parts.append(cd.GetType().Name)
        cstr = " [条件: " + ", ".join(parts) + "]"
    return f"{tname} {r.Dir} {val}{cstr}"


def value_key(r):
    tname = r.GetType().Name
    if tname == "LimitRule":
        return tname, str(r.Dir), int(r.LimitSize)
    if tname == "PriorityRule":
        return tname, str(r.Dir), str(r.Priority)
    if tname == "IgnoreRule":
        return tname, str(r.Dir), (bool(r.IgnoreData), bool(r.IgnoreLimit))
    if tname == "FwRule":
        return tname, str(r.Dir), str(r.Action)
    return tname, str(r.Dir), None


def signature(r):
    """值 + 条件指纹，用于判断"完全相同"。"""
    conds = list(r.Conditions) if r.Conditions else []
    return value_key(r) + (cond_fingerprint(conds),)


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------
def is_admin():
    try:
        return bool(ctypes.windll.shell32.IsUserAnAdmin())
    except Exception:
        return False


def main():
    ap = argparse.ArgumentParser(description="复制 NetLimiter 过滤器之间的规则")
    ap.add_argument("--src", default="Internet", help="源过滤器名称（默认 Internet）")
    ap.add_argument("--dst", default="True internet", help="目标过滤器名称（默认 True internet）")
    ap.add_argument("--apply", action="store_true", help="真正写入（默认只 dry-run 打印计划）")
    ap.add_argument(
        "--enable-mode",
        choices=("source", "on", "off"),
        default="source",
        help="新建规则的启用状态：source=跟随源(默认) / on=全部启用 / off=全部禁用",
    )
    ap.add_argument("--fix-conditions", action="store_true",
                    help="为已存在的同值目标规则补齐缺失的时间条件")
    ap.add_argument("--prune-source", action="store_true",
                    help="复制成功后删除源规则（危险，默认关闭）")
    ap.add_argument("--allow-no-admin", action="store_true", help="跳过管理员权限检查")
    ap.add_argument("--log", default=None, help="同时把输出写入指定文件（提权运行时回收日志用）")
    args = ap.parse_args()

    if not NETLIMITER_AVAILABLE:
        return 2

    if args.apply and not args.allow_no_admin and not is_admin():
        print("错误：修改 NetLimiter 需要管理员权限，请以管理员身份运行（或加 --allow-no-admin）。")
        return 2

    warnings = []

    def warn(msg):
        warnings.append(msg)
        print(f"  [warn] {msg}")

    client = NLClient()
    client.Connect()
    try:
        filters = list(client.Filters)
        by_name = {f.Name.lower(): f for f in filters if f.Name}
        src = by_name.get(args.src.lower())
        dst = by_name.get(args.dst.lower())
        if src is None:
            print(f"未找到源过滤器: {args.src}")
            print(f"可用: {', '.join(sorted(f.Name for f in filters if f.Name))}")
            return 2
        if dst is None:
            print(f"未找到目标过滤器: {args.dst}")
            print(f"可用: {', '.join(sorted(f.Name for f in filters if f.Name))}")
            return 2

        src_rules = [r for r in client.Rules if str(r.FilterId) == str(src.Id)]
        dst_rules = [r for r in client.Rules if str(r.FilterId) == str(dst.Id)]
        dst_sigs = {signature(r) for r in dst_rules}
        dst_vals = {value_key(r): r for r in dst_rules}

        print(f"源: {src.Name} (InternalId={src.InternalId}, Id={src.Id}) -> 规则 {len(src_rules)} 条")
        print(f"目标: {dst.Name} (InternalId={dst.InternalId}, Id={dst.Id}) -> 规则 {len(dst_rules)} 条")
        print("-" * 96)

        to_create = []
        to_fix = []
        for r in src_rules:
            sig = signature(r)
            vk = value_key(r)
            if sig in dst_sigs:
                print(f"[跳过] 已存在完全相同的规则: {describe(r)}  (src={r.Id})")
                continue
            if vk in dst_vals:
                target = dst_vals[vk]
                print(f"[差异] 目标已有同值规则但条件不同:")
                print(f"       源: {describe(r)}  (src={r.Id})")
                print(f"       目标: {describe(target)}  (dst={target.Id})")
                if args.fix_conditions:
                    to_fix.append((r, target))
                    print(f"       -> 计划补齐时间条件")
                else:
                    print(f"       -> 需加 --fix-conditions 才会补齐")
                continue
            en = r.IsEnabled
            if args.enable_mode == "on":
                en = True
            elif args.enable_mode == "off":
                en = False
            to_create.append((r, en))
            print(f"[新建] {describe(r)}  enabled={en}  (src={r.Id})")

        print("-" * 96)
        print(f"计划: 新建 {len(to_create)} 条，补齐条件 {len(to_fix)} 条"
              f"{'' if args.apply else '  （dry-run，加 --apply 才写入）'}")

        if not args.apply:
            return 0

        created = 0
        for r, en in to_create:
            try:
                new_rule, _ = clone_rule(r, warn)
                added = client.AddRule(str(dst.Id), new_rule)
                try:
                    added.IsEnabled = en
                    client.UpdateRule(added)
                except Exception as e:
                    warn(f"设置启用状态失败 ({r.Id}): {e}")
                created += 1
                print(f"  [OK] 已创建: {describe(added)}  Id={added.Id}  enabled={en}")
            except NotImplementedError as e:
                warn(str(e))
            except Exception as e:
                warn(f"创建规则失败 ({r.Id}): {e}")

        fixed = 0
        for src_r, dst_r in to_fix:
            try:
                conds, _ = clone_conditions(src_r, warn)
                if not conds:
                    warn(f"源规则无可克隆条件，跳过: {src_r.Id}")
                    continue
                lst = List[RuleCondition]()
                for cd in conds:
                    lst.Add(cd)
                dst_r.Conditions = lst
                client.UpdateRule(dst_r)
                fixed += 1
                print(f"  [OK] 已补齐条件: {dst_r.Id} -> {describe(dst_r)}")
            except Exception as e:
                warn(f"补齐条件失败 ({dst_r.Id}): {e}")

        pruned = 0
        if args.prune_source:
            done_ids = {signature(r) for r in to_create}
            for r in src_rules:
                if signature(r) in done_ids or any(r is s for s, _ in to_create):
                    try:
                        client.RemoveRule(r)
                        pruned += 1
                        print(f"  [OK] 已删除源规则 {r.Id}")
                    except Exception as e:
                        warn(f"删除源规则失败 ({r.Id}): {e}")

        print("-" * 96)
        print(f"完成: 新建 {created} 条，补齐 {fixed} 条，删除源 {pruned} 条，警告 {len(warnings)} 条")
        return 0
    finally:
        try:
            client.Close()
        except Exception:
            pass


class _Tee:
    """把 stdout/stderr 同时写到控制台和日志文件（提权运行时不复用控制台）。"""

    def __init__(self, *streams):
        self._streams = streams

    def write(self, s):
        for st in self._streams:
            try:
                st.write(s)
            except Exception:
                pass
        return len(s)

    def flush(self):
        for st in self._streams:
            try:
                st.flush()
            except Exception:
                pass


def _pick_log_arg(argv):
    for i, a in enumerate(argv):
        if a == "--log" and i + 1 < len(argv):
            return argv[i + 1]
        if a.startswith("--log="):
            return a.split("=", 1)[1]
    return None


if __name__ == "__main__":
    _log_path = _pick_log_arg(sys.argv)
    if _log_path:
        import os

        _parent = os.path.dirname(os.path.abspath(_log_path))
        if _parent:
            os.makedirs(_parent, exist_ok=True)
        _f = open(_log_path, "w", encoding="utf-8")
        sys.stdout = _Tee(sys.__stdout__, _f)
        sys.stderr = _Tee(sys.__stderr__, _f)
        try:
            _code = main()
        finally:
            _f.flush()
            _f.close()
        sys.exit(_code)
    sys.exit(main())
