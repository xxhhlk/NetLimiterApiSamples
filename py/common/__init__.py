# Common modules for NetLimiter Speed Monitor

import os

# 路由器规则链总开关（router_sampler 采样 + rule_checker 判定）
ENV_ROUTER_RULE_ENABLED = "NL_ROUTER_RULE_ENABLED"

_TRUE_VALUES = ("1", "true", "yes", "on", "y")
_FALSE_VALUES = ("0", "false", "no", "off", "n")


def env_flag(name: str, default: bool = True) -> bool:
    """解析布尔环境变量；缺省或无法识别时返回 default"""
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    value = raw.strip().lower()
    if value in _TRUE_VALUES:
        return True
    if value in _FALSE_VALUES:
        return False
    return default
