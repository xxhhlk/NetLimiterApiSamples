#!/usr/bin/env python3
"""
NetLimiter 监控系统主模块

多进程架构：
- 单入口启动所有子模块
- 子模块独立进程运行，互不干扰
- 主进程监控子进程状态，自动重启失败的模块
- 信号处理器确保优雅关闭
- daemon=True 确保子进程随主进程退出
"""

import os
import sys
import time
import signal
import json
import multiprocessing as mp
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional


def disable_quick_edit_mode():
    """禁用Windows控制台快速编辑模式，避免选中文本时程序暂停"""
    try:
        import ctypes
        from ctypes import wintypes
        
        # Windows API 常量
        ENABLE_QUICK_EDIT_MODE = 0x0040
        ENABLE_EXTENDED_FLAGS = 0x0080
        
        # 获取标准输入句柄
        STD_INPUT_HANDLE = -10
        kernel32 = ctypes.windll.kernel32
        
        # 获取当前控制台模式
        mode = wintypes.DWORD()
        hstdin = kernel32.GetStdHandle(STD_INPUT_HANDLE)
        kernel32.GetConsoleMode(hstdin, ctypes.byref(mode))
        
        # 禁用快速编辑模式
        new_mode = mode.value & ~ENABLE_QUICK_EDIT_MODE
        kernel32.SetConsoleMode(hstdin, new_mode | ENABLE_EXTENDED_FLAGS)
    except Exception:
        # 非Windows系统或失败时静默处理
        pass


# 在程序启动时禁用快速编辑模式
disable_quick_edit_mode()

# 添加当前目录到路径
sys.path.insert(0, str(Path(__file__).parent))

from common.logger import Logger


# 模块级别函数 - 用于 multiprocessing 启动（避免序列化问题）
def _run_speed_sampler():
    """运行速度采样器"""
    from speed_sampler import SpeedSampler
    sampler = SpeedSampler()
    sampler.run()


def _run_router_sampler():
    """运行路由器速度采样器"""
    from router_sampler import RouterSpeedSampler
    sampler = RouterSpeedSampler()
    sampler.run()


def _run_rule_checker():
    """运行规则检查器"""
    from rule_checker import RuleChecker
    checker = RuleChecker()
    checker.run()


class ModuleProcess:
    """模块进程管理器"""
    
    def __init__(self, name: str, target_func, logger: Logger):
        self.name = name
        self.target_func = target_func
        self.logger = logger
        self.process: Optional[mp.Process] = None
        self.start_time: Optional[datetime] = None
        self.restart_count = 0
    
    def start(self):
        """启动进程"""
        # 设置 SUPERVISOR_PID 环境变量，子进程通过它检测父进程是否存活
        # multiprocessing.Process 会继承父进程环境变量
        os.environ["SUPERVISOR_PID"] = str(os.getpid())

        self.process = mp.Process(
            target=self.target_func,
            name=self.name,
            daemon=True  # 确保随主进程退出
        )
        self.process.start()
        self.start_time = datetime.now()
        self.logger.info(
            f"启动模块: {self.name}, PID: {self.process.pid}, SupervisorPID: {os.getpid()}",
            event="MODULE_START"
        )
    
    def is_alive(self) -> bool:
        """检查进程是否存活"""
        return self.process is not None and self.process.is_alive()
    
    def terminate(self, timeout: float = 3.0):
        """终止进程"""
        if self.process is not None and self.process.is_alive():
            self.logger.info(f"终止模块: {self.name}", event="MODULE_TERMINATE")
            self.process.terminate()
            self.process.join(timeout=timeout)

            # 如果 terminate 无效，使用 kill
            if self.process.is_alive():
                self.logger.warn(f"强制终止模块: {self.name}", event="MODULE_KILL")
                self.process.kill()
                self.process.join(timeout=1)

            # 如果 kill 仍无效（进程卡在 CLR/native 调用里），用 taskkill /F 强制终止
            if self.process.is_alive():
                import subprocess
                pid = self.process.pid
                self.logger.warn(f"taskkill 强制终止模块: {self.name} (pid={pid})", event="MODULE_TASKKILL")
                try:
                    subprocess.run(["taskkill", "/F", "/PID", str(pid)],
                                   capture_output=True, timeout=5)
                    self.process.join(timeout=2)
                except Exception as e:
                    self.logger.error(f"taskkill 失败: {e}", event="MODULE_TASKKILL_FAIL")
    
    def restart(self):
        """重启进程"""
        self.terminate()
        self.restart_count += 1
        self.start()


class Supervisor:
    """进程监控器"""

    # 健康检查参数
    HEARTBEAT_TIMEOUT_SECONDS = 10   # 心跳超过该时长未更新视为卡住
    HEARTBEAT_GRACE_SECONDS = 30     # 模块启动后的宽限期（首个心跳尚未落盘）
    HEALTH_FAIL_TOLERANCE = 2        # 连续 N 次健康检查失败才重启（避免单次抖动误杀）

    # 模块配置
    MODULES = {
        "speed_sampler": {
            "description": "速度采样器",
            "restart_delay": 5,
            "max_restarts": 10,
            # 二级存活信号：主循环产出的数据文件（心跳线程活但主循环假死时靠它发现）
            "data_file": "qb_speed_data.json",
            "data_stale_seconds": 60,
        },
        "router_sampler": {
            "description": "路由器速度采样器",
            "restart_delay": 5,
            "max_restarts": 10,
            "data_file": "router_speed_data.json",
            "data_stale_seconds": 60,
        },
        "rule_checker": {
            "description": "规则检查器",
            "restart_delay": 5,
            "max_restarts": 10,
            "data_file": None,  # 无数据文件产出，仅用心跳
        },
    }
    
    def __init__(self):
        self.logger = Logger("supervisor")
        self.modules: Dict[str, ModuleProcess] = {}
        self.running = True
        self.monitor_interval = 12  # 监控间隔（秒）

        # 健康检查连续失败计数（按模块）
        self._health_failures: Dict[str, int] = {}
        # 心跳写入失败的已告警次数（避免刷屏）
        self._hb_write_warned: Dict[str, int] = {}
        # 数据文件陈旧已告警标记
        self._data_stale_warned: set = set()
        
        # 设置信号处理器
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        self.logger.info("Supervisor 初始化完成", event="INIT_OK")
    
    def _signal_handler(self, signum, frame):
        """信号处理器 - 优雅关闭"""
        sig_name = signal.Signals(signum).name
        self.logger.info(f"收到信号 {sig_name}，准备关闭所有模块...", event="SIGNAL")
        self.running = False
    
    def _get_module_target(self, name: str):
        """获取模块的目标函数"""
        # 使用模块级别函数，避免序列化实例方法
        targets = {
            "speed_sampler": _run_speed_sampler,
            "router_sampler": _run_router_sampler,
            "rule_checker": _run_rule_checker,
        }
        return targets.get(name)
    
    def start_all(self):
        """启动所有模块"""
        self.logger.info("启动所有模块...", event="START_ALL")
        
        for name, config in self.MODULES.items():
            try:
                target = self._get_module_target(name)
                if target:
                    module = ModuleProcess(name, target, self.logger)
                    module.start()
                    self.modules[name] = module
                    # 使用信号安全的等待方式
                    for _ in range(10):  # 1秒分10次等待，避免被信号中断
                        time.sleep(0.1)
            except Exception as e:
                self.logger.error(f"启动模块 {name} 失败: {e}", event="MODULE_START_ERROR")
        
        self.logger.info(f"所有模块已启动，共 {len(self.modules)} 个", event="STARTED_ALL")
    
    def stop_all(self):
        """停止所有模块"""
        self.logger.info("停止所有模块...", event="STOP_ALL")
        
        for name, module in self.modules.items():
            module.terminate()
        
        self.logger.info("所有模块已停止", event="STOPPED_ALL")
    
    @staticmethod
    def _heartbeat_dir() -> Path:
        return Path(os.environ.get('TEMP', '.')) / "nl_watchdog"

    @staticmethod
    def _module_uptime(module: ModuleProcess) -> float:
        """模块已运行秒数"""
        if module.start_time is None:
            return 0.0
        return (datetime.now() - module.start_time).total_seconds()

    def _check_heartbeat(self, module_name: str, module: ModuleProcess) -> bool:
        """
        检查模块心跳

        失败闭（fail-closed）：心跳文件缺失/过期/损坏一律视为异常。
        历史教训：router_sampler 曾整体假死 22 小时（主线程卡在原生调用里、心跳线程
        随 GIL 一起停），而旧实现"文件不存在 → 认为正常"导致 supervisor 完全无感。

        Args:
            module_name: 模块名称
            module: 模块进程管理器（用于启动宽限期判断）

        Returns:
            True: 心跳正常；False: 心跳异常
        """
        # 启动宽限期内不判失败（进程刚起，首个心跳可能尚未落盘）
        if self._module_uptime(module) < self.HEARTBEAT_GRACE_SECONDS:
            return True

        try:
            heartbeat_file = self._heartbeat_dir() / f"{module_name}.heartbeat.json"

            if not heartbeat_file.exists():
                return False

            data = json.loads(heartbeat_file.read_text(encoding='utf-8'))

            # 主动退出（优雅停止）不算卡死，进程存活与否由 is_alive 判定
            if data.get("status") == "STOPPED":
                return True

            # 心跳自身写入失败（如 %TEMP%\nl_watchdog 被清理/权限异常）——安全网异常，必须暴露
            write_errors = data.get("write_errors", 0) or 0
            if write_errors > 0 and self._hb_write_warned.get(module_name, 0) < write_errors:
                self._hb_write_warned[module_name] = write_errors
                self.logger.warn(
                    f"模块 {module_name} 心跳写入累计失败 {write_errors} 次，"
                    f"最近错误: {data.get('write_error', '?')}",
                    event="HEARTBEAT_WRITE_ERROR"
                )

            last_ok_str = data.get("last_ok")
            if not last_ok_str:
                return False

            last_ok = datetime.fromisoformat(last_ok_str)
            elapsed = (datetime.now() - last_ok).total_seconds()

            return elapsed < self.HEARTBEAT_TIMEOUT_SECONDS

        except Exception:
            # 读取/解析失败一律视为异常（fail-closed）
            return False

    def _check_data_file(self, module_name: str, module: ModuleProcess) -> bool:
        """
        二级存活信号：模块主循环产出的数据文件是否仍在更新

        心跳线程与主循环可能"不同步地"卡住：主循环阻塞在原生/网络调用而 GIL 被释放时，
        心跳照常更新，但模块已不再产出数据。此时只有数据文件 mtime 能发现问题。

        Returns:
            True: 数据文件新鲜或该模块无数据文件；False: 停止更新
        """
        config = self.MODULES.get(module_name, {})
        file_name = config.get("data_file")
        if not file_name:
            return True

        # 启动宽限期内不做判断
        if self._module_uptime(module) < self.HEARTBEAT_GRACE_SECONDS:
            return True

        stale_seconds = config.get("data_stale_seconds", 60)
        data_file = Path(os.environ.get('TEMP', '.')) / file_name

        try:
            if not data_file.exists():
                # 宽限期已过仍无数据文件 = 模块没在干活
                return False

            age = time.time() - data_file.stat().st_mtime
            if age > stale_seconds:
                if module_name not in self._data_stale_warned:
                    self._data_stale_warned.add(module_name)
                    self.logger.warn(
                        f"模块 {module_name} 数据文件 {file_name} 已 {age:.0f}s 未更新 "
                        f"(上限 {stale_seconds}s)，疑似主循环卡住",
                        event="MODULE_DATA_STALE"
                    )
                return False
            return True
        except Exception:
            # 无法读取时保守放行，避免误杀
            return True

    def monitor(self):
        """监控循环 - 同时检查进程状态、心跳状态与数据文件新鲜度"""
        self.logger.info(f"开始监控，间隔 {self.monitor_interval} 秒", event="MONITOR_START")

        # 给模块一些启动时间，首次不检查心跳
        heartbeat_check_delay = 15  # 15秒后开始心跳检查
        start_time = time.time()

        while self.running:
            try:
                for name, module in self.modules.items():
                    process_alive = module.is_alive()
                    reason = ""

                    if not process_alive:
                        reason = "进程已退出"
                    elif (time.time() - start_time) > heartbeat_check_delay:
                        failures = []
                        if not self._check_heartbeat(name, module):
                            failures.append("心跳超时(可能卡住)")
                        if not self._check_data_file(name, module):
                            failures.append("数据文件停止更新(主循环卡住)")

                        if not failures:
                            # 全部正常，清零连续失败计数
                            self._health_failures[name] = 0
                            continue

                        count = self._health_failures.get(name, 0) + 1
                        self._health_failures[name] = count
                        reason = " + ".join(failures)

                        if count < self.HEALTH_FAIL_TOLERANCE:
                            # 先容忍一次，下一轮仍异常才重启（避免单次抖动误杀）
                            self.logger.warn(
                                f"模块 {name} 健康检查异常: {reason} "
                                f"(连续 {count}/{self.HEALTH_FAIL_TOLERANCE})",
                                event="MODULE_UNHEALTHY"
                            )
                            continue
                    else:
                        continue

                    # 走到这里说明需要重启
                    config = self.MODULES.get(name, {})
                    max_restarts = config.get("max_restarts", 10)
                    restart_delay = config.get("restart_delay", 5)

                    if module.restart_count < max_restarts:
                        self.logger.warn(
                            f"模块 {name} {reason}，{restart_delay} 秒后重启 "
                            f"(重启次数: {module.restart_count + 1}/{max_restarts})",
                            event="MODULE_RESTART",
                            reason=reason
                        )
                        time.sleep(restart_delay)
                        module.restart()
                        # 重启后重置健康状态跟踪
                        self._health_failures[name] = 0
                        self._data_stale_warned.discard(name)
                    else:
                        self.logger.error(
                            f"模块 {name} 重启次数已达上限 {max_restarts}，不再重启",
                            event="MODULE_MAX_RESTARTS"
                        )

                time.sleep(self.monitor_interval)

            except Exception as e:
                self.logger.error(f"监控异常: {e}", event="MONITOR_ERROR")
                time.sleep(1)

    
    def run(self):
        """主运行方法"""
        self.logger.info("=" * 50, event="BANNER")
        self.logger.info("NetLimiter 监控系统启动", event="BANNER")
        self.logger.info("=" * 50, event="BANNER")
        
        try:
            self.start_all()
            self.logger.info("进入监控循环...", event="ENTER_MONITOR")
            self.monitor()
        except KeyboardInterrupt:
            self.logger.info("用户中断", event="USER_INTERRUPT")
        except Exception as e:
            self.logger.error(f"运行异常: {e}", event="RUN_ERROR", reason=str(e))
        finally:
            self.stop_all()
            self.logger.info("Supervisor 退出", event="EXIT")


def main():
    """主入口"""
    # Windows 多进程支持
    mp.freeze_support()

    supervisor = Supervisor()
    supervisor.run()


if __name__ == "__main__":
    main()
