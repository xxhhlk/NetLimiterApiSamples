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
        self.process = mp.Process(
            target=self.target_func,
            name=self.name,
            daemon=True  # 确保随主进程退出
        )
        self.process.start()
        self.start_time = datetime.now()
        self.logger.info(
            f"启动模块: {self.name}, PID: {self.process.pid}",
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
    
    def restart(self):
        """重启进程"""
        self.terminate()
        self.restart_count += 1
        self.start()


class Supervisor:
    """进程监控器"""
    
    # 模块配置
    MODULES = {
        "speed_sampler": {
            "description": "速度采样器",
            "restart_delay": 5,
            "max_restarts": 10,
        },
        "router_sampler": {
            "description": "路由器速度采样器",
            "restart_delay": 5,
            "max_restarts": 10,
        },
        "rule_checker": {
            "description": "规则检查器",
            "restart_delay": 5,
            "max_restarts": 10,
        },
    }
    
    def __init__(self):
        self.logger = Logger("supervisor")
        self.modules: Dict[str, ModuleProcess] = {}
        self.running = True
        self.monitor_interval = 5  # 监控间隔（秒）
        
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
    
    def _check_heartbeat(self, module_name: str, timeout_seconds: int = 10) -> bool:
        """
        检查模块心跳是否超时

        Args:
            module_name: 模块名称
            timeout_seconds: 心跳超时时间（秒）

        Returns:
            True: 心跳正常
            False: 心跳超时或文件不存在
        """
        try:
            heartbeat_dir = Path(os.environ.get('TEMP', '.')) / "nl_watchdog"
            heartbeat_file = heartbeat_dir / f"{module_name}.heartbeat.json"

            if not heartbeat_file.exists():
                # 心跳文件不存在，可能是模块刚启动还没写入
                return True

            data = json.loads(heartbeat_file.read_text(encoding='utf-8'))
            last_ok_str = data.get("last_ok")

            if not last_ok_str:
                return False

            last_ok = datetime.fromisoformat(last_ok_str)
            elapsed = (datetime.now() - last_ok).total_seconds()

            return elapsed < timeout_seconds

        except Exception:
            # 读取失败时保守处理，认为心跳正常
            return True

    def monitor(self):
        """监控循环 - 同时检查进程状态和心跳状态"""
        self.logger.info(f"开始监控，间隔 {self.monitor_interval} 秒", event="MONITOR_START")

        # 给模块一些启动时间，首次不检查心跳
        heartbeat_check_delay = 15  # 15秒后开始心跳检查
        start_time = time.time()

        while self.running:
            try:
                for name, module in self.modules.items():
                    process_alive = module.is_alive()
                    heartbeat_ok = True

                    # 进程存在时才检查心跳
                    if process_alive and (time.time() - start_time) > heartbeat_check_delay:
                        heartbeat_ok = self._check_heartbeat(name)

                    # 进程退出或心跳超时都需要重启
                    if not process_alive or not heartbeat_ok:
                        config = self.MODULES.get(name, {})
                        max_restarts = config.get("max_restarts", 10)
                        restart_delay = config.get("restart_delay", 5)

                        if not process_alive:
                            reason = "进程已退出"
                        else:
                            reason = "心跳超时（可能卡住）"

                        if module.restart_count < max_restarts:
                            self.logger.warn(
                                f"模块 {name} {reason}，{restart_delay} 秒后重启 "
                                f"(重启次数: {module.restart_count + 1}/{max_restarts})",
                                event="MODULE_RESTART",
                                reason=reason
                            )
                            time.sleep(restart_delay)
                            module.restart()
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
