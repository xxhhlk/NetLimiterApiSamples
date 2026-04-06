"""
心跳管理器 - 独立线程运行，不受主线程阻塞影响

这是解决 PowerShell 版本问题的核心：
- 心跳更新在独立线程中运行
- 即使主线程被 NetLimiter API 或 SSH 阻塞，心跳也能正常更新
- 与 PowerShell 版本的心跳文件格式完全兼容
"""

import threading
import time
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Optional


class HeartbeatManager:
    """独立心跳线程管理器"""
    
    def __init__(self, module_name: str, interval: float = 2.0):
        """
        初始化心跳管理器
        
        Args:
            module_name: 模块名称 (speed_sampler, router_speed_sampler, rule_checker)
            interval: 心跳更新间隔（秒），默认 2 秒
        """
        self.module_name = module_name
        self.interval = interval
        
        # 心跳状态
        self._status = "STARTING"
        self._error: Optional[str] = None
        self._loop_count = 0
        self._running = True
        
        # 心跳文件路径 (与 PowerShell 版本兼容)
        heartbeat_dir = Path(os.environ.get('TEMP', '.')) / "nl_watchdog"
        heartbeat_dir.mkdir(parents=True, exist_ok=True)
        self.heartbeat_file = heartbeat_dir / f"{module_name}.heartbeat.json"
        
        # 线程锁（保证线程安全）
        self._lock = threading.Lock()
        
        # 立即写入初始心跳
        self._write_heartbeat()
        
        # 启动心跳线程
        self._thread = threading.Thread(
            target=self._heartbeat_loop, 
            daemon=True,
            name=f"heartbeat-{module_name}"
        )
        self._thread.start()
    
    def _heartbeat_loop(self):
        """心跳线程主循环 - 独立于主线程运行"""
        while self._running:
            self._write_heartbeat()
            time.sleep(self.interval)
    
    def _write_heartbeat(self):
        """写入心跳文件"""
        with self._lock:
            data = {
                "module": self.module_name,
                "pid": os.getpid(),
                "last_ok": datetime.now().isoformat(),
                "loop_count": self._loop_count,
                "status": self._status
            }
            if self._error:
                data["error"] = self._error

            # 原子写入：先写临时文件，再替换（在锁保护下执行）
            try:
                tmp_file = str(self.heartbeat_file) + ".tmp"
                with open(tmp_file, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False)
                os.replace(tmp_file, self.heartbeat_file)
            except Exception:
                pass  # 忽略写入错误，避免影响主线程
    
    def update_status(self, status: str, error: Optional[str] = None):
        """
        更新状态（线程安全）
        
        Args:
            status: 状态值 (OK, STARTING, RECONNECTING, ERROR)
            error: 可选的错误信息
        """
        with self._lock:
            self._status = status
            self._error = error
    
    def increment_loop(self):
        """增加循环计数（线程安全）"""
        with self._lock:
            self._loop_count += 1
    
    def get_status(self) -> str:
        """获取当前状态"""
        with self._lock:
            return self._status
    
    def stop(self):
        """停止心跳线程"""
        self._running = False
    
    def is_running(self) -> bool:
        """检查心跳线程是否在运行"""
        return self._running and self._thread.is_alive()
