"""
日志工具 - 与 PowerShell 版本日志格式兼容

特性：
- JSON 结构化日志
- 同时输出到控制台和文件
- 与 PowerShell 版本日志格式一致，便于 supervisor 解析
- 文件句柄缓存，减少 I/O 开销
- 自动日志轮转（按大小）
- 自动清理 3 天前的日志
- 自动压缩旧日志（gzip）
"""

import json
import os
import atexit
import gzip
import glob
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, TextIO


class Logger:
    """结构化日志记录器 - 带文件句柄缓存和自动轮转"""

    # 日志管理配置
    MAX_FILE_SIZE = 5 * 1024 * 1024  # 5MB，超过则轮转
    MAX_DAYS = 3  # 保留 3 天
    ROTATE_COUNT = 3  # 每个日期保留最近 3 个轮转文件

    def __init__(self, module_name: str, log_dir: Optional[Path] = None):
        """
        初始化日志记录器

        Args:
            module_name: 模块名称
            log_dir: 日志目录，默认为 py/logs
        """
        self.module_name = module_name
        self.pid = os.getpid()

        # 日志目录
        if log_dir is None:
            self.log_dir = Path(__file__).parent.parent / "logs"
        else:
            self.log_dir = Path(log_dir)

        self.log_dir.mkdir(parents=True, exist_ok=True)

        # 当前日期
        self._current_date = datetime.now().strftime("%Y%m%d")

        # 日志文件路径
        self.log_file = self.log_dir / f"{module_name}_{self._current_date}.log"
        self.alert_file = self.log_dir / f"alerts_{self._current_date}.log"

        # 文件句柄缓存
        self._log_fh: Optional[TextIO] = None
        self._alert_fh: Optional[TextIO] = None

        # 当前文件大小
        self._current_size = self._get_file_size(self.log_file)

        # 注册退出时关闭文件
        atexit.register(self.close)

        # 启动时清理旧日志
        self._cleanup_old_logs()

    def _get_file_size(self, filepath: Path) -> int:
        """获取文件大小"""
        try:
            return filepath.stat().st_size
        except:
            return 0

    def _check_rotation(self):
        """检查是否需要轮转日志文件"""
        today = datetime.now().strftime("%Y%m%d")

        # 日期变化，直接切换新文件
        if today != self._current_date:
            self.close()
            self._current_date = today
            self.log_file = self.log_dir / f"{self.module_name}_{today}.log"
            self.alert_file = self.log_dir / f"alerts_{today}.log"
            self._current_size = 0
            return

        # 检查大小是否超限
        if self._current_size >= self.MAX_FILE_SIZE:
            self._rotate_log()

    def _rotate_log(self):
        """执行日志轮转"""
        self.close()

        # 轮转现有文件: xxx.log -> xxx.log.1, xxx.log.1 -> xxx.log.2
        for i in range(self.ROTATE_COUNT - 1, 0, -1):
            old_file = self.log_file.parent / f"{self.log_file.name}.{i}"
            new_file = self.log_file.parent / f"{self.log_file.name}.{i + 1}"
            if old_file.exists():
                if i == self.ROTATE_COUNT - 1:
                    # 最旧的删除
                    old_file.unlink()
                else:
                    old_file.rename(new_file)

        # 当前文件重命名为 .1
        if self.log_file.exists():
            self.log_file.rename(self.log_file.parent / f"{self.log_file.name}.1")

        self._current_size = 0

    def _cleanup_old_logs(self):
        """清理 3 天前的日志文件"""
        try:
            cutoff_date = datetime.now() - timedelta(days=self.MAX_DAYS)

            # 查找所有日志文件
            patterns = [
                str(self.log_dir / f"{self.module_name}_*.log*"),
                str(self.log_dir / f"alerts_*.log*"),
            ]

            for pattern in patterns:
                for filepath in glob.glob(pattern):
                    path = Path(filepath)
                    # 从文件名提取日期
                    try:
                        # 格式: module_YYYYMMDD.log[.N]
                        parts = path.stem.split('_')
                        if len(parts) >= 2:
                            date_str = parts[-1].split('.')[0]  # 处理 .log.1 情况
                            if date_str.isdigit() and len(date_str) == 8:
                                file_date = datetime.strptime(date_str, "%Y%m%d")
                                if file_date < cutoff_date:
                                    path.unlink()
                                    print(f"[LOG] 删除过期日志: {path.name}")
                    except:
                        continue

            # 压缩 2-3 天前的日志（保留昨天的不压缩）
            self._compress_old_logs()

        except Exception:
            pass  # 清理失败不影响主功能

    def _compress_old_logs(self):
        """压缩 2 天前的日志"""
        try:
            compress_cutoff = datetime.now() - timedelta(days=2)

            patterns = [
                str(self.log_dir / f"{self.module_name}_*.log"),
                str(self.log_dir / f"alerts_*.log"),
            ]

            for pattern in patterns:
                for filepath in glob.glob(pattern):
                    path = Path(filepath)
                    try:
                        # 跳过轮转文件 (.log.1, .log.2)
                        if '.' in path.stem:
                            continue

                        parts = path.stem.split('_')
                        if len(parts) >= 2:
                            date_str = parts[-1]
                            if date_str.isdigit() and len(date_str) == 8:
                                file_date = datetime.strptime(date_str, "%Y%m%d")
                                if file_date < compress_cutoff:
                                    gz_path = path.with_suffix('.log.gz')
                                    with open(path, 'rb') as f_in:
                                        with gzip.open(gz_path, 'wb') as f_out:
                                            f_out.writelines(f_in)
                                    path.unlink()
                                    print(f"[LOG] 压缩旧日志: {path.name} -> {gz_path.name}")
                    except:
                        continue

        except Exception:
            pass

    def _get_log_fh(self) -> Optional[TextIO]:
        """获取日志文件句柄（自动处理日期切换和轮转）"""
        # 检查是否需要轮转
        self._check_rotation()

        # 延迟打开文件
        if self._log_fh is None or self._log_fh.closed:
            try:
                self._log_fh = open(self.log_file, 'a', encoding='utf-8', buffering=1)
            except Exception:
                return None

        return self._log_fh

    def _get_alert_fh(self) -> Optional[TextIO]:
        """获取告警文件句柄"""
        today = datetime.now().strftime("%Y%m%d")

        # 日期变化时重新打开文件
        if today != self._current_date:
            self.close()
            self._current_date = today
            self.log_file = self.log_dir / f"{self.module_name}_{today}.log"
            self.alert_file = self.log_dir / f"alerts_{today}.log"

        if self._alert_fh is None or self._alert_fh.closed:
            try:
                self._alert_fh = open(self.alert_file, 'a', encoding='utf-8', buffering=1)
            except Exception:
                return None

        return self._alert_fh

    def close(self):
        """关闭文件句柄"""
        if self._log_fh and not self._log_fh.closed:
            try:
                self._log_fh.close()
            except Exception:
                pass
            self._log_fh = None

        if self._alert_fh and not self._alert_fh.closed:
            try:
                self._alert_fh.close()
            except Exception:
                pass
            self._alert_fh = None

    def _write_log(self, level: str, message: str, event: str = "", reason: str = ""):
        """写入日志"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        # 结构化日志数据
        log_data = {
            "ts": timestamp,
            "module": self.module_name,
            "pid": self.pid,
            "event": event,
            "reason": reason,
            "message": message
        }

        json_log = json.dumps(log_data, ensure_ascii=False, default=str)

        # 控制台输出
        console_line = f"[{timestamp}][{self.module_name.upper()}]"
        if event:
            console_line += f" event={event}"
        if reason:
            console_line += f" reason={reason}"
        console_line += f" {message}"

        if level == "ERROR":
            print(f"\033[91m{console_line}\033[0m")
        elif level == "WARN":
            print(f"\033[93m{console_line}\033[0m")
        else:
            print(console_line)

        # 文件输出（使用缓存的文件句柄）
        fh = self._get_log_fh()
        if fh:
            try:
                fh.write(json_log + '\n')
                fh.flush()
                # 更新大小计数（估算）
                self._current_size += len(json_log) + 1
            except Exception:
                pass

    def info(self, message: str, event: str = "", reason: str = ""):
        """记录信息日志"""
        self._write_log("INFO", message, event, reason)

    def warn(self, message: str, event: str = "", reason: str = ""):
        """记录警告日志"""
        self._write_log("WARN", message, event, reason)

    def error(self, message: str, event: str = "", reason: str = ""):
        """记录错误日志"""
        self._write_log("ERROR", message, event, reason)

    def debug(self, message: str, event: str = "", reason: str = ""):
        """记录调试日志"""
        self._write_log("DEBUG", message, event, reason)

    def alert(self, alert_type: str, message: str, details: Optional[dict] = None):
        """记录告警"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        alert_data = {
            "ts": timestamp,
            "module": self.module_name,
            "pid": self.pid,
            "alert_type": alert_type,
            "message": message
        }
        if details:
            alert_data["details"] = details

        json_alert = json.dumps(alert_data, ensure_ascii=False, default=str)

        # 写入告警文件
        fh = self._get_alert_fh()
        if fh:
            try:
                fh.write(json_alert + '\n')
                fh.flush()
            except Exception:
                pass

        # 同时写入主日志
        self.warn(f"ALERT: {alert_type} - {message}", event="ALERT", reason=alert_type)
