import logging
import logging.handlers
import os
import asyncio
import signal
import json
import csv
from datetime import datetime
from threading import Lock, RLock
from typing import Optional, Dict, Any, List, Set, Callable
import atexit
from collections import deque
import time
import psutil
import weakref
from pathlib import Path
import contextlib
import sys
from concurrent.futures import ThreadPoolExecutor
import queue
from dataclasses import dataclass, field


# ============================================================================
# Configuration
# ============================================================================

@dataclass
class LoggerConfig:
    """Immutable configuration for logging system"""
    
    # Directory setup
    log_dir: str = field(default_factory=lambda: str(Path.cwd() / "log"))
    csv_dir: str = field(default_factory=lambda: str(Path.cwd() / "csv"))
    metrics_dir: str = field(default_factory=lambda: str(Path.cwd() / "metrics"))
    
    # Core settings - computed dynamically
    queue_maxsize: int = field(init=False)
    flush_timeout: float = field(default_factory=lambda: float(os.getenv("LOG_FLUSH_TIMEOUT", "2.0")))
    worker_timeout: float = field(default_factory=lambda: float(os.getenv("LOG_WORKER_TIMEOUT", "0.05")))
    batch_size: int = field(default_factory=lambda: int(os.getenv("LOG_BATCH_SIZE", "10")))
    metrics_interval: int = field(default_factory=lambda: int(os.getenv("LOG_METRICS_INTERVAL", "10000")))
    
    # Feature flags
    enable_structured_logging: bool = field(default_factory=lambda: os.getenv("LOG_STRUCTURED", "false").lower() == "true")
    enable_metrics_export: bool = field(default_factory=lambda: os.getenv("LOG_METRICS_EXPORT", "true").lower() == "true")
    enable_batch_processing: bool = field(default_factory=lambda: os.getenv("LOG_BATCH_MODE", "true").lower() == "true")
    enable_durability_mode: bool = field(default_factory=lambda: os.getenv("LOG_DURABILITY", "false").lower() == "true")
    enable_async_console: bool = field(default_factory=lambda: os.getenv("LOG_ASYNC_CONSOLE", "true").lower() == "true")
    
    # Durability settings
    force_flush_interval: float = field(default_factory=lambda: float(os.getenv("LOG_FORCE_FLUSH_INTERVAL", "1.0")))
    critical_log_sync: bool = field(default_factory=lambda: os.getenv("LOG_CRITICAL_SYNC", "true").lower() == "true")
    
    # Console settings
    console_queue_size: int = field(default_factory=lambda: int(os.getenv("LOG_CONSOLE_QUEUE", "1000")))
    console_timeout: float = field(default_factory=lambda: float(os.getenv("LOG_CONSOLE_TIMEOUT", "0.01")))
    
    def __post_init__(self):
        # Dynamic sizing based on system memory
        available_memory_gb = psutil.virtual_memory().available / (1024**3)
        base_queue_size = min(int(available_memory_gb * 2000), 25000)
        object.__setattr__(self, 'queue_maxsize', int(os.getenv("LOG_QUEUE_SIZE", str(base_queue_size))))
        
        # Create directories
        for dir_path in [self.log_dir, self.csv_dir, self.metrics_dir]:
            Path(dir_path).mkdir(parents=True, exist_ok=True)
        
        # Strategy-specific log levels
        self._strategy_log_levels = {
            "STRA_1": getattr(logging, os.getenv("STRA_1_LOG_LEVEL", "INFO").upper()),
            "STRA_2": getattr(logging, os.getenv("STRA_2_LOG_LEVEL", "INFO").upper()),
            "default": getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper())
        }
    
    def get_strategy_level(self, strategy_id: str) -> int:
        return self._strategy_log_levels.get(strategy_id, self._strategy_log_levels['default'])
    
    def set_strategy_level(self, strategy_id: str, level: int):
        self._strategy_log_levels[strategy_id] = level


# Global configuration instance
config = LoggerConfig()


# ============================================================================
# Enhanced Metrics System
# ============================================================================

class LogMetrics:
    """Thread-safe metrics with enhanced durability tracking"""
    
    def __init__(self):
        self._lock = RLock()  # Re-entrant lock for nested calls
        
        # Core counters
        self.logs_processed = 0
        self.logs_dropped = 0
        self.logs_forced_sync = 0
        self.worker_errors = 0
        self.flush_timeouts = 0
        self.cross_thread_calls = 0
        self.batch_operations = 0
        self.json_validation_errors = 0
        self.dynamic_level_changes = 0
        self.console_drops = 0
        self.durability_syncs = 0
        
        # Performance tracking
        self.start_time = time.time()
        self.last_metrics_log = 0
        self.last_csv_export = 0
        self.last_force_flush = 0
        self.avg_batch_size = 0.0
        self.max_queue_size = 0
        self.worker_cpu_time = 0.0
        self.total_flush_time = 0.0
    
    def increment(self, counter: str, value: int = 1) -> None:
        """Thread-safe counter increment"""
        with self._lock:
            setattr(self, counter, getattr(self, counter, 0) + value)
    
    def update_batch_stats(self, batch_size: int) -> None:
        with self._lock:
            self.batch_operations += 1
            self.avg_batch_size = ((self.avg_batch_size * (self.batch_operations - 1)) + batch_size) / self.batch_operations
    
    def update_queue_size(self, size: int) -> None:
        with self._lock:
            self.max_queue_size = max(self.max_queue_size, size)
    
    def should_log_metrics(self) -> bool:
        with self._lock:
            return (self.logs_processed - self.last_metrics_log) >= config.metrics_interval
    
    def should_export_csv(self) -> bool:
        with self._lock:
            return config.enable_metrics_export and (time.time() - self.last_csv_export) >= 300
    
    def should_force_flush(self) -> bool:
        with self._lock:
            return config.enable_durability_mode and (time.time() - self.last_force_flush) >= config.force_flush_interval
    
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            uptime = time.time() - self.start_time
            processed = max(self.logs_processed, 1)  # Avoid division by zero
            
            return {
                "logs_processed": self.logs_processed,
                "logs_dropped": self.logs_dropped,
                "logs_forced_sync": self.logs_forced_sync,
                "worker_errors": self.worker_errors,
                "flush_timeouts": self.flush_timeouts,
                "cross_thread_calls": self.cross_thread_calls,
                "batch_operations": self.batch_operations,
                "json_validation_errors": self.json_validation_errors,
                "dynamic_level_changes": self.dynamic_level_changes,
                "console_drops": self.console_drops,
                "durability_syncs": self.durability_syncs,
                "avg_batch_size": round(self.avg_batch_size, 2),
                "max_queue_size": self.max_queue_size,
                "uptime_hours": round(uptime / 3600, 2),
                "logs_per_second": round(self.logs_processed / max(uptime, 1), 2),
                "error_rate_pct": round((self.worker_errors / processed) * 100, 4),
                "drop_rate_pct": round((self.logs_dropped / processed) * 100, 4),
                "avg_flush_time_ms": round((self.total_flush_time / max(self.durability_syncs, 1)) * 1000, 2)
            }
    
    def export_to_csv(self) -> None:
        if not config.enable_metrics_export:
            return
        
        with self._lock:
            csv_file = Path(config.metrics_dir) / f"log_metrics_{datetime.now().strftime('%Y-%m-%d')}.csv"
            metrics_data = self.to_dict()
            metrics_data["timestamp"] = datetime.now().isoformat()
            
            try:
                file_exists = csv_file.exists()
                with open(csv_file, 'a', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=metrics_data.keys())
                    if not file_exists:
                        writer.writeheader()
                    writer.writerow(metrics_data)
                
                self.last_csv_export = time.time()
            except Exception as e:
                # Failsafe - don't let metrics export crash the logger
                sys.stderr.write(f"[LOGGER] Metrics export error: {e}\n")


# Global metrics instance
metrics = LogMetrics()


# ============================================================================
# Enhanced Formatters
# ============================================================================

def validate_json_log(log_entry: Dict[str, Any]) -> bool:
    """Enhanced validation for structured logs"""
    if not config.enable_structured_logging:
        return True
    
    required_fields = ["timestamp", "level", "message"]
    try:
        return all(field in log_entry and log_entry[field] is not None for field in required_fields)
    except Exception:
        metrics.increment("json_validation_errors")
        return False


class EnhancedStructuredFormatter(logging.Formatter):
    """High-performance JSON structured logging with better error handling"""
    
    def format(self, record: logging.LogRecord) -> str:
        try:
            log_entry = {
                "timestamp": datetime.fromtimestamp(record.created).isoformat(),
                "level": record.levelname,
                "logger": record.name,
                "message": record.getMessage(),
                "module": getattr(record, 'module', 'unknown'),
                "function": getattr(record, 'funcName', 'unknown'),
                "line": getattr(record, 'lineno', 0),
                "thread_id": record.thread,
                "process_id": record.process
            }
            
            # Add exception info if present
            if record.exc_info:
                log_entry["exception"] = self.formatException(record.exc_info)
            
            # Add trading context if available
            for attr in ['strategy_id', 'order_id', 'symbol', 'trade_no', 'market_data']:
                if hasattr(record, attr):
                    value = getattr(record, attr)
                    if value is not None:
                        log_entry[attr] = value
            
            if validate_json_log(log_entry):
                return json.dumps(log_entry, default=str, ensure_ascii=False)
            else:
                # Fallback to simple format
                return f"{log_entry['timestamp']} [{log_entry['level']}] {log_entry['message']}"
                
        except Exception as e:
            # Emergency fallback - never let formatting crash
            return f"{datetime.now().isoformat()} [ERROR] Formatter error: {e} | Original: {record.getMessage()}"


class OptimizedTradingFormatter(logging.Formatter):
    """Optimized formatter for HFT systems with microsecond precision"""
    
    def __init__(self):
        # Pre-compile format for performance
        super().__init__('[%(asctime)s.%(msecs)03d] [%(levelname)-5s] [%(name)s] %(message)s', 
                         datefmt='%H:%M:%S')
        self._start_time = time.time()
    
    def formatTime(self, record, datefmt=None):
        """Optimized time formatting"""
        ct = datetime.fromtimestamp(record.created)
        return ct.strftime('%H:%M:%S')


# ============================================================================
# Async Console Handler
# ============================================================================

class AsyncConsoleWriter:
    """Non-blocking console writer to prevent stdout blocking"""
    
    def __init__(self):
        self._console_queue = asyncio.Queue(maxsize=config.console_queue_size)
        self._writer_task: Optional[asyncio.Task] = None
        self._shutdown = False
        
    async def start(self):
        """Start the console writer task"""
        if self._writer_task is None or self._writer_task.done():
            self._writer_task = asyncio.create_task(self._console_writer())
    
    async def write(self, message: str) -> bool:
        """Write message to console queue"""
        try:
            self._console_queue.put_nowait(message)
            return True
        except asyncio.QueueFull:
            metrics.increment("console_drops")
            return False
    
    async def _console_writer(self):
        """Background console writer"""
        while not self._shutdown:
            try:
                message = await asyncio.wait_for(
                    self._console_queue.get(), 
                    timeout=config.console_timeout
                )
                
                # Write to stdout with immediate flush
                sys.stdout.write(message)
                sys.stdout.flush()
                self._console_queue.task_done()
                
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                sys.stderr.write(f"[LOGGER] Console writer error: {e}\n")
                await asyncio.sleep(0.001)
    
    async def flush(self, timeout: float = 1.0):
        """Flush console queue"""
        try:
            await asyncio.wait_for(self._console_queue.join(), timeout=timeout)
        except asyncio.TimeoutError:
            pass
    
    def shutdown(self):
        """Shutdown console writer"""
        self._shutdown = True
        if self._writer_task and not self._writer_task.done():
            self._writer_task.cancel()


# ============================================================================
# Enhanced Async Logging Handler
# ============================================================================

class EnhancedAsyncLoggingHandler(logging.Handler):
    """Production-ready async logging handler with durability and anti-duplicate measures"""
    
    # Class-level registry to prevent duplicate handlers
    _instances: Dict[str, 'EnhancedAsyncLoggingHandler'] = {}
    _instances_lock = Lock()
    
    def __new__(cls, name: str = "default"):
        """Singleton per name to prevent duplicate handlers"""
        with cls._instances_lock:
            if name not in cls._instances:
                instance = super().__new__(cls)
                cls._instances[name] = instance
            return cls._instances[name]
    
    def __init__(self, name: str = "default"):
        if hasattr(self, '_initialized'):
            return
            
        super().__init__()
        self.name = name
        self.queue: Optional[asyncio.Queue] = None
        self._worker_task: Optional[asyncio.Task] = None
        self._shutdown_event: Optional[asyncio.Event] = None
        self._worker_started = False
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._recent_errors = deque(maxlen=50)
        self._console_writer: Optional[AsyncConsoleWriter] = None
        self._file_handlers: List[logging.Handler] = []
        self._sync_file_lock = Lock()  # For synchronous critical log writes
        self._logger_registry: Set[str] = set()  # Track attached loggers
        self._initialized = True
        
        # Setup handlers
        self._setup_file_handlers()
        if config.enable_async_console:
            self._console_writer = AsyncConsoleWriter()
        
        # Register cleanup
        atexit.register(self._cleanup_sync)
        self._setup_signal_handlers()
    
    def _setup_file_handlers(self):
        """Setup file handlers with rotation"""
        try:
            # Main log file with rotation
            main_log_file = Path(config.log_dir) / "hft_strategy.log"
            if hasattr(logging.handlers, 'TimedRotatingFileHandler'):
                main_handler = logging.handlers.TimedRotatingFileHandler(
                    filename=str(main_log_file),
                    when='midnight', interval=1, backupCount=7, encoding='utf-8'
                )
            else:
                today_str = datetime.now().strftime("%Y-%m-%d")
                log_file = Path(config.log_dir) / f"hft_strategy_{today_str}.log"
                main_handler = logging.FileHandler(str(log_file), mode='a', encoding='utf-8')
            
            # Choose formatter
            formatter = (EnhancedStructuredFormatter() if config.enable_structured_logging 
                        else OptimizedTradingFormatter())
            
            main_handler.setFormatter(formatter)
            main_handler.setLevel(logging.DEBUG)
            self._file_handlers.append(main_handler)
            
            # Critical errors log (always sync)
            if config.enable_durability_mode:
                critical_log = Path(config.log_dir) / "critical_errors.log"
                critical_handler = logging.FileHandler(str(critical_log), mode='a', encoding='utf-8')
                critical_handler.setFormatter(formatter)
                critical_handler.setLevel(logging.ERROR)
                critical_handler.addFilter(lambda record: record.levelno >= logging.ERROR)
                self._file_handlers.append(critical_handler)
            
        except Exception as e:
            sys.stderr.write(f"[LOGGER] Handler setup error: {e}\n")
    
    def attach_to_logger(self, logger_name: str) -> bool:
        """Safely attach handler to logger, preventing duplicates"""
        if logger_name in self._logger_registry:
            return False  # Already attached
        
        logger = logging.getLogger(logger_name)
        
        # Check if this handler is already attached
        if self in logger.handlers:
            return False
        
        logger.addHandler(self)
        self._logger_registry.add(logger_name)
        return True
    
    def detach_from_logger(self, logger_name: str) -> bool:
        """Safely detach handler from logger"""
        if logger_name not in self._logger_registry:
            return False
        
        logger = logging.getLogger(logger_name)
        if self in logger.handlers:
            logger.removeHandler(self)
        
        self._logger_registry.discard(logger_name)
        return True
    
    def set_strategy_log_level(self, strategy_id: str, level: int):
        """Dynamically change log level for specific strategy"""
        config.set_strategy_level(strategy_id, level)
        metrics.increment("dynamic_level_changes")
        self._log_to_stderr(f"Changed log level for {strategy_id} to {logging.getLevelName(level)}")
    
    def emit(self, record: logging.LogRecord) -> None:
        """Enhanced emit with durability and anti-blocking measures"""
        # Strategy-specific level filtering
        strategy_id = getattr(record, 'strategy_id', 'default')
        required_level = config.get_strategy_level(strategy_id)
        
        if record.levelno < required_level:
            return
        
        # Critical logs bypass queue in durability mode
        if (config.enable_durability_mode and config.critical_log_sync and 
            record.levelno >= logging.ERROR):
            self._sync_emit(record)
            metrics.increment("logs_forced_sync")
            return
        
        try:
            current_loop = asyncio.get_running_loop()
            
            if not self._worker_started or self._loop != current_loop:
                self._initialize_for_loop(current_loop)
            
            # Queue health monitoring
            current_queue_size = self.queue.qsize()
            metrics.update_queue_size(current_queue_size)
            
            # Adaptive queue management
            if current_queue_size > (config.queue_maxsize * 0.9):
                self._log_to_stderr(f"Queue critical: {current_queue_size}/{config.queue_maxsize}")
                # Emergency: drop oldest non-critical logs
                self._emergency_queue_cleanup()
            elif current_queue_size > (config.queue_maxsize * 0.8):
                self._log_to_stderr(f"Queue warning: {current_queue_size}/{config.queue_maxsize}")
            
            try:
                self.queue.put_nowait(record)
            except asyncio.QueueFull:
                metrics.increment("logs_dropped")
                self._handle_queue_full(record)
                    
        except RuntimeError:
            metrics.increment("cross_thread_calls")
            self._handle_cross_thread_logging(record)
        except Exception as e:
            self._log_to_stderr(f"Emit error: {e}")
            self._sync_emit(record)  # Fallback to sync
    
    def _emergency_queue_cleanup(self):
        """Emergency queue cleanup to prevent memory issues"""
        try:
            dropped = 0
            cleanup_target = min(config.batch_size * 2, self.queue.qsize() // 4)
            
            for _ in range(cleanup_target):
                try:
                    old_record = self.queue.get_nowait()
                    # Only drop non-critical records
                    if old_record.levelno < logging.WARNING:
                        self.queue.task_done()
                        dropped += 1
                    else:
                        # Put critical record back
                        self.queue.put_nowait(old_record)
                        break
                except asyncio.QueueEmpty:
                    break
            
            if dropped > 0:
                self._log_to_stderr(f"Emergency cleanup: dropped {dropped} non-critical logs")
                
        except Exception as e:
            self._log_to_stderr(f"Emergency cleanup error: {e}")
    
    def _handle_queue_full(self, record: logging.LogRecord):
        """Handle queue full scenario"""
        if record.levelno >= logging.ERROR:
            # Force sync write for errors
            self._sync_emit(record)
            metrics.increment("logs_forced_sync")
        else:
            # Drop the log but try emergency cleanup
            self._emergency_queue_cleanup()
            try:
                self.queue.put_nowait(record)
            except asyncio.QueueFull:
                pass  # Still full, drop it
    
    def _sync_emit(self, record: logging.LogRecord):
        """Synchronous emit for critical logs"""
        with self._sync_file_lock:
            try:
                for handler in self._file_handlers:
                    if handler.level <= record.levelno:
                        handler.handle(record)
                        handler.flush()  # Force flush for durability
                
                # Console output (blocking)
                if not config.enable_async_console:
                    console_handler = logging.StreamHandler()
                    formatter = (EnhancedStructuredFormatter() if config.enable_structured_logging 
                               else OptimizedTradingFormatter())
                    console_handler.setFormatter(formatter)
                    console_handler.handle(record)
                
                metrics.increment("logs_processed")
                
            except Exception as e:
                self._log_to_stderr(f"Sync emit error: {e}")
    
    def _initialize_for_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """Initialize worker for current event loop"""
        if self._loop != loop or not self._worker_started:
            self._loop = loop
            self.queue = asyncio.Queue(maxsize=config.queue_maxsize)
            self._shutdown_event = asyncio.Event()
            
            # Start console writer if enabled
            if self._console_writer:
                loop.create_task(self._console_writer.start())
            
            # Start main worker
            if config.enable_batch_processing:
                self._worker_task = loop.create_task(self._enhanced_batch_worker())
            else:
                self._worker_task = loop.create_task(self._enhanced_single_worker())
            
            self._worker_started = True
            self._log_to_stderr(f"Logger '{self.name}' initialized: batch_mode={config.enable_batch_processing}, queue_size={config.queue_maxsize}")
    
    async def _enhanced_batch_worker(self) -> None:
        """Enhanced batch processing worker with durability features"""
        batch_buffer = []
        last_flush = time.time()
        last_forced_flush = time.time()
        
        while not self._shutdown_event.is_set():
            try:
                try:
                    record = await asyncio.wait_for(self.queue.get(), timeout=config.worker_timeout)
                    batch_buffer.append(record)
                    
                    # Multiple flush triggers
                    current_time = time.time()
                    should_flush = (
                        len(batch_buffer) >= config.batch_size or
                        (current_time - last_flush) > 0.1 or  # Time-based flush
                        (config.enable_durability_mode and 
                         (current_time - last_forced_flush) > config.force_flush_interval)
                    )
                    
                    if should_flush:
                        await self._process_batch(batch_buffer)
                        
                        # Durability: force file system sync periodically
                        if config.enable_durability_mode and (current_time - last_forced_flush) > config.force_flush_interval:
                            await self._force_flush_to_disk()
                            last_forced_flush = current_time
                            metrics.increment("durability_syncs")
                        
                        metrics.update_batch_stats(len(batch_buffer))
                        batch_buffer.clear()
                        last_flush = current_time
                        
                except asyncio.TimeoutError:
                    if batch_buffer:
                        await self._process_batch(batch_buffer)
                        batch_buffer.clear()
                        last_flush = time.time()
                    
                    # Periodic maintenance
                    await self._periodic_maintenance()
                    continue
                    
            except asyncio.CancelledError:
                if batch_buffer:
                    await self._process_batch(batch_buffer)
                break
            except Exception as e:
                metrics.increment("worker_errors")
                self._recent_errors.append(f"{datetime.now().isoformat()}: {str(e)}")
                self._log_to_stderr(f"Batch worker error: {e}")
                await asyncio.sleep(0.01)
    
    async def _process_batch(self, batch: List[logging.LogRecord]) -> None:
        """Enhanced batch processing with async console support"""
        start_time = time.time()
        
        try:
            for record in batch:
                # File handlers (sync)
                for handler in self._file_handlers:
                    if handler.level <= record.levelno:
                        handler.handle(record)
                
                # Console output (async if enabled)
                if self._console_writer:
                    formatter = (EnhancedStructuredFormatter() if config.enable_structured_logging 
                               else OptimizedTradingFormatter())
                    message = formatter.format(record) + '\n'
                    await self._console_writer.write(message)
                
                metrics.increment("logs_processed")
                self.queue.task_done()
                
        except Exception as e:
            self._log_to_stderr(f"Batch processing error: {e}")
            # Mark tasks as done even on error to prevent deadlock
            for _ in batch:
                try:
                    self.queue.task_done()
                except ValueError:
                    pass
        
        metrics.worker_cpu_time += (time.time() - start_time)
    
    async def _enhanced_single_worker(self) -> None:
        """Enhanced single record processing"""
        last_forced_flush = time.time()
        
        while not self._shutdown_event.is_set():
            try:
                record = await asyncio.wait_for(self.queue.get(), timeout=config.worker_timeout)
                
                # Process single record
                for handler in self._file_handlers:
                    if handler.level <= record.levelno:
                        handler.handle(record)
                
                if self._console_writer:
                    formatter = (EnhancedStructuredFormatter() if config.enable_structured_logging 
                               else OptimizedTradingFormatter())
                    message = formatter.format(record) + '\n'
                    await self._console_writer.write(message)
                
                metrics.increment("logs_processed")
                self.queue.task_done()
                
                # Periodic forced flush for durability
                current_time = time.time()
                if (config.enable_durability_mode and 
                    (current_time - last_forced_flush) > config.force_flush_interval):
                    await self._force_flush_to_disk()
                    last_forced_flush = current_time
                    metrics.increment("durability_syncs")
                
                # Periodic maintenance
                await self._periodic_maintenance()
                    
            except asyncio.TimeoutError:
                await self._periodic_maintenance()
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                metrics.increment("worker_errors")
                self._recent_errors.append(f"{datetime.now().isoformat()}: {str(e)}")
                self._log_to_stderr(f"Single worker error: {e}")
    
    async def _periodic_maintenance(self):
        """Periodic maintenance tasks"""
        if metrics.should_log_metrics():
            self._log_metrics_summary()
        if metrics.should_export_csv():
            metrics.export_to_csv()
    
    async def _force_flush_to_disk(self):
        """Force flush all handlers to disk for durability"""
        start_time = time.time()
        try:
            for handler in self._file_handlers:
                handler.flush()
                # Force OS-level sync if available
                if hasattr(handler.stream, 'fileno'):
                    try:
                        os.fsync(handler.stream.fileno())
                    except (OSError, AttributeError):
                        pass
        except Exception as e:
            self._log_to_stderr(f"Force flush error: {e}")
        
        metrics.total_flush_time += (time.time() - start_time)
    
    def _log_metrics_summary(self):
        """Log periodic metrics summary"""
        try:
            metrics_summary = metrics.to_dict()
            self._log_to_stderr(f"[METRICS] {json.dumps(metrics_summary)}")
            with metrics._lock:
                metrics.last_metrics_log = metrics.logs_processed
        except Exception as e:
            self._log_to_stderr(f"Metrics summary error: {e}")
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get comprehensive performance statistics"""
        queue_size = self.queue.qsize() if self.queue else 0
        return {
            **metrics.to_dict(),
            "handler_name": self.name,
            "queue_size": queue_size,
            "queue_utilization_pct": round((queue_size / config.queue_maxsize) * 100, 2),
            "worker_running": self._worker_started,
            "batch_mode": config.enable_batch_processing,
            "durability_mode": config.enable_durability_mode,
            "async_console": config.enable_async_console,
            "attached_loggers": list(self._logger_registry),
            "avg_worker_cpu_ms": round(metrics.worker_cpu_time * 1000, 2),
            "recent_errors": list(self._recent_errors)[-5:],
            "config": {
                "queue_maxsize": config.queue_maxsize,
                "batch_size": config.batch_size,
                "worker_timeout": config.worker_timeout,
                "force_flush_interval": config.force_flush_interval
            }
        }
    
    def _setup_signal_handlers(self):
        """Setup graceful shutdown signal handlers"""
        def signal_handler(signum, frame):
            print(f"[Logger] Signal {signum} received, shutting down...")
            try:
                metrics.export_to_csv()
                self.close()
            except Exception as e:
                sys.stderr.write(f"[Logger] Signal handler error: {e}\n")
        
        try:
            signal.signal(signal.SIGINT, signal_handler)
            signal.signal(signal.SIGTERM, signal_handler)
        except (OSError, ValueError):
            pass
    
    def _handle_cross_thread_logging(self, record: logging.LogRecord) -> None:
        """Handle logging from different threads"""
        try:
            if self._loop and self._loop.is_running():
                future = asyncio.run_coroutine_threadsafe(self._async_emit(record), self._loop)
                # Don't wait for result to avoid blocking
                future.add_done_callback(lambda f: None)
            else:
                self._sync_emit(record)
        except Exception as e:
            self._log_to_stderr(f"Cross-thread error: {e}")
            self._sync_emit(record)
    
    async def _async_emit(self, record: logging.LogRecord) -> None:
        """Async emit for cross-thread calls"""
        try:
            await asyncio.wait_for(self.queue.put(record), timeout=0.05)
        except asyncio.TimeoutError:
            metrics.increment("logs_dropped")
            if record.levelno >= logging.ERROR:
                self._sync_emit(record)
    
    def _log_to_stderr(self, message: str) -> None:
        """Internal logging to stderr with thread safety"""
        try:
            timestamp = datetime.now().strftime('%H:%M:%S.%f')[:-3]
            sys.stderr.write(f"[{timestamp}] [LOGGER:{self.name}] {message}\n")
            sys.stderr.flush()
        except Exception:
            pass  # Don't let internal logging crash
    
    async def flush_async(self, timeout: float = None) -> Dict[str, Any]:
        """Enhanced async flush with better error handling"""
        if timeout is None:
            timeout = config.flush_timeout
        
        if not self._worker_started or not self.queue:
            return {"status": "not_started"}
        
        pending = self.queue.qsize()
        start_time = time.time()
        
        try:
            # Wait for queue to empty
            await asyncio.wait_for(self.queue.join(), timeout=timeout)
            
            # Flush console writer
            if self._console_writer:
                await self._console_writer.flush(timeout=min(timeout, 1.0))
            
            # Force disk flush
            if config.enable_durability_mode:
                await self._force_flush_to_disk()
            
            return {
                "status": "success",
                "pending_logs": pending,
                "flush_time_ms": int((time.time() - start_time) * 1000),
                "final_metrics": metrics.to_dict()
            }
            
        except asyncio.TimeoutError:
            metrics.increment("flush_timeouts")
            return {
                "status": "timeout",
                "pending_logs": pending,
                "remaining_logs": self.queue.qsize(),
                "partial_flush_time_ms": int((time.time() - start_time) * 1000)
            }
        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "pending_logs": pending
            }
    
    def flush_sync(self, timeout: float = None) -> Dict[str, Any]:
        """Thread-safe synchronous flush"""
        if not self._loop or not self._loop.is_running():
            return {"status": "no_loop"}
        
        try:
            future = asyncio.run_coroutine_threadsafe(
                self.flush_async(timeout), self._loop
            )
            return future.result(timeout=timeout or config.flush_timeout)
        except Exception as e:
            return {"status": "error", "error": str(e)}
    
    def close(self) -> Dict[str, Any]:
        """Enhanced close with better cleanup"""
        try:
            final_stats = self.get_performance_stats()
            
            # Signal shutdown
            if self._shutdown_event:
                self._shutdown_event.set()
            
            # Flush remaining logs
            flush_result = self.flush_sync(timeout=config.flush_timeout)
            
            # Close console writer
            if self._console_writer:
                self._console_writer.shutdown()
            
            # Cancel worker task
            if self._worker_task and not self._worker_task.done():
                self._worker_task.cancel()
            
            # Close file handlers
            for handler in self._file_handlers:
                try:
                    handler.close()
                except Exception:
                    pass
            
            # Export final metrics
            metrics.export_to_csv()
            
            # Clear registry
            self._logger_registry.clear()
            self._worker_started = False
            
            return {
                "status": "closed",
                "flush_result": flush_result,
                "final_stats": final_stats
            }
            
        except Exception as e:
            return {"status": "error", "error": str(e)}
        finally:
            super().close()
    
    def _cleanup_sync(self):
        """Synchronous cleanup for atexit"""
        try:
            result = self.close()
            self._log_to_stderr(f"Logger shutdown: {result.get('status', 'unknown')}")
        except Exception as e:
            sys.stderr.write(f"[LOGGER] Cleanup error: {e}\n")


# ============================================================================
# Enhanced Logger Factory
# ============================================================================

class TradingLoggerAdapter(logging.LoggerAdapter):
    """Enhanced logger adapter with trading context and performance tracking"""
    
    def __init__(self, logger: logging.Logger, extra: Dict[str, Any], strategy_id: str = None):
        super().__init__(logger, extra)
        self.strategy_id = strategy_id
        self._call_count = 0
        self._last_log_time = time.time()
    
    def process(self, msg, kwargs):
        """Enhanced processing with automatic context injection"""
        self._call_count += 1
        current_time = time.time()
        
        extra = kwargs.get('extra', {})
        extra.update(self.extra)
        
        if self.strategy_id:
            extra['strategy_id'] = self.strategy_id
        
        # Add performance context
        extra['log_sequence'] = self._call_count
        
        # Add time delta for performance analysis
        if self._call_count > 1:
            extra['time_delta_ms'] = round((current_time - self._last_log_time) * 1000, 3)
        
        self._last_log_time = current_time
        kwargs['extra'] = extra
        return msg, kwargs
    
    def set_context(self, **kwargs):
        """Dynamically update logging context"""
        self.extra.update(kwargs)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get adapter-level statistics"""
        return {
            "strategy_id": self.strategy_id,
            "call_count": self._call_count,
            "uptime_seconds": round(time.time() - (self._last_log_time if self._call_count == 1 else 
                                                 time.time() - self._call_count), 2)
        }


# ============================================================================
# Logger Management System
# ============================================================================

class LoggerManager:
    """Centralized logger management to prevent duplicates and manage lifecycle"""
    
    def __init__(self):
        self._loggers: Dict[str, logging.Logger] = {}
        self._adapters: Dict[str, TradingLoggerAdapter] = {}
        self._handlers: Dict[str, EnhancedAsyncLoggingHandler] = {}
        self._lock = RLock()
    
    def get_logger(self, name: str, strategy_id: str = None, 
                   extra_context: Dict[str, Any] = None) -> logging.Logger:
        """Get or create logger with anti-duplicate measures"""
        with self._lock:
            logger_key = f"{name}:{strategy_id}" if strategy_id else name
            
            if logger_key in self._loggers:
                return self._loggers[logger_key]
            
            # Create logger
            logger = logging.getLogger(name)
            logger.setLevel(config.get_strategy_level(strategy_id or 'default'))
            logger.propagate = False
            
            # Get or create handler
            handler_key = strategy_id or 'default'
            if handler_key not in self._handlers:
                self._handlers[handler_key] = EnhancedAsyncLoggingHandler(handler_key)
            
            handler = self._handlers[handler_key]
            
            # Attach handler (prevents duplicates)
            if handler.attach_to_logger(name):
                logger.addHandler(handler)
            
            # Create adapter if needed
            if strategy_id or extra_context:
                context = extra_context or {}
                adapter = TradingLoggerAdapter(logger, context, strategy_id)
                self._adapters[logger_key] = adapter
                self._loggers[logger_key] = adapter
            else:
                self._loggers[logger_key] = logger
            
            return self._loggers[logger_key]
    
    def remove_logger(self, name: str, strategy_id: str = None):
        """Remove logger and clean up resources"""
        with self._lock:
            logger_key = f"{name}:{strategy_id}" if strategy_id else name
            
            if logger_key in self._loggers:
                # Detach handler
                handler_key = strategy_id or 'default'
                if handler_key in self._handlers:
                    self._handlers[handler_key].detach_from_logger(name)
                
                # Clean up references
                del self._loggers[logger_key]
                if logger_key in self._adapters:
                    del self._adapters[logger_key]
    
    def set_strategy_level(self, strategy_id: str, level: int):
        """Set log level for all loggers of a strategy"""
        with self._lock:
            config.set_strategy_level(strategy_id, level)
            
            # Update existing loggers
            for key, logger_obj in self._loggers.items():
                if key.endswith(f":{strategy_id}"):
                    if isinstance(logger_obj, TradingLoggerAdapter):
                        logger_obj.logger.setLevel(level)
                    else:
                        logger_obj.setLevel(level)
            
            # Update handler
            if strategy_id in self._handlers:
                self._handlers[strategy_id].set_strategy_log_level(strategy_id, level)
    
    def get_all_stats(self) -> Dict[str, Any]:
        """Get comprehensive statistics for all managed components"""
        with self._lock:
            return {
                "loggers": {key: (adapter.get_stats() if isinstance(adapter, TradingLoggerAdapter) 
                                else {"type": "basic_logger"}) for key, adapter in self._loggers.items()},
                "handlers": {key: handler.get_performance_stats() 
                           for key, handler in self._handlers.items()},
                "global_metrics": metrics.to_dict(),
                "config": {
                    "queue_maxsize": config.queue_maxsize,
                    "batch_processing": config.enable_batch_processing,
                    "durability_mode": config.enable_durability_mode,
                    "async_console": config.enable_async_console
                }
            }
    
    async def flush_all(self, timeout: float = None) -> Dict[str, Dict[str, Any]]:
        """Flush all handlers"""
        results = {}
        for name, handler in self._handlers.items():
            results[name] = await handler.flush_async(timeout)
        return results
    
    def shutdown_all(self, timeout: float = None) -> Dict[str, Dict[str, Any]]:
        """Shutdown all handlers and export final metrics"""
        with self._lock:
            results = {}
            
            # Close all handlers
            for name, handler in self._handlers.items():
                results[name] = handler.close()
            
            # Clear all references
            self._loggers.clear()
            self._adapters.clear()
            self._handlers.clear()
            
            return {
                "handlers": results,
                "final_metrics": metrics.to_dict()
            }


# Global manager instance
logger_manager = LoggerManager()


# ============================================================================
# Public Interface & Convenience Functions
# ============================================================================

def get_logger(name: str, strategy_id: str = None, **context) -> logging.Logger:
    """
    Get optimized logger for trading applications
    
    Args:
        name: Logger name (typically __name__)
        strategy_id: Strategy identifier for separate log levels/contexts
        **context: Additional context to include in all log messages
    
    Returns:
        Configured logger (potentially wrapped in adapter)
    """
    return logger_manager.get_logger(name, strategy_id, context)


def set_strategy_log_level(strategy_id: str, level: int):
    """Set log level for a specific trading strategy"""
    logger_manager.set_strategy_level(strategy_id, level)


async def flush_all_loggers(timeout: float = None) -> Dict[str, Any]:
    """Flush all active loggers"""
    return await logger_manager.flush_all(timeout)


def get_logging_stats() -> Dict[str, Any]:
    """Get comprehensive logging system statistics"""
    return logger_manager.get_all_stats()


def shutdown_logging_system(timeout: float = None, export_metrics: bool = True) -> Dict[str, Any]:
    """
    Gracefully shutdown entire logging system
    
    Args:
        timeout: Maximum time to wait for flush operations
        export_metrics: Whether to export final metrics to CSV
    
    Returns:
        Shutdown results and performance statistics
    """
    if timeout is None:
        timeout = config.flush_timeout
    
    print(f"[Logger] Shutting down logging system (timeout={timeout}s)...")
    
    try:
        # Get final stats before shutdown
        final_stats = logger_manager.get_all_stats()
        
        # Export metrics if requested
        if export_metrics and config.enable_metrics_export:
            metrics.export_to_csv()
            print(f"[Logger] Final metrics exported to {config.metrics_dir}")
        
        # Shutdown all components
        shutdown_results = logger_manager.shutdown_all(timeout)
        
        # Print performance summary
        global_metrics = final_stats.get("global_metrics", {})
        if global_metrics:
            print(f"[Logger] Final Performance Summary:")
            print(f"  • Total processed: {global_metrics.get('logs_processed', 0):,} logs")
            print(f"  • Dropped: {global_metrics.get('logs_dropped', 0):,} ({global_metrics.get('drop_rate_pct', 0)}%)")
            print(f"  • Avg throughput: {global_metrics.get('logs_per_second', 0):.1f} logs/sec")
            print(f"  • Error rate: {global_metrics.get('error_rate_pct', 0)}%")
            print(f"  • Uptime: {global_metrics.get('uptime_hours', 0):.1f} hours")
            
            if config.enable_batch_processing:
                print(f"  • Avg batch size: {global_metrics.get('avg_batch_size', 0):.1f}")
            
            if config.enable_durability_mode:
                print(f"  • Durability syncs: {global_metrics.get('durability_syncs', 0):,}")
                print(f"  • Forced sync logs: {global_metrics.get('logs_forced_sync', 0):,}")
        
        return {
            "status": "success",
            "shutdown_results": shutdown_results,
            "final_stats": final_stats
        }
        
    except Exception as e:
        error_msg = f"Shutdown error: {e}"
        print(f"[Logger] {error_msg}")
        return {
            "status": "error",
            "error": error_msg
        }


# Create default logger instance
logger = get_logger(__name__)


# ============================================================================
# Usage Examples & Context Managers
# ============================================================================

@contextlib.asynccontextmanager
async def trading_session_logger(strategy_id: str, session_context: Dict[str, Any] = None):
    """Context manager for trading session logging"""
    session_logger = get_logger(f"trading.{strategy_id}", strategy_id, **(session_context or {}))
    
    session_logger.info("Trading session started", extra={"event": "session_start"})
    
    try:
        yield session_logger
    except Exception as e:
        session_logger.error(f"Trading session error: {e}", extra={"event": "session_error"})
        raise
    finally:
        session_logger.info("Trading session ended", extra={"event": "session_end"})
        await flush_all_loggers(timeout=2.0)


# ============================================================================
# Export Public Interface
# ============================================================================

__all__ = [
    # Core functions
    'get_logger',
    'set_strategy_log_level', 
    'flush_all_loggers',
    'get_logging_stats',
    'shutdown_logging_system',
    
    # Context managers
    'trading_session_logger',
    
    # Direct access (for advanced usage)
    'logger',
    'logger_manager',
    'metrics',
    'config',
    
    # Classes (for extension)
    'EnhancedAsyncLoggingHandler',
    'TradingLoggerAdapter',
    'LoggerManager'
]


# ============================================================================
# Example Usage
# ============================================================================

if __name__ == "__main__":
    async def demo_logging():
        """Demonstration of the enhanced logging system"""
        
        # Basic logger
        main_logger = get_logger(__name__)
        main_logger.info("System starting up")
        
        # Strategy-specific loggers
        stra1_logger = get_logger("strategy", "STRA_1", symbol="NIFTY", timeframe="1m")
        stra2_logger = get_logger("strategy", "STRA_2", symbol="BANKNIFTY", timeframe="5m")
        
        # Log some trading events
        stra1_logger.info("Order placed", extra={"order_id": "ORD001", "price": 18500, "qty": 100})
        stra1_logger.warning("Low volume detected", extra={"volume": 1500})
        
        stra2_logger.info("Signal generated", extra={"signal": "BUY", "confidence": 0.85})
        stra2_logger.error("Order rejected", extra={"order_id": "ORD002", "reason": "Insufficient margin"})
        
        # Change log level dynamically
        set_strategy_log_level("STRA_1", logging.DEBUG)
        stra1_logger.debug("Debug info after level change")
        
        # Flush and get stats
        await flush_all_loggers()
        stats = get_logging_stats()
        main_logger.info(f"Current stats: {stats['global_metrics']}")
        
        # Shutdown
        await asyncio.sleep(1)  # Let some logs process
        result = shutdown_logging_system()
        print(f"Shutdown result: {result['status']}")
    
    # Run demo
    asyncio.run(demo_logging())
