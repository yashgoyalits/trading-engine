import asyncio
import aiofiles
import os
from datetime import datetime

class AsyncLogger:
    LEVELS = {"DEBUG": 10, "INFO": 20, "ERROR": 30}

    def __init__(self, log_dir=None, min_level="INFO"):
        self.min_level = self.LEVELS.get(min_level.upper(), 20)
        self.log_dir = log_dir or os.path.join(os.getcwd(), "logger_files", "logs")
        os.makedirs(self.log_dir, exist_ok=True)

        self.log_file = self._get_log_file()
        self.log_queue = asyncio.Queue()
        self._worker_started = False

    # --- Get daily log file path ---
    def _get_log_file(self):
        today_str = datetime.now().strftime("%Y-%m-%d")
        return os.path.join(self.log_dir, f"main_script_{today_str}.log")

    # --- Timestamp with milliseconds ---
    def _get_timestamp(self):
        dt = datetime.now()
        millis = dt.microsecond // 1000
        return f"{dt.strftime('%Y-%m-%d %H:%M:%S')}.{millis:03d}"

    # --- Background async worker ---
    async def _log_worker(self):
        while True:
            level, msg = await self.log_queue.get()
            timestamp = self._get_timestamp()
            log_line = f"[{timestamp}] [{level}] {msg}\n"
            try:
                async with aiofiles.open(self._get_log_file(), "a") as f:
                    await f.write(log_line)
            except Exception as e:
                print(f"[LOGGER ERROR] Failed to write log: {e}")
            self.log_queue.task_done()

    # --- Ensure worker is running ---
    def _start_worker(self):
        if not self._worker_started:
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(self._log_worker())
                self._worker_started = True
            except RuntimeError:
                # No loop yet, will start later
                pass

    # --- Internal logging method ---
    def _log(self, level, msg):
        if self.LEVELS.get(level, 0) < self.min_level:
            return

        timestamp = self._get_timestamp()
        log_line = f"[{timestamp}] [{level}] {msg}"
        print(log_line)  # console output

        try:
            loop = asyncio.get_running_loop()
            self._start_worker()
            self.log_queue.put_nowait((level, msg))
        except RuntimeError:
            # fallback synchronous write if no loop
            with open(self._get_log_file(), "a") as f:
                f.write(f"{log_line}\n")

    # --- Public API ---
    def info(self, msg):
        self._log("INFO", msg)

    def error(self, msg):
        self._log("ERROR", msg)

    def debug(self, msg):
        self._log("DEBUG", msg)

    # --- Optional: flush queued logs before exit ---
    async def flush(self):
        if self._worker_started:
            await self.log_queue.join()


# --- Create a logger instance ---
logger = AsyncLogger(min_level="INFO")
