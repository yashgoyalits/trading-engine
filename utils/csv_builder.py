import aiofiles
import aiofiles.os
import os
import asyncio
import csv
from io import StringIO
from datetime import datetime
from utils.error_handling import error_handling
from data_model.data_model import TradeData
from dataclasses import asdict

@error_handling
class CSVBuilder:
    def __init__(self, event_bus, base_dir: str = None, prefix: str = "trades"):
        self.event_bus = event_bus
        self.base_dir = base_dir or os.path.join(os.getcwd(), "logger_files", "csv")
        os.makedirs(self.base_dir, exist_ok=True)
        self.prefix = prefix
        self.file_path = ""
        self._lock = asyncio.Lock()
        self._queue = None

        # Start the background task
        asyncio.create_task(self._start_consumer())

    async def _start_consumer(self):
        self._queue = self.event_bus.subscribe("trade_close")
        while True:
            trade: TradeData = await self._queue.get()
            if trade is None:  # Optional: use None to stop the consumer
                break
            await self._log_trade(trade)


    async def _log_trade(self, trade: TradeData):
        today_file = os.path.join(
            self.base_dir, f"{self.prefix}_{datetime.now().strftime('%Y-%m-%d')}.csv"
        )
        if self.file_path != today_file:
            self.file_path = today_file

        async with self._lock:
            file_exists = await aiofiles.os.path.exists(self.file_path)
            async with aiofiles.open(self.file_path, mode="a", newline="", encoding="utf-8") as f:
                row_dict = asdict(trade)
                output = StringIO()
                writer = csv.DictWriter(output, fieldnames=row_dict.keys())

                if not file_exists:
                    writer.writeheader()
                    await f.write(output.getvalue())
                    output = StringIO()
                    writer = csv.DictWriter(output, fieldnames=row_dict.keys())

                writer.writerow(row_dict)
                await f.write(output.getvalue())
