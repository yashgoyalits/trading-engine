import aiofiles
import aiofiles.os
import os
import asyncio
import csv
from io import StringIO
from datetime import datetime
from utils.error_handling import error_handling
from data_model.data_model import TradeDoneData
from dataclasses import asdict

@error_handling
class CSVBuilder:
    def __init__(self, base_dir: str = None, prefix: str = "trades"):
        self.base_dir = base_dir or os.path.join(os.getcwd(), "logger_files", "csv")
        os.makedirs(self.base_dir, exist_ok=True)
        self.prefix = prefix
        self.file_path = ""
        self._lock = asyncio.Lock()

    async def log_trade(self, trade: TradeDoneData):
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
