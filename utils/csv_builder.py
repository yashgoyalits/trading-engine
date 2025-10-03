import aiofiles
import aiofiles.os
import os
from datetime import datetime
from utils.error_handling import error_handling
import asyncio
import csv
from io import StringIO
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional, Any

@dataclass
class TradeRow:
    strategy_id: str
    trade_no: int
    order_id: str
    symbol: str
    position_id: str
    qty: int
    side: str
    entry_price: Optional[float]
    initial_stop_price: Optional[float]
    target_price: Optional[float]
    initial_sl_points: Optional[float]
    target_points: Optional[float]
    trailing_levels: List[Dict[str, Any]]
    trailing_history: List[Dict[str, Any]]

@error_handling
class CSVBuilder:
    def __init__(self, base_dir: str = None, prefix: str = "trades"):
        self.base_dir = base_dir or os.path.join(os.getcwd(), "logger_files", "csv")
        os.makedirs(self.base_dir, exist_ok=True)

        self.prefix = prefix
        self.file_path = ""
        self.header_written = False
        self._lock = asyncio.Lock()  # ensure async safety

    @staticmethod
    async def _format_time(dt_value: datetime = None) -> str:
        dt_value = dt_value or datetime.now()
        if isinstance(dt_value, datetime):
            return dt_value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        return str(dt_value)

    async def log_trade(self, trade_row: TradeRow):
        today_file = os.path.join(
            self.base_dir, f"{self.prefix}_{datetime.now().strftime('%Y-%m-%d')}.csv"
        )
        if self.file_path != today_file:
            self.file_path = today_file
            self.header_written = False

        async with self._lock:
            file_exists = await aiofiles.os.path.exists(self.file_path)

            async with aiofiles.open(self.file_path, mode="a", newline="", encoding="utf-8") as f:
                row_dict = asdict(trade_row)
                output = StringIO()
                writer = csv.DictWriter(output, fieldnames=row_dict.keys())

                # Write header only if file does not exist
                if not file_exists:
                    writer.writeheader()
                    await f.write(output.getvalue())
                    output = StringIO()  # reset buffer
                    writer = csv.DictWriter(output, fieldnames=row_dict.keys())
                    self.header_written = True  # not strictly needed now

                # Write row
                writer.writerow(row_dict)
                await f.write(output.getvalue())

