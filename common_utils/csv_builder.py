import aiofiles
import asyncio
from pathlib import Path
from datetime import datetime
from dataclasses import fields
from common_utils.error_handling import error_handling
from data_model.data_model import TradeData

@error_handling
class CSVBuilder:
    __slots__ = ('event_bus', 'base_dir', 'prefix', '_field_names', '_queue', '_consumer_task', '_shutdown')
    
    def __init__(self, event_bus, base_dir: str | None = None, prefix: str = "trades"):
        self.event_bus = event_bus
        self.base_dir = Path(base_dir) if base_dir else Path.cwd() / "logger_files" / "csv"
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.prefix = prefix
        self._field_names = None  # Cache field names
        self._queue = None
        self._consumer_task = None
        self._shutdown = asyncio.Event()
        
        self._consumer_task = asyncio.create_task(self._consume_trades())

    def _initialize_fields(self, trade: TradeData) -> None:
        if self._field_names is None:
            self._field_names = tuple(f.name for f in fields(trade))
    
    def _get_csv_header(self) -> str:
        return ','.join(self._field_names) + '\n'
    
    def _get_csv_row(self, trade: TradeData) -> str:
        values = (str(getattr(trade, field)).replace(',', ';') for field in self._field_names)
        return ','.join(values) + '\n'

    def _get_file_path(self) -> Path:
        return self.base_dir / f"{self.prefix}_{datetime.now():%Y-%m-%d}.csv"

    async def _consume_trades(self) -> None:
        self._queue = self.event_bus.subscribe("trade_close")
        
        try:
            while not self._shutdown.is_set():
                try:
                    trade = await asyncio.wait_for(self._queue.get(), timeout=0.5)
                    if self._field_names is None:
                        self._initialize_fields(trade)
                    await self._write_trade(trade)
                except asyncio.TimeoutError:
                    continue
        except asyncio.CancelledError:
            pass
        finally:
            await self._flush_remaining()

    async def _write_trade(self, trade: TradeData) -> None:
        file_path = self._get_file_path()
        file_exists = file_path.exists()
        
        async with aiofiles.open(file_path, mode='a', encoding='utf-8') as f:
            if not file_exists:
                await f.write(self._get_csv_header())
            await f.write(self._get_csv_row(trade))

    async def _flush_remaining(self) -> None:
        while not self._queue.empty():
            try:
                trade = self._queue.get_nowait()
                await self._write_trade(trade)
            except asyncio.QueueEmpty:
                break

    async def stop(self) -> None:
        self._shutdown.set()
        
        if self._consumer_task and not self._consumer_task.done():
            try:
                await asyncio.wait_for(self._consumer_task, timeout=5.0)
            except asyncio.TimeoutError:
                self._consumer_task.cancel()
                await asyncio.gather(self._consumer_task, return_exceptions=True)
