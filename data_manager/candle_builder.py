import asyncio
from datetime import datetime, timedelta
from typing import Dict, Optional
from .tick_processor import TickProcessor
from data_model.data_model import Tick, Candle

def align_to_candle_boundary(dt: datetime, tf: int) -> Optional[datetime]:
    session_start = dt.replace(hour=9, minute=15, second=0, microsecond=0)
    if dt < session_start:
        return None
    secs = (dt - session_start).total_seconds()
    return session_start + timedelta(seconds=int(secs // tf) * tf)

class CandleBuilder:
    __slots__ = ('tick_processor', 'event_bus', 'active', 'last_close', 'completion_tasks')

    def __init__(self, tick_processor: TickProcessor, event_bus):
        self.tick_processor = tick_processor
        self.event_bus = event_bus
        self.active: Dict[str, Candle] = {}
        self.last_close: Dict[str, float] = {}
        self.completion_tasks: Dict[str, asyncio.Task] = {}

    async def process_candle_tick(self, tick: Tick, tf: int) -> None:
        if not tick or tick.ltp is None:
            return

        symbol = tick.symbol
        now = datetime.now()
        start = align_to_candle_boundary(now, tf)
        if not start:
            return

        # Start new candle if window rolled over
        if symbol not in self.active or self.active[symbol].start_time != start:
            if symbol in self.active:
                await self._complete_candle(symbol, tf)

            self.active[symbol] = Candle(
                symbol=symbol,
                open=tick.ltp,
                high=tick.ltp,
                low=tick.ltp,
                close=tick.ltp,
                start_time=start,
                volume=tick.volume or 0,
            )

            # Schedule candle completion
            delay = (start + timedelta(seconds=tf) - now).total_seconds()
            if delay > 0:
                if task := self.completion_tasks.get(symbol):
                    task.cancel()
                self.completion_tasks[symbol] = asyncio.create_task(
                    self._scheduled_complete(symbol, tf, delay)
                )
        else:
            self.active[symbol].update(tick.ltp, tick.volume or 0)

    async def _scheduled_complete(self, symbol: str, tf: int, delay: float) -> None:
        try:
            await asyncio.sleep(delay)
            if symbol in self.active:
                await self._complete_candle(symbol, tf)
                self.active.pop(symbol, None)
        except asyncio.CancelledError:
            pass

    async def _complete_candle(self, symbol: str, tf: int) -> None:
        if not (candle := self.active.get(symbol)):
            return

        start = candle.start_time
        end = start + timedelta(seconds=tf)
        ticks = self.tick_processor.get_ticks_in_range(symbol, start.timestamp(), end.timestamp())

        if ticks:
            prices = [t.ltp for t in ticks]
            candle.open = ticks[0].ltp
            candle.high = max(prices)
            candle.low = min(prices)
            candle.close = ticks[-1].ltp
            candle.volume = sum(t.volume for t in ticks)
        elif (last := self.last_close.get(symbol)) is not None:
            candle.open = candle.high = candle.low = candle.close = last

        self.last_close[symbol] = candle.close

        # Publish Candle dataclass directly
        await self.event_bus.publish("candle", candle)

        # Cleanup old ticks
        self.tick_processor.cleanup_old_ticks(symbol, end.timestamp())
