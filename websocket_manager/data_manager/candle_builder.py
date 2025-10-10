import asyncio
from datetime import datetime, timedelta
from typing import Optional

def align_to_candle_boundary(dt: datetime, tf: int) -> Optional[datetime]:
    session_start = dt.replace(hour=9, minute=15, second=0, microsecond=0)
    if dt < session_start:
        return None
    secs = (dt - session_start).total_seconds()
    return session_start + timedelta(seconds=int(secs // tf) * tf)

class CandleBuilder:
    __slots__ = ('tick_processor', 'event_bus', 'active', 'last_close', 'completion_tasks')
    
    def __init__(self, tick_processor, event_bus):
        self.tick_processor = tick_processor
        self.event_bus = event_bus
        self.active: dict[str, dict] = {}
        self.last_close: dict[str, float] = {}
        self.completion_tasks: dict[str, asyncio.Task] = {}

    async def process_candle_tick(self, symbol: str, msg: dict, tf: int) -> None:
        if not (ltp := msg.get("ltp")) or not msg.get("exch_feed_time"):
            return
        
        if not (start := align_to_candle_boundary(datetime.now(), tf)):
            return

        if symbol not in self.active or self.active[symbol]["start_time"] != start:
            if symbol in self.active:
                await self._complete_candle(symbol, tf)
            
            self.active[symbol] = {
                "open": ltp, "high": ltp, "low": ltp, "close": ltp,
                "start_time": start, "volume": 0
            }
            
            # Schedule candle completion
            delay = (start + timedelta(seconds=tf) - datetime.now()).total_seconds()
            if delay > 0:
                if task := self.completion_tasks.get(symbol):
                    task.cancel()
                self.completion_tasks[symbol] = asyncio.create_task(
                    self._scheduled_complete(symbol, tf, delay)
                )
        else:
            c = self.active[symbol]
            c["high"] = max(c["high"], ltp)
            c["low"] = min(c["low"], ltp)
            c["close"] = ltp

    async def _scheduled_complete(self, symbol: str, tf: int, delay: float) -> None:
        try:
            await asyncio.sleep(delay)
            if symbol in self.active:
                await self._complete_candle(symbol, tf)
                self.active.pop(symbol, None)
        except asyncio.CancelledError:
            pass

    async def _complete_candle(self, symbol: str, tf: int) -> None:
        if not (c := self.active.get(symbol)):
            return
        
        start, end = c["start_time"], c["start_time"] + timedelta(seconds=tf)
        ticks = self.tick_processor.get_ticks_in_range(symbol, start.timestamp(), end.timestamp())

        if ticks:
            prices = [t["ltp"] for t in ticks]
            c.update({
                "open": ticks[0]["ltp"],
                "high": max(prices),
                "low": min(prices),
                "close": ticks[-1]["ltp"],
                "volume": sum(t.get("volume", 0) for t in ticks)
            })
        elif price := self.last_close.get(symbol):
            c.update({"open": price, "high": price, "low": price, "close": price})

        candle = {
            "open": c["open"], "high": c["high"], "low": c["low"], "close": c["close"],
            "volume": c.get("volume", 0),
            "time": c["start_time"].strftime("%Y-%m-%d %H:%M:%S"),
        }
        self.last_close[symbol] = candle["close"]

        await self.event_bus.publish("candle", (symbol, candle))
        self.tick_processor.cleanup_old_ticks(symbol, end.timestamp())
