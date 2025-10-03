import asyncio
from datetime import datetime, timedelta
from utils.logger import logger
from centeral_hub.event_bus import EventBus

def align_to_candle_boundary(dt, tf):
    session_start = dt.replace(hour=9, minute=15, second=0, microsecond=0)
    if dt < session_start:
        return None
    secs = (dt - session_start).total_seconds()
    return session_start + timedelta(seconds=int(secs // tf) * tf)

def get_candle_end_time(start, tf): 
    return start + timedelta(seconds=tf)

class CandleBuilder:
    def __init__(self, tick_processor):
        self.tick_processor = tick_processor
        self.active, self.last_close = {}, {}
        self.completion_tasks = {}  # Track scheduled completions

    async def process_candle_tick(self, symbol, msg, tf):
        ltp, ts = msg.get("ltp"), msg.get("exch_feed_time")
        if not ltp or not ts:
            return
        start = align_to_candle_boundary(datetime.now(), tf)
        if not start:
            return

        if symbol not in self.active or self.active[symbol]["start_time"] != start:
            if symbol in self.active:
                await self._complete_candle(symbol, tf)
            
            self.active[symbol] = {
                "open": ltp, "high": ltp, "low": ltp, "close": ltp,
                "start_time": start, "timeframe": tf, "volume": 0
            }
            
            # Schedule candle completion
            end_time = get_candle_end_time(start, tf)
            delay = (end_time - datetime.now()).total_seconds()
            if delay > 0:
                if symbol in self.completion_tasks:
                    self.completion_tasks[symbol].cancel()
                self.completion_tasks[symbol] = asyncio.create_task(
                    self._scheduled_complete(symbol, tf, delay)
                )
        else:
            c = self.active[symbol]
            c.update({
                "high": max(c["high"], ltp),
                "low": min(c["low"], ltp),
                "close": ltp
            })

    async def _scheduled_complete(self, symbol, tf, delay):
        await asyncio.sleep(delay)
        if symbol in self.active:
            await self._complete_candle(symbol, tf)
            if symbol in self.active:
                del self.active[symbol]

    async def _complete_candle(self, symbol, tf):
        # Your existing completion logic remains the same
        c = self.active.get(symbol)
        if not c:
            return
        start, end = c["start_time"], get_candle_end_time(c["start_time"], tf)
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
        elif symbol in self.last_close:
            p = self.last_close[symbol]
            c.update({"open": p, "high": p, "low": p, "close": p})

        candle = {
            "open": c["open"], "high": c["high"], "low": c["low"], "close": c["close"],
            "volume": c.get("volume", 0),
            "time": c["start_time"].strftime("%Y-%m-%d %H:%M:%S"),
        }
        self.last_close[symbol] = candle["close"]
        await EventBus.publish("candle", (symbol, candle))
        self.tick_processor.cleanup_old_ticks(symbol, end.timestamp())
