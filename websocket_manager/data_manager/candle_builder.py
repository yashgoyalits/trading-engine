#candle_builder.py
import asyncio
from datetime import datetime, timedelta
from utils.logger import logger

def align_to_candle_boundary(dt, tf):
    """Align datetime to nearest candle start (session starts 09:15)."""
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
        self.callbacks, self.active, self.last_close = {}, {}, {}

    def add_callback(self, symbol, callback):
        """Register a candle callback for a symbol."""
        self.callbacks.setdefault(symbol, []).append(callback)

    async def process_candle_tick(self, symbol, msg, tf):
        """Update or start a new candle from a tick."""
        ltp, ts = msg.get("ltp"), msg.get("exch_feed_time")
        if not ltp or not ts:
            return
        start = align_to_candle_boundary(datetime.now(), tf)
        if not start:
            return

        # New candle
        if symbol not in self.active or self.active[symbol]["start_time"] != start:
            if symbol in self.active:
                await self._complete_candle(symbol, tf)
            self.active[symbol] = {"open": ltp, "high": ltp, "low": ltp, "close": ltp,
                                   "start_time": start, "timeframe": tf, "volume": 0}
        else:  # Update candle
            c = self.active[symbol]
            c.update({
                "high": max(c["high"], ltp),
                "low": min(c["low"], ltp),
                "close": ltp
            })

    async def _complete_candle(self, symbol, tf):
        """Finalize and emit the last candle."""
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

        for cb in self.callbacks.get(symbol, []):
            try:
                res = cb(symbol, candle)
                if asyncio.iscoroutine(res):
                    await res
            except Exception as e:
                logger.error(f"[Candle Callback Error] {symbol}: {e}")

        self.tick_processor.cleanup_old_ticks(symbol, end.timestamp())

    async def check_and_complete_candles(self):
        """Force-complete candles based on clock if ticks stop arriving."""
        now = datetime.now()
        for sym, c in list(self.active.items()):
            if now >= get_candle_end_time(c["start_time"], c["timeframe"]):
                await self._complete_candle(sym, c["timeframe"])
                del self.active[sym]
