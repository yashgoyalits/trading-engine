#tick_processor.py
import asyncio
from datetime import datetime
from collections import deque
from utils.logger import logger

class TickProcessor:

    def __init__(self, max_ticks=1000):
        self.callbacks = {}
        self.tick_buffer = {}
        self.last_tick_time = {}
        self.max_ticks = max_ticks

    def add_callback(self, symbol, callback):
        """Register a tick callback for a symbol."""
        self.callbacks.setdefault(symbol, []).append(callback)

    async def process_tick(self, symbol, msg):
        """Process an incoming tick."""
        ltp, ts = msg.get("ltp"), msg.get("exch_feed_time")
        if not ltp or not ts:
            return False
        if self.last_tick_time.get(symbol) == ts:  # duplicate
            return False

        tick = {
            "ltp": ltp,
            "timestamp": ts,
            "datetime": datetime.fromtimestamp(ts),
            "volume": msg.get("volume", 0)
        }

        self.tick_buffer.setdefault(symbol, deque(maxlen=self.max_ticks)).append(tick)
        self.last_tick_time[symbol] = ts

        for cb in self.callbacks.get(symbol, []):
            try:
                res = cb(symbol, tick)
                if asyncio.iscoroutine(res):
                    await res
            except Exception as e:
                logger.error(f"[Tick Callback Error] {symbol}: {e}")
        return True

    def get_ticks_in_range(self, symbol, start, end):
        """Return ticks within a timestamp range."""
        return [t for t in self.tick_buffer.get(symbol, [])
                if start <= t["timestamp"] < end]

    def cleanup_old_ticks(self, symbol, cutoff):
        """Drop ticks older than cutoff timestamp."""
        buf = self.tick_buffer.get(symbol)
        while buf and buf[0]["timestamp"] < cutoff:
            buf.popleft()
