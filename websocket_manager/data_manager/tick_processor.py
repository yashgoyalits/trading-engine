from datetime import datetime
from collections import deque
from centeral_hub.event_bus import EventBus

class TickProcessor:
    def __init__(self, max_ticks=1000):
        self.tick_buffer = {}
        self.last_tick_time = {}
        self.max_ticks = max_ticks

    async def process_tick(self, symbol, msg, publish=True):
        ltp, ts = msg.get("ltp"), msg.get("exch_feed_time")
        if not ltp or not ts:
            return False
        if self.last_tick_time.get(symbol) == ts:
            return False

        tick = {
            "ltp": ltp,
            "timestamp": ts,
            "datetime": datetime.fromtimestamp(ts),
            "volume": msg.get("volume", 0)
        }

        self.tick_buffer.setdefault(symbol, deque(maxlen=self.max_ticks)).append(tick)
        self.last_tick_time[symbol] = ts

        # Only publish to EventBus when in tick mode
        if publish:
            await EventBus.publish("tick", (symbol, tick))
        
        return True


    def get_ticks_in_range(self, symbol, start, end):
        return [t for t in self.tick_buffer.get(symbol, [])
                if start <= t["timestamp"] < end]

    def cleanup_old_ticks(self, symbol, cutoff):
        buf = self.tick_buffer.get(symbol)
        while buf and buf[0]["timestamp"] < cutoff:
            buf.popleft()
