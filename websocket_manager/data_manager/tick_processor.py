from datetime import datetime
from collections import deque


class TickProcessor:
    __slots__ = ('event_bus', 'tick_buffer', 'last_tick_time', 'max_ticks')
    
    def __init__(self, event_bus, max_ticks=1000):
        self.event_bus = event_bus
        self.tick_buffer: dict[str, deque] = {}
        self.last_tick_time: dict[str, int] = {}
        self.max_ticks = max_ticks
        
    async def process_tick(self, symbol: str, msg: dict, publish: bool = True) -> bool:
        if not (ltp := msg.get("ltp")) or not (ts := msg.get("exch_feed_time")):
            return False
        if self.last_tick_time.get(symbol) == ts:
            return False
            
        tick = {
            "ltp": ltp,
            "timestamp": ts,
            "datetime": datetime.fromtimestamp(ts),
            "volume": msg.get("volume", 0)
        }
        
        if symbol not in self.tick_buffer:
            self.tick_buffer[symbol] = deque(maxlen=self.max_ticks)
        self.tick_buffer[symbol].append(tick)
        self.last_tick_time[symbol] = ts
        
        if publish:
            await self.event_bus.publish("tick", (symbol, tick))
            
        return True
    
    def get_ticks_in_range(self, symbol: str, start: float, end: float) -> list[dict]:
        if buf := self.tick_buffer.get(symbol):
            return [t for t in buf if start <= t["timestamp"] < end]
        return []
    
    def cleanup_old_ticks(self, symbol: str, cutoff: float) -> None:
        if buf := self.tick_buffer.get(symbol):
            while buf and buf[0]["timestamp"] < cutoff:
                buf.popleft()
    
    def cleanup_inactive_symbols(self, current_time: float, ttl: int = 3600) -> int:
        cutoff = current_time - ttl
        inactive = [s for s, ts in self.last_tick_time.items() if ts < cutoff]
        for symbol in inactive:
            self.tick_buffer.pop(symbol, None)
            self.last_tick_time.pop(symbol, None)
        return len(inactive)
