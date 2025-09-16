# event_bus.py file
import asyncio
from typing import Callable, Any, Dict, List, Tuple

class EventBus:
    def __init__(self):
        self.subscribers: Dict[str, List[Tuple[asyncio.Queue, Callable[[Any], bool]]]] = {}

    def subscribe(self, event_type: str, filter_fn: Callable[[Any], bool] = None) -> asyncio.Queue:
        q = asyncio.Queue(maxsize=1000)
        self.subscribers.setdefault(event_type, []).append((q, filter_fn))
        return q

    async def publish(self, event_type: str, data: Any):
        for q, filter_fn in self.subscribers.get(event_type, []):
            if filter_fn is None or filter_fn(data):
                try:
                    q.put_nowait(data)
                except asyncio.QueueFull:
                    _ = await q.get()
                    await q.put(data)

    # ---------------- Producer Callbacks ----------------
    def tick_callback(self, loop, symbol, tick):
        loop.call_soon_threadsafe(
            asyncio.create_task,
            self.publish("tick", (symbol, tick))
        )

    def candle_callback(self, loop, symbol, candle):
        loop.call_soon_threadsafe(
            asyncio.create_task,
            self.publish("candle", (symbol, candle))
        )

    def order_close_callback(self, loop, pos):
        loop.call_soon_threadsafe(
            asyncio.create_task,
            self.publish("trade_close", pos)
        )

    # ---------------- Wiring Helpers ----------------
    def wire_sources(self, ws_mgr, order_mgr, loop):
        # Candle feed
        ws_mgr.subscribe_symbol(
            "NSE:NIFTY50-INDEX",
            mode="candle",
            timeframe=30,
            callback=lambda symbol, candle: self.candle_callback(loop, symbol, candle)
        )

        # Order close feed
        order_mgr.register_close_callback(
            lambda pos: self.order_close_callback(loop, pos)
        )

# Global instance
event_bus = EventBus()
