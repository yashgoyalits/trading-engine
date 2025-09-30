import asyncio
from typing import Callable, Any, Dict, List, Tuple
from utils.error_handling import error_handling 

@error_handling
class EventBus:
    def __init__(self):
        self.subscribers: Dict[str, List[Tuple[asyncio.Queue, Callable[[Any], bool]]]] = {}

    def subscribe(self, event_type: str, filter_fn: Callable[[Any], bool] = None) -> asyncio.Queue:
        q = asyncio.Queue(maxsize=1000)
        self.subscribers.setdefault(event_type, []).append((q, filter_fn))
        return q

    async def publish(self, event_type: str, data: Any) -> None:
        for q, filter_fn in self.subscribers.get(event_type, []):
            if filter_fn is None or filter_fn(data):
                try:
                    q.put_nowait(data)
                except asyncio.QueueFull:
                    _ = await q.get()
                    await q.put(data)

event_bus = EventBus()
