import asyncio
from typing import Callable, Any, Dict, List, Tuple

class EventBus:
    def __init__(self):
        self._subscribers: Dict[str, List[Tuple[asyncio.Queue, Callable[[Any], bool]]]] = {}

    def subscribe( self, event_type: str, filter_fn: Callable[[Any], bool] = None, maxsize: int = 1000 ) -> asyncio.Queue:
        q = asyncio.Queue(maxsize=maxsize)
        self._subscribers.setdefault(event_type, []).append((q, filter_fn))
        return q

    async def publish(self, event_type: str, data: Any) -> None:
        subscribers = self._subscribers.get(event_type, [])
        if not subscribers:
            return

        for q, filter_fn in subscribers:
            if filter_fn is None or filter_fn(data):
                q.put_nowait(data)
                await q.get()
                q.put_nowait(data)
                    
