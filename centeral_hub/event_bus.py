import asyncio
from typing import Callable, Any, Dict, List, Tuple
from utils.error_handling import error_handling

class BoundedQueue(asyncio.Queue):
    def put_nowait_drop(self, item: Any):
        while self.full():
            # Remove oldest item safely
            removed = self.get_nowait() if not self.empty() else None
        self.put_nowait(item)

@error_handling
class EventBus:
    _subscribers: Dict[str, List[Tuple[BoundedQueue, Callable[[Any], bool]]]] = {}

    @classmethod
    def subscribe(cls, event_type: str, filter_fn: Callable[[Any], bool] = None) -> BoundedQueue:
        q = BoundedQueue(maxsize=1000)
        cls._subscribers.setdefault(event_type, []).append((q, filter_fn))
        return q

    @classmethod
    async def publish(cls, event_type: str, data: Any) -> None:
        subscribers = cls._subscribers.get(event_type, [])
        if not subscribers:
            return

        for q, filter_fn in subscribers:
            if filter_fn is None or filter_fn(data):
                # Drop oldest if full without exceptions
                if q.full() and not q.empty():
                    _ = q.get_nowait()
                q.put_nowait(data)
