import asyncio
from collections import defaultdict

class CenEventBus:
    def __init__(self):
        self.queues = defaultdict(asyncio.Queue)

    def publish(self, event_type, data):
        queue = self.queues[event_type]
        queue.put_nowait(data)

    async def subscribe(self, event_type):
        queue = self.queues[event_type]
        while True:
            event = await queue.get()
            yield event  
            queue.task_done()
