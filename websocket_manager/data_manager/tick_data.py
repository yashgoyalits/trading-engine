from dataclasses import dataclass
from datetime import datetime

@dataclass(slots=True)
class TickData:
    tick_symbol: str
    tick_ltp: float
    tick_exch_feed_time: int  

    def __post_init__(self):
        if isinstance(self.tick_exch_feed_time, int):
            self.tick_exch_feed_time = datetime.fromtimestamp(self.tick_exch_feed_time)
