from dataclasses import dataclass
from typing import List, Dict, Optional, Any
from datetime import datetime

@dataclass(slots=True)
class TradeData:
    trade_no: int
    strategy_id: str
    order_id: str
    stop_order_id: Optional[str] = None
    target_order_id: Optional[str] = None
    symbol: Optional[str] = None
    position_id: Optional[str] = None
    qty: Optional[int] = None
    side: Optional[str] = None
    entry_price: Optional[float] = None
    stop_price: Optional[float] = None
    target_price: Optional[float] = None
    sl_points: Optional[float] = None
    target_points: Optional[float] = None
    trailing_levels: List[Dict[str, Any]] | None = None

@dataclass(slots=True)
class Tick:
    symbol: str
    ltp: float
    timestamp: float
    datetime: datetime
    volume: int = 0

@dataclass(slots=True)
class Candle:
    symbol: str
    open: float
    high: float
    low: float
    close: float
    start_time: datetime
    volume: int = 0

    def update(self, ltp: float, volume: int = 0) -> None:
        self.high = max(self.high, ltp)
        self.low = min(self.low, ltp)
        self.close = ltp
        self.volume += volume

