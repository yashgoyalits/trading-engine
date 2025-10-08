from dataclasses import dataclass
from typing import List, Dict, Optional, Any

@dataclass(slots=True)
class TradeData:
    trade_no: int
    strategy_id: str
    order_id: str
    stop_order_id: str
    target_order_id: str
    symbol: str
    position_id: str
    qty: int
    side: str
    entry_price: Optional[float]
    initial_stop_price: Optional[float]
    target_price: Optional[float]
    initial_sl_points: Optional[float]
    target_points: Optional[float]
    trailing_levels: List[Dict[str, Any]]
    trailing_history: List[Dict[str, Any]]
