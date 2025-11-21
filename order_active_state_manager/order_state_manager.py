import asyncio
from typing import Dict
from common_utils.error_handling import error_handling
from data_model.data_model import TradeData

@error_handling
class TradeManager:
    def __init__(self, event_bus, strategy_id: str):
        self.event_bus = event_bus
        self.strategy_id = strategy_id
        self._trades: Dict[str, TradeData] = {}  # in-memory trades for this strategy
        self._lock = asyncio.Lock()

    
    async def add_trade(self, trade_no: int, strategy_id: str, main_order_id: str):
        trade_data = TradeData(
            trade_no=trade_no,
            strategy_id=self.strategy_id,
            order_id=main_order_id,
        )

        async with self._lock:
            self._trades[main_order_id] = trade_data

        return trade_data

    
    async def compute_trade_fields(self, main, stop, target) -> Dict:
        if not main:
            return {}

        entry = main.get("tradedPrice")
        stop_price = stop.get("stopPrice") if stop else None
        target_price = target.get("limitPrice") if target else None

        return {
            "stop_order_id": stop.get("id") if stop else None,
            "target_order_id": target.get("id") if target else None,
            "symbol": main.get("symbol"),
            "qty": main.get("qty"),
            "side": "BUY" if (target and entry < target_price) else "SELL",
            "entry_price": entry,
            "stop_price": stop_price,
            "target_price": target_price,
            "sl_points": (stop_price - entry) if stop_price else None,
            "target_points": (target_price - entry) if target_price else None,
            "trailing_levels": [
                {"threshold": entry + 3, "new_stop": entry + 0.1, "msg": "breakeven", "hit": False},
                {"threshold": entry + 10, "new_stop": entry + 0.2, "msg": "1st trail locked profit", "hit": False},
            ] if entry else [],
        }


    async def update_trade(self, main_order_id: str, update: Dict) -> TradeData | None:
        async with self._lock:
            activeTrade = self._trades.get(main_order_id)
            if not activeTrade:
                return None 

            # professional reducer-style state patch
            for key, value in update.items():
                if hasattr(activeTrade, key):
                    setattr(activeTrade, key, value)

            return activeTrade
    
    
    async def close_trade(self, main_order_id: str) -> None:
        async with self._lock:
            trade = self._trades.pop(main_order_id, None)
            if trade:
                await self.event_bus.publish("trade_close", trade)
