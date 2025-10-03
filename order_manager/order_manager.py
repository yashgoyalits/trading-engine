import asyncio
from typing import ClassVar, Dict, List, Optional
from utils.error_handling import error_handling
from data_model.data_model import TradeDoneData

@error_handling
class OrderManager:
    _registry: ClassVar[Dict[str, "OrderManager"]] = {}
    _lock: ClassVar[asyncio.Lock] = asyncio.Lock()

    def __init__(
        self,
        strategy_id: str,
        main_order_id: str,
        symbol: str,
        stop_order_id: Optional[str] = None,
        target_order_id: Optional[str] = None,
        entry_price: Optional[float] = None,
        initial_stop_price: Optional[float] = None,
        target_price: Optional[float] = None,
        trailing_levels: Optional[List[dict]] = None,
        position_id: Optional[str] = None,
    ):
        self.strategy_id = strategy_id
        self.main_order_id = main_order_id
        self.symbol = symbol
        self.stop_order_id = stop_order_id
        self.target_order_id = target_order_id
        self.entry_price = entry_price
        self.initial_stop_price = initial_stop_price
        self.target_price = target_price
        self.trailing_levels = trailing_levels or []
        self.trailing_history: List[dict] = []
        self.position_id = position_id

        # Auto-register
        OrderManager._registry[self.main_order_id] = self

    def __repr__(self):
        return (
            f"OrderManager(strategy={self.strategy_id}, "
            f"main_order_id={self.main_order_id}, symbol={self.symbol}, "
            f"entry_price={self.entry_price}, trailing_levels={self.trailing_levels})"
        )

    # ---------------- Class Methods ----------------
    @classmethod
    async def add_order(
        cls, fyers_order_placement, strategy_id: str, main_order_id: str, position_id: str, symbol: str
    ) -> "OrderManager":
        main, stop, target = await fyers_order_placement.get_main_stop_target_orders(main_order_id)
        if not main:
            raise ValueError(f"No main order found for {main_order_id}")

        order = cls(
            strategy_id=strategy_id,
            main_order_id=main_order_id,
            position_id=position_id,
            symbol=symbol,
            stop_order_id=stop.get("id") if stop else None,
            target_order_id=target.get("id") if target else None,
            entry_price=main.get("tradedPrice"),
            initial_stop_price=stop.get("stopPrice") if stop else None,
            target_price=target.get("limitPrice") if target else None,
            trailing_levels=[
                {"threshold": main.get("tradedPrice") + 3, "new_stop": main.get("tradedPrice") + 0.1, "msg": "breakeven"},
                {"threshold": main.get("tradedPrice") + 10, "new_stop": main.get("tradedPrice") + 0.2, "msg": "1st trail locked profit"},
            ] if main.get("tradedPrice") else [],
        )
        async with cls._lock:
            cls._registry[main_order_id] = order
        return order

    @classmethod
    async def update_order(cls, main_order_id: str, **kwargs) -> Optional["OrderManager"]:
        async with cls._lock:
            order = cls._registry.get(main_order_id)
            if not order:
                return None
            for k, v in kwargs.items():
                if hasattr(order, k):
                    setattr(order, k, v)
            return order

    @classmethod
    async def get_order(cls, main_order_id: str) -> Optional["OrderManager"]:
        async with cls._lock:
            return cls._registry.get(main_order_id)

    @classmethod
    async def remove_order(cls, main_order_id: str) -> Optional["OrderManager"]:
        async with cls._lock:
            return cls._registry.pop(main_order_id, None)

    @classmethod
    async def list_orders_for_strategy(cls, strategy_id: str) -> List["OrderManager"]:
        async with cls._lock:
            return [o for o in cls._registry.values() if o.strategy_id == strategy_id]

    # ---------------- Export as Data Model ----------------
    def to_trade_done_data(self, trade_no: int) -> TradeDoneData:
        side = "BUY" if self.target_price and self.entry_price and self.target_price > self.entry_price else "SELL"
        return TradeDoneData(
            strategy_id=self.strategy_id,
            trade_no=trade_no,
            order_id=self.main_order_id,
            symbol=self.symbol,
            position_id=self.position_id,
            qty=1,
            side=side,
            entry_price=self.entry_price,
            initial_stop_price=self.initial_stop_price,
            target_price=self.target_price,
            initial_sl_points=(self.initial_stop_price - self.entry_price) if self.entry_price and self.initial_stop_price else None,
            target_points=(self.target_price - self.entry_price) if self.entry_price and self.target_price else None,
            trailing_levels=self.trailing_levels,
            trailing_history=self.trailing_history,
        )
