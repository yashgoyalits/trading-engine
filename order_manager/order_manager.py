import asyncio
from typing import ClassVar, Dict, List
from order_manager.fyers_order_placement import fyers_order_placement
from utils.error_handling import error_handling
from utils.csv_builder import TradeRow

@error_handling
class OrderManager:
    # Class-level registry for all active orders
    _registry: ClassVar[Dict[str, "OrderManager"]] = {}
    _lock: ClassVar[asyncio.Lock] = asyncio.Lock()  # async-safe access

    def __init__(
        self,
        strategy_id: str,
        main_order_id: str,
        symbol: str,
        stop_order_id: str = None,
        target_order_id: str = None,
        entry_price: float = None,
        initial_stop_price: float = None,
        target_price: float = None,
        trailing_levels: List[dict] = None,
        position_id: str = None,
    ):
        self.strategy_id = strategy_id
        self.main_order_id = main_order_id
        self.stop_order_id = stop_order_id
        self.target_order_id = target_order_id
        self.symbol = symbol
        self.entry_price = entry_price
        self.initial_stop_price = initial_stop_price
        self.target_price = target_price
        self.trailing_levels = trailing_levels or []
        self.trailing_history = []
        self.position_id = position_id

        # Automatically register this order
        OrderManager._registry[self.main_order_id] = self

    def __repr__(self):
        return (
            f"OrderManager(strategy={self.strategy_id}, "
            f"main_order_id={self.main_order_id}, symbol={self.symbol}, "
            f"entry_price={self.entry_price}, trailing_levels={self.trailing_levels})"
            
        )

    # ---------------- Class Methods ----------------
    @classmethod
    async def add_order(cls, strategy_id: str, main_order_id: str, position_id: str, active_symbol: str):
        # Fetch broker orders
        main, stop, target = await fyers_order_placement.get_main_stop_target_orders(main_order_id)
        
        if not main:
            raise ValueError(f"No main order found for {main_order_id}")

        stop_order_id = stop.get("id") if stop else None
        target_order_id = target.get("id") if target else None
        entry_price = main.get("tradedPrice")
        initial_stop_price = stop.get("stopPrice") if stop else None
        target_price = target.get("limitPrice") if target else None

        # Precompute trailing levels if entry_price exists
        trailing_levels = []
        if entry_price is not None:
            trailing_levels = [
                {"threshold": entry_price + 3, "new_stop": entry_price + 0.1, "msg": "breakeven"},
                {"threshold": entry_price + 10, "new_stop": entry_price + 0.2, "msg": "1st trail locked profit"},
            ]

        # Create ActiveOrderState instance
        async with cls._lock:
            order = cls(
                strategy_id=strategy_id,
                main_order_id=main_order_id,
                position_id=position_id,
                symbol=active_symbol,
                stop_order_id=stop_order_id,
                target_order_id=target_order_id,
                entry_price=entry_price,
                initial_stop_price=initial_stop_price,
                target_price=target_price,
                trailing_levels=trailing_levels,
            )

            # Register the order
            cls._registry[main_order_id] = order
            return order


    @classmethod
    async def update_order(cls, main_order_id: str, **kwargs):
        async with cls._lock:
            order = cls._registry.get(main_order_id)
            if not order:
                return None
            for k, v in kwargs.items():
                if hasattr(order, k):
                    setattr(order, k, v)
            return order
        

    @classmethod
    async def get_order(cls, main_order_id):
        async with cls._lock:
            return cls._registry.get(main_order_id)

    @classmethod
    async def remove_order(cls, main_order_id):
        async with cls._lock:
            return cls._registry.pop(main_order_id, None)

    @classmethod
    async def list_orders_for_strategy(cls, strategy_id):
        async with cls._lock:
            return [o for o in cls._registry.values() if o.strategy_id == strategy_id]
    

    def to_trade_row(self, trade_no: int) -> TradeRow:
        side = "BUY" if self.target_price and self.entry_price and self.target_price > self.entry_price else "SELL"

        return TradeRow(
            strategy_id=self.strategy_id,
            trade_no=trade_no,
            order_id=self.main_order_id,
            symbol=self.symbol,
            position_id=self.position_id,
            qty=1,  # if you later add `qty` in OrderManager, replace here
            side=side,
            entry_price=self.entry_price,
            initial_stop_price=self.initial_stop_price,
            target_price=self.target_price,
            initial_sl_points=(self.initial_stop_price - self.entry_price)
            if self.entry_price and self.initial_stop_price else None,
            target_points=(self.target_price - self.entry_price)
            if self.entry_price and self.target_price else None,
            trailing_levels=self.trailing_levels,
            trailing_history=self.trailing_history,
        )
