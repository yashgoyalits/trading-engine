# strategy/strategy_one.py
import asyncio
from src.core.data_model import TradeData
from src.strategies.strategy_one.logic_manager import StrategyLogicManager
from src.strategies.strategy_one.trailing_manager import TrailingManager
from src.managers.order_state_manager import ActiveTradesManager
from src.managers.order_placement_manager import FyersOrderPlacement
from src.infrastructure.error_handling import error_handling
from src.infrastructure.logger import logger

@error_handling 
class StrategyOne:
    def __init__(self, event_bus, strategy_id, ws_mgr, loop, max_trades=1):
        self.event_bus = event_bus
        self.strategy_id = strategy_id
        self.ws_mgr = ws_mgr
        self.loop = loop
        self.max_trades = max_trades

        self.ActiveTradesManager = ActiveTradesManager(event_bus, strategy_id)
        self.StrategyLogicManager = StrategyLogicManager()
        self.FyersOrderPlacement = FyersOrderPlacement()
        self.TrailingManager = TrailingManager()

        self.candle_queue = self.event_bus.subscribe("candle")
        self.tick_queue = self.event_bus.subscribe("tick")
        self.trade_close_queue = self.event_bus.subscribe("fyers_position_update")
        self.order_queue = self.event_bus.subscribe("fyers_order_update")

        self.trades_done = 0
        self.active_trade_data_obj: TradeData | None = None
        self.tasks = []
        self.pending_order_queue = asyncio.Queue()


    async def process_orders(self, order, active_trade):
        parent_id = order.get("parentId")
        status = order.get("status")
        order_id = order.get("id")
        order_type = order.get("type")

        # ---------------- Parent Order updates ----------------
        if order_id == active_trade.order_id:  # Parent Order updates
            if order_type == 2 and status == 2:  # parent order filled at market price
                logger.info(f"Order Filled, ID: {order_id}")

                # only set these if they are not already present in the stored trade
                current = await self.ActiveTradesManager.get_active_trade()
                fields_to_set = {}
                if not getattr(current, "symbol", None):
                    fields_to_set["symbol"] = order.get("symbol")
                if not getattr(current, "qty", None):
                    fields_to_set["qty"] = order.get("qty")
                if not getattr(current, "side", None):
                    fields_to_set["side"] = order.get("side")
                if not getattr(current, "entry_price", None):
                    # keep your original choice: limitPrice
                    fields_to_set["entry_price"] = order.get("limitPrice")

                if fields_to_set:
                    await self.ActiveTradesManager.update_trade(active_trade.order_id, fields_to_set)

                # --- NEW: compute trailing_levels and save if not present ---
                # determine entry price (prefer already-saved, else the parent order value)
                current = await self.ActiveTradesManager.get_active_trade()
                entry = getattr(current, "entry_price", None) or order.get("limitPrice") or order.get("tradedPrice")
                if entry is not None:
                    # Only set trailing_levels if not already present
                    if not getattr(current, "trailing_levels", None):
                        # Build trailing levels relative to entry
                        trailing = [
                            {
                                "threshold": float(entry) + 3.0,
                                "new_stop": float(entry) + 0.1,
                                "msg": "breakeven",
                                "hit": False
                            },
                            {
                                "threshold": float(entry) + 10.0,
                                "new_stop": float(entry) + 0.2,
                                "msg": "1st trail locked profit",
                                "hit": False
                            }
                        ]
                        await self.ActiveTradesManager.update_trade(
                            active_trade.order_id,
                            {"trailing_levels": trailing}
                        )

                self.ws_mgr.subscribe_symbol("NSE:NIFTY2631024800CE", mode="tick")

        # ---------------- Child order updates ----------------
        if parent_id == active_trade.order_id:  # Child order updates

            # STOP child (type == 4) pending
            if order_type == 4 and status == 6:  # stop order

                current = await self.ActiveTradesManager.get_active_trade()
                # only set stop fields if not already present
                if not getattr(current, "stop_order_id", None) and not getattr(current, "stop_price", None):
                    await self.ActiveTradesManager.update_trade(
                        active_trade.order_id,
                        {
                            "stop_order_id": order_id,
                            "stop_price": order.get("stopPrice")
                        }
                    )

            # TARGET child (type == 1) pending
            if order_type == 1 and status == 6:  # target order

                current = await self.ActiveTradesManager.get_active_trade()
                # only set target fields if not already present
                if not getattr(current, "target_order_id", None) and not getattr(current, "target_price", None):
                    await self.ActiveTradesManager.update_trade(
                        active_trade.order_id,
                        {
                            "target_order_id": order_id,
                            "target_price": order.get("limitPrice")
                        }
                    )

            # Any child filled -> close trade (keep existing behavior)
            if status == 2:
                logger.info(f"Child Order Filled, Child Order ID: {order_id} for Parent ID: {parent_id}")
                self.ws_mgr.unsubscribe_symbol("NSE:NIFTY2631024800CE")
                await self.ActiveTradesManager.close_trade(active_trade.order_id)
                logger.info(f"[{self.strategy_id}] | Trade {self.trades_done} closed")

    # ------------------ Max Trade Check ------------------
    async def is_max_trade_reached(self):
        if self.trades_done >= self.max_trades:
            logger.info(f"[{self.strategy_id}] Max trade limit reached: {self.trades_done}/{self.max_trades}")
            for task in self.tasks:
                if not task.done():
                    task.cancel()
            return True
        return False

    
    # ------------------ Consumers ------------------
    async def candle_consumer(self):
        # Skip first candle
        _ = await self.candle_queue.get()
        logger.info("skipped candle")

        while True:
            candle = await self.candle_queue.get()
            active_trade = await self.ActiveTradesManager.get_active_trade()
            if active_trade is None:
                if await self.is_max_trade_reached():
                    break  
                
                condition_met = await self.StrategyLogicManager.check_entry_condition(
                    self.strategy_id, candle.symbol, candle
                )
                
                if condition_met:
                    order_response = await self.FyersOrderPlacement.place_order(
                        symbol="NSE:IDEA-EQ", qty=1, order_type=2, side=1, stop_loss=0.5, take_profit=2.0
                    )
                    if order_response.get('code') == 1101:
                        self.trades_done += 1
                        await self.ActiveTradesManager.add_trade(
                            self.trades_done, self.strategy_id, order_response.get("id") 
                        )
                        logger.info(f"Order Placed {order_response.get('id')}")
                    else:
                        logger.info(f"Order Placing FAILED {order_response}")

    
    async def tick_consumer(self):
        while True:
            tick = await self.tick_queue.get()
            active_trade = await self.ActiveTradesManager.get_active_trade()
            if active_trade and active_trade.trailing_levels != []:
                await self.TrailingManager.start_trailing_sl(
                    self.FyersOrderPlacement,
                    active_trade.trailing_levels,
                    active_trade.stop_order_id, 
                    active_trade.qty,
                    tick
                )
            else:
                logger.info("No active trade or No trailing levels")


    async def orders_consumer(self):
        while True:
            order = await self.order_queue.get()
            active_trade = await self.ActiveTradesManager.get_active_trade()

            if not active_trade: # No active trade found
                await self.pending_order_queue.put(order)
                continue

            while not self.pending_order_queue.empty(): # Process buffered orders
                pending_order = await self.pending_order_queue.get()
                await self.process_orders(pending_order, active_trade)

            await self.process_orders(order, active_trade)    # Process current order

    # ------------------ Run ------------------
    async def run(self):
        async with asyncio.TaskGroup() as tg:
            self.tasks = [
                tg.create_task(self.candle_consumer()),
                tg.create_task(self.tick_consumer()),
                tg.create_task(self.orders_consumer()),
            ]
