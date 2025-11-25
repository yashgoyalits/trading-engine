# strategy/strategy_one.py
import asyncio
from data_model.data_model import TradeData
from strategies.strategy_one.logic_manager import StrategyLogicManager
from strategies.strategy_one.trailing_manager import TrailingManager
from order_active_state_manager.order_state_manager import ActiveTradesManager
from order_placement_manager.order_placement_manager import FyersOrderPlacement
from common_utils.error_handling import error_handling
from common_utils.logger import logger
from strategies.strategy_interface import BaseStrategy

@error_handling 
class StrategyOne(BaseStrategy):
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

        if order_id == active_trade.order_id:  # Parent Order updates
            if status == 2:
                logger.info(f"Order Filled, ID: {order_id}") 
                self.ws_mgr.subscribe_symbol("NSE:NIFTY25D0926000CE", mode="tick")           

        if parent_id == active_trade.order_id: # Child order updates
            if status == 2:
                logger.info(f"Child Order Filled, ID: {order_id} for Parent ID: {parent_id}")
                self.ws_mgr.unsubscribe_symbol("NSE:NIFTY25D0926000CE")
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
        # _ = await self.candle_queue.get()
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

                        main, stop, target = await self.FyersOrderPlacement.get_main_stop_target_orders(order_response.get("id"))
                        trade_details = await self.ActiveTradesManager.compute_trade_fields(main, stop, target)
                        
                        await self.ActiveTradesManager.update_trade(order_response.get("id"), trade_details)
                        
                        logger.info(f"Order Placed {order_response.get("id")}")
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
