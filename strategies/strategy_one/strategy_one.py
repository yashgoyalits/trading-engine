# strategy/strategy_one.py
import asyncio
from data_model.data_model import TradeData
from strategies.strategy_one.logic_manager import StrategyLogicManager
from strategies.strategy_one.trailing_manager import TrailingManager
from order_active_state_manager.order_state_manager import TradeManager
from order_placement_manager.order_placement_manager import FyersOrderPlacement
from common_utils.error_handling import error_handling
from common_utils.logger import logger

@error_handling 
class StrategyOne():
    def __init__(self, event_bus, strategy_id, ws_mgr, loop, max_trades=1):
        self.event_bus = event_bus
        self.strategy_id = strategy_id
        self.ws_mgr = ws_mgr
        self.loop = loop
        self.max_trades = max_trades

        self.order_state_manager = TradeManager(event_bus, strategy_id)
        self.strategy_logic_manager = StrategyLogicManager()
        self.fyers_order_placement = FyersOrderPlacement()
        self.trailing_manager = TrailingManager()

        self.candle_queue = self.event_bus.subscribe("candle")
        self.tick_queue = self.event_bus.subscribe("tick")
        self.trade_close_queue = self.event_bus.subscribe("fyers_position_update")
        self.order_queue = self.event_bus.subscribe("fyers_order_update")

        self.trades_done = 0
        self.active_trade_data_obj: TradeData | None = None
        self.active_trades = []
        self.tasks = []

    # ------------------ Max Trade Check ------------------
    async def is_max_trade_reached(self):
        if self.trades_done >= self.max_trades:
            logger.info(f"[{self.strategy_id}] Max trade limit reached: {self.trades_done}/{self.max_trades}")
            for task in self.tasks:
                if not task.done():
                    task.cancel()
            return True
        return False
    
    # ------------------ Position Management ------------------
    async def manage_position(self, pos): 
        active_symbol = pos.get("symbol")
        net_qty = pos.get("netQty", 0)
        realized = pos.get("realized_profit", 0)

        if net_qty == 0:  #--- TRADE CLOSE ----- 
            if self.active_trade_data_obj.order_id:
                self.ws_mgr.unsubscribe_symbol("NSE:NIFTY25NOV26100CE")
                await self.order_state_manager.close_trade(self.active_trade_data_obj.order_id)
                logger.info(f"[{self.strategy_id}] | Trade {self.trades_done} closed | PNL: {realized}")
                self.active_trade_data_obj = None
            else:
                logger.info("No Order Found")


    # ------------------ Consumers ------------------
    async def candle_consumer(self):
        # Skip first candle
        _ = await self.candle_queue.get()
        logger.info("skipped candle")

        while True:
            candle = await self.candle_queue.get()
            if self.active_trade_data_obj is None:
                if await self.is_max_trade_reached():
                    break  
                condition_met = await self.strategy_logic_manager.check_entry_condition(
                    self.strategy_id, candle.symbol, candle
                )
                if condition_met:
                    order_response = await self.fyers_order_placement.place_order(
                        symbol="NSE:IDEA-EQ", qty=1, order_type=2, side=1, stop_loss=0.5, take_profit=2.0
                    )
                    if order_response.get('code') == 1101:
                        self.trades_done += 1
                        main, stop, target = await self.fyers_order_placement.get_main_stop_target_orders(order_response.get("id"))
                        self.ws_mgr.subscribe_symbol("NSE:NIFTY25NOV26100CE", mode="tick")
                        
                        self.active_trade_data_obj = await self.order_state_manager.add_trade(
                            self.trades_done, self.strategy_id, order_response.get("id")
                        )

                        trade_details = await self.order_state_manager.compute_trade_fields(main, stop, target)
                        
                        await self.order_state_manager.update_trade(order_response.get("id"), trade_details)
                        
                        logger.info(f"Order Placed {order_response.get("id")}")
                    else:
                        logger.info("Order is Not Placed")

    async def tick_consumer(self):
        while True:
            tick = await self.tick_queue.get()
            trade = self.active_trade_data_obj
            if trade and trade.trailing_levels:
                await self.trailing_manager.start_trailing_sl(
                    self.fyers_order_placement,
                    trade.trailing_levels,
                    trade.stop_order_id, 
                    trade.qty,
                    tick
                )
            else:
                logger.info("No active trade or trailing levels blank")


    async def broker_postion_consumer(self):
        while True:
            pos = await self.trade_close_queue.get()  
            await self.manage_position(pos)

    async def broker_order_consumer(self):
        while True:
            order = await self.order_queue.get()
            # logger.info(f"Order Update {order}")
            # if order.get("parentId"):
            #     if order.get("status") == 6:
            #         logger.info(f"Child Order Pending: {order.get("id")}")
            #     if order.get("status") == 2:
            #         logger.info(f"Child Order Filled: {order.get("id")}")
            # else:
            #     if order.get("status") == 6:
            #         logger.info(f"Parent Order Pending: {order.get("id")}")
            #     if order.get("status") == 2:
            #         logger.info(f"Parent Order Filled: {order.get("id")}")

    # ------------------ Run ------------------
    async def run(self):
        async with asyncio.TaskGroup() as tg:
            self.tasks = [
                tg.create_task(self.candle_consumer()),
                tg.create_task(self.tick_consumer()),
                tg.create_task(self.broker_postion_consumer()),
                tg.create_task(self.broker_order_consumer()),
            ]
