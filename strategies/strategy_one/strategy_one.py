# strategy/strategy_one.py
import asyncio
from strategies.strategy_one.strategy_one_logic import StrategyLogicManager
from strategies.strategy_one.strategy_one_trailling import strategy_one_trailing
from utils.csv_builder import CSVBuilder
from utils.logger import logger
from centeral_hub.event_bus import EventBus
from order_active_state_manager.order_state_manager import OrderManager
from order_placement_manager.fyers_order_placement import FyersOrderPlacement
from utils.error_handling import error_handling

@error_handling 
class StrategyOne:
    def __init__(self, strategy_id, ws_mgr, loop, max_trades=1):
        self.strategy_id = strategy_id
        self.ws_mgr = ws_mgr
        self.loop = loop
        self.max_trades = max_trades

        self.csv_builder = CSVBuilder()
        self.strategy_logic_manager = StrategyLogicManager()
        self.fyers_order_placement = FyersOrderPlacement()

        self.candle_queue = EventBus.subscribe("candle")
        self.tick_queue = EventBus.subscribe("tick")
        self.trade_close_queue = EventBus.subscribe("trade_close")

        self.trades_done = 0
        self.active_order_id = None

    # ------------------ Max Trade Check ------------------
    def is_max_trade_reached(self):
        if self.trades_done >= self.max_trades:
            logger.info(f"[{self.strategy_id}] Max trade limit reached: {self.trades_done}/{self.max_trades}")
            return True
        return False
    
    # ------------------ Position Management ------------------
    async def manage_position(self, pos): 
        active_symbol = pos.get("symbol")
        net_qty = pos.get("netQty", 0)
        realized = pos.get("realized_profit", 0)
        position_id = pos.get("id")

        if self.active_order_id and net_qty == 0:  #--- TRADE CLOSE ----- 
            self.ws_mgr.unsubscribe_symbol("NSE:NIFTY25OCT24800CE")
            order_obj = await OrderManager.get_order(self.active_order_id)
            if order_obj:
                trade_data = order_obj.to_trade_done_data(trade_no=self.trades_done)
                await self.csv_builder.log_trade(trade_data)
                await OrderManager.remove_order(self.active_order_id)
                logger.info(f"[{self.strategy_id}] Trade {self.trades_done} closed")
                logger.info(f"[{self.strategy_id}] Trade {self.trades_done} PNL: {realized}")
            self.active_order_id = None
        elif self.active_order_id: #--- TRADE OPEN -----  
            await OrderManager.add_order(self.fyers_order_placement, self.strategy_id, self.active_order_id, position_id, active_symbol)
            self.ws_mgr.subscribe_symbol(
                "NSE:NIFTY25OCT24800CE",
                mode="tick",
            )
            logger.info(f"[{self.strategy_id}] Position OPEN: {active_symbol}, Qty: {net_qty}")

    # ------------------ Consumers ------------------
    async def candle_consumer(self):
        # Skip candle
        _ = await self.candle_queue.get()
        logger.info("skipped candle")

        while True:
            symbol, candle = await self.candle_queue.get()
            if self.active_order_id is None:
                if self.is_max_trade_reached():
                    logger.info("cancelling all task")
                    for task in self.tasks:
                        if not task.done():
                            task.cancel()
                    break  
                condition_met = await self.strategy_logic_manager.check_entry_condition(self.strategy_id, symbol, candle)
                if condition_met:
                    order_response = await self.fyers_order_placement.place_order(symbol="NSE:IDEA-EQ", qty=1, order_type=2, side=1, stop_loss=0.5, take_profit=2.0)
                    self.active_order_id = order_response.get("id")
                    self.trades_done += 1
                    logger.info(f"[{self.strategy_id}] Order placed with ID: {self.active_order_id}")

    async def tick_consumer(self):
        while True:
            symbol, tick = await self.tick_queue.get()  
            if self.active_order_id:
                await strategy_one_trailing.start_trailing_sl(self.fyers_order_placement, self.strategy_id, symbol, self.active_order_id, tick)

    async def trade_close_consumer(self):
        while True:
            pos = await self.trade_close_queue.get()  
            await self.manage_position(pos)

    # ------------------ Run ------------------
    async def run(self):
        async with asyncio.TaskGroup() as tg:
            candle_task = tg.create_task(self.candle_consumer())
            tick_task = tg.create_task(self.tick_consumer())
            trade_close_task = tg.create_task(self.trade_close_consumer())
            
            self.tasks = [candle_task, tick_task, trade_close_task]
