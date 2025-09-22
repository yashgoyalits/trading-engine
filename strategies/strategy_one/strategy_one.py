# strategy/strategy_one.py
import asyncio
from strategies.strategy_one.strategy_one_logic import strategy_logic_manager
from strategies.strategy_one.strategy_one_trailling import strategy_one_trailing
from utils.csv_builder import csv_builder
from utils.logger import logger
from centeral_hub.event_bus import event_bus
from order_manager.order_manager import OrderManager
from utils.error_handling import error_handling

@error_handling 
class StrategyOne:
    def __init__(self, strategy_id, ws_mgr, loop, max_trades=1):
        self.strategy_id = strategy_id
        self.ws_mgr = ws_mgr
        self.loop = loop
        self.max_trades = max_trades

        self.candle_queue = event_bus.subscribe("candle")
        self.tick_queue = event_bus.subscribe("tick")
        self.trade_close_queue = event_bus.subscribe("trade_close")

        self.trades_done = 0
        self.active_order_id = None
        self.stop_event = asyncio.Event()

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
            self.ws_mgr.unsubscribe_symbol(active_symbol)
            order_obj = await OrderManager.get_order(self.active_order_id)
            if order_obj:
                await csv_builder.log_trade(self.trades_done, self.active_order_id, order_obj.to_dict())
                await OrderManager.remove_order(self.active_order_id)
                logger.info(f"[{self.strategy_id}] Trade {self.trades_done} closed")
                logger.info(f"[{self.strategy_id}] Trade {self.trades_done} PNL: {realized}")
            self.active_order_id = None
        elif self.active_order_id: #--- TRADE OPEN -----  
            await OrderManager.add_order(self.strategy_id, self.active_order_id, position_id, active_symbol)
            self.ws_mgr.subscribe_symbol(
                active_symbol,
                mode="tick",
                callback=lambda sym, tick: event_bus.tick_callback(self.loop, sym, tick)
            )
            logger.info(f"[{self.strategy_id}] Position OPEN: {active_symbol}, Qty: {net_qty}")

    # ------------------ Consumers ------------------
    async def candle_consumer(self):
        # Skip candle
        _ = await self.candle_queue.get()
        logger.info("skipped candle")

        while not self.stop_event.is_set():
            symbol, candle = await self.candle_queue.get()

            if self.active_order_id is None:
                if self.is_max_trade_reached():
                    self.stop_event.set()
                    break
                condition_met, self.active_order_id = await strategy_logic_manager.check_entry_condition(self.strategy_id, symbol, candle)
                if condition_met:
                    self.trades_done += 1
                    logger.info(f"[{self.strategy_id}] Order placed with ID: {self.active_order_id}")

    async def tick_consumer(self):
        while not self.stop_event.is_set():
            processed = False
            while True:
                if self.tick_queue.empty():
                    break
                symbol, tick = self.tick_queue.get_nowait()
                processed = True
                if self.active_order_id:
                    await strategy_one_trailing.start_trailing_sl(self.strategy_id, symbol, self.active_order_id, tick)
            if not processed:
                await asyncio.sleep(0.001)

    async def trade_close_consumer(self):
        while not self.stop_event.is_set():
            if self.trade_close_queue.empty():
                await asyncio.sleep(0.01)
                continue
            pos = self.trade_close_queue.get_nowait()
            await self.manage_position(pos)

    # ------------------ Run ------------------
    async def run(self):
        await asyncio.gather(
            self.candle_consumer(),
            self.tick_consumer(),
            self.trade_close_consumer()
        )
        logger.info(f"[{self.strategy_id} | ENDED")
        return