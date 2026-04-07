import asyncio
from src.core.data_model import Tick
from datetime import datetime
from src.candle_builder.candle_builder import CandleBuilder
from src.broker.fyers.ibroker import IBroker
from src.infrastructure.event_bus import EventBus
from src.infrastructure.logger import logger
from src.infrastructure.error_handling import error_handling
from src.broker.fyers.data_broker import FyersDataBroker
from src.broker.fyers.order_broker import FyersOrderPositionTracker

@error_handling
class FyersAdapter:
    def __init__( self, event_bus: EventBus, candle_builder = CandleBuilder):
        self.event_bus = event_bus
        self.candle_builder = candle_builder 
        self.symbols = {}
        self._running = False
        self.tick_queue = asyncio.Queue(maxsize=1000)
        self.data_broker = FyersDataBroker()
        self.order_broker = FyersOrderPositionTracker()

    async def start(self):
        if self._running:
            return
        self._running = True
        
        # Start data broker connection
        await self.data_broker.connect(self.tick_queue)
        await self.order_broker.connect(self.tick_queue)
        
        # Wait until data broker is connected
        while not self.data_broker.is_connected() and not self.order_broker.is_connected():
            await asyncio.sleep(0.1)

        # Start queue processor
        asyncio.create_task(self._process_broker_msg_queue())
    
    async def stop(self):
        if not self._running:
            return
        self._running = False
        
        await self.data_broker.disconnect()
        await self.order_broker.disconnect()
        
        logger.info("[Manager] Stopped all websockets")
    
    def subscribe_symbol(self, symbol, mode="candle", timeframe=30):
        if symbol not in self.symbols:
            self.symbols[symbol] = {"mode": mode, "timeframe": timeframe}
            if self._running and self.data_broker.is_connected():
                self.data_broker.subscribe([symbol])
    
    def unsubscribe_symbol(self, symbol):
        if symbol in self.symbols:
            if self._running and self.data_broker.is_connected():
                self.data_broker.unsubscribe([symbol])
            del self.symbols[symbol]

    async def _process_broker_msg_queue(self):
        while self._running:
            if not (raw_data := await self.tick_queue.get()):
                continue

            match raw_data.get("type"):
                case "raw_tick_data":
                    tick_data = raw_data["data"]
                    
                    symbol = tick_data.get("symbol")
                    tick_ltp = tick_data.get("ltp")
                    tick_exch_feed_time = tick_data.get("exch_feed_time")
                    tick_vol = tick_data.get("volume", 0)

                    if symbol not in self.symbols:
                        continue
                    
                    cfg = self.symbols[symbol]
                    
                    tick = Tick(
                        symbol=symbol,
                        ltp=tick_ltp,
                        timestamp=(ts := tick_exch_feed_time),
                        datetime=datetime.fromtimestamp(ts),
                        volume=tick_vol
                    )
                    
                    if cfg["mode"] == "tick":
                        await self.event_bus.publish("tick", tick)
                    elif cfg["mode"] == "candle": 
                        await self.candle_builder.process_candle_tick(tick, cfg["timeframe"])
                
                case "positions_data":
                    await self.event_bus.publish("fyers_position_update", raw_data["data"])
                
                case "orders_data":
                    await self.event_bus.publish("fyers_order_update", raw_data["data"])

