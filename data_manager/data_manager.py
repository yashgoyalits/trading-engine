import asyncio
from utils.logger import logger
from utils.error_handling import error_handling
from .tick_processor import TickProcessor
from data_model.data_model import Tick
from datetime import datetime
from .candle_builder import CandleBuilder
from broker.fyers_broker.ibroker import IBroker
from .idata_manager import IDataManager
from centeral_hub.event_bus import EventBus

@error_handling
class DataManager(IDataManager):
    def __init__( self, event_bus: EventBus, data_broker: IBroker, order_broker: IBroker = None, tick_processor = TickProcessor, candle_builder = CandleBuilder):
        self.data_broker = data_broker
        self.order_broker = order_broker
        self.event_bus = event_bus
        self.symbols = {}
        self._running = False
        self.tick_processor = tick_processor 
        self.candle_builder = candle_builder 
        self.tick_queue = asyncio.Queue(maxsize=1000)

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
                case "broker_raw_ticks":
                    message = raw_data["data"]
                    if not (symbol := message.get("symbol")) or symbol not in self.symbols:
                        continue
                    
                    cfg = self.symbols[symbol]
                    tick = Tick(
                        symbol=symbol,
                        ltp=message.get('ltp'),
                        timestamp=(ts := message.get("exch_feed_time")),
                        datetime=datetime.fromtimestamp(ts),
                        volume=message.get("volume", 0)
                    )
                    
                    if cfg["mode"] == "tick":
                        await self.tick_processor.process_tick(tick, publish=True)
                    elif await self.tick_processor.process_tick(tick, publish=False):
                        await self.candle_builder.process_candle_tick(tick, cfg["timeframe"])
                
                case "positions":
                    await self.event_bus.publish("fyers_position_update", raw_data["data"])

