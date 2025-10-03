import asyncio
from utils.logger import logger
from utils.error_handling import error_handling
from .tick_processor import TickProcessor
from .candle_builder import CandleBuilder
from .broker_interface import BrokerInterface

@error_handling
class BaseWSManager:
    def __init__(self, broker: BrokerInterface, tick_processor=None, candle_builder=None):
        self.broker = broker
        self.symbols = {}
        self._running = False
        self.tick_processor = tick_processor or TickProcessor()
        self.candle_builder = candle_builder or CandleBuilder(self.tick_processor)
        self.tick_queue = asyncio.Queue()
        self.log = logger

    def subscribe_symbol(self, symbol, mode="candle", timeframe=30):
        if symbol not in self.symbols:
            self.symbols[symbol] = {"mode": mode, "timeframe": timeframe}
            if self._running and self.broker.is_connected():
                self.broker.subscribe([symbol])

    def unsubscribe_symbol(self, symbol):
        if symbol in self.symbols:
            if self._running and self.broker.is_connected():
                self.broker.unsubscribe([symbol])
            del self.symbols[symbol]

    async def _process_broker_msg_queue(self):
        while self._running:
            message = await self.tick_queue.get()
            if not message:
                continue
            symbol = message.get("symbol")
            if not symbol or symbol not in self.symbols:
                continue
            cfg = self.symbols[symbol]
            
            if cfg["mode"] == "tick":
                await self.tick_processor.process_tick(symbol, message, publish=True)
            else:
                if await self.tick_processor.process_tick(symbol, message, publish=False):
                    await self.candle_builder.process_candle_tick(symbol, message, cfg["timeframe"])


    async def _precision_monitor(self):
        while self._running:
            await self.candle_builder.check_and_complete_candles()
            await asyncio.sleep(0.001)

    async def start(self):
        if self._running:
            return
        self._running = True

        # Start queue processor
        asyncio.create_task(self._process_broker_msg_queue())

        # Start broker connection
        await self.broker.connect(self.tick_queue)

        # Wait until broker is connected
        while not self.broker.is_connected():
            await asyncio.sleep(0.1)

        # Subscribe symbols
        if self.symbols:
            self.broker.subscribe(list(self.symbols.keys()))

        # Start candle precision monitor
        asyncio.create_task(self._precision_monitor())

        self.log.info("[Manager] Started")

    async def stop(self):
        if not self._running:
            return
        self._running = False
        self.log.info("[Manager] Stopped")
