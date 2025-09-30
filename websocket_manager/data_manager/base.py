import asyncio
from utils.error_handling import error_handling
from utils.logger import logger
from .tick_processor import TickProcessor
from .candle_builder import CandleBuilder

@error_handling
class BaseWSManager:
    def __init__(self, transport, tick_processor=None, candle_builder=None):
        self.transport = transport
        self.symbols = {}
        self._running = False
        self._loop = None
        self._queue = asyncio.Queue(maxsize=1000)
        self.tick_processor = tick_processor or TickProcessor()
        self.candle_builder = candle_builder or CandleBuilder(self.tick_processor)
        self.log = logger

    def subscribe_symbol(self, symbol, mode="candle", timeframe=30):
        if symbol not in self.symbols:
            self.symbols[symbol] = {"mode": mode, "timeframe": timeframe}
            if self._running:
                self.transport.subscribe([symbol])

    def unsubscribe_symbol(self, symbol):
        if symbol in self.symbols:
            if self._running:
                self.transport.unsubscribe([symbol])
            del self.symbols[symbol]

    async def _process_queue(self):
        while self._running:
            message = await self._queue.get()
            await self._process_message(message)

    async def _process_message(self, message):
        symbol = message.get("symbol")
        if not symbol or symbol not in self.symbols or not self._running:
            return
        cfg = self.symbols[symbol]
        if cfg["mode"] == "tick":
            await self.tick_processor.process_tick(symbol, message)
        else:
            if await self.tick_processor.process_tick(symbol, message):
                await self.candle_builder.process_candle_tick(symbol, message, cfg["timeframe"])

    async def _subscribe_all(self):
        if self.symbols:
            self.transport.subscribe(list(self.symbols.keys()))

    async def _precision_monitor(self):
        while self._running:
            await self.candle_builder.check_and_complete_candles()
            await asyncio.sleep(0.001)

    async def start(self, loop=None):
        if self._running:
            return
        self._running = True
        self._loop = loop or asyncio.get_running_loop()

        self.transport.start(
            loop=self._loop,
            queue=self._queue,
            on_open=lambda: asyncio.run_coroutine_threadsafe(self._subscribe_all(), self._loop),
            on_error=lambda e: self.log.error(f"[WS Error] {e}"),
            on_close=lambda e: self.log.info(f"[WS Closed] {e}")
        )

        asyncio.create_task(self._process_queue())
        asyncio.create_task(self._precision_monitor())

    async def stop(self):
        if not self._running:
            return
        self._running = False
        self.transport.close()
        self.log.info("[WS Manager] Stopped")
