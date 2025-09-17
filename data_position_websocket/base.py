#base.py
import asyncio
from utils.logger import logger
from .tick_processor import TickProcessor
from .candle_builder import CandleBuilder

class BaseWSManager:
    def __init__(self, transport, tick_processor=None, candle_builder=None):
        self.transport = transport
        self.symbols = {}  # {symbol: {"mode": "tick"|"candle", "timeframe": int}}
        self._running = False
        self._loop = None
        self.tick_processor = tick_processor or TickProcessor()
        self.candle_builder = candle_builder or CandleBuilder(self.tick_processor)
        self.log = logger

    def subscribe_symbol(self, symbol, mode="candle", timeframe=30, callback=None):
        if symbol not in self.symbols:
            self.symbols[symbol] = {"mode": mode, "timeframe": timeframe}
            if callback:
                (self.tick_processor if mode == "tick" else self.candle_builder).add_callback(symbol, callback)
            if self._running:
                self.transport.subscribe([symbol])

    def unsubscribe_symbol(self, symbol):
        if symbol in self.symbols:
            if self._running:
                self.transport.unsubscribe([symbol])
            del self.symbols[symbol]

    def _submit_task(self, coro):
        if self._running and self._loop:
            self._loop.call_soon_threadsafe(lambda: asyncio.create_task(coro))

    def _on_message(self, message):
        if self._running and self._loop:
            self._submit_task(self._process_message(message))

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

    def _on_open(self):
        self._submit_task(self._subscribe_all())

    async def _subscribe_all(self):
        if self.symbols:
            self.transport.subscribe(list(self.symbols.keys()))

    async def _precision_monitor(self):
        while self._running:
            await self.candle_builder.check_and_complete_candles()
            await asyncio.sleep(0.001)

    def start(self, loop=None):
        if self._running:
            return
        self._running = True
        self._loop = loop or asyncio.get_event_loop()
        self.transport.connect(
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=lambda m: self.log.info(f"[Data WS Error] {m}"),
            on_close=lambda m: self.log.info(f"[Data WS Closed] {m}"),
        )
        self._submit_task(self._precision_monitor())

    def stop(self):
        if not self._running:
            return
        self._running = False
        try:
            self.transport.close()
        finally:
            self.log.info("[WS Manager] Stopped")
