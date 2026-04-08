import os
import threading
import asyncio
import json
from dotenv import load_dotenv
from fyers_apiv3.FyersWebsocket import data_ws
from src.infrastructure.logger import logger
from src.infrastructure.error_handling import error_handling
from src.broker.writer import TickManager

load_dotenv()

@error_handling
class FyersDataBroker:
    def __init__(self, access_token=None):
        self._token = access_token or os.getenv("FYERS_ACCESS_TOKEN")
        if not self._token:
            logger.warning("FYERS_ACCESS_TOKEN is missing")
        self._socket = None
        self._thread = None
        self._running = False
        self._loop = None
        self._queue: asyncio.Queue = None
        self._connected = False

    async def connect(self, queue: asyncio.Queue):
        if self._running:
            return
        self._running = True
        self._loop = asyncio.get_running_loop()
        self._queue = queue

        self._thread = threading.Thread(target=self._run_ws, daemon=True)
        self._thread.start()

    def _run_ws(self):
        def _on_open():
            self._connected = True
            logger.info("Fyers Data Websocket Connected")

        def _on_message(message):
            if message["type"] == "if" or message["type"] == "sf":
                if self._running and self._queue:
                    asyncio.run_coroutine_threadsafe(self._queue.put({"type": "raw_tick_data", "data": message}), self._loop)

        def _on_error(error):
            logger.error(f"[Fyers] Error: {error}")

        def _on_close(msg):
            self._connected = False
            logger.info(f"[Fyers] Closed: {msg}")

        self._socket = data_ws.FyersDataSocket(
            access_token=self._token,
            reconnect=True,
            litemode=False,
            write_to_file=False,
            on_connect=_on_open,
            on_message=_on_message,
            on_error=_on_error,
            on_close=_on_close
        )
        self._socket.connect()
        self._socket.keep_running()

    async def disconnect(self):
        self._running = False
        self._connected = False
        if self._socket:
            self._socket.close_connection()
        logger.info("Fyers Data Websocket Disconnected")

    def subscribe(self, symbols: list):
        if self._socket and self._connected:
            self._socket.subscribe(symbols, "SymbolUpdate")
            logger.info(f"[Fyers] Subscribed: {symbols}")

    def unsubscribe(self, symbols: list):
        if self._socket and self._connected:
            self._socket.unsubscribe(symbols)
            logger.info(f"[Fyers] Unsubscribed: {symbols}")

    def is_connected(self) -> bool:
        return self._connected
