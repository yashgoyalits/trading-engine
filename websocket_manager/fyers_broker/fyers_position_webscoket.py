from fyers_apiv3.FyersWebsocket import order_ws
import asyncio
from utils.error_handling import error_handling
from utils.logger import logger
from dotenv import load_dotenv
import os
from .broker_interface import BrokerInterface

load_dotenv()

@error_handling
class FyersOrderPositionTracker(BrokerInterface):

    def __init__(self, access_token=None):
        client_id = os.getenv("CLIENT_ID")
        token = access_token or os.getenv("FYERS_ACCESS_TOKEN")

        if not client_id or not token:
            logger.warning("Missing CLIENT_ID or FYERS_ACCESS_TOKEN")

        self.access_token = f"{client_id}:{token}"
        self._loop: asyncio.AbstractEventLoop = None
        self._queue: asyncio.Queue = None     # <--- initialize attribute
        self._connected = False
        self._task = None

        self.fyers = order_ws.FyersOrderSocket(
            access_token=self.access_token,
            write_to_file=False,
            log_path=None,
            on_connect=self._on_open,
            on_close=self._on_close,
            on_error=self._on_error,
            on_positions=self._on_position,
        )

    def _on_open(self):
        self._connected = True
        self.fyers.subscribe(data_type="OnPositions")

    def _on_close(self, msg):
        self._connected = False
        logger.info(f"[Order WS] Closed: {msg}")

    def _on_error(self, msg):
        logger.error(f"[Order WS] Error: {msg}")

    def _on_position(self, msg):
        positions = msg.get("positions")
        if not positions:
            return

        positions_list = positions if isinstance(positions, list) else [positions]
        for pos in positions_list:
            if self._queue and self._loop:
                asyncio.run_coroutine_threadsafe(self._queue.put({"type": "positions", "data": pos}), self._loop)

    async def connect(self, queue: asyncio.Queue = None):
        self._queue = queue
        self._loop = asyncio.get_running_loop()
        self._task = self._loop.run_in_executor(None, self.fyers.connect)
        logger.info("Fyers Position Websocket Connected")

    async def disconnect(self):
        self._connected = False
        if self.fyers:
            try:
                self.fyers.keep_running = False
                if hasattr(self.fyers, "ws") and self.fyers.ws:
                    self.fyers.ws.close(status=1000, reason="Normal Closure")

                if self._task:
                    self._task.cancel()
                logger.info("Fyers Position Websocket Disconnected")
            except Exception as e:
                logger.error(f"[Order WS] Exception during disconnect: {e}")

    def subscribe(self, data):
        pass

    def unsubscribe(self, data):
        pass

    def is_connected(self) -> bool:
        return self._connected
