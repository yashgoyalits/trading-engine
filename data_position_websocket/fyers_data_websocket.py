#fyers_data_websocket.py
import os
import threading
from dotenv import load_dotenv
from fyers_apiv3.FyersWebsocket import data_ws
from utils.logger import logger
from .base import BaseWSManager

load_dotenv()
ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN", "")

class FyersTransport:
    def __init__(self, access_token, reconnect=True, litemode=False, write_to_file=False):
        self._socket = None
        self._opts = {
            "access_token": access_token,
            "reconnect": reconnect,
            "litemode": litemode,
            "write_to_file": write_to_file,
        }

    def connect(self, on_open, on_message, on_error, on_close):
        self._socket = data_ws.FyersDataSocket(
            on_connect=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
            **self._opts
        )
        self._socket.connect()
        self._socket.keep_running()

    def subscribe(self, symbols, topic=None):
        self._socket.subscribe(symbols, topic or "SymbolUpdate")

    def unsubscribe(self, symbols):
        self._socket.unsubscribe(symbols)

    def close(self):
        if self._socket:
            self._socket.close_connection()


class FyersWSManager(BaseWSManager):
    _instance = None
    _lock = threading.Lock()

    @staticmethod
    def get_instance():
        if FyersWSManager._instance is None:
            with FyersWSManager._lock:
                if FyersWSManager._instance is None:
                    FyersWSManager._instance = FyersWSManager()
        return FyersWSManager._instance

    def __init__(self, access_token=None):
        token = access_token or ACCESS_TOKEN
        if not token:
            logger.warning("FYERS_ACCESS_TOKEN is empty.")
        super().__init__(transport=FyersTransport(token))
