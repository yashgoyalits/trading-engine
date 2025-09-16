from fyers_apiv3.FyersWebsocket import order_ws
import threading
from utils.logger import logger

class FyersOrderManager:
    _instance = None

    @staticmethod
    def get_instance(access_token=None):
        if FyersOrderManager._instance is None:
            if access_token is None:
                raise ValueError("Access token required for first instantiation")
            FyersOrderManager._instance = FyersOrderManager(access_token)
        return FyersOrderManager._instance

    def __init__(self, access_token):
        if FyersOrderManager._instance is not None:
            raise Exception("This class is a singleton! Use get_instance() instead.")

        self.access_token = access_token
        self.callbacks = []         # list of callback functions for trade close
        self.events = {}            # symbol -> threading.Event for blocking

        # Initialize Fyers order WebSocket
        self.fyers = order_ws.FyersOrderSocket(
            access_token=self.access_token,
            write_to_file=False,
            log_path=None,
            on_connect=self.on_open,
            on_close=self.on_close,
            on_error=self.on_error,
            on_positions=self.on_position,
        )

    # ---------------- WebSocket Callbacks ----------------
    def on_open(self):
        self.fyers.subscribe(data_type="OnPositions")
        self.fyers.keep_running()

    def on_close(self, msg):
        logger.info("[Order WS] Closed:", msg)

    def on_error(self, msg):
        logger.info("[Order WS] Error:", msg)

    # ---------------- Position Handler ----------------
    def on_position(self, msg):
        if not self.fyers or not self._instance:
            return

        positions = msg.get("positions")
        if not positions:
            return

        positions_list = positions if isinstance(positions, list) else [positions]

        for pos in positions_list:
            symbol = pos.get("symbol")

            # --- Common handling for both open and closed ---
            for cb in self.callbacks:
                cb(pos)

            event = self.events.get(symbol)
            if event:
                event.set()
                self.events.pop(symbol, None)

    # ---------------- Public Methods ----------------
    def connect(self):
        self.fyers.connect()

    def close(self):
        if self.fyers:
            self.fyers.close()

    def register_close_callback(self, callback):
        if callable(callback):
            self.callbacks.append(callback)

    def wait_for_trade_close(self, symbol):
        """Block until specific trade closes (by symbol)"""
        event = threading.Event()
        self.events[symbol] = event
        event.wait()  # blocks until event.set() is called in on_position
        self.events.pop(symbol, None)

    def stop(self):
        if self.fyers:
            try:
                # signal the loop to stop
                self.fyers.keep_running = False

                # close websocket if active
                if hasattr(self.fyers, "ws") and self.fyers.ws:
                    self.fyers.ws.close(status=1000, reason="Normal Closure")

                # if a thread was used to run ws.run_forever(), join it
                if hasattr(self, "thread") and self.thread and self.thread.is_alive():
                    self.thread.join(timeout=2)

                logger.info("[Order WS] Closed cleanly.")
            except Exception as e:
                logger.info(f"[Order WS] Exception during close: {e}")

