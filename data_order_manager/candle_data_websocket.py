import threading
import time
from datetime import datetime, timedelta
from fyers_apiv3.FyersWebsocket import data_ws
import os
from dotenv import load_dotenv
from logger import logger

load_dotenv()
ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")


def get_candle_start(ts, tf=30):
    """
    Align exchange timestamp (epoch seconds) to exchange clock
    Candles start from 09:15:00 IST
    """
    dt = datetime.fromtimestamp(ts)
    seconds = dt.hour * 3600 + dt.minute * 60 + dt.second
    session_start = 9 * 3600 + 15 * 60  # 09:15 = 33300s
    if seconds < session_start:
        # before market open → ignore
        return None
    bucket = ((seconds - session_start) // tf) * tf
    aligned = session_start + bucket
    return datetime(dt.year, dt.month, dt.day) + timedelta(seconds=aligned)


class FyersWSManager:
    _instance = None

    @staticmethod
    def get_instance():
        if FyersWSManager._instance is None:
            FyersWSManager._instance = FyersWSManager()
        return FyersWSManager._instance

    def __init__(self):
        self.fyers = None
        self.symbols = {}  # {symbol: {...}}
        self.lock = threading.Lock()
        self._running = False
        self._flusher_thread = None

    # ---------------- Subscribe / Unsubscribe ----------------
    def subscribe_symbol(self, symbol, mode="candle", timeframe=30, callback=None):
        with self.lock:
            if symbol not in self.symbols:
                self.symbols[symbol] = {
                    "mode": mode,
                    "timeframe": timeframe,
                    "ohlc": {"open": None, "high": None, "low": None, "close": None, "start_time": None},
                    "last_ts": None,
                    "last_ltp": None,
                    "tick_callbacks": [],
                    "candle_callbacks": []
                }

            if callback:
                if mode == "tick":
                    self.symbols[symbol]["tick_callbacks"].append(callback)
                else:
                    self.symbols[symbol]["candle_callbacks"].append(callback)

            if self._running and self.fyers:
                self.fyers.subscribe([symbol], "SymbolUpdate")

    def unsubscribe_symbol(self, symbol):
        with self.lock:
            if symbol in self.symbols:
                if self._running and self.fyers:
                    self.fyers.unsubscribe([symbol])
                del self.symbols[symbol]

    # ---------------- WebSocket Event Handler ----------------
    def _on_message(self, message):
        if not self._running:
            return

        symbol = message.get("symbol")
        if not symbol or symbol not in self.symbols:
            return

        data = self.symbols[symbol]
        try:
            ltp = message.get("ltp") or data.get("last_ltp")
            exch_time = message.get("exch_feed_time")

            if ltp is None or exch_time is None:
                return

            data["last_ltp"] = ltp

            # prevent duplicates
            if exch_time == data["last_ts"]:
                return
            data["last_ts"] = exch_time

            if data["mode"] == "tick":
                for cb in data["tick_callbacks"]:
                    cb(symbol, {"ltp": ltp, "exch_feed_time": exch_time})
            else:
                tf = data["timeframe"]
                candle_time = get_candle_start(exch_time, tf)
                if candle_time is None:
                    return

                ohlc = data["ohlc"]
                if ohlc["start_time"] != candle_time:
                    # close previous candle
                    if ohlc["start_time"]:
                        candle_copy = ohlc.copy()
                        candle_copy["time"] = ohlc["start_time"].strftime("%Y-%m-%d %H:%M:%S")
                        for cb in data["candle_callbacks"]:
                            cb(symbol, candle_copy)

                    # new candle
                    data["ohlc"] = {"open": ltp, "high": ltp, "low": ltp, "close": ltp, "start_time": candle_time}
                else:
                    # update candle
                    ohlc["high"] = max(ohlc["high"], ltp)
                    ohlc["low"] = min(ohlc["low"], ltp)
                    ohlc["close"] = ltp

        except Exception as e:
            logger.info(f"[WS _on_message Error] {e}")

    def _on_open(self):
        with self.lock:
            if self.fyers and self.symbols:
                self.fyers.subscribe(list(self.symbols.keys()), "SymbolUpdate")

    def _on_error(self, msg):
        logger.info("[Data WS Error] %s", msg)

    def _on_close(self, msg):
        logger.info("[Data WS Closed] %s", msg)

    # ---------------- Candle Flusher ----------------
    def _flusher(self):
        """
        Background thread that forces candle closure at timeframe boundaries,
        even if no ticks arrive.
        """
        while self._running:
            now = datetime.now()
            with self.lock:
                for symbol, data in self.symbols.items():
                    if data["mode"] != "candle":
                        continue

                    ohlc = data["ohlc"]
                    tf = data["timeframe"]
                    if not ohlc["start_time"]:
                        continue

                    # if candle expired
                    if (now - ohlc["start_time"]).total_seconds() >= tf:
                        candle_copy = ohlc.copy()
                        candle_copy["time"] = ohlc["start_time"].strftime("%Y-%m-%d %H:%M:%S")
                        for cb in data["candle_callbacks"]:
                            cb(symbol, candle_copy)
                        # reset for next candle (wait for tick)
                        data["ohlc"] = {"open": None, "high": None, "low": None, "close": None, "start_time": None}

            time.sleep(1)

    # ---------------- Start / Stop ----------------
    def start(self):
        with self.lock:
            if self._running:
                return
            self._running = True

        self.fyers = data_ws.FyersDataSocket(
            access_token=ACCESS_TOKEN,
            litemode=False,
            write_to_file=False,
            reconnect=True,
            on_connect=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close
        )

        threading.Thread(target=self.fyers.connect, daemon=True).start()
        threading.Thread(target=self.fyers.keep_running, daemon=True).start()

        # start candle flusher
        self._flusher_thread = threading.Thread(target=self._flusher, daemon=True)
        self._flusher_thread.start()

    def stop(self):
        with self.lock:
            self._running = False
        if self.fyers:
            try:
                self.fyers.close_connection()
            except Exception as e:
                logger.info("Error closing Fyers connection: %s", e)

        if self._flusher_thread and self._flusher_thread.is_alive():
            self._flusher_thread.join(timeout=2)
