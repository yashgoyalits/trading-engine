import os
from datetime import datetime, timedelta
import asyncio
from fyers_apiv3.FyersWebsocket import data_ws
from dotenv import load_dotenv
from logger import logger

load_dotenv()
ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")


def get_candle_start(ts, tf=30):
    """Align exchange timestamp (epoch seconds) to exchange clock (09:15 start)."""
    dt = datetime.fromtimestamp(ts)
    seconds = dt.hour * 3600 + dt.minute * 60 + dt.second + dt.microsecond / 1e6
    session_start = 9 * 3600 + 15 * 60  # 09:15 = 33300s
    if seconds < session_start:
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
        self.symbols = {}  # symbol -> data
        self._running = False
        self._loop = asyncio.get_event_loop()

    # ---------------- Subscribe / Unsubscribe ----------------
    def subscribe_symbol(self, symbol, mode="candle", timeframe=30, callback=None):
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
        if symbol in self.symbols:
            if self._running and self.fyers:
                self.fyers.unsubscribe([symbol])
            del self.symbols[symbol]

    # ---------------- WS Event Handlers ----------------
    def _on_message(self, message):
        """Called in WS thread, schedule processing in main asyncio loop."""
        self._loop.call_soon_threadsafe(asyncio.create_task, self._process_message(message))

    async def _process_message(self, message):
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
            if exch_time == data["last_ts"]:
                return
            data["last_ts"] = exch_time

            if data["mode"] == "tick":
                for cb in data["tick_callbacks"]:
                    await self._safe_callback(cb, symbol, {"ltp": ltp, "exch_feed_time": exch_time})
            else:
                tf = data["timeframe"]
                candle_time = get_candle_start(exch_time, tf)
                if candle_time is None:
                    return

                ohlc = data["ohlc"]
                if ohlc["start_time"] != candle_time:
                    if ohlc["start_time"]:
                        candle_copy = ohlc.copy()
                        candle_copy["time"] = ohlc["start_time"].strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                        for cb in data["candle_callbacks"]:
                            await self._safe_callback(cb, symbol, candle_copy)
                    data["ohlc"] = {"open": ltp, "high": ltp, "low": ltp, "close": ltp, "start_time": candle_time}
                else:
                    ohlc["high"] = max(ohlc["high"], ltp)
                    ohlc["low"] = min(ohlc["low"], ltp)
                    ohlc["close"] = ltp

        except Exception as e:
            logger.info(f"[WS _process_message Error] {e}")

    async def _safe_callback(self, cb, symbol, data):
        try:
            result = cb(symbol, data)
            if asyncio.iscoroutine(result):
                await result
        except Exception as e:
            logger.info(f"[Callback Error] {symbol} {e}")

    def _on_open(self):
        self._loop.call_soon_threadsafe(asyncio.create_task, self._subscribe_all())

    async def _subscribe_all(self):
        if self.fyers and self.symbols:
            self.fyers.subscribe(list(self.symbols.keys()), "SymbolUpdate")

    def _on_error(self, msg):
        self._loop.call_soon_threadsafe(logger.info, f"[Data WS Error] {msg}")

    def _on_close(self, msg):
        self._loop.call_soon_threadsafe(logger.info, f"[Data WS Closed] {msg}")

    # ---------------- Candle Flusher ----------------
    async def _flusher(self):
        while self._running:
            now = datetime.now()
            for symbol, data in self.symbols.items():
                if data["mode"] != "candle":
                    continue
                ohlc = data["ohlc"]
                tf = data["timeframe"]
                if not ohlc["start_time"]:
                    continue
                elapsed = (now - ohlc["start_time"]).total_seconds()
                if elapsed >= tf:
                    candle_copy = ohlc.copy()
                    candle_copy["time"] = ohlc["start_time"].strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                    for cb in data["candle_callbacks"]:
                        await self._safe_callback(cb, symbol, candle_copy)
                    data["ohlc"] = {"open": None, "high": None, "low": None, "close": None, "start_time": None}
            await asyncio.sleep(0.01)

    # ---------------- Start / Stop ----------------
    def start(self):
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

        # WS connect runs in thread internally
        self.fyers.connect()
        self.fyers.keep_running()

        # Start flusher in main loop
        asyncio.get_event_loop().create_task(self._flusher())

    def stop(self):
        self._running = False
        if self.fyers:
            try:
                self.fyers.close_connection()
            except Exception as e:
                logger.info("Error closing Fyers connection: %s", e)
