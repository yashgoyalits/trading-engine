import os
from datetime import datetime, timedelta
import asyncio
import threading
from fyers_apiv3.FyersWebsocket import data_ws
from dotenv import load_dotenv
from utils.logger import logger

load_dotenv()
ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")

def align_to_candle_boundary(dt, tf=30):
    """Align datetime to candle boundary from 09:15:00 session start."""
    session_start = dt.replace(hour=9, minute=15, second=0, microsecond=0)
    if dt < session_start:
        return None
    seconds_since_start = (dt - session_start).total_seconds()
    candle_number = int(seconds_since_start // tf)
    return session_start + timedelta(seconds=candle_number * tf)

def get_candle_end_time(candle_start, tf=30):
    """Get candle end time."""
    return candle_start + timedelta(seconds=tf)

def get_current_candle_boundaries(tf=30):
    """Get current candle start and end times."""
    now = datetime.now()
    candle_start = align_to_candle_boundary(now, tf)
    if candle_start is None:
        return None, None
    return candle_start, get_candle_end_time(candle_start, tf)

class FyersWSManager:
    _instance = None
    _lock = threading.Lock()

    @staticmethod
    def get_instance():
        if FyersWSManager._instance is None:
            with FyersWSManager._lock:
                if FyersWSManager._instance is None:
                    FyersWSManager._instance = FyersWSManager()
        return FyersWSManager._instance

    def __init__(self):
        self.fyers = None
        self.symbols = {}
        self._running = False
        self._loop = None
        
    def subscribe_symbol(self, symbol, mode="candle", timeframe=30, callback=None):
        if symbol not in self.symbols:
            self.symbols[symbol] = {
                "mode": mode,
                "timeframe": timeframe,
                "ohlc": {"open": None, "high": None, "low": None, "close": None, 
                        "start_time": None, "volume": 0},
                "last_ts": None,
                "last_ltp": None,
                "tick_callbacks": [],
                "candle_callbacks": [],
                "last_candle_time": None,
                "tick_buffer": [],
                "last_tick_time": None,
                "candle_initialized": False,
                "previous_candle_close": None
            }

        if callback:
            target_callbacks = "tick_callbacks" if mode == "tick" else "candle_callbacks"
            self.symbols[symbol][target_callbacks].append(callback)

        if self._running and self.fyers:
            self.fyers.subscribe([symbol], "SymbolUpdate")

    def unsubscribe_symbol(self, symbol):
        if symbol in self.symbols:
            if self._running and self.fyers:
                self.fyers.unsubscribe([symbol])
            del self.symbols[symbol]

    def _on_message(self, message):
        """Optimized message handler."""
        if self._running and self._loop:
            self._loop.call_soon_threadsafe(self._create_process_task, message)

    def _create_process_task(self, message):
        asyncio.create_task(self._process_message(message))

    async def _process_message(self, message):
        symbol = message.get("symbol")
        if not symbol or symbol not in self.symbols or not self._running:
            return

        data = self.symbols[symbol]
        if data["mode"] == "tick":
            await self._process_tick(message, symbol, data)
        else:
            await self._process_candle(message, symbol, data)

    async def _process_tick(self, message, symbol, data):
        """Fast tick processing."""
        ltp, exch_time = message.get("ltp"), message.get("exch_feed_time")
        if ltp is None or exch_time is None or exch_time == data["last_ts"]:
            return
            
        data.update({"last_ts": exch_time, "last_ltp": ltp})
        tick_data = {"ltp": ltp, "exch_feed_time": exch_time}
        
        for cb in data["tick_callbacks"]:
            try:
                result = cb(symbol, tick_data)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as e:
                logger.info(f"[Tick Callback Error] {symbol}: {e}")

    async def _process_candle(self, message, symbol, data):
        """Process candle using hybrid approach."""
        ltp, exch_time = message.get("ltp"), message.get("exch_feed_time")
        if ltp is None or exch_time is None or exch_time == data["last_ts"]:
            return

        # Update tick data
        data.update({
            "last_ts": exch_time, 
            "last_ltp": ltp, 
            "last_tick_time": datetime.now()
        })

        tf = data["timeframe"]
        current_candle_start, _ = get_current_candle_boundaries(tf)
        if current_candle_start is None:
            return

        # Add tick to buffer
        tick_data = {
            "ltp": ltp,
            "timestamp": exch_time,
            "system_time": data["last_tick_time"],
            "datetime": datetime.fromtimestamp(exch_time)
        }
        data["tick_buffer"].append(tick_data)
        
        # Clean old ticks
        buffer_cutoff_ts = (current_candle_start - timedelta(seconds=tf * 5)).timestamp()
        data["tick_buffer"] = [t for t in data["tick_buffer"] if t["timestamp"] >= buffer_cutoff_ts]

        # Initialize or update candle
        if not data["candle_initialized"] or data["ohlc"]["start_time"] != current_candle_start:
            await self._initialize_candle(data, ltp, current_candle_start)
        else:
            await self._update_candle(data, current_candle_start, tf)

    async def _initialize_candle(self, data, ltp, candle_start):
        """Initialize new candle."""
        ohlc = data["ohlc"]
        ohlc.update({"open": ltp, "high": ltp, "low": ltp, "close": ltp, 
                    "start_time": candle_start, "volume": 0})
        data["candle_initialized"] = True

    async def _update_candle(self, data, current_candle_start, tf):
        """Update candle in real-time using validated ticks."""
        candle_start_ts = current_candle_start.timestamp()
        candle_end_ts = (current_candle_start + timedelta(seconds=tf)).timestamp()
        
        # Get valid ticks for current candle
        valid_ticks = [t for t in data["tick_buffer"] 
                      if candle_start_ts <= t["timestamp"] < candle_end_ts]
        
        if valid_ticks:
            valid_ticks.sort(key=lambda x: x["timestamp"])
            prices = [t["ltp"] for t in valid_ticks]
            
            data["ohlc"].update({
                "open": valid_ticks[0]["ltp"],
                "high": max(prices),
                "low": min(prices),
                "close": valid_ticks[-1]["ltp"]
            })

    async def _complete_candle(self, symbol, data):
        """Complete candle with maximum exchange matching."""
        ohlc = data["ohlc"]
        if not ohlc["start_time"] or not data["candle_initialized"]:
            return

        tf = data["timeframe"]
        candle_start = ohlc["start_time"]
        candle_end = get_candle_end_time(candle_start, tf)
        
        candle_start_ts = candle_start.timestamp()
        candle_end_ts = candle_end.timestamp()
        
        # Get valid ticks for completed candle
        valid_ticks = [t for t in data["tick_buffer"] 
                      if candle_start_ts <= t["timestamp"] < candle_end_ts]
        
        if valid_ticks:
            valid_ticks.sort(key=lambda x: x["timestamp"])
            prices = [t["ltp"] for t in valid_ticks]
            
            ohlc.update({
                "open": valid_ticks[0]["ltp"],
                "high": max(prices),
                "low": min(prices),
                "close": valid_ticks[-1]["ltp"]
            })
        else:
            # Handle missing ticks
            if data["last_ltp"] is not None:
                ohlc["close"] = data["last_ltp"]
            elif data["previous_candle_close"] is not None:
                close_price = data["previous_candle_close"]
                ohlc.update({"close": close_price, "open": close_price, 
                            "high": close_price, "low": close_price})

        data["previous_candle_close"] = ohlc["close"]

        # Create candle output
        candle_output = {
            "open": ohlc["open"],
            "high": ohlc["high"], 
            "low": ohlc["low"],
            "close": ohlc["close"],
            "volume": ohlc.get("volume", 0),
            "time": ohlc["start_time"].strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            "start_time": ohlc["start_time"],
            "valid_ticks_count": len(valid_ticks),
            "exchange_matched": len(valid_ticks) > 0
        }
        
        # Fire callbacks if not duplicate
        if data["last_candle_time"] != ohlc["start_time"]:
            data["last_candle_time"] = ohlc["start_time"]
            
            for cb in data["candle_callbacks"]:
                try:
                    result = cb(symbol, candle_output)
                    if asyncio.iscoroutine(result):
                        await result
                except Exception as e:
                    logger.info(f"[Candle Callback Error] {symbol}: {e}")
        
        # Clean processed ticks
        data["tick_buffer"] = [t for t in data["tick_buffer"] if t["timestamp"] >= candle_end_ts]

    def _on_open(self):
        if self._loop:
            self._loop.call_soon_threadsafe(lambda: asyncio.create_task(self._subscribe_all()))

    async def _subscribe_all(self):
        if self.fyers and self.symbols:
            symbols_list = list(self.symbols.keys())
            self.fyers.subscribe(symbols_list, "SymbolUpdate")

    def _on_error(self, msg):
        if self._loop:
            self._loop.call_soon_threadsafe(logger.info, f"[Data WS Error] {msg}")

    def _on_close(self, msg):
        if self._loop:
            self._loop.call_soon_threadsafe(logger.info, f"[Data WS Closed] {msg}")

    async def _precision_monitor(self):
        """Monitor and complete candles based on system clock."""
        while self._running:
            now = datetime.now()
            
            for symbol, data in list(self.symbols.items()):
                if (data["mode"] != "candle" or not data["candle_initialized"] 
                    or not data["ohlc"]["start_time"]):
                    continue
                    
                tf = data["timeframe"]
                candle_end_time = get_candle_end_time(data["ohlc"]["start_time"], tf)
                
                if now >= candle_end_time:
                    await self._complete_candle(symbol, data)
                    
                    # Reset for next candle
                    data["candle_initialized"] = False
                    data["ohlc"] = {"open": None, "high": None, "low": None, 
                                  "close": None, "start_time": None, "volume": 0}
                
            await asyncio.sleep(0.001)

    def start(self):
        """Start WebSocket manager."""
        if self._running:
            return

        self._running = True
        self._loop = asyncio.get_event_loop()
        
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

        self.fyers.connect()
        self.fyers.keep_running()
        asyncio.create_task(self._precision_monitor())

    def stop(self):
        """Stop WebSocket manager."""
        self._running = False
        if self.fyers:
            self.fyers.close_connection()
            logger.info("[WS Manager] Stopped")

    def get_current_candle(self, symbol):
        """Get current incomplete candle."""
        if (symbol in self.symbols and self.symbols[symbol]["mode"] == "candle" 
            and self.symbols[symbol]["candle_initialized"] 
            and self.symbols[symbol]["ohlc"]["start_time"]):
            
            ohlc = self.symbols[symbol]["ohlc"]
            return {
                "open": ohlc["open"],
                "high": ohlc["high"],
                "low": ohlc["low"], 
                "close": ohlc["close"],
                "volume": ohlc.get("volume", 0),
                "start_time": ohlc["start_time"],
                "time": ohlc["start_time"].strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            }
        return None

    def get_last_price(self, symbol):
        """Get last price for symbol."""
        return self.symbols.get(symbol, {}).get("last_ltp")

    def get_candle_quality_stats(self, symbol):
        """Get candle quality statistics."""
        if symbol in self.symbols:
            data = self.symbols[symbol]
            return {
                "buffer_size": len(data["tick_buffer"]),
                "last_tick_time": data["last_tick_time"],
                "candle_initialized": data["candle_initialized"],
                "previous_candle_close": data["previous_candle_close"]
            }
        return None
