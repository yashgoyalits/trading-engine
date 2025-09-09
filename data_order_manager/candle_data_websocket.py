import os
from datetime import datetime, timedelta
import asyncio
import threading
from fyers_apiv3.FyersWebsocket import data_ws
from dotenv import load_dotenv
from logger import logger
import time


load_dotenv()
ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")


def get_precise_candle_start(ts, tf=30):
    """Get precise candle start time aligned to exchange clock (09:15 start)."""
    dt = datetime.fromtimestamp(ts)
    
    # Session starts at 09:15:00
    session_start_hour = 9
    session_start_minute = 15
    session_start_second = 0
    
    # Create session start time for the same date
    session_start = dt.replace(
        hour=session_start_hour, 
        minute=session_start_minute, 
        second=session_start_second, 
        microsecond=0
    )
    
    # If current time is before session start, return None
    if dt < session_start:
        return None
    
    # Calculate seconds since session start
    seconds_since_start = (dt - session_start).total_seconds()
    
    # Calculate which candle bucket this belongs to
    candle_number = int(seconds_since_start // tf)
    
    # Return the start time of this candle
    candle_start = session_start + timedelta(seconds=candle_number * tf)
    
    return candle_start


def get_candle_end_time(candle_start, tf=30):
    """Get exact end time of a candle."""
    return candle_start + timedelta(seconds=tf)


def tick_belongs_to_candle(tick_timestamp, candle_start, tf=30):
    """Check if tick timestamp belongs to specific candle timeframe."""
    if candle_start is None:
        return False
    
    candle_end = get_candle_end_time(candle_start, tf)
    tick_dt = datetime.fromtimestamp(tick_timestamp)
    
    # Tick belongs to candle if: candle_start <= tick_time < candle_end
    return candle_start <= tick_dt < candle_end


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
        self._locks = {}
        self._thread_executor = None
        
    def subscribe_symbol(self, symbol, mode="candle", timeframe=30, callback=None):
        if symbol not in self.symbols:
            self.symbols[symbol] = {
                "mode": mode,
                "timeframe": timeframe,
                "ohlc": {
                    "open": None, 
                    "high": None, 
                    "low": None, 
                    "close": None, 
                    "start_time": None,
                    "volume": 0
                },
                "last_ts": None,
                "last_ltp": None,
                "tick_callbacks": [],
                "candle_callbacks": [],
                "last_candle_time": None,
                "tick_buffer": [],  # Buffer to store ticks with timestamps
                "valid_close_price": None,  # Last valid close price for current candle
                "valid_close_timestamp": None  # Timestamp of valid close price
            }
            self._locks[symbol] = asyncio.Lock() if self._loop else None

        if callback:
            if mode == "tick":
                self.symbols[symbol]["tick_callbacks"].append(callback)
            else:
                self.symbols[symbol]["candle_callbacks"].append(callback)

        if self._running and self.fyers:
            try:
                self.fyers.subscribe([symbol], "SymbolUpdate")
            except Exception as e:
                logger.info(f"[Subscribe Error] {symbol}: {e}")

    def unsubscribe_symbol(self, symbol):
        if symbol in self.symbols:
            if self._running and self.fyers:
                try:
                    self.fyers.unsubscribe([symbol])
                except Exception as e:
                    logger.info(f"[Unsubscribe Error] {symbol}: {e}")
            del self.symbols[symbol]
            if symbol in self._locks:
                del self._locks[symbol]

    def _on_message(self, message):
        """Optimized message handler with minimal latency."""
        if not self._running or not self._loop:
            return
            
        # Schedule with high priority
        self._loop.call_soon_threadsafe(self._create_process_task, message)

    def _create_process_task(self, message):
        """Create processing task in event loop."""
        asyncio.create_task(self._process_message(message))

    async def _process_message(self, message):
        if not self._running:
            return

        symbol = message.get("symbol")
        if not symbol or symbol not in self.symbols:
            return

        # Fast path - no lock for tick mode
        data = self.symbols[symbol]
        if data["mode"] == "tick":
            await self._process_tick_fast(message, symbol, data)
        else:
            await self._process_candle_timestamp_aware(message, symbol, data)

    async def _process_tick_fast(self, message, symbol, data):
        """Fast tick processing without locks."""
        try:
            ltp = message.get("ltp")
            exch_time = message.get("exch_feed_time")
            
            if ltp is None or exch_time is None:
                return
                
            # Skip duplicate timestamps
            if exch_time == data["last_ts"]:
                return
                
            data["last_ts"] = exch_time
            data["last_ltp"] = ltp

            # Fire callbacks immediately
            tick_data = {"ltp": ltp, "exch_feed_time": exch_time}
            for cb in data["tick_callbacks"]:
                try:
                    result = cb(symbol, tick_data)
                    if asyncio.iscoroutine(result):
                        await result
                except Exception as e:
                    logger.info(f"[Tick Callback Error] {symbol}: {e}")
                    
        except Exception as e:
            logger.info(f"[Tick Process Error] {symbol}: {e}")

    async def _process_candle_timestamp_aware(self, message, symbol, data):
        """Process candle with strict timestamp validation."""
        try:
            ltp = message.get("ltp")
            exch_time = message.get("exch_feed_time")
            
            if ltp is None or exch_time is None:
                return

            # Skip duplicate timestamps
            if exch_time == data["last_ts"]:
                return
                
            data["last_ts"] = exch_time
            data["last_ltp"] = ltp

            tf = data["timeframe"]
            tick_candle_start = get_precise_candle_start(exch_time, tf)
            
            if tick_candle_start is None:
                return

            ohlc = data["ohlc"]
            current_candle_start = ohlc["start_time"]
            
            # Add tick to buffer with timestamp
            tick_data = {
                "ltp": ltp,
                "timestamp": exch_time,
                "candle_start": tick_candle_start
            }
            data["tick_buffer"].append(tick_data)
            
            # Keep buffer size manageable (last 100 ticks)
            if len(data["tick_buffer"]) > 100:
                data["tick_buffer"] = data["tick_buffer"][-100:]

            if current_candle_start is None:
                # First tick - initialize candle
                ohlc["open"] = ltp
                ohlc["high"] = ltp
                ohlc["low"] = ltp
                ohlc["close"] = ltp
                ohlc["start_time"] = tick_candle_start
                ohlc["volume"] = 0
                data["valid_close_price"] = ltp
                data["valid_close_timestamp"] = exch_time
                
            elif tick_candle_start == current_candle_start:
                # Tick belongs to current candle - validate timestamp
                if tick_belongs_to_candle(exch_time, current_candle_start, tf):
                    ohlc["high"] = max(ohlc["high"], ltp)
                    ohlc["low"] = min(ohlc["low"], ltp)
                    ohlc["close"] = ltp
                    data["valid_close_price"] = ltp
                    data["valid_close_timestamp"] = exch_time
                else:
                    # Timestamp validation failed - this tick is for next candle
                    await self._complete_candle_with_validation(symbol, data)
                    # Start new candle
                    await self._start_new_candle(symbol, data, ltp, tick_candle_start, exch_time)
                    
            else:
                # New candle detected
                await self._complete_candle_with_validation(symbol, data)
                # Start new candle
                await self._start_new_candle(symbol, data, ltp, tick_candle_start, exch_time)

        except Exception as e:
            logger.info(f"[Candle Process Error] {symbol}: {e}")

    async def _complete_candle_with_validation(self, symbol, data):
        """Complete candle ensuring close price is from correct timeframe."""
        try:
            ohlc = data["ohlc"]
            if not ohlc["start_time"]:
                return

            tf = data["timeframe"]
            candle_start = ohlc["start_time"]
            candle_end = get_candle_end_time(candle_start, tf)
            
            # Find the last valid tick that belongs to this candle
            valid_close = None
            valid_close_ts = None
            
            # Search buffer for last valid tick in this candle timeframe
            for tick in reversed(data["tick_buffer"]):
                if tick_belongs_to_candle(tick["timestamp"], candle_start, tf):
                    valid_close = tick["ltp"]
                    valid_close_ts = tick["timestamp"]
                    break
            
            # Use the validated close price
            if valid_close is not None:
                ohlc["close"] = valid_close
                
            # Prepare and flush candle
            candle_output = {
                "open": ohlc["open"],
                "high": ohlc["high"], 
                "low": ohlc["low"],
                "close": ohlc["close"],
                "volume": ohlc.get("volume", 0),
                "time": candle_start.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                "start_time": candle_start
            }
            
            # Prevent duplicate flushes
            if data["last_candle_time"] != candle_start:
                data["last_candle_time"] = candle_start
                
                # Fire all callbacks
                for cb in data["candle_callbacks"]:
                    try:
                        result = cb(symbol, candle_output)
                        if asyncio.iscoroutine(result):
                            await result
                    except Exception as e:
                        logger.info(f"[Candle Callback Error] {symbol}: {e}")
                        
        except Exception as e:
            logger.info(f"[Complete Candle Error] {symbol}: {e}")

    async def _start_new_candle(self, symbol, data, ltp, candle_start, timestamp):
        """Start new candle with proper initialization."""
        ohlc = data["ohlc"]
        ohlc["open"] = ltp
        ohlc["high"] = ltp
        ohlc["low"] = ltp
        ohlc["close"] = ltp
        ohlc["start_time"] = candle_start
        ohlc["volume"] = 0
        data["valid_close_price"] = ltp
        data["valid_close_timestamp"] = timestamp

    def _on_open(self):
        """WebSocket connection opened."""
        if self._loop:
            self._loop.call_soon_threadsafe(self._create_subscribe_task)

    def _create_subscribe_task(self):
        """Create subscription task."""
        asyncio.create_task(self._subscribe_all())

    async def _subscribe_all(self):
        """Subscribe to all symbols."""
        if self.fyers and self.symbols:
            try:
                symbols_list = list(self.symbols.keys())
                self.fyers.subscribe(symbols_list, "SymbolUpdate")
            except Exception as e:
                logger.info(f"[Subscribe All Error] {e}")

    def _on_error(self, msg):
        """WebSocket error handler."""
        if self._loop:
            self._loop.call_soon_threadsafe(logger.info, f"[Data WS Error] {msg}")

    def _on_close(self, msg):
        """WebSocket close handler."""
        if self._loop:
            self._loop.call_soon_threadsafe(logger.info, f"[Data WS Closed] {msg}")

    async def _precision_monitor(self):
        """Monitor and complete candles based on exact timing."""
        while self._running:
            try:
                current_time = time.time()
                current_dt = datetime.fromtimestamp(current_time)
                
                for symbol, data in list(self.symbols.items()):
                    if data["mode"] != "candle":
                        continue
                        
                    ohlc = data["ohlc"]
                    if not ohlc["start_time"]:
                        continue
                        
                    tf = data["timeframe"]
                    candle_end_time = get_candle_end_time(ohlc["start_time"], tf)
                    
                    # Complete candle if current time >= candle end time
                    if current_dt >= candle_end_time:
                        # Check if we haven't already completed this candle
                        next_candle_start = get_precise_candle_start(current_time, tf)
                        if next_candle_start and next_candle_start > ohlc["start_time"]:
                            await self._complete_candle_with_validation(symbol, data)
                            
                            # Clear candle data
                            data["ohlc"] = {
                                "open": None, "high": None, "low": None, 
                                "close": None, "start_time": None, "volume": 0
                            }
                            data["valid_close_price"] = None
                            data["valid_close_timestamp"] = None
                            
            except Exception as e:
                logger.info(f"[Precision Monitor Error] {e}")
                
            # Monitor every 5ms for precision
            await asyncio.sleep(0.005)

    def start(self):
        """Start the WebSocket manager with optimized settings."""
        if self._running:
            return

        self._running = True
        self._loop = asyncio.get_event_loop()
        
        # Update locks for current loop
        for symbol in self.symbols:
            self._locks[symbol] = asyncio.Lock()

        try:
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

            # Connect WebSocket
            self.fyers.connect()
            self.fyers.keep_running()

            # Start precision monitor
            asyncio.create_task(self._precision_monitor())
            
        except Exception as e:
            logger.info(f"[WS Start Error] {e}")
            self._running = False

    def stop(self):
        """Stop the WebSocket manager."""
        self._running = False
        if self.fyers:
            try:
                self.fyers.close_connection()
                logger.info("[WS Manager] Stopped")
            except Exception as e:
                logger.info(f"[WS Stop Error] {e}")

    def get_current_candle(self, symbol):
        """Get current incomplete candle data."""
        if symbol in self.symbols:
            data = self.symbols[symbol]
            if data["mode"] == "candle" and data["ohlc"]["start_time"]:
                ohlc = data["ohlc"]
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
        """Get last traded price for symbol."""
        if symbol in self.symbols:
            return self.symbols[symbol]["last_ltp"]
        return None
