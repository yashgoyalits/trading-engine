import numpy as np
from datetime import datetime

class Candle:
    __slots__ = ("symbol", "open", "high", "low", "close", "start_time", "volume")

    def __init__(self, symbol, open, high, low, close, start_time, volume):
        self.symbol = symbol
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.start_time = start_time
        self.volume = volume

    def __repr__(self):
        return (
            f"Candle(symbol={self.symbol}, O={self.open}, H={self.high}, "
            f"L={self.low}, C={self.close}, V={self.volume}, "
            f"T={self.start_time})"
        )


class CandleBuilder:
    def __init__(self, event_bus, max_ticks=2000):
        self.event_bus = event_bus

        self.buffers = {}
        self.count = {}
        self.active = {}
        self.last_bucket = {}

        self.max_ticks = max_ticks

    # ---------- CORE MECHANISM ----------
    # Normalize → bucket → state transition

    def _normalize_ts(self, ts):
        # ms → sec
        return ts / 1000 if ts > 1e12 else ts

    def _get_bucket(self, ts, tf):
        return int(ts // tf)

    def _ensure(self, symbol):
        if symbol not in self.buffers:
            self.buffers[symbol] = np.zeros((self.max_ticks, 3))
            self.count[symbol] = 0

    # ---------- MAIN PIPELINE ----------
    async def process_candle_tick(self, tick, tf):
        if tick.ltp is None or tick.timestamp is None:
            return

        symbol = tick.symbol
        self._ensure(symbol)

        ts = self._normalize_ts(tick.timestamp)

        # ring buffer write (optional analytics)
        idx = self.count[symbol] % self.max_ticks
        self.buffers[symbol][idx] = [ts, tick.ltp, tick.volume or 0]
        self.count[symbol] += 1

        bucket = self._get_bucket(ts, tf)

        # -------- FIRST TICK --------
        if symbol not in self.last_bucket:
            self.last_bucket[symbol] = bucket
            self.active[symbol] = [ts, tick.ltp, tick.ltp, tick.ltp, tick.ltp, tick.volume or 0]
            return

        # -------- BUCKET SWITCH --------
        if bucket != self.last_bucket[symbol]:
            await self._close_candle(symbol)

            # new bucket
            self.last_bucket[symbol] = bucket
            self.active[symbol] = [ts, tick.ltp, tick.ltp, tick.ltp, tick.ltp, tick.volume or 0]

        # -------- SAME BUCKET --------
        else:
            c = self.active[symbol]
            c[2] = max(c[2], tick.ltp)   # high
            c[3] = min(c[3], tick.ltp)   # low
            c[4] = tick.ltp              # close
            c[5] += tick.volume or 0     # volume

    # ---------- CLOSING ----------
    async def _close_candle(self, symbol):
        start_ts, o, h, l, c, v = self.active[symbol]

        candle = Candle(
            symbol=symbol,
            open=float(o),
            high=float(h),
            low=float(l),
            close=float(c),
            start_time=datetime.fromtimestamp(start_ts),
            volume=int(v)
        )

        await self.event_bus.publish("candle", candle)