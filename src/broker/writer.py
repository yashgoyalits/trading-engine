import orjson
import struct
import mmap
import os
import shm_config as config

class TickManager:
    TICK_STRUCT = struct.Struct("<qqdddddq")

    def __init__(self):
        self.shm_path = config.SHM_PATH
        self.slot_size = config.SLOT_SIZE
        self._shm_size = self.slot_size * config.MAX_SYMBOLS
        self.symbol_offsets = config.SYMBOLS.copy()

        # Next Slot Index: ab tick_base se calculate hoga
        existing = [v["tick_base"] for v in self.symbol_offsets.values()]
        self._next_slot_idx = (max(existing) // self.slot_size) + 1 if existing else 0

        self._setup_shm()

    def _setup_shm(self):
        if not os.path.exists(self.shm_path):
            with open(self.shm_path, "wb") as f:
                f.write(b'\x00' * self._shm_size)
        self.fd = open(self.shm_path, "r+b")
        self.buf = mmap.mmap(self.fd.fileno(), self._shm_size)

    def write(self, message):
        # Fyers already parsed dict deta hai, orjson sirf bytes/str handle karta hai
        data = message if isinstance(message, dict) else orjson.loads(message)

        base = self.symbol_offsets[data['symbol']]["tick_base"]

        curr_seq = struct.unpack_from("<q", self.buf, base)[0]
        struct.pack_into("<q", self.buf, base, curr_seq + 1)

        self.TICK_STRUCT.pack_into(
            self.buf, base,
            curr_seq + 2,
            data['exch_feed_time'],
            data['ltp'],
            data['open_price'],
            data['high_price'],
            data['low_price'],
            data['prev_close_price'],
            0
        )