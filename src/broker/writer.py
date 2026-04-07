import orjson
import struct
import mmap
import os

class TickManager:
    # Define the binary format: q (seq), dddd d (prices), q (time)
    # Total 56 bytes per slot
    TICK_STRUCT = struct.Struct("<qdddddq")
    SLOT_SIZE = 256

    def __init__(self, shm_path="/tmp/tick_shm", symbols=None):
        self.shm_path = shm_path
        self.symbols = symbols
        self.symbol_offsets = {sym: i * self.SLOT_SIZE for i, sym in enumerate(self.symbols)}
        
        self._shm_size = self.SLOT_SIZE * len(self.symbols)
        self._setup_shm()

    def _setup_shm(self):
        # Create file if not exists
        if not os.path.exists(self.shm_path):
            with open(self.shm_path, "wb") as f:
                f.write(b'\x00' * self._shm_size)
        
        self.fd = open(self.shm_path, "r+b")
        self.buf = mmap.mmap(self.fd.fileno(), self._shm_size)

    def write(self, message):
        # 1. Fast Validation & Parsing
        if not message: return
        
        try:
            if isinstance(message, (bytes, bytearray, str)):
                data = orjson.loads(message)
            else:
                data = message
            
            if data.get('type') not in ('if', 'sf'): return
            
            symbol = data.get('symbol')
            base = self.symbol_offsets.get(symbol)
            if base is None: return

            # 2. Prepare Data
            # Read current sequence
            curr_seq = struct.unpack_from("<q", self.buf, base)[0]
            
            # 3. Atomic-like Write (Seqlock)
            # Mark as "Writing" (Odd number)
            struct.pack_into("<q", self.buf, base, curr_seq + 1)
            
            # Write all fields in one block (Offsets 8 to 56)
            self.TICK_STRUCT.pack_into(
                self.buf, base,
                curr_seq + 2, # Final sequence (Even number)
                data['ltp'],
                data['high_price'],
                data['low_price'],
                data['open_price'],
                data['prev_close_price'],
                data['exch_feed_time']
            )

        except (orjson.JSONDecodeError, KeyError, TypeError) as e:
            print(f"[WARN] TickWrite Error: {e}")

    def close(self):
        self.buf.close()
        self.fd.close()

