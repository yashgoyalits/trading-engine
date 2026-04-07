import mmap
import struct
import time
import os
import shm_config as config

class MarketReader:
    # q (seq), q (ts), d (ltp), d (open), d (high), d (low), d (close), q (vol)
    TICK_STRUCT = struct.Struct("<qqdddddq")
    
    def __init__(self):
        self.shm_path = config.SHM_PATH
        self.slot_size = config.SLOT_SIZE
        self.max_symbols = config.MAX_SYMBOLS
        self._shm_size = self.slot_size * self.max_symbols
        self.symbol_offsets = config.SYMBOLS.copy()
        self._setup_shm()

    def _setup_shm(self):
        if not os.path.exists(self.shm_path):
            raise FileNotFoundError(f"SHM file {self.shm_path} not found. Writer start karein pehle.")
        
        self.fd = open(self.shm_path, "rb")
        self.buf = mmap.mmap(self.fd.fileno(), self._shm_size, access=mmap.ACCESS_READ)

    def read_symbol(self, symbol):
        sym_config = self.symbol_offsets.get(symbol)
        if sym_config is None:
            return None

        # Nested dict se tick_base nikalo
        base = sym_config["tick_base"]

        while True:
            seq_before = struct.unpack_from("<q", self.buf, base)[0]
            
            if seq_before & 1:
                continue

            data = self.TICK_STRUCT.unpack_from(self.buf, base)
            
            seq_after = struct.unpack_from("<q", self.buf, base)[0]

            if seq_before == seq_after:
                return {
                    'seq':    data[0],
                    'ts':     data[1],
                    'ltp':    data[2],
                    'open':   data[3],
                    'high':   data[4],
                    'low':    data[5],
                    'close':  data[6],
                    'volume': data[7]
                }

    def close(self):
        self.buf.close()
        self.fd.close()


if __name__ == "__main__":
    reader = MarketReader()
    target_symbol = "NSE:NIFTY50-INDEX"
    last_seq = -1

    print(f"Monitoring {target_symbol}...")
    
    try:
        while True:
            tick = reader.read_symbol(target_symbol)
            
            if tick and tick['seq'] != last_seq:
                print(f"SEQ: {tick['seq']} | LTP: {tick['ltp']:.2f} | TS: {tick['ts']}")
                last_seq = tick['seq']
            
            time.sleep(0.0001)
    except KeyboardInterrupt:
        reader.close()