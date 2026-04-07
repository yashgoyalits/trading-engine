# shm_config.py

MAX_SYMBOLS = 1000
SLOT_SIZE = 256
SHM_PATH = "/tmp/tick_data"

# DHAYAN DEIN: Yahan ':' use hoga aur values integers honi chahiye
SYMBOLS = {
    "NSE:NIFTY50-INDEX": 0,
    "NSE:BANKNIFTY-INDEX": 256
}


# --- CANDLE CONFIG ---
CANDLE_SHM_PATH = "/tmp/candle_data"
CANDLE_TF_SLOT  = 64    # Ek candle ka size (OHLCV)
MAX_HISTORY     = 10   # Kitni candles save karni hain
# Ek symbol ka total area: (100 candles * 64 bytes) + 8 bytes for Index
CANDLE_SYM_BLOCK = (CANDLE_TF_SLOT * MAX_HISTORY) + 64 

# Candle Offsets (Inside 64 bytes)
C_TS    = 0   # Timestamp (q)
C_OPEN  = 8   # (d)
C_HIGH  = 16  # (d)
C_LOW   = 24  # (d)
C_CLOSE = 32  # (d)
C_VOL   = 40  # (q)
# [48:64] Reserved for Indicators (RSI/SMA)

# --- SYMBOL ADDRESSES ---
SYMBOLS = {
    "NSE:NIFTY50-INDEX": {
        "tick_base": 0,
        "candle_base": 0
    },
    "NSE:BANKNIFTY-INDEX": {
        "tick_base": 256,
        "candle_base": CANDLE_SYM_BLOCK # 6464 bytes ke baad
    },
}