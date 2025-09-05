import csv
import os
import json
from datetime import datetime

# -----------------------------
# Config
# -----------------------------
CSV_DIR = os.path.join(os.getcwd(), "csv")
CSV_FILE = os.path.join(CSV_DIR, f"trades_{datetime.now().strftime('%Y-%m-%d')}.csv")


async def _format_time(dt_value=None):
    if dt_value is None:
        dt_value = datetime.now()
    if isinstance(dt_value, datetime):
        return dt_value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    return str(dt_value)


async def log_trade(trade_no: int, order_id: str, details: dict, file_path=CSV_FILE):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    entry = details.get("entry_price")
    stop = details.get("initial_stop_price")
    target = details.get("target_price")
    qty = details.get("qty", 1)

    side = details.get("side")
    if side is None:
        side = "BUY" if entry and target and target > entry else "SELL"

    trade_row = {
        "trade_no": trade_no,
        "order_id": order_id,
        "symbol": details.get("symbol"),
        "qty": qty,
        "side": side,
        "entry_price": entry,
        "initial_stop_price": stop,
        "target_price": target,
        "initial_sl_points": (stop - entry) if entry and stop else None,
        "target_points": (target - entry) if entry and target else None,
        "order_exit_time": await _format_time(),
        "trailing_levels": json.dumps(details.get("trailing_levels", [])),
        "trailing_history": json.dumps(details.get("trailing_history", [])),
        "timestamp": await _format_time()
    }

    # Write to CSV
    file_exists = os.path.isfile(file_path)
    with open(file_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(trade_row.keys()))
        if not file_exists:
            writer.writeheader()
        writer.writerow(trade_row)
