from order_placement import place_order, modify_order
from datetime import datetime, timedelta
import calendar
from utils.logger import logger
import math
from order_manager import OrderManager

async def check_entry_condition(symbol, candle):
    o, h, l, c = candle["open"], candle["high"], candle["low"], candle["close"]

    ce_symbol, pe_symbol = await find_strike_price_atm(candle["close"])
    
    logger.info( f"[Candle] {candle['time']} | {symbol} | " f"open: {o}, high: {h}, low: {l}, close: {c}" )

    if h == l:
        return False, None

    body_percentage = abs(c - o) / (h - l) * 100

    if body_percentage < 5:
        return False, None

    if c != o:  # skip doji-like candles where open == close
        side = 1 if c > o else 1   # 1 = BUY, -1 = SELL
        stop_loss, take_profit = 0.5, 2.0
        strike_price_name = ce_symbol if c > o else pe_symbol 

        #to check the time order was sent to the exchange
        logger.info(f"last line before order placing to check the time next line is of order placement {strike_price_name}")
        order_response = await place_order(symbol="NSE:IDEA-EQ", qty=1, order_type=2, side=side, stop_loss=stop_loss, take_profit=take_profit)
        logger.info("order id received first line after order placement")

        order_id = order_response.get("id")

        return True, order_id

    return False, None

#trailling function after order get placed || receving tick from consumer
async def start_trailing_sl(active_order_id: str, symbol: str, tick: dict):
    # Get order from OrderManager
    order_obj = await OrderManager.get_order(active_order_id)
    if not order_obj:
        return

    tick_ltp = tick.get("ltp")
    if tick_ltp is None:
        return

    stop_order_id = order_obj.stop_order_id

    for level in order_obj.trailing_levels:
        # Skip if already applied successfully
        if any(hist["level"] == level["msg"] for hist in order_obj.trailing_history):
            continue

        if tick_ltp > level["threshold"]:
            try:
                res = await modify_order(
                    stop_order_id,
                    order_type=4,
                    limit_price=level["new_stop"],
                    stop_price=level["new_stop"],
                    qty=1,
                )
            except Exception as e:
                logger.error(f"[Trailing SL Error] {symbol} | Level: {level['msg']} | {e}")
                continue

            if res.get('code') == 1102:
                # Record successful update
                order_obj.trailing_history.append({
                    "ltp": tick_ltp,
                    "level": level["msg"],
                    "stop_price": level["new_stop"]
                })

                # Update in OrderManager
                await OrderManager.update_order(active_order_id, trailing_history=order_obj.trailing_history)

                logger.info(
                    f"[Trailing SL Update] {symbol} | New Stop: {level['new_stop']} ({level['msg']}) LTP: {tick_ltp}"
                )
                break
            else:
                logger.warning(f"[Trailing SL Failed] {symbol} | Level: {level['msg']} | Response: {res}")
                continue


MONTH_ABBR = ['', 'JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']

async def next_tuesday_expiry(base_date: str = None) -> tuple[str, bool]:
    # Use passed date or today
    today = datetime.today()
    # today = datetime.strptime("2025-10-12", "%Y-%m-%d") 

    # Find next Tuesday
    days_ahead = (1 - today.weekday()) % 7  # Tuesday = 1 (Mon=0)
    days_ahead = days_ahead or 7            # if today is Tue, take next week
    tuesday = today + timedelta(days=days_ahead)

    # Last Tuesday of the month
    last_day = calendar.monthrange(tuesday.year, tuesday.month)[1]
    last_tuesday = last_day - ((datetime(tuesday.year, tuesday.month, last_day).weekday() - 1) % 7)

    if tuesday.day == last_tuesday:
        # Monthly expiry → YY + MON_ABBR
        expiry_str = f"{tuesday.year % 100}{MONTH_ABBR[tuesday.month]}"
        return expiry_str, True
    else:
        # Weekly expiry → YYMMDD
        expiry_str = f"{tuesday.year % 100}{tuesday.month}{tuesday.day:02d}"
        return expiry_str, False


async def find_strike_price_atm(spot_price: float):
    ce_strike = math.floor(spot_price / 50) * 50
    pe_strike = math.ceil(spot_price / 50) * 50

    expiry_str, _ = await next_tuesday_expiry()

    ce_symbol = f"NSE:NIFTY{expiry_str}{ce_strike}CE"
    pe_symbol = f"NSE:NIFTY{expiry_str}{pe_strike}PE"

    return ce_symbol, pe_symbol

