from order_management import place_order, modify_order, get_main_stop_target_orders
from datetime import datetime, timedelta
import calendar
from logger import logger
from active_order_state import ActiveOrderState, ORDER_BOOK

async def check_strategy_condition(symbol, candle):
    o, h, l, c = candle["open"], candle["high"], candle["low"], candle["close"]
    
    logger.info( f"[Candle] {candle['time']} | {symbol} | " f"open: {o}, high: {h}, low: {l}, close: {c}" )

    # ✅ Avoid zero division
    if h == l:
        return False, None, None

    body_percentage = abs(c - o) / (h - l) * 100

    # ✅ Only trade if body is significant
    if body_percentage < 5:
        return False, None, None

    # ✅ Bullish or bearish candle check
    if c != o:  # skip doji-like candles where open == close
        side = 1 if c > o else 1   # 1 = BUY, -1 = SELL
        candle_type = "Bullish" if c > o else "Bearish"
        stop_loss, take_profit = 0.5, 2.0

        strike_price_name = await find_strike_price(c, candle_type)

        order_response = await place_order(symbol="NSE:IDEA-EQ", qty=1, order_type=2, side=side, stop_loss=stop_loss, take_profit=take_profit)
        
        order_id = order_response.get("id")

        return True, order_id, strike_price_name

    return False, None, None


async def save_order_details_to_global(order_id: str, strike_price_name: str) -> ActiveOrderState:
    # ✅ Get main, stop, and target orders
    main, stop, target = await get_main_stop_target_orders(order_id)

    if not main:
        raise ValueError(f"No main order found for {order_id}")

    stop_order_id = stop.get("id") if stop else None
    target_order_id = target.get("id") if target else None

    # ✅ Pull prices directly from order objects (avoid redundant API calls)
    entry_price = main.get("tradedPrice")
    initial_stop_price = stop.get("stopPrice") if stop else None
    target_price = target.get("limitPrice") if target else None

    # ✅ Precompute trailing levels
    trailing_levels = []
    if entry_price:
        trailing_levels = [
            {"threshold": entry_price + 3, "new_stop": entry_price + 0.1, "msg": "breakeven"},
            {"threshold": entry_price + 10, "new_stop": entry_price + 0.2, "msg": "1st trail locked profit"},
        ]

    order = ActiveOrderState(
        main_order_id=order_id,
        stop_order_id=stop_order_id,
        target_order_id=target_order_id,
        symbol=strike_price_name,
        entry_price=entry_price,
        initial_stop_price=initial_stop_price,
        target_price=target_price,
        trailing_levels=trailing_levels,
    )

    ORDER_BOOK.add(order_id, order)
    return order



async def start_trailing_sl(active_order_id: str, symbol: str, tick: dict):
    order_obj = ORDER_BOOK.get(active_order_id)
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
                # ✅ Do NOT break; will retry on next tick
                continue

            if res.get('code') == 1102:
                # Record successful update
                order_obj.trailing_history.append({
                    "ltp": tick_ltp,
                    "level": level["msg"],
                    "stop_price": level["new_stop"]
                })

                logger.info(
                    f"[Trailing SL Update] {symbol} | Time: {tick.get('exch_feed_time')} "
                    f"New Stop: {level['new_stop']} ({level['msg']}) LTP: {tick_ltp}"
                )

                # ✅ Only stop processing after success
                break
            else:
                # Modification failed, log it and retry on next tick
                logger.warning(f"[Trailing SL Failed] {symbol} | Level: {level['msg']} | Response: {res}")
                continue


async def next_tuesday() -> str:
    today = datetime.today()
    # Tuesday = 1 (Monday=0, Sunday=6)
    days_ahead = 1 - today.weekday()
    if days_ahead <= 0:
        days_ahead += 7
    tuesday = today + timedelta(days=days_ahead)

    # Last Tuesday of the month check
    last_day_of_month = calendar.monthrange(tuesday.year, tuesday.month)[1]
    last_tuesday_day = last_day_of_month - ((datetime(tuesday.year, tuesday.month, last_day_of_month).weekday() - 1) % 7)

    year = tuesday.year % 100  # last two digits

    if tuesday.day == last_tuesday_day:
        # Last Tuesday: month abbreviation only
        month_str = tuesday.strftime('%b').upper()
        return f"{year}{month_str}"
    else:
        # Normal Tuesday: month without leading zero, day two digits
        return f"{year}{tuesday.month}{tuesday.day:02d}"


async def find_strike_price(spot_price: float, candle_type: str) -> str:
    option_type = {"Bullish": "CE", "Bearish": "PE"}.get(candle_type)
    if not option_type:
        raise ValueError("candle_type must be 'Bullish' or 'Bearish'")

    atm_strike = round(spot_price / 50) * 50
    expiry = await next_tuesday()
    return f"NSE:NIFTY{expiry}{atm_strike}{option_type}"