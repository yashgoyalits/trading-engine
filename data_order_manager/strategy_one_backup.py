import asyncio
from strategy_one_logic import check_strategy_condition, start_trailing_sl, save_order_details_to_global
from active_order_state import ORDER_BOOK, order_to_dict
from csv_logging import log_trade 
from logger import logger

from concurrent.futures import ProcessPoolExecutor
executor = ProcessPoolExecutor(max_workers=1)


async def strategy_one(ws_mgr, order_mgr, loop, max_trades):
    trades_done = 0
    stop_event = asyncio.Event()
    active_order_id = None
    active_strike_price_name = None

    # ---------------- Direct Callbacks ----------------
    async def on_candle(symbol, candle):
        nonlocal trades_done, active_order_id, active_strike_price_name

        if trades_done < max_trades and active_order_id is None:
            # offload CPU heavy condition check
            condition_met, order_id, strike_name = await asyncio.get_running_loop().run_in_executor(
                executor, check_strategy_condition, symbol, candle
            )

            if condition_met:
                active_order_id = order_id
                active_strike_price_name = strike_name
                trades_done += 1

                asyncio.create_task(save_order_details_to_global(order_id, strike_name))
                logger.info(f"Order placed with ID: {order_id}")

                ws_mgr.subscribe_symbol(
                    active_strike_price_name, mode="tick", callback=on_tick
                )

        if active_order_id is None and trades_done >= max_trades:
            logger.info("Max trade reached")
            stop_event.set()

    def on_tick(symbol, tick):
        nonlocal active_order_id
        if active_order_id:
            # direct call, no queue
            start_trailing_sl(active_order_id, symbol, tick)

    async def on_trade_close(pos):
        nonlocal active_order_id, trades_done, active_strike_price_name

        ws_mgr.unsubscribe_symbol(active_strike_price_name)

        if ORDER_BOOK.get(active_order_id):
            log_trade(trades_done, active_order_id, order_to_dict(ORDER_BOOK.get(active_order_id)))
            ORDER_BOOK.remove(active_order_id)

        logger.info(f"[strategy_one] Trade {trades_done} closed")

        active_strike_price_name = None
        active_order_id = None

    # ---------------- Register Callbacks ----------------
    ws_mgr.subscribe_symbol("NSE:NIFTY50-INDEX", mode="candle", timeframe=30, callback=on_candle)
    order_mgr.register_close_callback(lambda pos: asyncio.create_task(on_trade_close(pos)))

    # Wait until stop_event is triggered
    await stop_event.wait()
    logger.info("Strategy1 strategy ending...................")
