import asyncio
from strategy_one_logic import check_entry_condition, start_trailing_sl
from utils.csv_builder import log_trade 
from utils.logger import logger
from event_bus import event_bus
from order_manager import OrderManager

async def strategy_one(strategy_id, ws_mgr, loop, max_trades):
    
    candle_queue = event_bus.subscribe("candle")
    tick_queue = event_bus.subscribe("tick")
    trade_close_queue = event_bus.subscribe("trade_close")
    
    trades_done = 0
    stop_event = asyncio.Event()
    active_order_id = None   
    active_strike_price_name = None
    
    # ---------------- Consumers ----------------
    async def candle_consumer():
        #skip candles
        _ = await candle_queue.get()
        logger.info("skipped candle")

        nonlocal trades_done, active_order_id, active_strike_price_name
        while not stop_event.is_set():
            symbol, candle = await candle_queue.get()

            #check for max trade limit 
            if active_order_id is None and trades_done >= max_trades:
                logger.info(f"[Max trade Limit Reached | Trade Done: {trades_done} | Max Trade Limit: {max_trades}]")
                stop_event.set()
                break
            
            if trades_done < max_trades and active_order_id is None:
                #check for entry condition
                condition_met, active_order_id, active_strike_price_name = await check_entry_condition(symbol, candle)
                #after order placed 
                if condition_met:
                    trades_done += 1
                    #save the order details to dic
                    await OrderManager.add_order(strategy_id, active_order_id, active_strike_price_name)
                    #subscribe the strike price
                    ws_mgr.subscribe_symbol(
                        active_strike_price_name,
                        mode="tick",
                        callback=lambda sym, tick: event_bus.tick_callback(loop, sym, tick)
                    )
                    logger.info(f"Order placed with ID: {active_order_id}")


    async def tick_consumer():
        nonlocal active_order_id
        while not stop_event.is_set():
            processed = False
            while True:
                if tick_queue.empty():
                    break
                symbol, tick = tick_queue.get_nowait()
                processed = True
                if active_order_id:
                    await start_trailing_sl(active_order_id, symbol, tick)
            
            if not processed:
                await asyncio.sleep(0.001)

    #only detect the trade closes not opening
    async def trade_close_consumer():
        nonlocal active_order_id, trades_done, active_strike_price_name
        while not stop_event.is_set():
            if trade_close_queue.empty():
                await asyncio.sleep(0.01)
                continue
                
            pos = trade_close_queue.get_nowait()
            #trade close for that sysmbol and strategy
            if "NSE:IDEA-EQ" in pos["id"] and pos.get("netQty", None) == 0:
                #unsubscibe tick for that symbol | used for trailling
                ws_mgr.unsubscribe_symbol(active_strike_price_name)
                # Save trade details to CSV
                order_obj = await OrderManager.get_order(active_order_id)
                if order_obj:
                    await log_trade(trades_done, active_order_id, order_obj.to_dict())
                    await OrderManager.remove_order(active_order_id)
                    logger.info(f"[Order Closed] {active_order_id} removed from OrderManager")
                    logger.info(f"[strategy_one] Trade {trades_done} closed")
                #reset 
                active_strike_price_name = None
                active_order_id = None

    # ---------------- Run All Consumers ----------------
    await asyncio.gather(
        candle_consumer(),
        tick_consumer(), 
        trade_close_consumer()
    )
    
    logger.info("[Strategy 1 Ended]")
    return
