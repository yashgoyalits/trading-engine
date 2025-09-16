import asyncio
from strategy.strategy_one_logic import check_entry_condition, start_trailing_sl
from utils.csv_builder import log_trade 
from utils.logger import logger
from centeral_hub.event_bus import event_bus
from order_manager.order_manager import OrderManager

async def strategy_one(strategy_id, ws_mgr, loop, max_trades):
    
    candle_queue = event_bus.subscribe("candle")
    tick_queue = event_bus.subscribe("tick")
    trade_close_queue = event_bus.subscribe("trade_close")
    
    trades_done = 0
    stop_event = asyncio.Event()
    active_order_id = None   
    
    # ---------------- Consumers ----------------
    async def candle_consumer():
        #skip candles
        _ = await candle_queue.get()
        logger.info("skipped candle")

        nonlocal trades_done, active_order_id
        while not stop_event.is_set():
            symbol, candle = await candle_queue.get()
            #check for max trade limit 
            if active_order_id is None and trades_done >= max_trades:
                logger.info(f"[Max trade Limit Reached | Trade Done: {trades_done} | Max Trade Limit: {max_trades}]")
                stop_event.set()
                break
            
            if trades_done < max_trades and active_order_id is None:
                #check for entry condition
                condition_met, active_order_id = await check_entry_condition(symbol, candle)
                #after order placed 
                if condition_met:
                    trades_done += 1
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

    #detch postion open and close
    async def trade_close_consumer():
        nonlocal active_order_id, trades_done
        while not stop_event.is_set():
            if trade_close_queue.empty():
                await asyncio.sleep(0.01)
                continue
                
            pos = trade_close_queue.get_nowait()

            active_symbol = pos.get("symbol")
            net_qty = pos.get("netQty", 0)
            realized = pos.get("realized_profit", 0)
            position_id = pos.get("id")

            #trade close for that sysmbol 
            if position_id == pos["id"] and pos.get("netQty", None) == 0:
                ws_mgr.unsubscribe_symbol(active_symbol)
                order_obj = await OrderManager.get_order(active_order_id)
                if order_obj:
                    await log_trade(trades_done, active_order_id, order_obj.to_dict())
                    await OrderManager.remove_order(active_order_id)
                    logger.info(f"[strategy_one] Trade {trades_done} closed")
                    logger.info(f"[strategy_one] Trade {trades_done} PNL : {realized}")
                active_order_id = None
            else: #trade open for that sysmbol
                ws_mgr.subscribe_symbol(
                    active_symbol,
                    mode="tick",
                    callback=lambda sym, tick: event_bus.tick_callback(loop, sym, tick)
                )
                logger.info(f"[Position OPEN] {active_symbol}, Qty: {net_qty}")
                await OrderManager.add_order(strategy_id, active_order_id, position_id, active_symbol)


    # ---------------- Run All Consumers ----------------
    await asyncio.gather(
        candle_consumer(),
        tick_consumer(), 
        trade_close_consumer()
    )
    
    logger.info("[Strategy 1 Ended]")
    return
