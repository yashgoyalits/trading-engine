from utils.logger import logger
from order_manager.order_manager import OrderManager
from order_manager.fyers_order_placement import fyers_order_placement
from utils.error_handling import error_handling     

@error_handling
class StrategyOneTrailing:
    def __init__(self):
        pass

    async def start_trailing_sl(self, strategy_id, symbol: str, active_order_id: str, tick: dict):
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
                    res = await fyers_order_placement.modify_order(
                        stop_order_id,
                        order_type=4,
                        limit_price=level["new_stop"],
                        stop_price=level["new_stop"],
                        qty=1,
                    )
                except Exception as e:
                    logger.error(f"[{strategy_id}] | [Trailing SL Error] {symbol} | Level: {level['msg']} | {e}")
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
                        f"[{strategy_id}] | [Trailing SL Update] {symbol} | New Stop: {level['new_stop']} ({level['msg']}) LTP: {tick_ltp}"
                    )
                    break
                else:
                    logger.warning(f"[{strategy_id}] | [Trailing SL Failed] {symbol} | Level: {level['msg']} | Response: {res}")
                    continue

strategy_one_trailing = StrategyOneTrailing()