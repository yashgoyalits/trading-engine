from common_utils.logger import logger
from common_utils.error_handling import error_handling
from data_model.data_model import Tick

@error_handling
class TrailingManager:
    async def start_trailing_sl(self, fyers_order_placement, trailing_levels, stop_order_id, qty, tick: Tick):
        if tick.ltp is None:
            logger.info("LTP is None")
            return 
        if trailing_levels is None:
            logger.info("No trailing levels")
            return 
        if stop_order_id is None:
            logger.info("No stop order id")
            return 
        if qty is None:
            logger.info("No qty")
            return 

        for level in trailing_levels:
            if level.get("hit"):
                continue

            if tick.ltp > level.get("threshold", float("inf")):
                res = await fyers_order_placement.modify_order(
                    stop_order_id,
                    order_type=4,
                    limit_price=level.get("new_stop"),
                    stop_price=level.get("new_stop"),
                    qty=qty,
                )

                if res.get('code') == 1102:
                    level["hit"] = True  # Mark as triggered
                    logger.info(f"Trailing SL updated | {level.get('msg')} | LTP: {tick.ltp}")
                else:
                    logger.error(f"Failed to update trailing SL | {res}")