from order_manager.fyers_order_placement import fyers_order_placement
from utils.logger import logger
from strategies.strategy_one.option_helper import OptionHelper
from utils.error_handling import error_handling

@error_handling
class StrategyLogicManager:
    def __init__(self):
        pass

    async def check_entry_condition(self, strategy_id, symbol: str, candle: dict):
        o, h, l, c = candle["open"], candle["high"], candle["low"], candle["close"]

        ce_symbol, pe_symbol = await OptionHelper.find_strike_price_atm(candle["close"])
        
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
            logger.info(f"[{strategy_id}]last line Order Placemment {strike_price_name}")
            order_response = await fyers_order_placement.place_order(symbol="NSE:IDEA-EQ", qty=1, order_type=2, side=side, stop_loss=stop_loss, take_profit=take_profit)
            logger.info(f"[{strategy_id}] First Line After Order Id")

            order_id = order_response.get("id")

            return True, order_id

        return False, None


strategy_logic_manager = StrategyLogicManager()