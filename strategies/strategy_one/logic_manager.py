from utils.logger import logger
from strategies.strategy_one.option_helper import OptionHelper
from utils.error_handling import error_handling
from data_model.data_model import Candle

@error_handling
class StrategyLogicManager:
    def __init__(self):
        pass

    async def check_entry_condition(self, strategy_id, symbol: str, candle: Candle):
        o, h, l, c = candle.open, candle.high, candle.low, candle.close

        ce_symbol, pe_symbol = await OptionHelper.find_strike_price_atm(candle.close)
        
        logger.info( f"[Candle] {candle.start_time} | {symbol} | " f"open: {o}, high: {h}, low: {l}, close: {c}" )

        if h == l:
            return False

        body_percentage = abs(c - o) / (h - l) * 100

        if body_percentage < 5:
            return False

        if c != o:  # skip doji-like candles where open == close
            side = 1 if c > o else 1   # 1 = BUY, -1 = SELL
            stop_loss, take_profit = 0.5, 2.0
            strike_price_name = ce_symbol if c > o else pe_symbol 

            return True

        return False
