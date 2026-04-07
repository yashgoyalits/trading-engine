import asyncio
import uvloop
from src.broker.fyers.data_broker import FyersDataBroker
from src.broker.fyers.order_broker import FyersOrderPositionTracker
from src.adapter.fyers.fyers_adapter import FyersAdapter
from src.candle_builder.candle_builder import CandleBuilder
from src.strategies.strategy_one.strategy_handler import StrategyOne
from src.infrastructure.event_bus import EventBus
from src.utils.csv_builder import CSVBuilder
from src.infrastructure.error_handling import error_handling
from src.infrastructure.logger import logger
import os

@error_handling
async def main():
    
    loop = asyncio.get_running_loop()
    
    await logger.start()
    
    logger.info("ALGO STARTED")

    event_bus = EventBus()
    csv_builder = CSVBuilder(event_bus)
    candle_builder = CandleBuilder(event_bus=event_bus)
    ws_mgr = FyersAdapter(event_bus=event_bus, candle_builder=candle_builder)
    
    await ws_mgr.start()
    
    # Subscribe symbols
    ws_mgr.subscribe_symbol("NSE:NIFTY50-INDEX", mode="candle", timeframe=30)
    
    logger.info("ALL RESOURCES SUBSCRIBED")
    
    # Run strategy
    strategy_one = StrategyOne(event_bus, "STRATEGY_ONE", ws_mgr, max_trades=1)
    
    await asyncio.gather(
        strategy_one.run()
    )

    # Stop all connections
    await ws_mgr.stop()
    
    logger.info("[Main] Program terminated")
    
    await logger.stop()
    await csv_builder.stop()
    
    print("Exit")
    
    os._exit(0)

if __name__ == "__main__":
    uvloop.install()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Manual Interrupted")
