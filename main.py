import asyncio
from broker.fyers_broker.fyers_data_websocket import FyersDataBroker
from broker.fyers_broker.fyers_position_webscoket import FyersOrderPositionTracker
from data_manager.data_manager import DataManager
from strategies.strategy_one.strategy_one import StrategyOne
from utils.logger import logger
from utils.error_handling import error_handling
import os
from centeral_hub.event_bus import EventBus
from utils.csv_builder import CSVBuilder
from data_manager.tick_processor import TickProcessor
from data_manager.candle_builder import CandleBuilder

@error_handling
async def main():
    
    loop = asyncio.get_running_loop()
    
    await logger.start(loop)
    
    logger.info("ALGO STARTED")

    # Initialize 
    data_socket = FyersDataBroker()
    position_order_socket = FyersOrderPositionTracker()
    event_bus = EventBus()
    csv_builder = CSVBuilder(event_bus)
    tick_processor = TickProcessor(event_bus=event_bus)
    candle_builder = CandleBuilder(tick_processor=tick_processor, event_bus=event_bus)
    ws_mgr = DataManager(event_bus=event_bus, data_broker=data_socket, order_broker=position_order_socket, tick_processor=tick_processor, candle_builder=candle_builder)
    
    await ws_mgr.start()
    
    # Subscribe symbols
    ws_mgr.subscribe_symbol("NSE:NIFTY50-INDEX", mode="candle", timeframe=30)
    
    logger.info("ALL RESOURCES SUBSCRIBED")
    
    # Run strategy
    strategy = StrategyOne(event_bus, "strategy_one", ws_mgr, loop, max_trades=1)
    await strategy.run()

    # Stop all connections
    await ws_mgr.stop()
    
    logger.info("[Main] Program terminated")
    
    await logger.stop()
    await csv_builder.stop()
    
    print("Exit")
    
    os._exit(0)

if __name__ == "__main__":
    asyncio.run(main())
