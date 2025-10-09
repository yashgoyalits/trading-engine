import asyncio
from websocket_manager.fyers_broker.fyers_data_websocket import FyersDataBroker
from websocket_manager.fyers_broker.fyers_position_webscoket import FyersOrderPositionTracker
from websocket_manager.data_manager.base import FyersWSManager
from strategies.strategy_one.strategy_one import StrategyOne
from utils.logger import logger
from utils.error_handling import error_handling
import os
from centeral_hub.event_bus import EventBus
from utils.csv_builder import CSVBuilder

@error_handling
async def main():
    
    loop = asyncio.get_running_loop()
    
    await logger.start(loop)
    
    logger.info("ALGO STARTED")

    # Initialize 
    data_socket = FyersDataBroker()
    position_order_socket = FyersOrderPositionTracker()
    event_bus = EventBus()
    _ = CSVBuilder(event_bus)
    ws_mgr = FyersWSManager(event_bus=event_bus, data_broker=data_socket, order_broker=position_order_socket)
    
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
    
    os._exit(0)

if __name__ == "__main__":
    asyncio.run(main())
