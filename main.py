#main.py file
import asyncio
from data_position_websocket.order_websocket_manager import FyersOrderManager
from data_position_websocket.candle_data_websocket import FyersWSManager
from strategy.strategy_one import strategy_one  
from utils.logger import logger
import os
from dotenv import load_dotenv
from centeral_hub.event_bus import event_bus
from utils.error_handling import error_handling

load_dotenv()

@error_handling
async def main():
    loop = asyncio.get_running_loop()

    ws_mgr = FyersWSManager.get_instance()
    ws_mgr.start()
    order_mgr = FyersOrderManager.get_instance(access_token=COMBINED_TOKEN)
    order_mgr.connect()

    logger.info("Algo started |................................")

    event_bus.wire_sources(ws_mgr, order_mgr, loop)

    await strategy_one("strategy_one", ws_mgr, loop, max_trades=1)

    ws_mgr.stop()
    order_mgr.stop()

    logger.info("[Main] Program terminated |....................")
    
    os._exit(0)

if __name__ == "__main__":

    COMBINED_TOKEN = f'{os.getenv("CLIENT_ID")}:{os.getenv("FYERS_ACCESS_TOKEN")}'

    #run main funtion
    asyncio.run(main())
