import asyncio
from data_order_manager.order_websocket_manager import FyersOrderManager
from data_order_manager.candle_data_websocket import FyersWSManager
from strategy_one import strategy_one  
from logger import logger
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import time

load_dotenv()
COMBINED_TOKEN = f'{os.getenv("CLIENT_ID")}:{os.getenv("FYERS_ACCESS_TOKEN")}'

def wait_until_precise(target_time_str, extra_seconds=0):
    target_hour, target_minute = map(int, target_time_str.split(":"))
    now = datetime.now()
    target = now.replace(hour=target_hour, minute=target_minute, second=0, microsecond=0)
    target += timedelta(seconds=extra_seconds)
    if target <= now:
        target += timedelta(days=1)
    while True:
        remaining = (target - datetime.now()).total_seconds()
        if remaining <= 0:
            break
        time.sleep(min(remaining, 0.035))  # 35 ms max sleep

async def main():
    loop = asyncio.get_running_loop()

    ws_mgr = FyersWSManager.get_instance()
    ws_mgr.start()

    order_mgr = FyersOrderManager.get_instance(access_token=COMBINED_TOKEN)
    order_mgr.connect()

    # wait_until_precise("09:22", extra_seconds=35) #9:16:35

    await strategy_one(ws_mgr, order_mgr, loop, max_trades=1)

    ws_mgr.stop()
    order_mgr.stop()

    logger.info("[Main] Program terminated.")
    
    os._exit(0)

if __name__ == "__main__":
    asyncio.run(main())
