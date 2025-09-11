from fyers_apiv3 import fyersModel
import os
from dotenv import load_dotenv

load_dotenv()  
fyers = fyersModel.FyersModel(client_id=os.getenv("CLIENT_ID"), token=os.getenv("FYERS_ACCESS_TOKEN"), is_async=True, log_path=None)

# Function to place an order
async def place_order(symbol: str, qty: int, order_type: int, side: int, stop_loss: float, take_profit: float):
    order_data = {
        "symbol": symbol,
        "qty": qty,
        "type": order_type,
        "side": side,
        "productType": "BO", 
        "validity": "DAY",
        "disclosedQty": 0,
        "offlineOrder": False,
        "stopLoss": stop_loss, 
        "takeProfit": take_profit 
    }
    return await fyers.place_order(order_data)


#function to modify 
async def modify_order(order_id: str, order_type: int, limit_price: float, stop_price: float, qty: int):
    order_data = {
        "id": order_id,
        "type": order_type, 
        "limitPrice": limit_price, 
        "stopPrice": stop_price,
        "qty": qty
    }
    return await fyers.modify_order(order_data)

#function- to get the complete orderbook 
async def get_main_stop_target_orders(parent_id: str):
    # ✅ fetch all orders
    response = await fyers.orderbook()
    orders = response.get("orderBook", [])

    main_order, stop_order, target_order = None, None, None

    for order in orders:
        oid = order.get("id")
        pid = order.get("parentId")

        if oid == parent_id:
            main_order = order
        elif pid == parent_id:
            # stop-loss order → has stopPrice
            if order.get("stopPrice", 0) > 0:
                stop_order = order
            # target order → has limitPrice
            elif order.get("limitPrice", 0) > 0:
                target_order = order

    return main_order, stop_order, target_order




