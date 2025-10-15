from fyers_apiv3 import fyersModel
import os
from dotenv import load_dotenv
from utils.error_handling import error_handling

load_dotenv()

@error_handling
class FyersOrderPlacement:
    def __init__(self, client_id: str = None, access_token: str = None, is_async: bool = True, log_path=None):
        self.client_id = client_id or os.getenv("CLIENT_ID")
        self.access_token = access_token or os.getenv("FYERS_ACCESS_TOKEN")
        self.fyers = fyersModel.FyersModel(
            client_id=self.client_id, 
            token=self.access_token, 
            is_async=is_async, 
            log_path=log_path
        )

    async def place_order(self, symbol: str, qty: int, order_type: int, side: int, stop_loss: float, take_profit: float):
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
        return await self.fyers.place_order(order_data)

    async def modify_order(self, order_id: str, order_type: int, limit_price: float, stop_price: float, qty: int):
        order_data = {
            "id": order_id,
            "type": order_type, 
            "limitPrice": limit_price, 
            "stopPrice": stop_price,
            "qty": qty
        }
        return await self.fyers.modify_order(order_data)

    async def get_main_stop_target_orders(self, parent_id: str):
        response = await self.fyers.orderbook()
        orders = response.get("orderBook", [])

        main_order, stop_order, target_order = None, None, None

        for order in orders:
            oid = order.get("id")
            pid = order.get("parentId")

            if oid == parent_id:
                main_order = order
            elif pid == parent_id:
                if order.get("stopPrice", 0) > 0:
                    stop_order = order
                elif order.get("limitPrice", 0) > 0:
                    target_order = order

        return main_order, stop_order, target_order