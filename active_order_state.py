
class ActiveOrderState:
    def __init__(
        self,
        main_order_id: str,
        symbol: str,
        stop_order_id: str = None,
        target_order_id: str = None,
        entry_price: float = None,
        initial_stop_price: float = None,
        target_price: float = None,
        trailing_levels: list[dict] = None
    ):
        self.main_order_id = main_order_id
        self.stop_order_id = stop_order_id
        self.target_order_id = target_order_id
        self.symbol = symbol
        self.entry_price = entry_price
        self.initial_stop_price = initial_stop_price
        self.target_price = target_price
        self.trailing_levels: list[dict] = trailing_levels or []
        self.trailing_history: list[dict] = []

    def __repr__(self):
        return (
            f"ActiveOrderState("
            f"main_order_id={self.main_order_id}, "
            f"stop_order_id={self.stop_order_id}, "
            f"target_order_id={self.target_order_id}, "
            f"symbol={self.symbol}, "
            f"entry_price={self.entry_price}, "
            f"initial_stop_price={self.initial_stop_price}, "
            f"target_price={self.target_price}, "
            f"trailing_levels={self.trailing_levels}, "
            f"trailing_history={self.trailing_history}"
            f")"
        )
    

class OrderBook:
    def __init__(self):
        self._orders = {}

    def add(self, order_id, order_obj):
        self._orders[order_id] = order_obj

    def get(self, order_id):
        return self._orders.get(order_id)

    def remove(self, order_id):
        return self._orders.pop(order_id, None)

    def all(self):
        return self._orders


# 🔑 Create a single global OrderBook instance
ORDER_BOOK = OrderBook()


def order_to_dict(order_obj):
    return {
        "main_order_id": order_obj.main_order_id,
        "stop_order_id": order_obj.stop_order_id,
        "target_order_id": order_obj.target_order_id,
        "symbol": order_obj.symbol,
        "entry_price": order_obj.entry_price,
        "initial_stop_price": order_obj.initial_stop_price,
        "target_price": order_obj.target_price,
        "trailing_levels": list(order_obj.trailing_levels),   # ✅ new
        "trailing_history": list(order_obj.trailing_history),
    }
