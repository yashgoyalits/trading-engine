from src.adapter.fyers.fyers_adapter import FyersAdapter

BROKER_ADAPTER_CLASS = {
    "fyers": FyersAdapter,
}

def get_broker_bundle(broker_name: str):
    adapter_class = BROKER_ADAPTER_CLASS.get(broker_name.lower())

    if not adapter_class:
        raise ValueError(f"Unknown broker: {broker_name}")

    return {
        "adapter_class": adapter_class
    }