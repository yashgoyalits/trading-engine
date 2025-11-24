# strategies/base_strategy.py
import abc
from typing import Optional
from data_model.data_model import TradeData

class BaseStrategy(abc.ABC):
    def __init__(self, event_bus, strategy_id, ws_mgr, loop, max_trades: int = 1):
        self.event_bus = event_bus
        self.strategy_id = strategy_id
        self.ws_mgr = ws_mgr
        self.loop = loop
        self.max_trades = max_trades
        self.trades_done: int = 0

    # ------------------ Consumers ------------------
    @abc.abstractmethod
    async def candle_consumer(self):
        """Consume candle events."""

    @abc.abstractmethod
    async def tick_consumer(self):
        """Consume tick events."""

    @abc.abstractmethod
    async def orders_consumer(self):
        """Consume broker position events."""

    # ------------------ Run ------------------
    @abc.abstractmethod
    async def run(self):
        """Run the strategy and manage tasks."""
