from abc import ABC, abstractmethod

class ICandleBuilder(ABC):
    @abstractmethod
    async def add_tick(self, timestamp: float, price: float, volume: float):
        """Add a tick to the candle builder."""
        pass

    @abstractmethod
    async def get_last_candle(self):
        """Return the last completed candle if any."""
        pass
