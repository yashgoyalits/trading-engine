from datetime import datetime, timedelta
import calendar
import math
from utils.error_handling import error_handling

MONTH_ABBR = {
    1: "JAN", 2: "FEB", 3: "MAR", 4: "APR",
    5: "MAY", 6: "JUN", 7: "JUL", 8: "AUG",
    9: "SEP", 10: "OCT", 11: "NOV", 12: "DEC"
}

@error_handling
class OptionHelper:

    async def next_tuesday_expiry() -> tuple[str, bool]:
        today = datetime.today()
        # today = datetime.strptime("2025-10-12", "%Y-%m-%d") 

        # Find next Tuesday
        days_ahead = (1 - today.weekday()) % 7  # Tuesday = 1 (Mon=0)
        days_ahead = days_ahead or 7            # if today is Tue, take next week
        tuesday = today + timedelta(days=days_ahead)

        # Last Tuesday of the month
        last_day = calendar.monthrange(tuesday.year, tuesday.month)[1]
        last_tuesday = last_day - ((datetime(tuesday.year, tuesday.month, last_day).weekday() - 1) % 7)

        if tuesday.day == last_tuesday: # Monthly expiry → YY + MON_ABBR
            expiry_str = f"{tuesday.year % 100}{MONTH_ABBR[tuesday.month]}"
            return expiry_str, True
        else: # Weekly expiry → YYMMDD
            expiry_str = f"{tuesday.year % 100}{tuesday.month}{tuesday.day:02d}"
            return expiry_str, False

    @classmethod
    async def find_strike_price_atm(self, spot_price: float) -> tuple[str, str]:
        ce_strike = math.floor(spot_price / 50) * 50
        pe_strike = math.ceil(spot_price / 50) * 50

        expiry_str, _ = await self.next_tuesday_expiry()

        ce_symbol = f"NSE:NIFTY{expiry_str}{ce_strike}CE"
        pe_symbol = f"NSE:NIFTY{expiry_str}{pe_strike}PE"

        return ce_symbol, pe_symbol
