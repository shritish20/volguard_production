import requests
from datetime import datetime, time
import pytz

IST = pytz.timezone("Asia/Kolkata")

class MarketCalendar:
    MARKET_OPEN = time(9, 15)
    MARKET_CLOSE = time(15, 30)

    def __init__(self, access_token: str):
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json"
        }

    def is_market_open_now(self) -> bool:
        now = datetime.now(IST)
        if not self.is_trading_day(now.date()):
            return False
        return self.MARKET_OPEN <= now.time() <= self.MARKET_CLOSE

    def is_trading_day(self, day) -> bool:
        url = "https://api.upstox.com/v2/market/holidays"
        resp = requests.get(url, headers=self.headers, timeout=5)
        holidays = resp.json().get("data", [])
        return day.strftime("%Y-%m-%d") not in {h["date"] for h in holidays}
