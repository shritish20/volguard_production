import pandas as pd
import requests
import asyncio
import io
import pytz
from datetime import datetime, timedelta, date
from dataclasses import dataclass
from typing import Optional, List, Dict
from app.utils.logger import logger

@dataclass
class ParticipantData:
    fut_long: float
    fut_short: float
    fut_net: float
    call_long: float
    call_short: float
    call_net: float
    put_long: float
    put_short: float
    put_net: float
    stock_net: float

@dataclass
class ExternalMetrics:
    fii: Optional[ParticipantData]
    dii: Optional[ParticipantData]
    pro: Optional[ParticipantData]
    client: Optional[ParticipantData]
    fii_net_change: float 
    flow_regime: str      # STRONG_SHORT / STRONG_LONG / NEUTRAL
    event_risk: str       # HIGH / MEDIUM / LOW
    data_date: str

class ParticipantClient:
    """
    VolGuard 4.1 Participant Client.
    Scrapes NSE for FII/DII Data + Manual Event Calendar Overlay.
    """
    
    def __init__(self):
        # Configuration from v30.1
        self.FII_STRONG_LONG = 50000
        self.FII_STRONG_SHORT = -50000
        self.FII_MODERATE = 20000
        
        # --- MANUAL EVENT CALENDAR ---
        # Add dates here to force "HIGH" Risk (format: YYYY-MM-DD)
        # The Regime Engine will automatically apply a -3.0 penalty on these days.
        self.DANGER_DATES = [
            "2024-02-01", # Budget Day
            "2024-06-04", # Election Results
            "2025-02-01", # Next Budget
        ]
        
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "en-US,en;q=0.9"
        }

    async def fetch_metrics(self) -> ExternalMetrics:
        try:
            return await asyncio.to_thread(self._fetch_sync)
        except Exception as e:
            logger.error(f"Participant Data Fetch Failed: {str(e)}")
            return self._get_fallback_metrics()

    def _fetch_sync(self) -> ExternalMetrics:
        dates = self._get_trading_dates()
        today_date = dates[0]
        yest_date = dates[1]
        
        # 1. Check Event Risk (Manual Override)
        current_date_str = date.today().strftime("%Y-%m-%d")
        event_risk = "HIGH" if current_date_str in self.DANGER_DATES else "LOW"

        # 2. Fetch NSE Data
        df_today = self._fetch_oi_csv(today_date)
        df_yest = self._fetch_oi_csv(yest_date)

        if df_today is None:
            return self._get_fallback_metrics(event_risk)

        today_data = self._process_participant_data(df_today)
        yest_data = self._process_participant_data(df_yest) if df_yest is not None else {}

        fii_net_change = 0.0
        if today_data.get('FII') and yest_data.get('FII'):
            fii_net_change = today_data['FII'].fut_net - yest_data['FII'].fut_net

        flow_regime = "NEUTRAL"
        if today_data.get('FII'):
            fii_net = today_data['FII'].fut_net
            if fii_net > self.FII_STRONG_LONG: 
                flow_regime = "STRONG_LONG"
            elif fii_net < self.FII_STRONG_SHORT: 
                flow_regime = "STRONG_SHORT"
            elif abs(fii_net) > self.FII_MODERATE: 
                flow_regime = "MODERATE_LONG" if fii_net > 0 else "MODERATE_SHORT"

        return ExternalMetrics(
            fii=today_data.get('FII'),
            dii=today_data.get('DII'),
            pro=today_data.get('Pro'),
            client=today_data.get('Client'),
            fii_net_change=fii_net_change,
            flow_regime=flow_regime,
            event_risk=event_risk, # <--- Passes the Manual Risk to Regime Engine
            data_date=today_date.strftime('%d-%b-%Y')
        )

    # ... [Rest of the methods: _fetch_oi_csv, _process_participant_data remain identical] ...
    
    def _fetch_oi_csv(self, date_obj: date) -> Optional[pd.DataFrame]:
        """Scrapes the CSV from NSE Archives."""
        date_str = date_obj.strftime('%d%m%Y')
        url = f"https://archives.nseindia.com/content/nsccl/fao_participant_oi_{date_str}.csv"
        try:
            r = requests.get(url, headers=self.headers, timeout=5)
            if r.status_code == 200:
                content = r.content.decode('utf-8')
                lines = content.splitlines()
                for idx, line in enumerate(lines[:20]):
                    if "Future Index Long" in line:
                        df = pd.read_csv(io.StringIO(content), skiprows=idx)
                        df.columns = df.columns.str.strip()
                        return df
            return None
        except Exception:
            return None

    def _process_participant_data(self, df: pd.DataFrame) -> Dict[str, ParticipantData]:
        data = {}
        participants = ["FII", "DII", "Client", "Pro"]
        for p in participants:
            try:
                row = df[df['Client Type'].astype(str).str.contains(p, case=False, na=False)].iloc[0]
                def get_val(col):
                    return float(str(row[col]).replace(',', ''))
                data[p] = ParticipantData(
                    fut_long=get_val('Future Index Long'),
                    fut_short=get_val('Future Index Short'),
                    fut_net=get_val('Future Index Long') - get_val('Future Index Short'),
                    call_long=get_val('Option Index Call Long'),
                    call_short=get_val('Option Index Call Short'),
                    call_net=get_val('Option Index Call Long') - get_val('Option Index Call Short'),
                    put_long=get_val('Option Index Put Long'),
                    put_short=get_val('Option Index Put Short'),
                    put_net=get_val('Option Index Put Long') - get_val('Option Index Put Short'),
                    stock_net=get_val('Future Stock Long') - get_val('Future Stock Short')
                )
            except Exception:
                data[p] = None
        return data

    def _get_trading_dates(self) -> List[datetime]:
        tz = pytz.timezone('Asia/Kolkata')
        now = datetime.now(tz)
        dates = []
        candidate = now
        if candidate.hour < 18: 
            candidate -= timedelta(days=1)
        while len(dates) < 2:
            if candidate.weekday() < 5: 
                dates.append(candidate)
            candidate -= timedelta(days=1)
        return dates

    def _get_fallback_metrics(self, event_risk="LOW") -> ExternalMetrics:
        return ExternalMetrics(
            fii=None, dii=None, pro=None, client=None,
            fii_net_change=0.0,
            flow_regime="NEUTRAL",
            event_risk=event_risk,
            data_date="N/A"
        )
