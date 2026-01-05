# app/core/ev/capital_buckets.py

from dataclasses import dataclass
from typing import Dict

@dataclass
class CapitalBucket:
    name: str
    allocation_pct: float
    max_daily_loss_pct: float
    allow_overnight: bool
    active: bool = True

class CapitalBucketEngine:
    """
    Controls *where* capital is allowed to work.
    """

    def __init__(self, total_capital: float):
        self.total_capital = total_capital

        self.buckets: Dict[str, CapitalBucket] = {
            "INTRADAY": CapitalBucket("INTRADAY", 0.20, 0.005, False),
            "WEEKLY": CapitalBucket("WEEKLY", 0.50, 0.01, True),
            "MONTHLY": CapitalBucket("MONTHLY", 0.30, 0.015, True),
        }

    def get_bucket_capital(self, bucket: str) -> float:
        b = self.buckets.get(bucket)
        if not b or not b.active:
            return 0.0
        return self.total_capital * b.allocation_pct

    def disable_bucket(self, bucket: str):
        if bucket in self.buckets:
            self.buckets[bucket].active = False

    def enable_bucket(self, bucket: str):
        if bucket in self.buckets:
            self.buckets[bucket].active = True

    def enforce_regime(self, regime: str):
        if regime == "LONG_VOL":
            self.disable_bucket("INTRADAY")
            self.disable_bucket("WEEKLY")
        elif regime == "DEFENSIVE":
            self.disable_bucket("INTRADAY")
        else:
            # Re-enable all in benign conditions
            for bucket in self.buckets.values():
                bucket.active = True

    def apply_drawdown_rules(self, weekly_drawdown_pct: float):
        if weekly_drawdown_pct > 0.02: self.disable_bucket("INTRADAY")
        if weekly_drawdown_pct > 0.04: self.disable_bucket("MONTHLY")
