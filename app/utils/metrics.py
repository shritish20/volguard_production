# app/utils/metrics.py
"""
Production Metrics for VolGuard
Exports Prometheus metrics for Grafana dashboards
"""
import time
from prometheus_client import Counter, Histogram, Gauge, Info
from functools import wraps
import asyncio
import logging

logger = logging.getLogger(__name__)

# === COUNTERS ===
orders_placed = Counter(
    'volguard_orders_placed_total',
    'Total orders placed',
    ['side', 'strategy', 'status']
)

orders_failed = Counter(
    'volguard_orders_failed_total',
    'Total order failures',
    ['reason']
)

api_calls = Counter(
    'volguard_api_calls_total',
    'External API calls',
    ['endpoint', 'status']
)

safety_violations = Counter(
    'volguard_safety_violations_total',
    'Safety system violations',
    ['type', 'severity']
)

# === GAUGES ===
active_positions = Gauge(
    'volguard_active_positions',
    'Current number of active positions'
)

net_delta = Gauge(
    'volguard_net_delta',
    'Current portfolio delta'
)

available_margin = Gauge(
    'volguard_available_margin_inr',
    'Available margin in INR'
)

daily_pnl = Gauge(
    'volguard_daily_pnl_inr',
    'Daily realized PnL in INR'
)

system_state = Gauge(
    'volguard_system_state',
    'Current system state (0=NORMAL, 1=DEGRADED, 2=HALTED, 3=EMERGENCY)'
)

data_quality_score = Gauge(
    'volguard_data_quality',
    'Data quality score (0-1)'
)

# === HISTOGRAMS ===
cycle_duration = Histogram(
    'volguard_supervisor_cycle_seconds',
    'Supervisor cycle duration',
    buckets=[0.5, 1.0, 2.0, 3.0, 5.0, 10.0]
)

order_placement_duration = Histogram(
    'volguard_order_placement_seconds',
    'Order placement duration',
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0]
)

# === INFO ===
system_info = Info('volguard_system', 'System information')

# === DECORATORS ===
def track_duration(metric: Histogram):
    """Decorator to track async function duration"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            start = time.time()
            try:
                result = await func(*args, **kwargs)
                return result
            finally:
                duration = time.time() - start
                metric.observe(duration)
        return wrapper
    return decorator

def track_api_call(endpoint: str):
    """Decorator to track API calls"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                result = await func(*args, **kwargs)
                api_calls.labels(endpoint=endpoint, status='success').inc()
                return result
            except Exception as e:
                api_calls.labels(endpoint=endpoint, status='error').inc()
                raise
        return wrapper
    return decorator

# === HELPER FUNCTIONS ===
def record_order_placed(side: str, strategy: str, status: str):
    """Record order placement"""
    orders_placed.labels(side=side, strategy=strategy, status=status).inc()

def record_order_failed(reason: str):
    """Record order failure"""
    orders_failed.labels(reason=reason).inc()

def update_portfolio_metrics(positions: list, pnl: float, margin: float):
    """Update portfolio metrics"""
    active_positions.set(len(positions))
    daily_pnl.set(pnl)
    available_margin.set(margin)

def record_safety_violation(violation_type: str, severity: str):
    """Record safety violation"""
    safety_violations.labels(type=violation_type, severity=severity).inc()

def set_system_state(state: str):
    """Set current system state"""
    state_map = {
        'NORMAL': 0,
        'DEGRADED': 1,
        'HALTED': 2,
        'EMERGENCY': 3
    }
    system_state.set(state_map.get(state, 0))
