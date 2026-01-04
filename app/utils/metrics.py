# app/utils/metrics.py
"""
PRODUCTION-READY Metrics System for VolGuard
Exports comprehensive Prometheus metrics for monitoring and alerting.
"""
import time
import platform
import socket
import sys
from prometheus_client import Counter, Histogram, Gauge, Info, Summary, Enum
from functools import wraps
import asyncio
import logging
from typing import Dict, Any, Optional, Callable
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime

logger = logging.getLogger(__name__)

# ============================================
# 1. CORE TRADING METRICS
# ============================================

# Order execution
orders_placed_total = Counter(
    'volguard_orders_placed_total',
    'Total orders placed by the system',
    ['side', 'strategy', 'instrument_type', 'order_type', 'status']
)

orders_failed_total = Counter(
    'volguard_orders_failed_total',
    'Total order failures',
    ['failure_reason', 'phase', 'instrument_type']
)

order_execution_duration = Histogram(
    'volguard_order_execution_duration_seconds',
    'Time taken to execute an order',
    [0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0, 10.0],  # buckets
    ['order_type', 'side']  # labelnames
)

# Position metrics
active_positions = Gauge(
    'volguard_active_positions',
    'Current number of active positions',
    ['strategy', 'instrument_type']
)

position_pnl = Gauge(
    'volguard_position_pnl',
    'Current PnL of active positions',
    ['strategy', 'instrument_type']
)

position_delta = Gauge(
    'volguard_position_delta',
    'Current delta of active positions',
    ['strategy']
)

position_vega = Gauge(
    'volguard_position_vega',
    'Current vega of active positions',
    ['strategy']
)

position_theta = Gauge(
    'volguard_position_theta',
    'Current theta of active positions',
    ['strategy']
)

# Capital metrics
available_margin = Gauge(
    'volguard_available_margin',
    'Available margin in account (INR)'
)

used_margin = Gauge(
    'volguard_used_margin',
    'Margin used by current positions (INR)'
)

margin_utilization = Gauge(
    'volguard_margin_utilization_ratio',
    'Margin utilization ratio (used/available)'
)

daily_pnl = Gauge(
    'volguard_daily_pnl',
    'Daily realized PnL (INR)'
)

cumulative_pnl = Gauge(
    'volguard_cumulative_pnl',
    'Cumulative PnL since start (INR)'
)

# ============================================
# 2. SYSTEM PERFORMANCE METRICS
# ============================================

supervisor_cycle_duration = Histogram(
    'volguard_supervisor_cycle_duration_seconds',
    'Duration of supervisor cycles',
    [0.1, 0.5, 1.0, 2.0, 3.0, 5.0, 10.0],  # buckets
    ['phase']  # labelnames
)

cycle_overrun = Counter(
    'volguard_cycle_overrun_total',
    'Number of cycles that exceeded target interval'
)

data_fetch_duration = Histogram(
    'volguard_data_fetch_duration_seconds',
    'Time taken to fetch market data',
    ['data_source', 'data_type']
)

risk_calc_duration = Histogram(
    'volguard_risk_calc_duration_seconds',
    'Time taken for risk calculations',
    ['calculation_type']
)

decision_duration = Histogram(
    'volguard_decision_duration_seconds',
    'Time taken for trading decisions'
)

# ============================================
# 3. MARKET DATA METRICS
# ============================================

market_data_quality = Gauge(
    'volguard_market_data_quality',
    'Quality score of market data (0-1)'
)

market_data_latency = Gauge(
    'volguard_market_data_latency_seconds',
    'Latency of market data'
)

market_data_errors = Counter(
    'volguard_market_data_errors_total',
    'Market data fetch errors',
    ['source', 'error_type']
)

spot_price = Gauge(
    'volguard_spot_price',
    'Current spot price',
    ['symbol']
)

vix_value = Gauge(
    'volguard_vix_value',
    'Current VIX value'
)

option_chain_quality = Gauge(
    'volguard_option_chain_quality',
    'Quality of option chain data (0-1)',
    ['expiry']
)

# ============================================
# 4. RISK METRICS
# ============================================

portfolio_stress_loss = Gauge(
    'volguard_portfolio_stress_loss',
    'Maximum loss in stress test scenarios (INR)',
    ['scenario']
)

risk_limit_breaches = Counter(
    'volguard_risk_limit_breaches_total',
    'Number of risk limit breaches',
    ['limit_type', 'severity']
)

var_95 = Gauge(
    'volguard_var_95',
    'Value at Risk at 95% confidence (INR)'
)

expected_shortfall = Gauge(
    'volguard_expected_shortfall',
    'Expected shortfall (CVaR) at 95% confidence (INR)'
)

greeks_exposure = Gauge(
    'volguard_greeks_exposure',
    'Exposure to Greek risks',
    ['greek']
)

# ============================================
# 5. SYSTEM HEALTH METRICS
# ============================================

system_state = Enum(
    'volguard_system_state',
    'Current system state',
    states=['NORMAL', 'DEGRADED', 'HALTED', 'EMERGENCY', 'SHUTDOWN']
)

execution_mode = Enum(
    'volguard_execution_mode',
    'Current execution mode',
    states=['SHADOW', 'SEMI_AUTO', 'FULL_AUTO']
)

component_health = Gauge(
    'volguard_component_health',
    'Health status of system components (0=down, 1=up)',
    ['component']
)

connection_errors = Counter(
    'volguard_connection_errors_total',
    'Connection errors to external services',
    ['service', 'error_type']
)

api_call_duration = Histogram(
    'volguard_api_call_duration_seconds',
    'Duration of API calls to external services',
    ['service', 'endpoint', 'status']
)

api_call_errors = Counter(
    'volguard_api_call_errors_total',
    'API call errors',
    ['service', 'endpoint', 'status_code']
)

# ============================================
# 6. BUSINESS METRICS
# ============================================

trades_per_day = Counter(
    'volguard_trades_per_day',
    'Number of trades executed per day',
    ['strategy', 'result']
)

win_rate = Gauge(
    'volguard_win_rate',
    'Win rate of trades',
    ['strategy', 'timeframe']
)

profit_factor = Gauge(
    'volguard_profit_factor',
    'Profit factor (gross profit / gross loss)',
    ['strategy', 'timeframe']
)

sharpe_ratio = Gauge(
    'volguard_sharpe_ratio',
    'Sharpe ratio of strategy',
    ['strategy', 'timeframe']
)

max_drawdown = Gauge(
    'volguard_max_drawdown',
    'Maximum drawdown experienced (INR)',
    ['strategy', 'timeframe']
)

recovery_factor = Gauge(
    'volguard_recovery_factor',
    'Recovery factor (net profit / max drawdown)',
    ['strategy', 'timeframe']
)

# ============================================
# 7. DECISION METRICS
# ============================================

regime_detected = Enum(
    'volguard_regime_detected',
    'Current market regime',
    states=[
        'CASH', 'DEFENSIVE', 'NEUTRAL', 'MODERATE_SHORT', 
        'AGGRESSIVE_SHORT', 'ULTRA_AGGRESSIVE', 'LONG_VOL'
    ]
)

regime_score = Gauge(
    'volguard_regime_score',
    'Score of current regime (0-10)'
)

edge_detected = Enum(
    'volguard_edge_detected',
    'Primary edge detected',
    states=['NONE', 'LONG_VOL', 'SHORT_VOL', 'CALENDAR', 'VERTICAL', 'SKEW']
)

edge_score = Gauge(
    'volguard_edge_score',
    'Score of primary edge'
)

volatility_regime = Enum(
    'volguard_volatility_regime',
    'Current volatility regime',
    states=['LOW', 'NORMAL', 'HIGH', 'EXTREME']
)

# ============================================
# 8. INFO METRICS
# ============================================

system_info = Info('volguard_system', 'System information')
version_info = Info('volguard_version', 'Version information')
deployment_info = Info('volguard_deployment', 'Deployment information')

# ============================================
# METRIC HELPER CLASSES
# ============================================

@dataclass
class OrderMetrics:
    """Metrics for a single order"""
    side: str
    strategy: str
    instrument_type: str
    order_type: str
    quantity: int
    price: float
    timestamp: datetime

@dataclass
class CycleMetrics:
    """Metrics for a supervisor cycle"""
    cycle_id: str
    start_time: float
    end_time: float
    data_quality: float
    positions_count: int
    adjustments_count: int
    executions_count: int
    errors_count: int

class MetricsCollector:
    """Central metrics collector for VolGuard"""
    
    def __init__(self):
        self.start_time = time.time()
        self.orders = []
        self.cycles = []
        self._initialized = False
        
    def initialize(self, environment: str, version: str):
        """Initialize metrics with system info"""
        system_info.info({
            'name': 'VolGuard Trading System',
            'environment': environment,
            'start_time': datetime.now().isoformat()
        })
        
        version_info.info({
            'version': version,
            'python_version': sys.version
        })
        
        deployment_info.info({
            'hostname': socket.gethostname(),
            'platform': platform.platform()
        })
        
        self._initialized = True
        logger.info(f"Metrics system initialized for {environment} v{version}")
    
    def record_order(self, order: OrderMetrics, status: str, error: Optional[str] = None):
        """Record an order execution"""
        if not self._initialized:
            logger.warning("Metrics not initialized")
            return
        
        self.orders.append(order)
        
        # Increment counters
        orders_placed_total.labels(
            side=order.side,
            strategy=order.strategy,
            instrument_type=order.instrument_type,
            order_type=order.order_type,
            status=status
        ).inc()
        
        if error:
            orders_failed_total.labels(
                failure_reason=error,
                phase='execution',
                instrument_type=order.instrument_type
            ).inc()
    
    def record_cycle(self, cycle: CycleMetrics):
        """Record a supervisor cycle"""
        if not self._initialized:
            return
        
        self.cycles.append(cycle)
        
        # Record cycle duration
        duration = cycle.end_time - cycle.start_time
        supervisor_cycle_duration.labels(phase='full').observe(duration)
        
        # Check for overrun
        if duration > 3.0:  # 3 second target
            cycle_overrun.inc()
        
        # Update data quality
        market_data_quality.set(cycle.data_quality)
        
        # Update position count
        active_positions.labels(strategy='all', instrument_type='all').set(cycle.positions_count)
    
    def update_portfolio_metrics(self, positions: list, pnl: float, margin: float):
        """Update portfolio metrics"""
        if not self._initialized:
            return
        
        # Update capital metrics
        available_margin.set(margin)
        daily_pnl.set(pnl)
        
        # Update position metrics
        if positions:
            position_pnl.labels(strategy='all', instrument_type='all').set(
                sum(p.get('pnl', 0) for p in positions)
            )
    
    def update_system_state(self, state: str, mode: str):
        """Update system state"""
        if not self._initialized:
            return
        
        system_state.state(state)
        execution_mode.state(mode)
    
    def update_component_health(self, component: str, healthy: bool):
        """Update component health status"""
        if not self._initialized:
            return
        
        component_health.labels(component=component).set(1 if healthy else 0)
    
    def record_api_call(self, service: str, endpoint: str, duration: float, 
                       success: bool, status_code: Optional[int] = None):
        """Record an API call"""
        if not self._initialized:
            return
        
        status = 'success' if success else 'error'
        api_call_duration.labels(
            service=service,
            endpoint=endpoint,
            status=status
        ).observe(duration)
        
        if not success:
            api_call_errors.labels(
                service=service,
                endpoint=endpoint,
                status_code=str(status_code) if status_code else 'unknown'
            ).inc()
    
    def get_summary(self) -> Dict[str, Any]:
        """Get metrics summary"""
        uptime = time.time() - self.start_time
        
        return {
            'uptime_seconds': uptime,
            'total_orders': len(self.orders),
            'total_cycles': len(self.cycles),
            'avg_cycle_duration': (
                sum(c.end_time - c.start_time for c in self.cycles) / len(self.cycles)
                if self.cycles else 0
            ),
            'current_time': datetime.now().isoformat()
        }

# ============================================
# DECORATORS AND CONTEXT MANAGERS
# ============================================

def track_duration(metric: Histogram, **labels):
    """Decorator to track function duration"""
    def decorator(func: Callable):
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            start = time.time()
            try:
                result = func(*args, **kwargs)
                return result
            finally:
                duration = time.time() - start
                metric.labels(**labels).observe(duration)
        
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            start = time.time()
            try:
                result = await func(*args, **kwargs)
                return result
            finally:
                duration = time.time() - start
                metric.labels(**labels).observe(duration)
        
        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
    return decorator

def track_api_call_metrics(service: str, endpoint: str):
    """Decorator to track API calls with metrics"""
    def decorator(func: Callable):
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            start = time.time()
            try:
                result = func(*args, **kwargs)
                duration = time.time() - start
                api_call_duration.labels(
                    service=service,
                    endpoint=endpoint,
                    status='success'
                ).observe(duration)
                return result
            except Exception as e:
                duration = time.time() - start
                api_call_duration.labels(
                    service=service,
                    endpoint=endpoint,
                    status='error'
                ).observe(duration)
                raise
        
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            start = time.time()
            try:
                result = await func(*args, **kwargs)
                duration = time.time() - start
                api_call_duration.labels(
                    service=service,
                    endpoint=endpoint,
                    status='success'
                ).observe(duration)
                return result
            except Exception as e:
                duration = time.time() - start
                api_call_duration.labels(
                    service=service,
                    endpoint=endpoint,
                    status='error'
                ).observe(duration)
                raise
        
        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
    return decorator

@contextmanager
def measure_duration(metric: Histogram, **labels):
    """Context manager to measure code block duration"""
    start = time.time()
    try:
        yield
    finally:
        duration = time.time() - start
        metric.labels(**labels).observe(duration)

# ============================================
# CONVENIENCE FUNCTIONS
# ============================================

# Global metrics collector instance
collector = MetricsCollector()

def record_order_placed(side: str, strategy: str, instrument_type: str, 
                       order_type: str, status: str = 'PLACED'):
    """Convenience function to record order placement"""
    orders_placed_total.labels(
        side=side,
        strategy=strategy,
        instrument_type=instrument_type,
        order_type=order_type,
        status=status
    ).inc()

def record_order_failed(failure_reason: str, phase: str = 'execution', 
                       instrument_type: str = 'OPTION'):
    """Convenience function to record order failure"""
    orders_failed_total.labels(
        failure_reason=failure_reason,
        phase=phase,
        instrument_type=instrument_type
    ).inc()

def update_portfolio_metrics_simple(positions_count: int, pnl: float, margin: float):
    """Simple portfolio metrics update"""
    active_positions.labels(strategy='all', instrument_type='all').set(positions_count)
    daily_pnl.set(pnl)
    available_margin.set(margin)
    
    # Calculate margin utilization
    if margin > 0:
        utilization = (margin - available_margin._value.get()) / margin
        margin_utilization.set(utilization)

def record_safety_violation(violation_type: str, severity: str = 'MEDIUM'):
    """Record safety violation"""
    risk_limit_breaches.labels(
        limit_type=violation_type,
        severity=severity
    ).inc()

def set_system_state_simple(state: str):
    """Set system state"""
    system_state.state(state)

def update_data_quality(quality_score: float):
    """Update data quality metric"""
    market_data_quality.set(quality_score)

def update_component_health_simple(component: str, healthy: bool):
    """Update component health"""
    component_health.labels(component=component).set(1 if healthy else 0)
