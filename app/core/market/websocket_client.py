import logging
import threading
import time
from typing import Dict, List, Optional, Callable
import upstox_client
from upstox_client.rest import ApiException

logger = logging.getLogger(__name__)


class MarketDataFeed:
    """
    VolGuard Production WebSocket Client - Fully Aligned with Upstox SDK
    
    Features:
    ✅ SDK-native auto-reconnect (no manual threading loops)
    ✅ Exponential backoff via SDK configuration
    ✅ Thread-safe data cache with locks
    ✅ Comprehensive event handling (6 callbacks)
    ✅ Dynamic subscribe/unsubscribe support
    ✅ Health monitoring with staleness detection
    ✅ Graceful shutdown with cleanup
    ✅ Rate limit protection (429 detection)
    """
    
    def __init__(
        self, 
        access_token: str, 
        instrument_keys: List[str],
        mode: str = "full",
        auto_reconnect_enabled: bool = True,
        reconnect_interval: int = 10,
        max_retries: int = 5
    ):
        """
        Initialize WebSocket client
        
        Args:
            access_token: Upstox API access token
            instrument_keys: List of instrument keys to subscribe
            mode: Subscription mode - "full", "ltpc", "option_greeks", or "full_d30"
            auto_reconnect_enabled: Enable SDK auto-reconnect
            reconnect_interval: Seconds between reconnect attempts
            max_retries: Maximum reconnection attempts
        """
        self.access_token = access_token
        self.instrument_keys = instrument_keys
        self.mode = mode
        
        # Auto-reconnect configuration
        self.auto_reconnect_enabled = auto_reconnect_enabled
        self.reconnect_interval = reconnect_interval
        self.max_retries = max_retries
        
        # Thread-Safe Data Cache
        self._cache_lock = threading.Lock()
        self._latest_data: Dict[str, Dict] = {}
        self._last_update_time = time.time()
        
        # SDK Setup
        self.configuration = upstox_client.Configuration()
        self.configuration.access_token = self.access_token
        self.api_client = upstox_client.ApiClient(self.configuration)
        self.streamer: Optional[upstox_client.MarketDataStreamerV3] = None
        
        # Connection State
        self.is_connected = False
        self._connection_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._reconnect_exhausted = False
        
        # Statistics
        self._messages_received = 0
        self._reconnect_attempts = 0
        self._last_error: Optional[str] = None
        
        # Custom event handlers (optional)
        self._custom_handlers: Dict[str, List[Callable]] = {
            "open": [],
            "close": [],
            "message": [],
            "error": [],
            "reconnecting": [],
            "reconnect_stopped": []
        }
    
    # ============================================================================
    # PUBLIC API
    # ============================================================================
    
    async def connect(self):
        """
        Start WebSocket connection in background thread.
        Non-blocking - returns immediately.
        """
        if self._connection_thread and self._connection_thread.is_alive():
            logger.warning("WebSocket already running")
            return
        
        self._stop_event.clear()
        self._reconnect_exhausted = False
        
        self._connection_thread = threading.Thread(
            target=self._run_connection,
            daemon=True,
            name="UpstoxWebSocket"
        )
        self._connection_thread.start()
        
        logger.info("🚀 WebSocket thread started")
    
    async def disconnect(self):
        """
        Gracefully disconnect WebSocket and cleanup resources.
        """
        logger.info("🛑 Initiating WebSocket disconnect...")
        self._stop_event.set()
        
        if self.streamer:
            try:
                self.streamer.disconnect()
                logger.info("✅ Streamer disconnected")
            except Exception as e:
                logger.error(f"Disconnect error: {e}")
        
        self.is_connected = False
        
        # Wait for thread to finish (with timeout)
        if self._connection_thread and self._connection_thread.is_alive():
            self._connection_thread.join(timeout=5.0)
            if self._connection_thread.is_alive():
                logger.warning("⚠️ Connection thread didn't terminate cleanly")
        
        logger.info("✅ WebSocket fully disconnected")
    
    def subscribe(self, instrument_keys: List[str], mode: Optional[str] = None):
        """
        Add new instruments dynamically.
        
        Args:
            instrument_keys: List of instrument keys to subscribe
            mode: Optional mode override (defaults to instance mode)
        """
        if not self.streamer or not self.is_connected:
            logger.warning("❌ Cannot subscribe - WebSocket not connected")
            return False
        
        try:
            sub_mode = mode or self.mode
            self.streamer.subscribe(instrument_keys, sub_mode)
            logger.info(f"✅ Subscribed to {len(instrument_keys)} symbols in {sub_mode} mode")
            return True
        except Exception as e:
            logger.error(f"❌ Subscribe failed: {e}")
            return False
    
    def unsubscribe(self, instrument_keys: List[str]):
        """
        Remove instruments from subscription.
        
        Args:
            instrument_keys: List of instrument keys to unsubscribe
        """
        if not self.streamer or not self.is_connected:
            logger.warning("❌ Cannot unsubscribe - WebSocket not connected")
            return False
        
        try:
            self.streamer.unsubscribe(instrument_keys)
            logger.info(f"✅ Unsubscribed from {len(instrument_keys)} symbols")
            
            # Clean up cache
            with self._cache_lock:
                for key in instrument_keys:
                    self._latest_data.pop(key, None)
            
            return True
        except Exception as e:
            logger.error(f"❌ Unsubscribe failed: {e}")
            return False
    
    def change_mode(self, instrument_keys: List[str], mode: str):
        """
        Change subscription mode for existing instruments.
        
        Args:
            instrument_keys: List of instrument keys
            mode: New mode - "full", "ltpc", "option_greeks", or "full_d30"
        """
        if not self.streamer or not self.is_connected:
            logger.warning("❌ Cannot change mode - WebSocket not connected")
            return False
        
        try:
            self.streamer.change_mode(instrument_keys, mode)
            logger.info(f"✅ Changed mode to {mode} for {len(instrument_keys)} symbols")
            return True
        except Exception as e:
            logger.error(f"❌ Change mode failed: {e}")
            return False
    
    # ============================================================================
    # DATA ACCESS METHODS
    # ============================================================================
    
    def get_latest_quote(self, instrument_key: str) -> Optional[float]:
        """
        Get Last Traded Price (LTP) for a specific instrument.
        
        Args:
            instrument_key: Instrument identifier
            
        Returns:
            LTP as float, or None if not available
        """
        with self._cache_lock:
            data = self._latest_data.get(instrument_key)
            return data.get("ltp") if data else None
    
    def get_latest_greeks(self) -> Dict[str, Dict]:
        """
        Get option Greeks for all subscribed instruments.
        Used by trading supervisor for risk calculations.
        
        Returns:
            Dictionary mapping instrument_key -> greeks dict
        """
        with self._cache_lock:
            return {
                k: v.get("greeks")
                for k, v in self._latest_data.items()
                if "greeks" in v and v["greeks"] is not None
            }
    
    def get_all_quotes(self) -> Dict[str, float]:
        """
        Get LTP for all subscribed instruments.
        Used by portfolio and risk engines.
        
        Returns:
            Dictionary mapping instrument_key -> ltp
        """
        with self._cache_lock:
            return {
                k: v.get("ltp")
                for k, v in self._latest_data.items()
                if "ltp" in v and v["ltp"] is not None
            }
    
    def get_full_data(self, instrument_key: str) -> Optional[Dict]:
        """
        Get complete cached data for an instrument.
        
        Args:
            instrument_key: Instrument identifier
            
        Returns:
            Full data dictionary including ltp, greeks, timestamp, etc.
        """
        with self._cache_lock:
            return self._latest_data.get(instrument_key)
    
    def get_all_data(self) -> Dict[str, Dict]:
        """
        Get complete cached data for all instruments.
        
        Returns:
            Dictionary mapping instrument_key -> full data dict
        """
        with self._cache_lock:
            return dict(self._latest_data)
    
    # ============================================================================
    # HEALTH & STATUS
    # ============================================================================
    
    def is_healthy(self) -> bool:
        """
        Check if WebSocket is healthy and receiving data.
        
        Returns:
            True if connected and data is fresh (< 30s old)
        """
        if not self.is_connected:
            return False
        
        if self._reconnect_exhausted:
            return False
        
        # Check data freshness (30 second threshold)
        data_age = time.time() - self._last_update_time
        return data_age < 30.0
    
    def get_stats(self) -> Dict:
        """
        Get WebSocket statistics.
        
        Returns:
            Dictionary with connection stats
        """
        return {
            "is_connected": self.is_connected,
            "is_healthy": self.is_healthy(),
            "messages_received": self._messages_received,
            "reconnect_attempts": self._reconnect_attempts,
            "reconnect_exhausted": self._reconnect_exhausted,
            "last_error": self._last_error,
            "last_update_time": self._last_update_time,
            "data_age_seconds": time.time() - self._last_update_time,
            "cached_instruments": len(self._latest_data)
        }
    
    # ============================================================================
    # CUSTOM EVENT HANDLERS
    # ============================================================================
    
    def on(self, event: str, handler: Callable):
        """
        Register custom event handler.
        
        Args:
            event: Event name - "open", "close", "message", "error", "reconnecting", "reconnect_stopped"
            handler: Callback function
        """
        if event in self._custom_handlers:
            self._custom_handlers[event].append(handler)
        else:
            logger.warning(f"Unknown event type: {event}")
    
    def _trigger_custom_handlers(self, event: str, *args, **kwargs):
        """Trigger all registered custom handlers for an event"""
        for handler in self._custom_handlers.get(event, []):
            try:
                handler(*args, **kwargs)
            except Exception as e:
                logger.error(f"Custom handler error for {event}: {e}")
    
    # ============================================================================
    # INTERNAL CONNECTION LOGIC
    # ============================================================================
    
    def _run_connection(self):
        """
        Main connection loop - runs in background thread.
        Let SDK handle reconnection logic internally.
        """
        try:
            # Initialize streamer
            self.streamer = upstox_client.MarketDataStreamerV3(
                self.api_client,
                self.instrument_keys,
                self.mode
            )
            
            # Register SDK event callbacks
            self.streamer.on("open", self._on_open)
            self.streamer.on("message", self._on_message)
            self.streamer.on("error", self._on_error)
            self.streamer.on("close", self._on_close)
            self.streamer.on("reconnecting", self._on_reconnecting)
            self.streamer.on("autoReconnectStopped", self._on_reconnect_stopped)
            
            # Configure SDK auto-reconnect
            if self.auto_reconnect_enabled:
                self.streamer.auto_reconnect(
                    True, 
                    self.reconnect_interval, 
                    self.max_retries
                )
                logger.info(
                    f"🔄 Auto-reconnect enabled: "
                    f"{self.reconnect_interval}s interval, "
                    f"{self.max_retries} max retries"
                )
            else:
                self.streamer.auto_reconnect(False)
                logger.info("⚠️ Auto-reconnect disabled")
            
            # Connect (blocking call - SDK manages everything)
            logger.info(f"🔌 Connecting to Upstox WebSocket ({self.mode} mode)...")
            self.streamer.connect()
            
        except Exception as e:
            logger.critical(f"❌ WebSocket connection failed: {e}", exc_info=True)
            self.is_connected = False
            self._last_error = str(e)
    
    # ============================================================================
    # SDK EVENT CALLBACKS
    # ============================================================================
    
    def _on_open(self):
        """Called when WebSocket connection is established"""
        logger.info("✅ WebSocket Connected")
        self.is_connected = True
        self._last_update_time = time.time()
        self._reconnect_attempts = 0
        
        # Trigger custom handlers
        self._trigger_custom_handlers("open")
    
    def _on_close(self):
        """Called when WebSocket connection is closed"""
        logger.warning("⚠️ WebSocket Closed")
        self.is_connected = False
        
        # Trigger custom handlers
        self._trigger_custom_handlers("close")
    
    def _on_error(self, error):
        """Called when WebSocket encounters an error"""
        error_str = str(error)
        logger.error(f"❌ WebSocket Error: {error_str}")
        self._last_error = error_str
        
        # Check for rate limiting
        if "429" in error_str:
            logger.critical(
                "🚨 RATE LIMIT HIT (HTTP 429) - Too many subscriptions! "
                "Reduce instrument count or check API limits."
            )
        
        # Trigger custom handlers
        self._trigger_custom_handlers("error", error)
    
    def _on_reconnecting(self):
        """Called when SDK initiates reconnection attempt"""
        self._reconnect_attempts += 1
        logger.info(
            f"🔄 Reconnecting to WebSocket... "
            f"(Attempt {self._reconnect_attempts}/{self.max_retries})"
        )
        self.is_connected = False
        
        # Trigger custom handlers
        self._trigger_custom_handlers("reconnecting")
    
    def _on_reconnect_stopped(self):
        """Called when SDK exhausts all reconnection attempts"""
        logger.critical(
            f"🛑 Auto-reconnect exhausted after {self.max_retries} attempts. "
            "Manual intervention required."
        )
        self.is_connected = False
        self._reconnect_exhausted = True
        
        # Trigger custom handlers
        self._trigger_custom_handlers("reconnect_stopped")
    
    def _on_message(self, message):
        """
        Process incoming market data messages.
        
        Message structure (full mode):
        {
            "type": "full",
            "feeds": {
                "NSE_EQ|INE123...": {
                    "ltpc": {"ltp": 123.45, "ltt": 1234567890, ...},
                    "ff": {
                        "marketFF": {
                            "ltp": 123.45,
                            "ltq": 100,
                            "open": 120.0,
                            "high": 125.0,
                            "low": 119.0,
                            "close": 122.0,
                            ...
                        }
                    },
                    "optionGreeks": {
                        "delta": 0.5,
                        "gamma": 0.01,
                        "theta": -0.05,
                        "vega": 0.1,
                        "iv": 18.5
                    },
                    ...
                }
            }
        }
        """
        try:
            self._messages_received += 1
            self._last_update_time = time.time()
            
            feeds = message.get("feeds", {})
            if not feeds:
                return
            
            with self._cache_lock:
                for instrument_key, feed in feeds.items():
                    entry = {
                        "timestamp": time.time(),
                        "raw": feed  # Keep raw data for advanced usage
                    }
                    
                    # ============================================================
                    # Extract LTP (Last Traded Price)
                    # ============================================================
                    ltp = None
                    
                    # Try full mode data first
                    if "ff" in feed and "marketFF" in feed["ff"]:
                        market_ff = feed["ff"]["marketFF"]
                        ltp = market_ff.get("ltp")
                    
                    # Fallback to ltpc mode
                    if ltp is None and "ltpc" in feed:
                        ltp = feed["ltpc"].get("ltp")
                    
                    if ltp is not None:
                        entry["ltp"] = float(ltp)
                    
                    # ============================================================
                    # Extract OHLC (if available in full mode)
                    # ============================================================
                    if "ff" in feed and "marketFF" in feed["ff"]:
                        market_ff = feed["ff"]["marketFF"]
                        entry["ohlc"] = {
                            "open": market_ff.get("open"),
                            "high": market_ff.get("high"),
                            "low": market_ff.get("low"),
                            "close": market_ff.get("close"),
                            "volume": market_ff.get("volume")
                        }
                    
                    # ============================================================
                    # Extract Option Greeks
                    # ============================================================
                    if "optionGreeks" in feed:
                        greeks_data = feed["optionGreeks"]
                        entry["greeks"] = {
                            "delta": greeks_data.get("delta"),
                            "gamma": greeks_data.get("gamma"),
                            "theta": greeks_data.get("theta"),
                            "vega": greeks_data.get("vega"),
                            "iv": greeks_data.get("iv")  # Implied Volatility
                        }
                    
                    # ============================================================
                    # Extract Depth (if available)
                    # ============================================================
                    if "marketLevel" in feed:
                        entry["depth"] = feed["marketLevel"]
                    
                    # Update or create cache entry
                    if instrument_key in self._latest_data:
                        self._latest_data[instrument_key].update(entry)
                    else:
                        self._latest_data[instrument_key] = entry
            
            # Trigger custom message handlers
            self._trigger_custom_handlers("message", message)
            
        except Exception as e:
            logger.error(f"❌ Message processing error: {e}", exc_info=True)


# ============================================================================
# USAGE EXAMPLE
# ============================================================================

if __name__ == "__main__":
    import asyncio
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    async def main():
        # Initialize
        feed = MarketDataFeed(
            access_token="YOUR_ACCESS_TOKEN",
            instrument_keys=["NSE_INDEX|Nifty 50", "NSE_INDEX|Nifty Bank"],
            mode="full",
            auto_reconnect_enabled=True,
            reconnect_interval=10,
            max_retries=5
        )
        
        # Register custom handlers
        def on_connection_open():
            print("🎉 Custom handler: Connection opened!")
        
        def on_message_received(message):
            print(f"📨 Custom handler: Received message with {len(message.get('feeds', {}))} feeds")
        
        feed.on("open", on_connection_open)
        feed.on("message", on_message_received)
        
        # Connect
        await feed.connect()
        
        # Wait for connection
        await asyncio.sleep(3)
        
        # Check health
        print(f"\n📊 Stats: {feed.get_stats()}")
        
        # Get data
        print(f"\n💰 All quotes: {feed.get_all_quotes()}")
        print(f"\n📈 All Greeks: {feed.get_latest_greeks()}")
        
        # Subscribe to more instruments
        await asyncio.sleep(5)
        feed.subscribe(["NSE_EQ|INE002A01018"])  # Reliance
        
        # Keep running
        print("\n✅ WebSocket running. Press Ctrl+C to stop.")
        try:
            while True:
                await asyncio.sleep(10)
                print(f"📊 Stats: {feed.get_stats()}")
        except KeyboardInterrupt:
            print("\n🛑 Stopping...")
        
        # Cleanup
        await feed.disconnect()
    
    asyncio.run(main())
