# app/core/market/websocket_client.py

import logging
import threading
import time
import asyncio
from typing import Dict, List, Optional
import upstox_client
from upstox_client.rest import ApiException

logger = logging.getLogger(__name__)

class MarketDataFeed:
    """
    VolGuard WebSocket Client with AUTO-RECONNECTION (VolGuard 3.0+)
    
    Features:
    - Automatic reconnection with exponential backoff
    - Connection health monitoring
    - Graceful degradation
    - Thread-safe data access
    """
    
    def __init__(self, access_token: str, instrument_keys: List[str]):
        self.access_token = access_token
        self.instrument_keys = instrument_keys
        
        # Internal Cache (Thread Safe)
        self._cache_lock = threading.Lock()
        self._latest_data: Dict[str, Dict] = {}
        self._last_update_time = time.time()
        
        # SDK Components
        self.configuration = upstox_client.Configuration()
        self.configuration.access_token = self.access_token
        self.api_client = upstox_client.ApiClient(self.configuration)
        self.streamer = None
        
        # Control Flags
        self.is_connected = False
        self._stop_event = threading.Event()
        self._reconnect_event = threading.Event()
        
        # Reconnection Logic
        self._reconnect_attempts = 0
        self._max_reconnect_attempts = 10
        self._base_reconnect_delay = 2  # seconds
        self._max_reconnect_delay = 300  # 5 minutes
        
        # Thread references
        self.thread = None
        self._reconnect_thread = None
    
    async def connect(self):
        """
        Starts the WebSocket in a non-blocking background thread.
        Enables automatic reconnection.
        """
        try:
            self._stop_event.clear()
            self._initialize_streamer()
            
            # Start main connection thread
            self.thread = threading.Thread(target=self._run_streamer, daemon=True)
            self.thread.start()
            
            # Start reconnection monitor thread
            self._reconnect_thread = threading.Thread(
                target=self._reconnection_monitor, 
                daemon=True
            )
            self._reconnect_thread.start()
            
            logger.info("WebSocket threads started with auto-reconnection enabled")
            
        except Exception as e:
            logger.error(f"Failed to initialize WebSocket: {e}")
    
    def _initialize_streamer(self):
        """Initialize or reinitialize the streamer"""
        try:
            self.streamer = upstox_client.MarketDataStreamerV3(
                self.api_client,
                self.instrument_keys,
                "full"  # Mode for Index data
            )
            
            # Attach Callbacks
            self.streamer.on("open", self._on_open)
            self.streamer.on("message", self._on_message)
            self.streamer.on("error", self._on_error)
            self.streamer.on("close", self._on_close)
            
        except Exception as e:
            logger.error(f"Streamer initialization failed: {e}")
            raise
    
    def _run_streamer(self):
        """Blocking call wrapped in thread"""
        while not self._stop_event.is_set():
            try:
                logger.info("Attempting WebSocket connection...")
                self.streamer.connect()
                
                # If we reach here, connection was closed
                if not self._stop_event.is_set():
                    logger.warning("WebSocket connection closed unexpectedly")
                    self._reconnect_event.set()
                    
            except Exception as e:
                logger.error(f"WebSocket connection error: {e}")
                self.is_connected = False
                
                if not self._stop_event.is_set():
                    self._reconnect_event.set()
                    # Wait before retry
                    time.sleep(5)
    
    def _reconnection_monitor(self):
        """
        Monitors connection health and triggers reconnection.
        Implements exponential backoff.
        """
        while not self._stop_event.is_set():
            # Wait for reconnection signal or timeout
            if self._reconnect_event.wait(timeout=30):
                # Reconnection needed
                if self._stop_event.is_set():
                    break
                
                self._reconnect_event.clear()
                
                # Check if we've exceeded max attempts
                if self._reconnect_attempts >= self._max_reconnect_attempts:
                    logger.critical(
                        f"Max reconnection attempts ({self._max_reconnect_attempts}) exceeded. "
                        "Manual intervention required."
                    )
                    # Reset counter and wait longer
                    self._reconnect_attempts = 0
                    time.sleep(self._max_reconnect_delay)
                    continue
                
                # Calculate backoff delay
                delay = min(
                    self._base_reconnect_delay * (2 ** self._reconnect_attempts),
                    self._max_reconnect_delay
                )
                
                logger.info(
                    f"Reconnection attempt {self._reconnect_attempts + 1}/{self._max_reconnect_attempts} "
                    f"in {delay}s..."
                )
                
                time.sleep(delay)
                
                # Attempt reconnection
                try:
                    # Reinitialize streamer
                    self._initialize_streamer()
                    self._reconnect_attempts += 1
                    
                    # Connection attempt will happen in main thread
                    logger.info("Streamer reinitialized, waiting for connection...")
                    
                except Exception as e:
                    logger.error(f"Reconnection failed: {e}")
            
            # Also check connection health periodically
            elif not self.is_healthy():
                logger.warning("Connection unhealthy detected by monitor")
                self._reconnect_event.set()
    
    def is_healthy(self) -> bool:
        """
        Heartbeat Check with improved logic.
        Returns False if:
        - Not connected
        - No data received for 15 seconds (increased from 10)
        - Connection state is stale
        """
        if not self.is_connected:
            return False
        
        # Check data freshness
        time_since_update = time.time() - self._last_update_time
        
        # Allow longer gap for low-activity periods
        max_gap = 15.0
        
        return time_since_update < max_gap
    
    def get_latest_greeks(self) -> Dict:
        """
        Returns any exchange-provided Greeks (from 'option_greeks' mode).
        Thread-safe access.
        """
        with self._cache_lock:
            return {
                k: v.get("greeks")
                for k, v in self._latest_data.items()
                if "greeks" in v
            }
    
    def get_latest_quote(self, instrument_key: str) -> Optional[float]:
        """
        Zero-latency LTP lookup.
        Thread-safe access.
        """
        with self._cache_lock:
            data = self._latest_data.get(instrument_key)
            if data:
                return data.get("ltp")
        return None
    
    def get_all_quotes(self) -> Dict[str, float]:
        """
        Get all latest quotes.
        Thread-safe access.
        """
        with self._cache_lock:
            return {
                k: v.get("ltp")
                for k, v in self._latest_data.items()
                if "ltp" in v
            }
    
    # ==================================================================
    # CALLBACKS (Internal)
    # ==================================================================
    
    def _on_open(self):
        """Connection opened successfully"""
        logger.info("✅ WebSocket Connected")
        self.is_connected = True
        self._reconnect_attempts = 0  # Reset counter on successful connection
        self._last_update_time = time.time()
    
    def _on_close(self, message):
        """Connection closed"""
        logger.warning(f"WebSocket Closed: {message}")
        self.is_connected = False
        
        # Trigger reconnection if not shutting down
        if not self._stop_event.is_set():
            self._reconnect_event.set()
    
    def _on_error(self, error):
        """Error occurred"""
        logger.error(f"WebSocket Error: {error}")
        
        # Don't trigger reconnection for every error
        # Some errors are transient
        if "connection" in str(error).lower() or "timeout" in str(error).lower():
            if not self._stop_event.is_set():
                self._reconnect_event.set()
    
    def _on_message(self, message):
        """
        Parses Protobuf message from SDK and updates cache.
        Thread-safe update.
        """
        try:
            # Update heartbeat
            self._last_update_time = time.time()
            
            # Extract data (SDK-specific structure)
            payload = message.get("feeds", {})
            
            with self._cache_lock:
                for key, feed in payload.items():
                    # Parse LTP
                    ltp = None
                    
                    # Try different possible structures
                    if "ff" in feed and "marketFF" in feed["ff"]:
                        ltp = feed["ff"]["marketFF"].get("ltp")
                    elif "ltpc" in feed:
                        ltp = feed["ltpc"].get("ltp")
                    
                    # Parse Greeks (if available)
                    greeks = feed.get("optionGreeks")
                    
                    # Build entry
                    entry = {"timestamp": time.time()}
                    if ltp:
                        entry["ltp"] = float(ltp)
                    if greeks:
                        entry["greeks"] = greeks
                    
                    # Update or merge
                    if key in self._latest_data:
                        self._latest_data[key].update(entry)
                    else:
                        self._latest_data[key] = entry
            
        except Exception as e:
            logger.error(f"Message parse error: {e}")
    
    async def disconnect(self):
        """
        Graceful shutdown with cleanup.
        """
        logger.info("Disconnecting WebSocket...")
        
        # Signal threads to stop
        self._stop_event.set()
        self._reconnect_event.set()
        
        # Close connection
        self.is_connected = False
        
        # Wait for threads to finish (with timeout)
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=5)
        
        if self._reconnect_thread and self._reconnect_thread.is_alive():
            self._reconnect_thread.join(timeout=5)
        
        logger.info("WebSocket disconnected")
    
    def get_connection_stats(self) -> Dict:
        """
        Get connection statistics for monitoring.
        """
        return {
            "is_connected": self.is_connected,
            "is_healthy": self.is_healthy(),
            "reconnect_attempts": self._reconnect_attempts,
            "last_update_seconds_ago": time.time() - self._last_update_time,
            "cached_instruments": len(self._latest_data),
            "thread_alive": self.thread.is_alive() if self.thread else False
        }
