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
    VolGuard WebSocket Client (Final Production Version)
    Features:
    - Safety Brakes (Prevents 429 bans)
    - Auto-Reconnect
    - Full Data Access (Greeks + Quotes)
    """
    
    def __init__(self, access_token: str, instrument_keys: List[str]):
        self.access_token = access_token
        self.instrument_keys = instrument_keys
        
        # Thread-Safe Data Cache
        self._cache_lock = threading.Lock()
        self._latest_data: Dict[str, Dict] = {}
        self._last_update_time = time.time()
        
        # SDK Setup
        self.configuration = upstox_client.Configuration()
        self.configuration.access_token = self.access_token
        self.api_client = upstox_client.ApiClient(self.configuration)
        self.streamer = None
        
        # Control Flags
        self.is_connected = False
        self._stop_event = threading.Event()
        self._reconnect_event = threading.Event()
        
        # Threads
        self.thread = None
        self._reconnect_thread = None
    
    async def connect(self):
        """Starts the WebSocket in background."""
        try:
            self._stop_event.clear()
            self._initialize_streamer()
            
            self.thread = threading.Thread(target=self._run_streamer, daemon=True)
            self.thread.start()
            
            self._reconnect_thread = threading.Thread(target=self._reconnection_monitor, daemon=True)
            self._reconnect_thread.start()
            
            logger.info("WebSocket threads started.")
        except Exception as e:
            logger.error(f"Failed to initialize WebSocket: {e}")
    
    def _initialize_streamer(self):
        try:
            self.streamer = upstox_client.MarketDataStreamerV3(
                self.api_client,
                self.instrument_keys,
                "full"
            )
            self.streamer.on("open", self._on_open)
            self.streamer.on("message", self._on_message)
            self.streamer.on("error", self._on_error)
            self.streamer.on("close", self._on_close)
        except Exception as e:
            logger.error(f"Streamer init failed: {e}")
            raise
    
    def _run_streamer(self):
        """Main Loop with Safety Brake"""
        while not self._stop_event.is_set():
            try:
                logger.info("🔌 Connecting to Upstox...")
                self.streamer.connect()
            except Exception as e:
                logger.error(f"WebSocket Error: {e}")
            
            # SAFETY BRAKE: Wait 5s before retrying to avoid 429 Ban
            if not self._stop_event.is_set():
                logger.warning("⚠️ Connection dropped. Cooling down for 5 seconds...")
                time.sleep(5)
    
    def _reconnection_monitor(self):
        """Health Check"""
        while not self._stop_event.is_set():
            time.sleep(10)
            if not self.is_healthy() and not self._stop_event.is_set():
                # The main loop will handle reconnect automatically via the sleep
                pass
    
    def is_healthy(self) -> bool:
        if not self.is_connected:
            return False
        # If no data for 30s, consider it stale (Markets might be closed)
        return (time.time() - self._last_update_time) < 30.0
    
    # ==================================================================
    # DATA ACCESS METHODS (Restored)
    # ==================================================================
    
    def get_latest_quote(self, instrument_key: str) -> Optional[float]:
        """Get just the LTP for a symbol"""
        with self._cache_lock:
            data = self._latest_data.get(instrument_key)
            return data.get("ltp") if data else None

    def get_latest_greeks(self) -> Dict[str, Dict]:
        """Get Greeks for all symbols (Used by Supervisor)"""
        with self._cache_lock:
            return {
                k: v.get("greeks")
                for k, v in self._latest_data.items()
                if "greeks" in v
            }

    def get_all_quotes(self) -> Dict[str, float]:
        """Get LTP map for all symbols (Used by Risk Engine)"""
        with self._cache_lock:
            return {
                k: v.get("ltp")
                for k, v in self._latest_data.items()
                if "ltp" in v
            }

    def subscribe(self, instrument_keys: List[str]):
        """Dynamic Subscription"""
        if self.streamer and self.is_connected:
            try:
                self.streamer.subscribe(instrument_keys, "full")
                logger.info(f"Subscribed to {len(instrument_keys)} symbols")
            except Exception:
                pass

    async def disconnect(self):
        self._stop_event.set()
        self.is_connected = False

    # ==================================================================
    # CALLBACKS (Safe Version)
    # ==================================================================
    def _on_open(self, *args):
        logger.info("✅ WebSocket Connected!")
        self.is_connected = True
        self._last_update_time = time.time()
    
    def _on_close(self, *args):
        logger.warning(f"WebSocket Closed: {args}")
        self.is_connected = False
    
    def _on_error(self, *args):
        logger.error(f"WebSocket Error: {args}")
        if args and "429" in str(args[0]):
            logger.critical("⛔ RATE LIMIT HIT (429). Waiting...")

    def _on_message(self, message, *args):
        try:
            self._last_update_time = time.time()
            payload = message.get("feeds", {})
            with self._cache_lock:
                for key, feed in payload.items():
                    entry = {"timestamp": time.time()}
                    
                    # Extract LTP
                    if "ff" in feed and "marketFF" in feed["ff"]:
                        entry["ltp"] = float(feed["ff"]["marketFF"].get("ltp"))
                    elif "ltpc" in feed:
                        entry["ltp"] = float(feed["ltpc"].get("ltp"))
                    
                    # Extract Greeks
                    if "optionGreeks" in feed:
                        entry["greeks"] = feed.get("optionGreeks")
                    
                    if key in self._latest_data:
                        self._latest_data[key].update(entry)
                    else:
                        self._latest_data[key] = entry
        except Exception:
            pass
