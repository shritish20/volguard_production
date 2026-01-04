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
    VolGuard WebSocket Client (VolGuard 3.0)
    
    Architecture:
    - Uses Upstox SDK (Protobuf support).
    - Runs in a background thread (Daemon).
    - Feeds 'Supervisor' with zero-latency LTP and Greeks.
    - Mode: 'full' for Index, 'option_greeks' for Options.
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

    async def connect(self):
        """
        Starts the WebSocket in a non-blocking background thread.
        """
        try:
            self.streamer = upstox_client.MarketDataStreamerV3(
                self.api_client, 
                self.instrument_keys, 
                "full" # Default mode for Indices
            )
            
            # Attach Callbacks
            self.streamer.on("open", self._on_open)
            self.streamer.on("message", self._on_message)
            self.streamer.on("error", self._on_error)
            self.streamer.on("close", self._on_close)
            
            # Start in Thread
            self.thread = threading.Thread(target=self._run_streamer, daemon=True)
            self.thread.start()
            
            logger.info("WebSocket Thread Started")
            
        except Exception as e:
            logger.error(f"Failed to initialize WebSocket: {e}")

    def _run_streamer(self):
        """Blocking call wrapped in thread"""
        try:
            # Auto-reconnect enabled by SDK default, but we monitor it
            self.streamer.connect() 
        except Exception as e:
            logger.error(f"WebSocket Loop Crashed: {e}")
            self.is_connected = False

    def is_healthy(self) -> bool:
        """
        Heartbeat Check.
        Returns False if no data received for 10 seconds.
        """
        if not self.is_connected:
            return False
        return (time.time() - self._last_update_time) < 10.0

    def get_latest_greeks(self) -> Dict:
        """
        Returns any exchange-provided Greeks (from 'option_greeks' mode).
        Used by Supervisor as a backup source.
        """
        with self._cache_lock:
            # Filter for keys that have greek data
            return {
                k: v.get("greeks") 
                for k, v in self._latest_data.items() 
                if "greeks" in v
            }

    def get_latest_quote(self, instrument_key: str) -> Optional[float]:
        """Zero-latency LTP lookup"""
        with self._cache_lock:
            data = self._latest_data.get(instrument_key)
            if data:
                return data.get("ltp")
            return None

    # ==================================================================
    # CALLBACKS (Internal)
    # ==================================================================

    def _on_open(self):
        logger.info("WebSocket Connected via SDK")
        self.is_connected = True
        # Logic to subscribe to specific modes if needed
        # self.streamer.subscribe(option_keys, "option_greeks")

    def _on_close(self, message):
        logger.warning(f"WebSocket Closed: {message}")
        self.is_connected = False

    def _on_error(self, error):
        logger.error(f"WebSocket Error: {error}")

    def _on_message(self, message):
        """
        Parses Protobuf message from SDK and updates cache.
        """
        try:
            # The SDK decodes the protobuf into a dictionary/object 'message'
            # Structure depends on the 'mode' (full vs ltpc)
            
            # Update Heartbeat
            self._last_update_time = time.time()
            
            # Extract Data (Generic Parser)
            # Note: SDK response structure varies. We look for common fields.
            payload = message.get("feeds", {})
            
            with self._cache_lock:
                for key, feed in payload.items():
                    # Parse LTP
                    ltp = feed.get("ff", {}).get("marketFF", {}).get("ltp")
                    if not ltp:
                        ltp = feed.get("ltpc", {}).get("ltp")
                    
                    # Parse Greeks (if available in 'option_greeks' mode)
                    greeks = feed.get("optionGreeks")
                    
                    entry = {"timestamp": time.time()}
                    if ltp: entry["ltp"] = float(ltp)
                    if greeks: entry["greeks"] = greeks
                    
                    # Update or Merge
                    if key in self._latest_data:
                        self._latest_data[key].update(entry)
                    else:
                        self._latest_data[key] = entry
                        
        except Exception as e:
            logger.error(f"Message Parse Error: {e}")
