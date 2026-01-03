import upstox_client
import threading
import logging
from typing import List, Dict
from datetime import datetime

logger = logging.getLogger(__name__)

class UpstoxFeedService:
    """
    Real-time data feed using Upstox V3 Streamers.
    """
    def __init__(self, access_token: str):
        self.config = upstox_client.Configuration()
        self.config.access_token = access_token
        self.client = upstox_client.ApiClient(self.config)
        self.streamer = None
        self._lock = threading.Lock()
        self._cache = {}
        self.is_connected = False

    async def connect(self):
        try:
            self.streamer = upstox_client.MarketDataStreamerV3(self.client)
            self.streamer.on("open", self._on_open)
            self.streamer.on("message", self._on_message)
            self.streamer.on("error", self._on_error)
            self.streamer.auto_reconnect(True, 10, 5)
            self.streamer.connect()
            self.is_connected = True
        except Exception as e:
            logger.error(f"WS Connect Error: {e}")

    def update_subscriptions(self, keys: List[str]):
        if self.streamer and self.is_connected:
            self.streamer.subscribe(keys, "option_greeks") # Subscribe to Greeks

    def get_latest_greeks(self) -> Dict:
        with self._lock:
            return self._cache.copy()

    def _on_open(self): logger.info("Websocket Connected")
    def _on_error(self, e): logger.error(f"Websocket Error: {e}")
    
    def _on_message(self, msg):
        try:
            feeds = msg.get("feeds", {})
            with self._lock:
                for k, v in feeds.items():
                    og = v.get("og", {}) # Option Greeks
                    if og:
                        self._cache[k] = {
                            "delta": og.get("delta", 0),
                            "gamma": og.get("gamma", 0),
                            "iv": og.get("iv", 0),
                            "timestamp": datetime.now()
                        }
        except: pass
    
    async def disconnect(self):
        if self.streamer: self.streamer.disconnect()
