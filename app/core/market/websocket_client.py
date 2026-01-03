# app/core/market/websocket_client.py

import upstox_client
import threading
import logging
import time
from typing import List, Dict
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class UpstoxFeedService:
    """
    PRODUCTION-GRADE WebSocket Feed
    - Auto reconnect
    - Staleness detection
    - Subscription recovery
    - Fail-closed behavior
    """

    STALE_AFTER_SECONDS = 15  # Greeks older than this are INVALID

    def __init__(self, access_token: str):
        self.config = upstox_client.Configuration()
        self.config.access_token = access_token

        self.client = upstox_client.ApiClient(self.config)
        self.streamer = None

        self._lock = threading.RLock()
        self._cache: Dict[str, Dict] = {}
        self._subscriptions: List[str] = []

        self._connected = False
        self._last_message_ts: float = 0.0

    # ======================================================
    # CONNECTION MANAGEMENT
    # ======================================================
    async def connect(self):
        """Initializes and connects websocket"""
        try:
            self.streamer = upstox_client.MarketDataStreamerV3(self.client)

            self.streamer.on("open", self._on_open)
            self.streamer.on("message", self._on_message)
            self.streamer.on("error", self._on_error)
            self.streamer.on("close", self._on_close)

            # Aggressive reconnect (prod safe)
            self.streamer.auto_reconnect(True, 5, 10)

            self.streamer.connect()
            logger.info("WebSocket connect initiated")

        except Exception as e:
            logger.critical(f"WebSocket connect failed: {e}")
            self._connected = False

    async def disconnect(self):
        """Graceful shutdown"""
        try:
            if self.streamer:
                self.streamer.disconnect()
        except Exception as e:
            logger.warning(f"WebSocket disconnect error: {e}")
        finally:
            self._connected = False

    # ======================================================
    # SUBSCRIPTIONS
    # ======================================================
    def update_subscriptions(self, keys: List[str]):
        """
        Safe to call multiple times.
        Subscriptions are restored after reconnects.
        """
        if not keys:
            return

        with self._lock:
            self._subscriptions = list(set(keys))

        if self.streamer and self._connected:
            try:
                self.streamer.subscribe(self._subscriptions, "option_greeks")
                logger.info(f"Subscribed to {len(self._subscriptions)} instruments")
            except Exception as e:
                logger.error(f"Subscription failed: {e}")

    # ======================================================
    # DATA ACCESS (FAIL-CLOSED)
    # ======================================================
    def get_latest_greeks(self) -> Dict:
        """
        Returns ONLY FRESH greeks.
        Stale data is automatically dropped.
        """
        now = datetime.utcnow()

        with self._lock:
            fresh = {}
            for k, v in self._cache.items():
                ts = v.get("timestamp")
                if not ts:
                    continue

                age = (now - ts).total_seconds()
                if age <= self.STALE_AFTER_SECONDS:
                    fresh[k] = v

            return fresh

    def is_healthy(self) -> bool:
        """
        Health check for supervisor / monitoring
        """
        if not self._connected:
            return False
        if time.time() - self._last_message_ts > self.STALE_AFTER_SECONDS:
            return False
        return True

    # ======================================================
    # EVENT HANDLERS
    # ======================================================
    def _on_open(self):
        logger.info("WebSocket connected")
        self._connected = True

        # Restore subscriptions after reconnect
        if self._subscriptions:
            try:
                self.streamer.subscribe(self._subscriptions, "option_greeks")
                logger.info("Subscriptions restored after reconnect")
            except Exception as e:
                logger.error(f"Resubscribe failed: {e}")

    def _on_close(self, code=None, reason=None):
        logger.warning(f"WebSocket closed ({code}): {reason}")
        self._connected = False

    def _on_error(self, error):
        logger.error(f"WebSocket error: {error}")
        self._connected = False

    def _on_message(self, msg):
        try:
            feeds = msg.get("feeds", {})
            now = datetime.utcnow()

            with self._lock:
                for k, v in feeds.items():
                    og = v.get("og", {})
                    if not og:
                        continue

                    self._cache[k] = {
                        "delta": og.get("delta", 0.0),
                        "gamma": og.get("gamma", 0.0),
                        "iv": og.get("iv", 0.0),
                        "timestamp": now,
                    }

                self._last_message_ts = time.time()

        except Exception as e:
            logger.exception(f"WebSocket message parse error: {e}")
