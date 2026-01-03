import upstox_client
import threading
import logging
from typing import Dict, List
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class UpstoxFeedService:
    """
    Upstox V3 Market Data WebSocket (OPTION GREEKS ONLY)

    Design rules:
    - Greeks are NON-AUTHORITATIVE
    - Safe to fail silently
    - Timestamp guarded
    """

    MAX_STALENESS_SECONDS = 10  # Greeks older than this are ignored

    def __init__(self, access_token: str):
        self.config = upstox_client.Configuration()
        self.config.access_token = access_token

        self.api_client = upstox_client.ApiClient(self.config)
        self.streamer = None

        self._lock = threading.Lock()
        self._greeks_cache: Dict[str, Dict] = {}
        self._subscribed_keys: List[str] = []

        self.is_connected = False

    # ======================================================
    # Connection Lifecycle
    # ======================================================
    async def connect(self):
        try:
            self.streamer = upstox_client.MarketDataStreamerV3(
                self.api_client
            )

            self.streamer.on("open", self._on_open)
            self.streamer.on("message", self._on_message)
            self.streamer.on("error", self._on_error)
            self.streamer.on("reconnecting", self._on_reconnecting)
            self.streamer.on("autoReconnectStopped", self._on_reconnect_stopped)

            # Conservative reconnect
            self.streamer.auto_reconnect(True, 10, 5)
            self.streamer.connect()

            logger.info("Upstox WebSocket connect initiated")

        except Exception as e:
            logger.error(f"WebSocket connect failed: {e}")

    async def disconnect(self):
        try:
            if self.streamer:
                self.streamer.disconnect()
            self.is_connected = False
            logger.info("Upstox WebSocket disconnected")
        except Exception:
            pass

    # ======================================================
    # Subscription Management
    # ======================================================
    def subscribe(self, instrument_keys: List[str]):
        """
        Subscribe to option greeks.
        Safe to call multiple times.
        """
        if not instrument_keys:
            return

        with self._lock:
            self._subscribed_keys = list(set(instrument_keys))

        if self.streamer and self.is_connected:
            try:
                self.streamer.subscribe(
                    self._subscribed_keys,
                    "option_greeks"
                )
                logger.info(f"Subscribed to {len(self._subscribed_keys)} instruments (greeks)")
            except Exception as e:
                logger.error(f"Subscription failed: {e}")

    def unsubscribe_all(self):
        if self.streamer and self._subscribed_keys:
            try:
                self.streamer.unsubscribe(self._subscribed_keys)
            except Exception:
                pass

        with self._lock:
            self._subscribed_keys = []
            self._greeks_cache.clear()

    # ======================================================
    # Public API
    # ======================================================
    def get_latest_greeks(self) -> Dict[str, Dict]:
        """
        Returns ONLY fresh greeks.
        Stale entries are automatically dropped.
        """
        now = datetime.utcnow()
        fresh = {}

        with self._lock:
            for k, v in self._greeks_cache.items():
                ts = v.get("timestamp")
                if not ts:
                    continue

                if (now - ts).total_seconds() <= self.MAX_STALENESS_SECONDS:
                    fresh[k] = v

        return fresh

    # ======================================================
    # WebSocket Event Handlers
    # ======================================================
    def _on_open(self):
        logger.info("Upstox WebSocket connected")
        self.is_connected = True

        # Re-subscribe after reconnect
        if self._subscribed_keys:
            try:
                self.streamer.subscribe(
                    self._subscribed_keys,
                    "option_greeks"
                )
                logger.info("Re-subscribed after reconnect")
            except Exception:
                pass

    def _on_message(self, message):
        try:
            feeds = message.get("feeds", {})
            now = datetime.utcnow()

            with self._lock:
                for instrument_key, payload in feeds.items():
                    og = payload.get("og")
                    if not og:
                        continue

                    self._greeks_cache[instrument_key] = {
                        "delta": og.get("delta", 0.0),
                        "gamma": og.get("gamma", 0.0),
                        "iv": og.get("iv", 0.0),
                        "timestamp": now
                    }

        except Exception:
            # NEVER crash supervisor due to WS noise
            pass

    def _on_error(self, error):
        logger.error(f"WebSocket error: {error}")

    def _on_reconnecting(self, msg):
        logger.warning(f"WebSocket reconnecting: {msg}")
        self.is_connected = False

    def _on_reconnect_stopped(self, msg):
        logger.critical(f"WebSocket auto-reconnect stopped: {msg}")
        self.is_connected = False
