# VolGuard Production Deployment Checklist

## 🔴 Pre-Flight (Do NOT Skip)

### 1. Environment & Config
- [ ] **Secrets Check**: Ensure `.env` contains the correct `UPSTOX_ACCESS_TOKEN` and `POSTGRES_PASSWORD`.
- [ ] **Config Audit**: Verify `app/config.py` matches your capital limits (e.g., `MAX_DAILY_LOSS=20000`).
- [ ] **Dependencies**: Run `pip install -r requirements.txt` to ensure `tenacity` and `py_vollib_vectorized` are installed.

### 2. Data Integrity
- [ ] **Instrument Master**: Delete `data/complete.json.gz` (if exists) and let `InstrumentRegistry` download a fresh copy on startup.
- [ ] **Database Migration**: Run `alembic upgrade head` to ensure the `decision_journal` table exists.
- [ ] **Time Sync**: Ensure your server time is synced (`chronyd` or `ntp`). Trading relies on millisecond precision.

### 3. Safety Drills
- [ ] **Kill Switch Test**:
    1. Start the system in `SHADOW` mode.
    2. Manually trigger `emergency_executor.execute_emergency_action({"type": "GLOBAL_KILL_SWITCH"})` (via a script or API).
    3. Verify in logs that it attempts to cancel orders/close positions.
- [ ] **Data Cutoff Test**:
    1. Disconnect your internet or block the Upstox domain in `/etc/hosts`.
    2. Verify `DataQualityGate` triggers `DEGRADED` mode within 15 seconds.

---

## 🟡 Launch Sequence

1. **Start Database & Redis**
   ```bash
   docker-compose up -d postgres redis
