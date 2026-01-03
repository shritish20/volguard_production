# scripts/emergency_stop.py

import os
import sys
import json
import socket
import requests
import tempfile
from datetime import datetime
from dotenv import load_dotenv

# ==========================================================
# ENV SETUP
# ==========================================================
load_dotenv()

# DO NOT reuse broker tokens for admin control
ADMIN_KEY = os.getenv("VOLGUARD_ADMIN_KEY")
API_BASE = os.getenv("VOLGUARD_API_URL", "http://localhost:8000")

API_ENDPOINT = f"{API_BASE}/api/v1/admin/emergency_stop"
KILL_FILE = "KILL_SWITCH.TRIGGER"


# ==========================================================
# UTILITIES
# ==========================================================
def atomic_write(path: str, content: str):
    """Atomic file write to prevent corruption"""
    directory = os.path.dirname(os.path.abspath(path)) or "."
    with tempfile.NamedTemporaryFile("w", dir=directory, delete=False) as tf:
        tf.write(content)
        temp_name = tf.name
    os.replace(temp_name, path)


def build_kill_payload(reason: str) -> dict:
    return {
        "action": "GLOBAL_KILL_SWITCH",
        "reason": reason,
        "timestamp": datetime.utcnow().isoformat(),
        "host": socket.gethostname(),
        "pid": os.getpid(),
        "initiator": "MANUAL_SCRIPT",
    }


# ==========================================================
# MAIN LOGIC
# ==========================================================
def main():
    print("\n🚨🚨🚨 VOLGUARD EMERGENCY STOP 🚨🚨🚨\n")
    print("This action will:")
    print("  • Stop all trading loops")
    print("  • Force-close open positions")
    print("  • Lock system in EMERGENCY mode\n")

    if not ADMIN_KEY:
        print("❌ VOLGUARD_ADMIN_KEY not set in environment.")
        sys.exit(1)

    confirm = input("Type 'KILL' to confirm: ").strip()
    if confirm != "KILL":
        print("Aborted.")
        return

    reason = input("Enter reason (required): ").strip()
    if not reason:
        print("❌ Reason is mandatory.")
        return

    payload = build_kill_payload(reason)
    headers = {
        "X-Admin-Key": ADMIN_KEY,
        "Content-Type": "application/json",
    }

    # ======================================================
    # PRIMARY: API SIGNAL
    # ======================================================
    try:
        print(f"\n📡 Sending kill signal to {API_ENDPOINT} ...")
        resp = requests.post(
            API_ENDPOINT,
            headers=headers,
            json=payload,
            timeout=5,
        )

        if resp.status_code == 200:
            data = resp.json()
            if data.get("status") in ("KILLED", "EMERGENCY"):
                print("\n✅ SUCCESS: System acknowledged emergency stop.")
                print(json.dumps(data, indent=2))
                return
            else:
                print("\n⚠️ API responded but did not confirm kill:")
                print(data)

        else:
            print(f"\n❌ API ERROR {resp.status_code}")
            print(resp.text)

    except Exception as e:
        print(f"\n❌ API unreachable: {e}")

    # ======================================================
    # FALLBACK: FILE-BASED KILL SWITCH
    # ======================================================
    print("\n🆘 FALLBACK ACTIVATED: Writing KILL_SWITCH.TRIGGER file")

    try:
        atomic_write(KILL_FILE, json.dumps(payload, indent=2))
        print(f"✅ Kill switch file created at {os.path.abspath(KILL_FILE)}")
        print("Supervisor will halt on next cycle.")
    except Exception as e:
        print(f"❌ FAILED to write kill file: {e}")
        sys.exit(2)


# ==========================================================
if __name__ == "__main__":
    main()
