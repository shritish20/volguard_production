# scripts/emergency_stop.py

import os
import sys
import json
import socket
import requests
import tempfile
from datetime import datetime
from dotenv import load_dotenv

# Load Environment
load_dotenv()

# Configuration
API_URL = os.getenv("API_URL", "http://localhost:8000") # Adjust port if needed
ADMIN_SECRET = os.getenv("ADMIN_SECRET")
KILL_FILE = "KILL_SWITCH.TRIGGER"

def atomic_write(path: str, content: str):
    """
    Atomic write ensures the Supervisor doesn't read a half-written file.
    """
    directory = os.path.dirname(os.path.abspath(path)) or "."
    with tempfile.NamedTemporaryFile("w", dir=directory, delete=False) as tf:
        tf.write(content)
        temp_name = tf.name
    os.replace(temp_name, path)

def main():
    print("\n" + "!"*60)
    print("🚨  VOLGUARD EMERGENCY STOP INITIATED  🚨")
    print("!"*60 + "\n")
    
    print("This will:")
    print("1. Send STOP signal to Trading API.")
    print("2. Create filesystem Lock File (Hard Kill).")
    print("3. Force all loops to terminate immediately.\n")

    # 1. Confirmation
    confirm = input("Type 'KILL' to confirm: ").strip()
    if confirm != "KILL":
        print("❌ Aborted.")
        return

    reason = input("Enter Reason (Required): ").strip()
    if not reason:
        print("❌ Reason is required.")
        return

    payload = {
        "action": "GLOBAL_KILL_SWITCH",
        "reason": reason,
        "timestamp": datetime.utcnow().isoformat(),
        "initiator": f"MANUAL_SCRIPT@{socket.gethostname()}"
    }

    # 2. Try Graceful API Kill
    print(f"\n📡 Attempting API Kill ({API_URL})...")
    try:
        headers = {"x-admin-key": ADMIN_SECRET, "Content-Type": "application/json"}
        # Adjust endpoint to match your API router if you created one, 
        # otherwise this log serves as the record.
        # Since we haven't built a specific API endpoint for this in dashboard.py, 
        # we rely heavily on the FILE fallback, but this request helps if you add the route later.
        
        # NOTE: If you haven't added an endpoint for this in dashboard.py, this might 404.
        # That is fine, the fallback is the real key.
        resp = requests.post(f"{API_URL}/api/v1/system/emergency_stop", json=payload, headers=headers, timeout=3)
        if resp.status_code == 200:
            print("✅ API Acknowledged Stop Command.")
        else:
            print(f"⚠️ API did not acknowledge (Status: {resp.status_code}). Proceeding to Hard Kill.")
    except Exception as e:
        print(f"⚠️ API Unreachable ({e}). Proceeding to Hard Kill.")

    # 3. Hard Kill (File System)
    print("\n🔒 Engaging Hard Kill Switch (File System)...")
    try:
        atomic_write(KILL_FILE, json.dumps(payload, indent=2))
        print(f"✅ SUCCESS: Kill file created at {os.path.abspath(KILL_FILE)}")
        print("The Supervisor will detect this within 3 seconds and shut down.")
    except Exception as e:
        print(f"❌ CRITICAL FAILURE: Could not write kill file: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
