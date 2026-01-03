import requests
import sys
import os
from dotenv import load_dotenv

# Load Env to get the Token
load_dotenv()

API_URL = "http://localhost:8000/api/v1/admin/emergency_stop"
# Using Upstox Token as Admin Key based on the API implementation above
ADMIN_KEY = os.getenv("UPSTOX_ACCESS_TOKEN") 

def main():
    print("!!! DANGER: YOU ARE ABOUT TO TRIGGER THE GLOBAL KILL SWITCH !!!")
    print("This will:")
    print("  1. Stop all trading loops.")
    print("  2. Attempt to close all open positions (Liquidation).")
    print("  3. Lock the system in EMERGENCY mode.")
    
    confirm = input("Type 'KILL' to confirm: ")
    if confirm != "KILL":
        print("Aborted.")
        return

    reason = input("Enter reason for logs: ") or "Manual Intervention"

    try:
        headers = {"X-Admin-Key": ADMIN_KEY, "Content-Type": "application/json"}
        payload = {"reason": reason, "action": "GLOBAL_KILL_SWITCH"}
        
        print(f"Sending signal to {API_URL}...")
        resp = requests.post(API_URL, json=payload, headers=headers, timeout=5)
        
        if resp.status_code == 200:
            print("\n✅ SUCCESS: KILL SIGNAL RECEIVED.")
            print("Response:", resp.json())
            print("CHECK YOUR LOGS IMMEDIATELY: tail -f logs/volguard.log")
        else:
            print(f"\n❌ FAILED: API Error {resp.status_code}")
            print(resp.text)
            
    except Exception as e:
        print(f"\n❌ FAILED: Could not connect to API. Is it running?")
        print(f"Error: {e}")
        print("\nFALLBACK: Manually creating 'KILL_SWITCH.TRIGGER' file...")
        with open("KILL_SWITCH.TRIGGER", "w") as f:
            f.write(f"GLOBAL_KILL_SWITCH|{reason}")
        print("✅ Fallback file created. Supervisor should detect this on next cycle.")

if __name__ == "__main__":
    main()
