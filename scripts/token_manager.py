import os
import requests
import sys
import time
from dotenv import load_dotenv

# Load existing environment variables
load_dotenv()

CLIENT_ID = os.getenv("UPSTOX_CLIENT_ID")
CLIENT_SECRET = os.getenv("UPSTOX_CLIENT_SECRET")
ENV_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")


def update_env_file(key, new_value):
    """Reads .env, updates the specific key, and writes it back."""
    try:
        with open(ENV_PATH, "r") as f:
            lines = f.readlines()
    except FileNotFoundError:
        lines = []

    new_lines = []
    key_found = False
    
    for line in lines:
        if line.startswith(f"{key}="):
            new_lines.append(f"{key}={new_value}\n")
            key_found = True
        else:
            new_lines.append(line)
    
    if not key_found:
        new_lines.append(f"\n{key}={new_value}\n")

    with open(ENV_PATH, "w") as f:
        f.writelines(new_lines)
    print(f"✅ Updated {key} in {ENV_PATH}")

def fetch_access_token():
    print("🔄 Attempting Auto-Login via API v3...")
    
    if not CLIENT_ID or not CLIENT_SECRET:
        print("❌ SKIPPING: UPSTOX_CLIENT_ID or UPSTOX_CLIENT_SECRET missing in .env")
        return False

    url = f'https://api.upstox.com/v3/login/auth/token/request/{CLIENT_ID}'
    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json',
    }
    data = {'client_secret': CLIENT_SECRET}

    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.post(url, headers=headers, json=data, timeout=10)
            
            if response.status_code == 200:
                resp_json = response.json()
                token = resp_json.get("access_token") or resp_json.get("data", {}).get("access_token")
                
                if token:
                    print(f"🎉 Success! Token fetched: {token[:10]}...******")
                    update_env_file("UPSTOX_ACCESS_TOKEN", token)
                    return True
            
            print(f"⚠️ Attempt {attempt+1}/{max_retries} Failed: {response.status_code} - {response.text}")
            
        except Exception as e:
            print(f"❌ Attempt {attempt+1}/{max_retries} Error: {e}")
        
        # Exponential backoff: 2s, 4s, 8s
        if attempt < max_retries - 1:
            sleep_time = 2 ** (attempt + 1)
            print(f"   Retrying in {sleep_time} seconds...")
            time.sleep(sleep_time)

    print("❌ CRITICAL: Auto-Login failed after all attempts.")
    return False

if __name__ == "__main__":
    success = fetch_access_token()
    if not success:
        sys.exit(1) # Exit with error code so the main script knows to stop
