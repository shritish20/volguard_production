# scripts/token_manager.py

import os
import requests
import sys
import time
import tempfile
from dotenv import load_dotenv

# ==========================================================
# ENV SETUP
# ==========================================================
load_dotenv()

CLIENT_ID = os.getenv("UPSTOX_CLIENT_ID")
CLIENT_SECRET = os.getenv("UPSTOX_CLIENT_SECRET")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_PATH = os.path.join(BASE_DIR, ".env")

TOKEN_KEY = "UPSTOX_ACCESS_TOKEN"


# ==========================================================
# UTILITIES
# ==========================================================
def _atomic_write(path: str, content: str):
    """Write file atomically to prevent corruption"""
    dir_name = os.path.dirname(path)
    with tempfile.NamedTemporaryFile("w", dir=dir_name, delete=False) as tf:
        tf.write(content)
        temp_name = tf.name
    os.replace(temp_name, path)


def update_env_file(key: str, new_value: str):
    """Safely update or append key=value in .env"""
    try:
        with open(ENV_PATH, "r") as f:
            lines = f.readlines()
    except FileNotFoundError:
        lines = []

    updated = False
    out = []

    for line in lines:
        if line.strip().startswith(f"{key}="):
            out.append(f"{key}={new_value}\n")
            updated = True
        else:
            out.append(line)

    if not updated:
        out.append(f"\n{key}={new_value}\n")

    _atomic_write(ENV_PATH, "".join(out))
    print(f"✅ Updated {key} in {ENV_PATH}")


def _validate_token(token: str) -> bool:
    """Basic sanity check"""
    return isinstance(token, str) and len(token) > 20


# ==========================================================
# TOKEN FETCH LOGIC
# ==========================================================
def fetch_access_token() -> bool:
    print("🔄 Attempting Upstox Auto-Login (v3)")

    if not CLIENT_ID or not CLIENT_SECRET:
        print("❌ Missing UPSTOX_CLIENT_ID or UPSTOX_CLIENT_SECRET")
        return False

    url = f"https://api.upstox.com/v3/login/auth/token/request/{CLIENT_ID}"
    headers = {
        "accept": "application/json",
        "Content-Type": "application/json",
    }
    payload = {"client_secret": CLIENT_SECRET}

    max_retries = 3

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=10)

            if resp.status_code == 200:
                data = resp.json()
                token = data.get("access_token") or data.get("data", {}).get("access_token")

                if _validate_token(token):
                    print(f"🎉 Token fetched successfully: {token[:10]}...******")
                    update_env_file(TOKEN_KEY, token)
                    return True

                print("❌ Invalid token received from API")

            else:
                print(f"⚠️ Attempt {attempt}: {resp.status_code} - {resp.text}")

        except Exception as e:
            print(f"❌ Attempt {attempt} failed: {e}")

        if attempt < max_retries:
            sleep = 2 ** attempt
            print(f"⏳ Retrying in {sleep}s...")
            time.sleep(sleep)

    print("❌ CRITICAL: Token refresh failed after retries")
    return False


# ==========================================================
# ENTRYPOINT
# ==========================================================
if __name__ == "__main__":
    ok = fetch_access_token()
    if not ok:
        sys.exit(1)
