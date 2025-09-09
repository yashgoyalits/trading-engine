import requests
import json
import os
# from dotenv import load_dotenv

# load_dotenv()

# --- 🔐 Credentials (Replace these with environment variables if needed) ---
APP_ID_HASH = "ac10a60166f494b5c09cbaba5f07fa34f3ff65ef26aa0583f99078b4d186a6c5"
REFRESH_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdWQiOlsiZDoxIiwiZDoyIiwieDowIiwieDoxIiwieDoyIl0sImF0X2hhc2giOiJnQUFBQUFCb3Y1LTR0MEhRczJDWXB3ZTRNbjhudHFjVENTRHE0OC1SY1pTSllqSUhqU21LT2J1YmNDa1o4Vi1tNUt1UnZ6bjVCbmlCYktITlZZOGdETlRnLUlHTEY1V0RSMWJ2MlJKaFB5ZTJyUkt3UnBiNVJERT0iLCJkaXNwbGF5X25hbWUiOiIiLCJvbXMiOiJLMSIsImhzbV9rZXkiOiI1NDQ5YzNkMDA2MjVjYTUwM2JhMWRhNGIyNTZiNzU3ZmQzNjhiYmY4Y2VlNzA5MjFmOTY2NDM5NyIsImlzRGRwaUVuYWJsZWQiOiJZIiwiaXNNdGZFbmFibGVkIjoiTiIsImZ5X2lkIjoiRkFCMDYzNzgiLCJhcHBUeXBlIjoxMDAsImV4cCI6MTc1ODY3MzgwMCwiaWF0IjoxNzU3Mzg4NzI4LCJpc3MiOiJhcGkuZnllcnMuaW4iLCJuYmYiOjE3NTczODg3MjgsInN1YiI6InJlZnJlc2hfdG9rZW4ifQ.3PWYmDrOrcQE-MJT5TNhOcapyamSGayM3ZmhiYBWohQ"
PIN = "1999"  # Consider moving to env var or input()

# --- Endpoint ---
URL = "https://api-t1.fyers.in/api/v3/validate-refresh-token"

# --- 📦 Payload ---
payload = {
    "grant_type": "refresh_token",
    "appIdHash": APP_ID_HASH,
    "refresh_token": REFRESH_TOKEN,
    "pin": PIN
}

# --- 📡 Headers ---
headers = {
    "Content-Type": "application/json"
}

def update_env_file(key: str, value: str, env_path: str = ".env"):
    """Update or create a .env variable safely."""
    lines = []

    # Read existing .env file (if it exists)
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            lines = f.readlines()

    # Remove any existing line for this key
    lines = [line for line in lines if not line.strip().startswith(f"{key}=")]

    # Add new key=value
    lines.append(f"{key}={value}\n")

    # Write back
    with open(env_path, "w") as f:
        f.writelines(lines)

    print(f"✅ Updated {key} in {env_path}")


def main():
    print("🔄 Requesting new access token...")

    try:
        response = requests.post(URL, headers=headers, data=json.dumps(payload))
    except Exception as e:
        print(f"❌ Request failed: {e}")
        return

    if response.status_code != 200:
        print(f"❌ HTTP Error {response.status_code}: {response.text}")
        return

    try:
        data = response.json()
    except Exception as e:
        print(f"❌ Failed to parse JSON: {e}")
        return

    if data.get("s") != "ok":
        print(f"⚠️ API Error: {data}")
        return

    access_token = data.get("access_token")
    if not access_token:
        print("⚠️ No access token in response.")
        return

    print(f"✅ New Access Token: {access_token[:10]}...")

    # Save to .env
    update_env_file("FYERS_ACCESS_TOKEN", access_token)

    print("🔐 Access token updated in .env file.")


if __name__ == "__main__":
    main()
