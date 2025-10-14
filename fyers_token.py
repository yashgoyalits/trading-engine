import requests
from datetime import date
import json
import os
from dotenv import load_dotenv
from utils.logger import logger

load_dotenv()

PIN = os.getenv("PIN")  

# --- Endpoint ---
URL = "https://api-t1.fyers.in/api/v3/validate-refresh-token"

# --- Payload template ---
def build_payload():
    return {
        "grant_type": "refresh_token",
        "appIdHash": os.getenv("APP_ID_HASH"),
        "refresh_token": os.getenv("FYERS_REFRESH_TOKEN"),
        "pin": PIN
    }

# --- 📡 Headers ---
HEADERS = {
    "Content-Type": "application/json"
}

def update_env_file(key: str, value: str, env_path: str = ".env"):
    
    lines = []

    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            lines = f.readlines()

    lines = [line for line in lines if not line.strip().startswith(f"{key}=")]

    lines.append(f"{key}={value}\n")

    with open(env_path, "w") as f:
        f.writelines(lines)

def generate_access_token():

    logger.info("Requesting new access token...")

    payload = build_payload()

    try:
        response = requests.post(URL, headers=HEADERS, data=json.dumps(payload))
    except Exception as e:
        logger.info(f"Request failed: {e}")
        return None

    if response.status_code != 200:
        logger.info(f"HTTP Error {response.status_code}: {response.text}")
        return None

    try:
        data = response.json()
    except Exception as e:
        logger.info(f"Failed to parse JSON: {e}")
        return None

    if data.get("s") != "ok":
        logger.info(f"API Error: {data}")
        return None

    access_token = data.get("access_token")
    if not access_token:
        logger.info("No access token in response.")
        return None

    # Save to .env
    update_env_file("FYERS_ACCESS_TOKEN", access_token)

    update_env_file("FYERS_ACCESS_TOKEN_DATE", str(date.today()))

    logger.info("Access token updated in .env file.")

    return access_token

generate_access_token()
