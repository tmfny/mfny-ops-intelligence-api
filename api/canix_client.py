import requests
import os
from dotenv import load_dotenv

load_dotenv()

CANIX_BASE = "https://api.canix.com/api/v1"

API_KEY = os.getenv("CANIX_API_KEY")

print("CANIX KEY LOADED:", API_KEY)  # ← ADD THIS LINE

headers = {
    "x-api-key": API_KEY,
    "Content-Type": "application/json"
}

import time

CACHE = {}
CACHE_TTL = 300 # seconds (5 minutes)

def fetch_all(endpoint, limit=200):

    now = time.time()

    # check cache first
    if endpoint in CACHE:

        cached_data, timestamp = CACHE[endpoint]

        if now - timestamp < CACHE_TTL:
            print(f"Using cached data for {endpoint}")
            return cached_data

    print(f"Fetching fresh data for {endpoint}")

    all_data = []
    offset = 0

    while True:

        params = {
            "limit": limit,
            "offset": offset
        }

        url = f"{CANIX_BASE}{endpoint}"

        r = requests.get(url, headers=headers, params=params)

        if r.status_code != 200:
            return {
                "error": r.text,
                "status": r.status_code
            }

        data = r.json()

        if not data:
            break

        all_data.extend(data)

        offset += limit

    # store in cache
    CACHE[endpoint] = (all_data, now)

    print(f"CACHED {len(all_data)} records for {endpoint}")

    return all_data

def get_packages():
    return fetch_all("/packages")


def get_batches():
    return fetch_all("/manu_batches")


def get_runs():
    print("🚨 get_runs() CALLED 🚨")

    all_runs = []
    page = 1
    limit = 100

    while True:
        print(f"🚨 Fetching page {page} 🚨")

        url = f"{CANIX_BASE}/manu_batch_runs"

        params = {
            "page": page,
            "limit": limit
        }

        res = requests.get(url, headers=headers, params=params)

        print("STATUS:", res.status_code)
        print("TEXT PREVIEW:", res.text[:500])

        if res.status_code != 200:
            print("[get_runs] ERROR:", res.status_code, res.text)
            break

        json_data = res.json()

        if isinstance(json_data, dict) and "data" in json_data:
            data = json_data["data"]
        elif isinstance(json_data, list):
            data = json_data
        else:
            print("[get_runs] Unexpected format:", json_data)
            break
        
        if not data:
            print("[get_runs] No more data")
            break

        all_runs.extend(data)

        if len(data) < limit:
            break

        page += 1

    print(f"[get_runs] Total runs fetched: {len(all_runs)}")
    return all_runs

