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
    page = 1
    all_runs = []
    seen_ids = set()

    while True:
        print(f"📦 Fetching page {page}")

        url = f"{CANIX_BASE}/manu_batch_runs?page={page}&limit=100"

        res = requests.get(url, headers=headers)

        if res.status_code != 200:
            print("❌ Failed request:", res.status_code, res.text)
            break

        data = res.json()

        if not data or len(data) == 0:
            print("✅ No more data")
            break

        first_id = data[0].get("id")

        if first_id in seen_ids:
            print("🛑 Duplicate page detected — stopping")
            break

        seen_ids.add(first_id)

        all_runs.extend(data)

        page += 1

        if page > 50:
            print("🛑 Page cap reached")
            break

    print(f"✅ Total runs fetched: {len(all_runs)}")

    return all_runs