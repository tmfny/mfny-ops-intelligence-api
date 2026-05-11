import requests
import os
from dotenv import load_dotenv

load_dotenv()

CANIX_BASE = "https://api.canix.com/api/v1"

API_KEY = os.getenv("CANIX_API_KEY")

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

    MAX_RECORDS = 2000

    while True:

        params = {
            "limit": limit,
            "offset": offset
        }

        url = f"{CANIX_BASE}{endpoint}"

        for attempt in range(3):
            r = requests.get(url, headers=headers, params=params, timeout=60)

            if r.status_code == 200:
                break

            print(f"❌ {endpoint} attempt {attempt+1} failed:", r.status_code)

            time.sleep(2)

        else:
            print("🚨 All retries failed — using cached fallback if available")
            return CACHE.get(endpoint, ([], 0))[0]
        
        try:
            data = r.json()
        except Exception as e:
            print(f"❌ JSON parse failed for {endpoint}:", e)
            return CACHE.get(endpoint, ([], 0))[0]

        if not data:
            break

        all_data.extend(data)

        # HARD LIMIT
        if len(all_data) >= MAX_RECORDS:
            print(f"Reached MAX_RECORDS ({MAX_RECORDS}), stopping early")
            all_data = all_data[:MAX_RECORDS]
            break

        offset += limit

    # store in cache
    if not all_data:
        print("⚠️ No data fetched — keeping previous cache")
        return CACHE.get(endpoint, ([], 0))[0]

    CACHE[endpoint] = (all_data, now)

    print(f"CACHED {len(all_data)} records for {endpoint}")

    return all_data

def get_packages():
    return fetch_all("/packages")


def get_batches():
    return fetch_all("/manu_batches")

def get_runs():
    print("Fetching runs (single pull)...")

    url = f"{CANIX_BASE}/manu_batch_runs"

    for attempt in range(3):
        try:
            res = requests.get(
                url,
                headers=headers,
                params={"limit": 200},
                timeout=60
            )

            if res.status_code == 200:
                data = res.json()
                print(f"✅ Runs fetched: {len(data)}")
                return data

            print(f"❌ Runs attempt {attempt+1} failed:", res.status_code)

        except Exception as e:
            print(f"❌ Runs exception attempt {attempt+1}:", e)

        time.sleep(3)

    print("🚨 Runs failed after retries — returning empty (warm_cache will protect)")

    return []