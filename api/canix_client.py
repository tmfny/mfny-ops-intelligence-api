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


def get_runs(limit=200):
    return fetch_all("/manu_batch_runs", limit)