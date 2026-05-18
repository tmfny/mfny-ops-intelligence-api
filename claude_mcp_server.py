from fastmcp import FastMCP
import requests
import subprocess
import time
import os

API = "https://transmitted-qty-selecting-colin.trycloudflare.com"
PROJECT_PATH = "/Users/tmiles/Desktop/mfny_ops_platform"

def ensure_api_running():
    try:
        requests.get(API + "/docs", timeout=2)
    except:
        subprocess.Popen(
            [f"{PROJECT_PATH}/venv/bin/uvicorn", "api.main:app", "--reload"],
            cwd=PROJECT_PATH,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        time.sleep(3)

# ensure_api_running() # disabled - using Cloudflare Tunnel now
mcp = FastMCP("mfny_ops")

@mcp.tool()
def active_runs():
    """Return currently active manufacturing runs."""
    try:
        return requests.get(f"{API}/ops/active_runs", timeout=90).json()
    except Exception as e:
        return {"error": str(e), "status": "timeout_or_unavailable"}

@mcp.tool()
def batch_progress():
    """Return batch workflow progress."""
    try:
        return requests.get(f"{API}/ops/batch_progress", timeout=90).json()
    except Exception as e:
        return {"error": str(e), "status": "timeout_or_unavailable"}

@mcp.tool()
def ops_overview():
    """Return facility overview metrics."""
    try:
        return requests.get(f"{API}/ops/overview", timeout=90).json()
    except Exception as e:
        return {"error": str(e), "status": "timeout_or_unavailable"}

if __name__ == "__main__":
    mcp.run()

@mcp.tool()
def production_dag():
    """Return production workflow graph for all batches."""
    try:
        return requests.get(f"{API}/ops/production_dag", timeout=90).json()
    except Exception as e:
        return {"error": str(e), "status": "timeout_or_unavailable"}

@mcp.tool()
def next_actions():
    """Return prioritized recommended next actions for operations."""
    try:
        return requests.get(f"{API}/ops/next_actions", timeout=90).json()
    except Exception as e:
        return {"error": str(e), "status": "timeout_or_unavailable"}

@mcp.tool()
def inventory_risk():
    """Return runs that may stall due to missing inventory."""
    try:
        return requests.get(f"{API}/ops/inventory_risk", timeout=90).json()
    except Exception as e:
        return {"error": str(e), "status": "timeout_or_unavailable"}