from fastmcp import FastMCP
import requests
import subprocess
import time
import os

API = "http://127.0.0.1:8000"

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

ensure_api_running()

mcp = FastMCP("mfny_ops")

@mcp.tool()
def active_runs():
    """Return currently active manufacturing runs."""
    return requests.get(f"{API}/ops/active_runs").json()

@mcp.tool()
def batch_progress():
    """Return batch workflow progress."""
    return requests.get(f"{API}/ops/batch_progress").json()

@mcp.tool()
def ops_overview():
    """Return facility overview metrics."""
    return requests.get(f"{API}/ops/overview").json()

if __name__ == "__main__":
    mcp.run()

@mcp.tool()
def production_dag():
    """Return production workflow graph for all batches."""
    return requests.get(f"{API}/ops/production_dag").json()

@mcp.tool()
def next_actions():
    """Return prioritized recommended next actions for operations."""
    return requests.get(f"{API}/ops/next_actions").json()

@mcp.tool()
def inventory_risk():
    """Return runs that may stall due to missing inventory."""
    return requests.get(f"{API}/ops/inventory_risk").json()