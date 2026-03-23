import requests

API = "http://127.0.0.1:8000"

def get_active_runs():
    return requests.get(f"{API}/ops/active_runs").json()

def get_batch_progress():
    return requests.get(f"{API}/ops/batch_progress").json()

def get_overview():
    return requests.get(f"{API}/ops/overview").json()


if __name__ == "__main__":

    print("Active Runs")
    print(get_active_runs())

    print("\nBatch Progress")
    print(get_batch_progress())

    print("\nOverview")
    print(get_overview())