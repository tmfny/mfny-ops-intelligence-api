import streamlit as st
import requests
import pandas as pd

API = "http://127.0.0.1:8000"

def fetch(endpoint):
    try:
        r = requests.get(f"{API}{endpoint}")
        r.raise_for_status()
        return r.json()
    except Exception:
        st.error(f"API error: {endpoint}")
        return []

st.title("MFNY Ops Command Center")

# --------------------------
# Approval Queue
# --------------------------

st.subheader("Runs Awaiting Approval")

approval = fetch("/ops/approval_queue")

if len(approval) == 0:
    st.info("No runs currently awaiting approval in facility 4511.")
else:
    st.dataframe(approval)

# --------------------------
# Active Runs
# --------------------------

st.subheader("Active Runs")

active = fetch("/ops/active_runs")

if len(active) == 0:
    st.info("No active runs currently.")
else:
    st.dataframe(active)

# --------------------------
# Overview Metrics
# --------------------------

overview = fetch("/ops/overview")

if overview:
    st.metric("Packages", overview.get("packages_count", 0))
    st.metric("Batches", overview.get("batches_count", 0))
    st.metric("Runs", overview.get("runs_count", 0))
else:
    st.warning("Overview data unavailable.")

# --------------------------
# Production Graph
# --------------------------

st.subheader("Production Graph")

graph = fetch("/ops/production_graph")

if len(graph) == 0:
    st.info("No production graph data available.")
else:

    rows = []

    for run in graph:
        rows.append({
            "Run ID": run["run_id"],
            "Run Name": run["name"],
            "Status": run["status"],
            "Inputs": len(run["inputs"]),
            "Outputs": len(run["outputs"])
        })

    df = pd.DataFrame(rows)

    st.dataframe(df)

st.subheader("Batch Progress")

batches = fetch("/ops/batch_progress")

if len(batches) == 0:
    st.info("No batch workflow data.")
else:

    for batch in batches:

        st.markdown(f"### {batch['batch_name']}")

        rows = []

        for step in batch["steps"]:
            rows.append({
                "Step": step["run_order"],
                "Run": step["run_name"],
                "Status": step["status"]
            })

        df = pd.DataFrame(rows)

        st.dataframe(df)