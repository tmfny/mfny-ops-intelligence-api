import threading
cache_lock = threading.Lock()
from fastapi import APIRouter
from api.canix_client import get_runs, get_batches, get_packages
from api.constants import ACTIVE_STATUSES, APPROVAL_STATUSES, COMPLETED_STATUSES
import time
import concurrent.futures
from datetime import datetime, timezone
from api.ops_model import (
    build_production_model,
    build_production_dag,
    find_stalled_batches,
    find_bottlenecks,
    calculate_yields,
    calculate_throughput,
    estimate_batch_eta,
    material_flow,
    get_next_actions
)

RUNS_BATCHES_CACHE = {
    "runs": None,
    "batches": None,
    "batch_map": None,
    "last_refresh": 0
}

PACKAGES_CACHE = {
    "packages": None,
    "last_refresh": 0
}

RUNS_BATCHES_TTL = 3000  # 1 minute
PACKAGES_TTL = 300  # 5 minutes

router = APIRouter()

def log_timing(name, start):
    print(f"{name} took {round(time.time() - start, 2)}s")

def load_runs_batches():

    now = time.time()

    # ✅ CACHE HIT
    if (
        RUNS_BATCHES_CACHE["runs"] is not None and
        RUNS_BATCHES_CACHE["batches"] is not None and
        now - RUNS_BATCHES_CACHE["last_refresh"] <= RUNS_BATCHES_TTL
    ):
        print("Using cached runs/batches")
        return (
            RUNS_BATCHES_CACHE["runs"],
            RUNS_BATCHES_CACHE["batches"],
            RUNS_BATCHES_CACHE["batch_map"]
        )
    
    if (
        RUNS_BATCHES_CACHE["runs"] is not None and
        RUNS_BATCHES_CACHE["batches"] is not None
    ):
        print("serving stale cache for runs/batches")
        return (
            RUNS_BATCHES_CACHE["runs"],
            RUNS_BATCHES_CACHE["batches"],
            RUNS_BATCHES_CACHE["batch_map"]
        )

    # ✅ CACHE MISS → fetch
    print("Fetching fresh runs/batches...")

    with concurrent.futures.ThreadPoolExecutor() as executor:
        runs_future = executor.submit(get_runs)
        batches_future = executor.submit(get_batches)

        try:
            runs = runs_future.result()
        except Exception as e:
            print("Failed to fetch runs:", repr(e))
            from api.canix_client import CACHE
            if "/manu_batch_runs" in CACHE:
                runs, _ = CACHE["/manu_batch_runs"]
                print(f"Recovered {len(runs)} runs from canix cache")
            else:
                runs = []

        try:
            batches = batches_future.result(timeout=60)
        except Exception as e:
            print("Failed to fetch batches:", repr(e))
            batches = []

    if isinstance(runs, list):
        runs = runs[:1000]
    else:
        print("Ruins fetch returned unexpected type:", type(runs), runs)
        runs = []

    batch_map = {b.get("id"): b for b in batches if b.get("id")}

    with cache_lock:
        RUNS_BATCHES_CACHE["runs"] = runs
        RUNS_BATCHES_CACHE["batches"] = batches
        RUNS_BATCHES_CACHE["batch_map"] = batch_map
        RUNS_BATCHES_CACHE["last_refresh"] = now

        return (
            RUNS_BATCHES_CACHE["runs"],
            RUNS_BATCHES_CACHE["batches"],
            RUNS_BATCHES_CACHE["batch_map"]
        )


def load_packages():

    now = time.time()

    if (
        PACKAGES_CACHE["packages"] is not None and
        now - PACKAGES_CACHE["last_refresh"] <= PACKAGES_TTL
    ):
        print("Using cached packages")
        with cache_lock:
            return PACKAGES_CACHE["packages"]
        
    print("Fetching fresh packages...")

    try:
        packages = get_packages()
    except Exception as e:
        print("Failed to fetch packages:", e)
        packages = []

    with cache_lock:
        PACKAGES_CACHE["packages"] = packages
        PACKAGES_CACHE["last_refresh"] = now

        return PACKAGES_CACHE["packages"]


def load_ops_data_full():
    runs, batches, batch_map = load_runs_batches()
    packages = load_packages()
    return runs, batches, batch_map, packages

def build_material_pressure(runs, batch_map):

    pressures = []

    for run in runs:

        if not isinstance(run, dict):
            continue

        status = run.get("status")

        if status not in ACTIVE_STATUSES and status not in APPROVAL_STATUSES:
            continue

        cannabis_inputs = run.get("cannabis_inputs", [])
        cannabis_outputs = run.get("cannabis_outputs", [])

        batch = batch_map.get(run.get("manufacturing_batch_id"), {})

        input_qty = sum(i.get("quantity", 0) for i in cannabis_inputs)
        output_qty = sum(o.get("quantity", 0) for o in cannabis_outputs)

        if input_qty == 0:
            pressure = "NO_INPUTS"
        elif input_qty > 0 and output_qty == 0 and status == "OPEN":
            pressure = "READY_NOT_STARTED"
        elif input_qty > 0 and output_qty == 0:
            pressure = "IN_PROGRESS"
        elif input_qty > 0 and output_qty > 0 and status != "SUBMITTED":
            pressure = "AWAITING_COMPLETION"
        else:
            continue

        pressures.append({
            "batch_name": batch.get("name"),
            "run_name": run.get("name"),
            "status": status,
            "input_quantity": round(input_qty, 2),
            "output_quantity": round(output_qty, 2),
            "pressure": pressure
        })

    return pressures

@router.get("/ops/active_runs")
def active_runs():

    start = time.time()

    runs, batches, batch_map = load_runs_batches()

    active = []

    for run in runs:

        if run.get("status") in ACTIVE_STATUSES:

            batch = batch_map.get(run.get("manufacturing_batch_id"), {})

            active.append({
                "batch_name": batch.get("name"),
                "run_name": run.get("name"),
                "status": run.get("status"),
                "run_id": run.get("id"),
                "run_order": run.get("order"),
                "created_at": run.get("created_at")
            })

    log_timing("/ops/active_runs", start)
    return active

@router.get("/ops/approval_queue")
def approval_queue():

    start = time.time()

    runs, batches, batch_map = load_runs_batches()

    queue = []

    for run in runs:

        if not isinstance(run, dict):
            continue

        status = run.get("status")

        if status in APPROVAL_STATUSES:

            batch = batch_map.get(run.get("manufacturing_batch_id"), {})

            queue.append({
                "batch_name": batch.get("name"),
                "run_name": run.get("name"),
                "status": status,
                "created_at": run.get("created_at")
            })

    log_timing("/ops/approval_queue", start)
    return queue

@router.get("/ops/production_graph")
def production_graph():

    runs, batches, batch_map = load_runs_batches()

    batches_grouped = {}

    for run in runs:

        if not isinstance(run, dict):
            continue

        batch_id = run.get("manufacturing_batch_id")
        batch = batch_map.get(batch_id, {})

        batch_name = batch.get("name")

        if batch_id not in batches_grouped:

            batches_grouped[batch_id] = {
                "batch_id": batch_id,
                "batch_name": batch_name,
                "runs": []
            }

        batches_grouped[batch_id]["runs"].append({

            "run_id": run.get("id"),
            "run_order": run.get("order"),
            "run_name": run.get("name"),
            "status": run.get("status"),
            "created_at": run.get("created_at"),

            "inputs": run.get("cannabis_inputs", []),
            "outputs": run.get("cannabis_outputs", [])

        })

    # sort runs inside each batch
    for batch in batches_grouped.values():

        batch["runs"] = sorted(
            batch["runs"],
            key=lambda r: r.get("run_order") or 0
        )

    return list(batches_grouped.values())

@router.get("/ops/batch_progress")
def batch_progress():

    runs, batches, batch_map = load_runs_batches()

    batches_grouped = {}

    for run in runs:

        if not isinstance(run, dict):
            continue

        status = run.get("status")

        if(
            status not in ACTIVE_STATUSES
            and status not in APPROVAL_STATUSES
            and status not in COMPLETED_STATUSES
        ):
            continue

        batch_id = run.get("manufacturing_batch_id")

        if not batch_id:
            continue

        if batch_id not in batches_grouped:
            batches_grouped[batch_id] = []

        batches_grouped[batch_id].append(run)

    progress = []

    for batch_id, batch_runs in batches_grouped.items():

        completed = 0

        batch_runs_sorted = sorted(batch_runs, key=lambda r: r.get("order", 0))

        batch_name = batch_map.get(batch_id, {}).get("name")

        run_steps = []

        for r in batch_runs_sorted:

            if r.get("status") in COMPLETED_STATUSES:
                completed += 1

            run_steps.append({
                "run_id": r.get("id"),
                "run_order": r.get("order"),
                "run_name": r.get("name"),
                "status": r.get("status")
            })

        progress.append({
            "batch_id": batch_id,
            "batch_name": batch_name,
            "total_runs": len(batch_runs_sorted),
            "completed_runs": completed,
            "remaining_runs": len(batch_runs_sorted) - completed,
            "steps": run_steps
        })

    return progress

@router.get("/ops/bottlenecks")
def bottlenecks():

    runs, batches, batch_map = load_runs_batches()

    model = build_production_model(runs, batches)

    return find_bottlenecks(model)

@router.get("/ops/blockers")
def blockers():

    runs, batches, batch_map = load_runs_batches()

    blocked = []

    for run in runs:

        if not isinstance(run, dict):
            continue

        status = run.get("status")

        if status not in ACTIVE_STATUSES and status not in APPROVAL_STATUSES:
            continue

        name = (run.get("name") or "").lower()

        # ignore steps that normally have no inputs
        if "pack" in name or "label" in name or "clean" in name:
            continue

        cannabis_inputs = run.get("cannabis_inputs", [])
        non_cannabis_inputs = run.get("non_cannabis_inputs", [])

        if len(cannabis_inputs) == 0 and len(non_cannabis_inputs) == 0:

            batch_id = run.get("manufacturing_batch_id")

            if not batch_id:
                continue

            batch = batch_map.get(batch_id, {})

            blocked.append({
                "batch_name": batch.get("name"),
                "run_name": run.get("name"),
                "status": status
            })

    return blocked

PACKAGING_KEYWORDS = ["pack", "box", "label", "tube"]

@router.get("/ops/packaging_queue")
def packaging_queue():

    runs, batches, batch_map = load_runs_batches()

    queue = []

    for run in runs:

        if not isinstance(run, dict):
            continue

        status = run.get("status")

        if status not in ACTIVE_STATUSES and status not in APPROVAL_STATUSES:
            continue

        name = (run.get("name") or "").lower()

        if not any(k in name for k in PACKAGING_KEYWORDS):
            continue

        batch_id = run.get("manufacturing_batch_id")

        if not batch_id:
            continue

        batch = batch_map.get(batch_id, {})

        queue.append({
            "batch_name": batch.get("name"),
            "run_name": run.get("name"),
            "status": status,
            "created_at": run.get("created_at")
        })

    return queue

@router.get("/ops/run_timeline")
def run_timeline():

    runs, batches, batch_map = load_runs_batches()

    timeline = []

    for run in runs:

        if not isinstance(run, dict):
            continue

        status = run.get("status")

        if(
            status not in ACTIVE_STATUSES
            and status not in APPROVAL_STATUSES
            and status not in COMPLETED_STATUSES
        ):
            continue

        batch = batch_map.get(run.get("manufacturing_batch_id"), {})

        timeline.append({
            "run_id": run.get("id"),
            "batch_name": batch.get("name"),
            "run_name": run.get("name"),
            "status": status,
            "created_at": run.get("created_at"),
            "run_order": run.get("order")
        })

    timeline_sorted = sorted(
        timeline,
        key=lambda r: r.get("created_at") or ""
    )

    return timeline_sorted

@router.get("/ops/yield_analysis")
def yield_analysis():

    runs, batches, batch_map = load_runs_batches()

    return calculate_yields(runs)

@router.get("/ops/inventory_risk")
def inventory_risk():

    runs, batches, batch_map = load_runs_batches()

    risks = []

    now = datetime.now(timezone.utc)

    for run in runs:

        if not isinstance(run, dict):
            continue

        status = run.get("status")

        if status not in ACTIVE_STATUSES:
            continue

        name = (run.get("name") or "").lower()

        # ignore packaging steps
        if "pack" in name or "label" in name or "clean" in name:
            continue

        cannabis_inputs = run.get("cannabis_inputs", [])
        non_cannabis_inputs = run.get("non_cannabis_inputs", [])

        batch = batch_map.get(run.get("manufacturing_batch_id"), {})

        # risk 1 — missing inputs
        if len(cannabis_inputs) == 0 and len(non_cannabis_inputs) == 0:

            risks.append({
                "batch_name": batch.get("name"),
                "run_name": run.get("name"),
                "status": status,
                "risk": "No inputs attached"
            })

        # risk 2 — run waiting too long
        created_at = run.get("created_at")

        if created_at:

            try:
                created_dt = datetime.fromisoformat(created_at.replace("Z","+00:00"))
                age_hours = (now - created_dt).total_seconds() / 3600

                if age_hours > 48 and status == "OPEN":

                    risks.append({
                        "batch_name": batch.get("name"),
                        "run_name": run.get("name"),
                        "status": status,
                        "risk": "Run waiting >48 hours"
                    })

            except:
                pass

    return risks

@router.get("/ops/inventory_health")
def inventory_health():

    start = time.time()

    runs, batches, batch_map, packages = load_ops_data_full()

    total_packages = 0
    total_quantity = 0

    available_quantity = 0
    reserved_quantity = 0

    inactive_packages = 0

    for p in packages:

        if not isinstance(p, dict):
            continue

        total_packages += 1

        weight = p.get("weight") or 0
        status = p.get("status")
        is_active = p.get("is_active", True)

        total_quantity += weight

        if status == "Active":
            available_quantity += weight

        elif status == "Allocated":
            reserved_quantity += weight

        if not is_active:
            inactive_packages += 1

    log_timing("/ops/inventory_health", start)
    return {
        "packages": total_packages,
        "total_quantity": round(total_quantity, 2),
        "available_quantity": round(available_quantity, 2),
        "reserved_quantity": round(reserved_quantity, 2),
        "inactive_packages": inactive_packages
    }

@router.get("/ops/production_dag")
def production_dag():

    runs, batches, batch_map = load_runs_batches()

    model = build_production_model(runs, batches)

    dag = build_production_dag(model)

    return dag

@router.get("/ops/stalled_batches")
def stalled_batches():

    runs, batches, batch_map = load_runs_batches()

    model = build_production_model(runs, batches)

    return find_stalled_batches(model)

@router.get("/ops/next_actions")
def next_actions():

    runs, batches, batch_map = load_runs_batches()

    return get_next_actions(runs, batch_map)

@router.get("/ops/throughput")
def throughput():

    runs, batches, batch_map = load_runs_batches()

    model = build_production_model(runs, batches)

    return calculate_throughput(model)

@router.get("/ops/batch_eta")
def batch_eta():

    start = time.time()

    runs, batches, batch_map = load_runs_batches()

    model = build_production_model(runs, batches)

    log_timing("/ops/batch_eta", start)
    return estimate_batch_eta(model)

@router.get("/ops/material_flow")
def material_flow_endpoint():

    runs, batches, batch_map = load_runs_batches()

    return material_flow(runs)

@router.get("/ops/material_pressure")
def material_pressure():

    runs, batches, batch_map = load_runs_batches()

    return build_material_pressure(runs, batch_map)

from collections import defaultdict
from datetime import datetime, timezone, timedelta

@router.get("/ops/production_velocity")
def production_velocity():

    runs, batches, batch_map = load_runs_batches()

    now = datetime.now(timezone.utc)
    last_week = now - timedelta(days=7)

    completed_runs = 0
    cycle_times = []
    step_counts = defaultdict(int)

    for run in runs:

        if not isinstance(run, dict):
            continue

        status = run.get("status")

        if status not in COMPLETED_STATUSES:
            continue

        start_date = run.get("start_date")
        end_date = run.get("end_date")

        if not start_date or not end_date:
            continue

        try:
            start_dt = datetime.fromisoformat(start_date)
            end_dt = datetime.fromisoformat(end_date)
        except:
            continue

        # only include runs completed in last 7 days
        if end_dt.replace(tzinfo=timezone.utc) < last_week:
            continue

        completed_runs += 1

        cycle_hours = (end_dt - start_dt).total_seconds() / 3600
        cycle_times.append(cycle_hours)

        step = run.get("name")
        step_counts[step] += 1

    avg_cycle = sum(cycle_times) / len(cycle_times) if cycle_times else 0

    steps = [
        {
            "step": step,
            "completed_runs": count
        }
        for step, count in step_counts.items()
    ]

    steps_sorted = sorted(steps, key=lambda x: x["completed_runs"], reverse=True)

    return {
        "runs_completed_last_7_days": completed_runs,
        "average_run_cycle_hours": round(avg_cycle, 2),
        "step_velocity": steps_sorted
    }

@router.get("/ops/system_context")
def system_context():

    start = time.time()

    runs, batches, batch_map = load_runs_batches()

    model = build_production_model(runs, batches)

    bottlenecks = find_bottlenecks(model)
    stalled = find_stalled_batches(model)
    blockers_list = []  # optional: reuse blockers() if needed

    pressure = build_material_pressure(runs, batch_map)
    actions = get_next_actions(runs, batch_map)

    from collections import Counter

    pressure_counts = Counter(p["pressure"] for p in pressure)

    # summary counts
    active = sum(1 for r in runs if r.get("status") in ACTIVE_STATUSES)
    approval = sum(1 for r in runs if r.get("status") in APPROVAL_STATUSES)
    completed = sum(1 for r in runs if r.get("status") in COMPLETED_STATUSES)

    summary = {
        "total_runs": len(runs),
        "active_runs": active,
        "approval_queue": approval,
        "completed_runs": completed,
        "stalled_batches": len(stalled)
    }

    system_state = {
        "primary_bottleneck": bottlenecks[0]["step"] if bottlenecks else None,
        "pressure_level": "HIGH" if len(pressure) > 20 else "NORMAL",
        "system_health": "DEGRADED" if len(stalled) > 10 else "HEALTHY"
    }

    alerts = []

    if len(stalled) > 10:
        alerts.append("High number of stalled batches")

    if pressure_counts.get("NO_INPUTS", 0) > 5:
        alerts.append("Multiple runs missing material")

    if bottlenecks and bottlenecks[0]["runs_waiting"] > 10:
        alerts.append(f"Bottleneck at {bottlenecks[0]['step']}")


    log_timing("/ops/system_context", start)
    return {
        "summary": summary,
        "system_state": system_state,
        "bottlenecks": bottlenecks[:5],
        "material_pressure": dict(pressure_counts),
        "stalled_batches": stalled[:10],
        "next_actions": actions[:10],
        "alerts": alerts
    }

@router.get("/ops/overview")
def overview():

    start = time.time()

    runs, batches, batch_map = load_runs_batches()

    if not runs:
        return {
            "total_runs": 0,
            "active_runs": 0,
            "approval_queue": 0,
            "completed_runs": 0,
            "total_batches": len(batches) if batches else 0
        }

    active = sum(1 for r in runs if r.get("status") in ACTIVE_STATUSES)
    approval = sum(1 for r in runs if r.get("status") in APPROVAL_STATUSES)
    completed = sum(1 for r in runs if r.get("status") in COMPLETED_STATUSES)

    result = {
        "total_runs": len(runs),
        "active_runs": active,
        "approval_queue": approval,
        "completed_runs": completed,
        "total_batches": len(batches)
    }

    log_timing("/ops/overview", start)
    return result

import threading

def warm_cache():
    try:
        print("Warming cache...")
        load_runs_batches()
        load_packages()
        print("Cache warmed")
    except Exception as e:
        print("Cache warm failed:", e)

threading.Thread(target=warm_cache, daemon=True).start()

def background_refresh():
    while True:
        time.sleep(240)
        try:
            load_runs_batches()
            load_packages()
            print("Backgrouund cache refreshed")
        except Exception as e:
            print("Background refresh failed:", e)

threading.Thread(target=background_refresh, daemon=True).start()

@router.get("/health")
def health():
    return {
        "status": "ok",
        "runs_cached": RUNS_BATCHES_CACHE["runs"] is not None,
        "packages_cached": PACKAGES_CACHE["packages"] is not None
    }

# ADD this function right after the background_refresh function

def keep_alive_ping():
    while True:
        time.sleep(270)  # every 4.5 minutes
        try:
            requests.get("https://web-production-a1b2b.up.railway.app/health", timeout=10)
            print("Keep-alive ping sent")
        except Exception as e:
            print("Keep-alive failed:", e)

threading.Thread(target=keep_alive_ping, daemon=True).start()