from fastapi import APIRouter
from api.canix_client import get_runs, get_batches, get_packages
from api.constants import ACTIVE_STATUSES, APPROVAL_STATUSES, COMPLETED_STATUSES
import time
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

OPS_CACHE = {
    "runs": None,
    "batches": None,
    "packages": None,
    "batch_map": None,
    "last_refresh": 0
}

CACHE_TTL = 60  # seconds

router = APIRouter()

def load_ops_data():

    now = time.time()

    if (
        OPS_CACHE["runs"] is None or
        OPS_CACHE["batches"] is None or
        now - OPS_CACHE["last_refresh"] > CACHE_TTL
    ):

        runs = get_runs()
        batches = get_batches()

        # only refresh packages occasionally
        if OPS_CACHE["packages"] is None or now - OPS_CACHE["last_refresh"] > 300:
            OPS_CACHE["packages"] = get_packages()

        packages = OPS_CACHE["packages"]

        batch_map = {b["id"]: b for b in batches}

        OPS_CACHE["runs"] = runs
        OPS_CACHE["batches"] = batches
        OPS_CACHE["packages"] = packages
        OPS_CACHE["batch_map"] = batch_map
        OPS_CACHE["last_refresh"] = now

    return (
        OPS_CACHE["runs"],
        OPS_CACHE["batches"],
        OPS_CACHE["batch_map"],
        OPS_CACHE["packages"]
    )

@router.get("/ops/active_runs")
def active_runs():

    runs, batches, batch_map, packages = load_ops_data()

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

    return active

@router.get("/ops/approval_queue")
def approval_queue():

    runs, batches, batch_map, packages = load_ops_data()

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

    return queue

@router.get("/ops/production_graph")
def production_graph():

    runs, batches, batch_map, packages = load_ops_data()

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

    runs, batches, batch_map, packages = load_ops_data()

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

    runs, batches, batch_map, packages = load_ops_data()

    model = build_production_model(runs, batches)

    return find_bottlenecks(model)

@router.get("/ops/blockers")
def blockers():

    runs, batches, batch_map, packages = load_ops_data()

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

    runs, batches, batch_map, packages = load_ops_data()

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

    runs, batches, batch_map, packages = load_ops_data()

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

    runs, batches, batch_map, packages = load_ops_data()

    return calculate_yields(runs)

@router.get("/ops/inventory_risk")
def inventory_risk():

    runs, batches, batch_map, packages = load_ops_data()

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

    runs, batches, batch_map, packages = load_ops_data()

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

    return {
        "packages": total_packages,
        "total_quantity": round(total_quantity, 2),
        "available_quantity": round(available_quantity, 2),
        "reserved_quantity": round(reserved_quantity, 2),
        "inactive_packages": inactive_packages
    }

@router.get("/ops/production_dag")
def production_dag():

    runs, batches, batch_map, packages = load_ops_data()

    model = build_production_model(runs, batches)

    dag = build_production_dag(model)

    return dag

@router.get("/ops/stalled_batches")
def stalled_batches():

    runs, batches, batch_map, packages = load_ops_data()

    model = build_production_model(runs, batches)

    return find_stalled_batches(model)

@router.get("/ops/next_actions")
def next_actions():

    runs, batches, batch_map, packages = load_ops_data()

    return get_next_actions(runs, batch_map)

@router.get("/ops/throughput")
def throughput():

    runs, batches, batch_map, packages = load_ops_data()

    model = build_production_model(runs, batches)

    return calculate_throughput(model)

@router.get("/ops/batch_eta")
def batch_eta():

    runs, batches, batch_map, packages = load_ops_data()

    model = build_production_model(runs, batches)

    return estimate_batch_eta(model)

@router.get("/ops/material_flow")
def material_flow_endpoint():

    runs, batches, batch_map, packages = load_ops_data()

    return material_flow(runs)

@router.get("/ops/material_pressure")
def material_pressure():

    runs, batches, batch_map, packages = load_ops_data()

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

        # � 1. No material attached
        if input_qty == 0:
            pressure = "NO_INPUTS"

        # � 2. Material attached but nothing started
        elif input_qty > 0 and output_qty == 0 and status == "OPEN":
            pressure = "READY_NOT_STARTED"

        # � 3. Material in process (good, but still pressure)
        elif input_qty > 0 and output_qty == 0:
            pressure = "IN_PROGRESS"

        # � 4. Output exists but not finalized
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

from collections import defaultdict
from datetime import datetime, timezone, timedelta

@router.get("/ops/production_velocity")
def production_velocity():

    runs, batches, batch_map, packages = load_ops_data()

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

    runs, batches, batch_map, packages = load_ops_data()

    model = build_production_model(runs, batches)

    bottlenecks = find_bottlenecks(model)
    stalled = find_stalled_batches(model)
    blockers_list = []  # optional: reuse blockers() if needed

    pressure = material_pressure()
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

    return {
        "summary": summary,
        "system_state": system_state,
        "bottlenecks": bottlenecks[:5],
        "material_pressure": dict(pressure_counts),
        "stalled_batches": stalled[:10],
        "next_actions": actions[:10],
        "alerts": alerts
    }