from collections import defaultdict

from api.constants import ACTIVE_STATUSES, APPROVAL_STATUSES, COMPLETED_STATUSES

def build_production_model(runs, batches):

    batch_map = {b["id"]: b for b in batches}

    batches_grouped = defaultdict(list)

    for run in runs:

        if not isinstance(run, dict):
            continue

        batch_id = run.get("manufacturing_batch_id")

        batches_grouped[batch_id].append(run)

    model = []

    for batch_id, batch_runs in batches_grouped.items():

        batch = batch_map.get(batch_id, {})

        sorted_runs = sorted(batch_runs, key=lambda r: r.get("order", 0))

        model.append({
            "batch_id": batch_id,
            "batch_name": batch.get("name"),
            "runs": sorted_runs
        })

    return model

def build_production_dag(model):

    dag = []

    for batch in model:

        runs = batch["runs"]

        nodes = []
        edges = []

        previous = None

        for run in runs:

            run_id = run.get("id")

            nodes.append({
                "run_id": run_id,
                "run_name": run.get("name"),
                "status": run.get("status"),
                "order": run.get("order")
            })

            if previous:
                edges.append({
                    "from": previous,
                    "to": run_id
                })

            previous = run_id

        dag.append({
            "batch_id": batch["batch_id"],
            "batch_name": batch["batch_name"],
            "nodes": nodes,
            "edges": edges
        })

    return dag

def find_stalled_batches(model):

    stalled = []

    for batch in model:

        batch_name = batch["batch_name"]

        for run in batch["runs"]:

            status = run.get("status")

            cannabis_inputs = run.get("cannabis_inputs", [])
            non_cannabis_inputs = run.get("non_cannabis_inputs", [])

            # stalled if run open and no inputs
            if status == "OPEN" and len(cannabis_inputs) == 0 and len(non_cannabis_inputs) == 0:

                stalled.append({
                    "batch_name": batch_name,
                    "run_name": run.get("name"),
                    "status": status
                })

                break

    return stalled

def find_bottlenecks(model):

    step_counts = defaultdict(int)

    for batch in model:

        for run in batch["runs"]:

            status = run.get("status")

            if status in ACTIVE_STATUSES or status in APPROVAL_STATUSES:

                step = (run.get("name") or "").strip().lower()

                step_counts[step] += 1

                break

    results = []

    for step, count in step_counts.items():

        results.append({
            "step": step,
            "runs_waiting": count
        })

    results_sorted = sorted(results, key=lambda x: x["runs_waiting"], reverse=True)

    return results_sorted

def calculate_yields(runs):

    results = []

    for run in runs:

        inputs = run.get("cannabis_inputs", [])
        outputs = run.get("cannabis_outputs", [])

        input_qty = sum(i.get("quantity", 0) for i in inputs)
        output_qty = sum(o.get("quantity", 0) for o in outputs)

        if input_qty == 0:
            continue

        yield_pct = round((output_qty / input_qty) * 100, 2)

        results.append({
            "run_id": run.get("id"),
            "run_name": run.get("name"),
            "yield_percent": yield_pct,
            "input_qty": input_qty,
            "output_qty": output_qty
        })

    return results

def calculate_throughput(model):

    step_counts = {}

    for batch in model:

        for run in batch.get("runs", []):

            status = (run.get("status") or "").upper()
            step = run.get("name")

            # skip steps not yet configured
            if status == "PENDING_CONFIGURATION":
                continue

            # first active step = batch is waiting here
            step_counts[step] = step_counts.get(step, 0) + 1
            break

    results = [
        {"step": step, "batches_waiting": count}
        for step, count in step_counts.items()
    ]

    return sorted(results, key=lambda x: x["batches_waiting"], reverse=True)

def estimate_batch_eta(model):

    results = []

    for batch in model:

        total_runs = len(batch["runs"])
        completed = 0
        errored_runs = 0

        for run in batch["runs"]:

            status = run.get("status")

            if status in COMPLETED_STATUSES:
                completed += 1

            if status == "ERRORED":
                errored_runs += 1

        progress = 0
        if total_runs > 0:
            progress = round((completed / total_runs) * 100, 1)

        results.append({
            "batch_name": batch["batch_name"],
            "total_steps": total_runs,
            "completed_steps": completed,
            "progress_percent": progress,
            "errored_runs": errored_runs
        })

    return results

def material_flow(runs):

    VALID_STATUSES = ACTIVE_STATUSES | APPROVAL_STATUSES | COMPLETED_STATUSES

    def classify_flow(input_qty, output_qty):
        if input_qty == 0 and output_qty == 0:
            return "NOT_STARTED"
        if input_qty > 0 and output_qty == 0:
            return "IN_PROGRESS"
        if input_qty > 0 and output_qty > 0:
            return "COMPLETED"
        return "UNKNOWN"

    flow = []

    for run in runs:

        status = run.get("status")

        if status not in VALID_STATUSES:
            continue

        inputs = run.get("cannabis_inputs", [])
        outputs = run.get("cannabis_outputs", [])

        input_qty = sum(i.get("quantity", 0) for i in inputs)
        output_qty = sum(o.get("quantity", 0) for o in outputs)

        flow_status = classify_flow(input_qty, output_qty)

        flow.append({
            "run_name": run.get("name"),
            "status": status,

            "input_count": len(inputs),
            "output_count": len(outputs),

            "input_quantity": round(input_qty, 2),
            "output_quantity": round(output_qty, 2),

            "net_quantity": round(output_qty - input_qty, 2),

            "flow_status": flow_status
        })

    return flow

def get_material_pressure(runs, batch_map):

    pressures = []

    for run in runs:

        if not isinstance(run, dict):
            continue

        status = run.get("status")

        if status not in ACTIVE_STATUSES and status not in APPROVAL_STATUSES:
            continue

        cannabis_inputs = run.get("cannabis_inputs", [])
        cannabis_outputs = run.get("cannabis_outputs", [])

        input_qty = sum(i.get("quantity", 0) for i in cannabis_inputs)
        output_qty = sum(o.get("quantity", 0) for o in cannabis_outputs)

        batch = batch_map.get(run.get("manufacturing_batch_id"), {})

        if input_qty == 0:
            pressure = "NO_INPUTS"

        elif input_qty > 0 and output_qty == 0 and status == "OPEN":
            pressure = "READY_NOT_STARTED"

        elif input_qty > 0 and output_qty == 0:
            pressure = "IN_PROGRESS"

        elif input_qty > 0 and output_qty > 0 and status not in COMPLETED_STATUSES:
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

from datetime import datetime, timezone

def get_next_actions(runs, batch_map):

    batches_grouped = {}

    for run in runs:

        if not isinstance(run, dict):
            continue

        batch_id = run.get("manufacturing_batch_id")

        if batch_id not in batches_grouped:
            batches_grouped[batch_id] = []

        batches_grouped[batch_id].append(run)

    actions = []

    now = datetime.now(timezone.utc)

    for batch_id, batch_runs in batches_grouped.items():

        batch = batch_map.get(batch_id, {})
        batch_name = batch.get("name")

        sorted_runs = sorted(batch_runs, key=lambda r: r.get("order", 0))

        for run in sorted_runs:

            status = run.get("status")

            if status in COMPLETED_STATUSES:
                continue

            run_name = run.get("name")
            created_at = run.get("created_at")

            age_hours = None

            if created_at:
                try:
                    created_dt = datetime.fromisoformat(created_at.replace("Z","+00:00"))
                    age_hours = round((now - created_dt).total_seconds() / 3600, 1)
                except:
                    pass

            if age_hours and age_hours > 720:
                continue

            cannabis_inputs = run.get("cannabis_inputs", [])
            cannabis_outputs = run.get("cannabis_outputs", [])

            input_qty = sum(i.get("quantity", 0) for i in cannabis_inputs)
            output_qty = sum(o.get("quantity", 0) for o in cannabis_outputs)

            # 1. APPROVAL BLOCK
            if status in APPROVAL_STATUSES:

                actions.append({
                    "priority": 1,
                    "batch_name": batch_name,
                    "run_name": run_name,
                    "run_order": run.get("order"),
                    "status": status,
                    "age_hours": age_hours,
                    "action": "Approve run"
                })
                break

            # 2. NO MATERIAL
            if input_qty == 0:

                actions.append({
                    "priority": 2,
                    "batch_name": batch_name,
                    "run_name": run_name,
                    "status": status,
                    "age_hours": age_hours,
                    "action": "Attach material / investigate missing inputs"
                })
                break

            # 3. READY BUT NOT STARTED
            if status == "OPEN" and input_qty > 0:

                actions.append({
                    "priority": 2,
                    "batch_name": batch_name,
                    "run_name": run_name,
                    "status": status,
                    "age_hours": age_hours,
                    "action": "Start run"
                })
                break

            # 4. IN PROGRESS BUT STALLED
            if input_qty > 0 and output_qty == 0 and age_hours and age_hours > 24:

                actions.append({
                    "priority": 3,
                    "batch_name": batch_name,
                    "run_name": run_name,
                    "status": status,
                    "age_hours": age_hours,
                    "action": "Check stalled run"
                })
                break

            # 5. OUTPUT CREATED BUT NOT SUBMITTED
            if input_qty > 0 and output_qty > 0 and status != "SUBMITTED":

                actions.append({
                    "priority": 2,
                    "batch_name": batch_name,
                    "run_name": run_name,
                    "status": status,
                    "age_hours": age_hours,
                    "action": "Finalize / submit run"
                })
                break

            # DEFAULT
            actions.append({
                "priority": 4,
                "batch_name": batch_name,
                "run_name": run_name,
                "status": status,
                "age_hours": age_hours,
                "action": "Advance production"
            })

            break

    actions_sorted = sorted(
        actions,
        key=lambda a: (a["priority"], -(a["age_hours"] or 0))
    )

    return actions_sorted