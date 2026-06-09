"""
Phase 6 synthesis endpoints.

These endpoints aggregate or synthesize data across multiple bronze tables
or views to surface higher-level operational signals — material pressure,
system alerts, etc.
"""
from fastapi import APIRouter, HTTPException
from api.bq_client import query_view
import logging
import os
import json
from anthropic import Anthropic
from pydantic import BaseModel

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/ops/material_pressure")
def material_pressure():
    """
    Returns 4 buckets representing where material is queued/stuck in the
    production funnel:
      - fresh_frozen_awaiting_extraction
      - cured_flower_awaiting_production
      - extracted_awaiting_decarb
      - concentrate_ready_for_production

    Each bucket includes total package count, total weight, count of packages
    over 90 days old (aging signal), and the oldest package age in days.

    Filters: facility 4511 only (post-METRC), status='In Progress' (unallocated).
    """
    try:
        rows = query_view("v_material_pressure")
        return {
            "source": "bigquery",
            "view": "v_material_pressure",
            "rows": rows,
            "row_count": len(rows)
        }
    except Exception as e:
        logger.error(f"material_pressure failed: {e}")
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")


@router.get("/ops/system_alerts")
def system_alerts():
    """
    Returns a prioritized list of operational alerts surfaced from across
    the production system. Each alert includes:
      - level: CRITICAL, WARNING, INFO, or OK
      - severity_order: numeric ordering for display (1=CRITICAL, ..., 99=OK)
      - message: human-readable description
      - source: which check produced this alert (errored_runs, compliance_overdue, etc.)
      - reference_id: optional ID of the underlying run/step for drill-down

    Alert sources include: errored runs, compliance overdue items, stalled batches,
    bottlenecks, cold extraction, approval queue aging.

    If no real alerts exist, returns a single OK row indicating "all systems normal."
    """
    try:
        rows = query_view("v_system_alerts")
        return {
            "source": "bigquery",
            "view": "v_system_alerts",
            "rows": rows,
            "row_count": len(rows)
        }
    except Exception as e:
        logger.error(f"system_alerts failed: {e}")
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")
    

# --- add near the top of synthesis.py, with the existing imports ---
import asyncio
from fastapi import Request  # (APIRouter, HTTPException already imported)

# The six views that make up the exec snapshot.
# Locked from live INFORMATION_SCHEMA — these names are verified, not assumed.
SNAPSHOT_VIEWS = [
    "v_system_alerts",
    "v_bottleneck_summary",
    "v_material_pressure",
    "v_active_batches",
    "v_next_actions",
    "v_expiring_skus",
    "v_executive_flow",
    "v_executive_production_summary",
    "v_executive_inventory_summary",
    "v_sales_status_strip",
    "v_inventory_valuation_summary",
]


# ---------------------------------------------------------------------------
# DATA TRUST DECLARATIONS
# Single source of truth for what the exec tool can and cannot stand behind.
# Every view/field added to the snapshot MUST be vetted here before shipping.
# The prompt is given these notes so Claude never confidently reports data we
# know is unreliable, and describes ambiguous fields precisely.
# ---------------------------------------------------------------------------
DATA_TRUST_NOTES = (
    "DATA RELIABILITY NOTES (respect these strictly):\n"
    "- Payment/collection data is NOT tracked reliably in this system "
    "(MFNY uses QuickBooks for money owed, not this data source). NEVER report "
    "amounts paid, collected, or outstanding. You may report order REVENUE and "
    "order VALUE, which are reliable.\n"
    "- 'Sellable inventory' counts (sales_summary) are packages available to sell "
    "at the distribution facility specifically. 'Inventory valuation' counts cover "
    "the valued inventory set, a different population. If both appear, describe what "
    "each refers to rather than treating them as the same number.\n"
    "- Inventory values are on a standard wholesale-price basis ('book value'), "
    "not market or realized value.\n"
)


async def fetch_snapshot() -> dict[str, list[dict]]:
    """
    Fetch all snapshot views concurrently.

    query_view is synchronous/blocking, so each call runs in the default
    threadpool executor via run_in_executor. asyncio.gather then lets them
    overlap — turning ~8s sequential into ~2s parallel. The BigQuery client
    is a thread-safe singleton, so concurrent queries are safe.
    """
    loop = asyncio.get_running_loop()

    # Per-view WHERE filters. Only v_expiring_skus needs one: it returns ~158
    # rows but ~157 are status 'OK' and 90+ days out — noise for an exec brief.
    # Filtering to non-OK keeps the bundle lean and lets the view define what's
    # noteworthy. Everything else fetches in full.
    filters = {
        "v_expiring_skus": "expiry_status != 'OK'",
    }

    tasks = {
        view: loop.run_in_executor(None, query_view, view, filters.get(view))
        for view in SNAPSHOT_VIEWS
    }
    results = {}
    for view, task in tasks.items():
        try:
            results[view] = await task
        except Exception as e:
            logger.error(f"snapshot fetch failed for {view}: {e}")
            results[view] = []
    return results


def compress_snapshot(snapshot: dict[str, list[dict]]) -> dict:
    """
    Shape the full snapshot into a lean payload for the LLM prompt.

    Operational multi-row views get compressed (top-N / summarized).
    Executive views are already single-row pre-aggregates — sent whole, no compression.
    """
    # v_next_actions is priority-ordered — top 10 only.
    next_actions = sorted(
        snapshot.get("v_next_actions", []),
        key=lambda r: r.get("priority", 999)
    )[:10]

    # v_bottleneck_summary — all rows, trimmed to meaningful columns.
    bottleneck_cols = ["step_name", "batches_waiting", "avg_days_waiting",
                       "max_days_waiting", "batches_over_7_days"]
    bottlenecks = [
        {k: r.get(k) for k in bottleneck_cols}
        for r in snapshot.get("v_bottleneck_summary", [])
    ]

    # Executive views are single-row aggregates — unwrap the list to the one row
    # for a cleaner prompt (or empty dict if the view returned nothing).
    def _one(view):
        rows = snapshot.get(view, [])
        return rows[0] if rows else {}

    return {
        # --- Operational: what needs attention right now ---
        "system_alerts": snapshot.get("v_system_alerts", []),
        "next_actions": next_actions,
        "bottlenecks": bottlenecks,
        "material_pressure": snapshot.get("v_material_pressure", []),
        "expiring_skus": snapshot.get("v_expiring_skus", []),
        # --- Executive: overall status, money, trends (pre-summarized) ---
        "pipeline_flow": _one("v_executive_flow"),
        "production_trends": _one("v_executive_production_summary"),
        "inventory_summary": _one("v_executive_inventory_summary"),
        # sales_summary: drop mtd_paid — Canix payment data is unreliable (money
        # owed lives in QuickBooks, not here). Revenue/order values are kept.
        "sales_summary": {k: v for k, v in _one("v_sales_status_strip").items()
                          if k != "mtd_paid"},
        "inventory_valuation": _one("v_inventory_valuation_summary"),
    }


@router.get("/ops/snapshot")
async def snapshot():
    """
    TEMPORARY diagnostic endpoint — assembles the full exec snapshot and
    returns it raw, with per-view row counts. Lets us verify the bundle
    fetches correctly and measure latency before adding the LLM layer.
    Remove once /ask is wired.
    """
    data = await fetch_snapshot()
    return {
        "row_counts": {view: len(rows) for view, rows in data.items()},
        "snapshot": data,
    }


# Anthropic client — reads ANTHROPIC_API_KEY from env locally,
# from the Secret-Manager-injected env var in Cloud Run. Module-level
# so it's created once, not per request.
_anthropic = Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

EXEC_MODEL = "claude-haiku-4-5"  # fast end; bump to Sonnet only if prose quality disappoints


class AskRequest(BaseModel):
    question: str
    history: list[dict] = []  # prior turns: [{"role": "user"/"assistant", "content": "..."}]


def build_system_prompt(snapshot: dict) -> str:
    return (
        "You are the operations analyst for MFNY, a licensed cannabis processor. "
        "You brief executives — the CFO and operations leads — who want fast, clear, "
        "accurate readouts of the business right now.\n\n"
        "The snapshot below covers: current production status, bottlenecks, stalled "
        "batches, material in the pipeline, items needing attention, expiring product, "
        "the farm-to-product flow, month-over-month production trends, inventory levels "
        "and days-of-inventory, sales and revenue (month-to-date), and inventory "
        "valuation.\n\n"
        "RULES:\n"
        "- Answer ONLY from the LIVE OPERATIONS SNAPSHOT below. Never invent or estimate "
        "numbers not present in it. If the snapshot doesn't contain the answer, say so plainly.\n"
        "- BE BRIEF. Lead with the direct answer in the first sentence. An executive should get "
        "the point from the first line alone. Most answers should be 2-4 sentences.\n"
        "- Write in plain prose, not markdown. NO bold, NO headers, NO bullet-point lists unless "
        "the answer is genuinely a list of items — and even then keep it tight.\n"
        "- Use plain business language, not database or SQL terms.\n"
        "- For inventory value, refer to it as 'book value' (it is valued on a standard price "
        "basis). Report dollar figures clearly (e.g. $4.5M).\n"
        "- Name risks as risks (compliance, stalled batches, expiring product) and say why they "
        "matter in a few words — don't pad with action plans unless asked.\n\n"
        f"{DATA_TRUST_NOTES}\n"
        "LIVE OPERATIONS SNAPSHOT (current as of the most recent data sync):\n"
        f"{json.dumps(snapshot, separators=(',', ':'))}"
    )


@router.post("/ops/ask")
async def ask(req: AskRequest):
    try:
        import time
        t0 = time.time()
        snapshot = await fetch_snapshot()
        t1 = time.time()
        logger.warning(f"[ask] snapshot fetch: {t1-t0:.2f}s")


        lean = compress_snapshot(snapshot)
        system_prompt = build_system_prompt(lean)
        messages = req.history + [{"role": "user", "content": req.question}]

        response = _anthropic.messages.create(
            model=EXEC_MODEL,
            max_tokens=512,
            system=system_prompt,
            messages=messages,
        )
        t2 = time.time()
        logger.warning(f"[ask] anthropic call: {t2-t1:.2f}s | "
                       f"in={response.usage.input_tokens} out={response.usage.output_tokens}")

        narrative = "".join(b.text for b in response.content if b.type == "text")
        return {"narrative": narrative}
    except Exception as e:
        logger.error(f"/ops/ask failed: {e}")
        raise HTTPException(status_code=500, detail=f"Ask failed: {str(e)}")