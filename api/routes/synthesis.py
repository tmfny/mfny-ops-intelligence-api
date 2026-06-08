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
]


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

    fetch_snapshot() returns everything (full truth, kept for /ops/snapshot
    diagnostics). This compresses it to what an exec answer actually needs —
    collapsing the heaviest views to summaries, sending the risk/worklist
    views in full. Target: ~14k tokens -> ~3k.
    """
    def _f(v):
        # completion_pct comes back as a STRING ("25.0") in the JSON, not a
        # float — cast safely, default 0.0 on anything unparseable.
        try:
            return float(v)
        except (TypeError, ValueError):
            return 0.0

    active = snapshot.get("v_active_batches", [])
    # Summarize the 42-row active-batches view down to counts + averages.
    # batch_health here is a LIFECYCLE state (PENDING_START / IN_PROGRESS),
    # not a quality flag — so we report the lifecycle breakdown, not "at risk".
    from collections import Counter
    lifecycle = Counter(b.get("batch_health") for b in active)
    completions = [_f(b.get("completion_pct")) for b in active]
    avg_completion = round(sum(completions) / len(completions), 1) if completions else 0.0

    active_summary = {
        "total_active_batches": len(active),
        "by_lifecycle_state": dict(lifecycle),
        "avg_completion_pct": avg_completion,
    }

    # v_next_actions is priority-ordered — send the top 10, drop the low-priority tail.
    next_actions = sorted(
        snapshot.get("v_next_actions", []),
        key=lambda r: r.get("priority", 999)
    )[:10]

    # v_bottleneck_summary — keep all rows, trim to the columns that carry meaning.
    bottleneck_cols = ["step_name", "batches_waiting", "avg_days_waiting",
                       "max_days_waiting", "batches_over_7_days"]
    bottlenecks = [
        {k: r.get(k) for k in bottleneck_cols}
        for r in snapshot.get("v_bottleneck_summary", [])
    ]

    return {
        # Full — the curated risk signal, small and central.
        "system_alerts": snapshot.get("v_system_alerts", []),
        # Top 10 by priority.
        "next_actions": next_actions,
        # All rows, trimmed columns.
        "bottlenecks": bottlenecks,
        # Full — tiny.
        "material_pressure": snapshot.get("v_material_pressure", []),
        # Full — already trimmed to non-OK.
        "expiring_skus": snapshot.get("v_expiring_skus", []),
        # Summary only — the big token win.
        "active_batches_summary": active_summary,
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
    """
    Frame Claude as an ops analyst briefing executives, and hand it the live
    snapshot as ground truth. The snapshot is injected as compact JSON — Claude
    answers ONLY from it, never invents figures.
    """
    return (
        "You are the operations analyst for MFNY, a licensed cannabis processor. "
        "You brief executives — the CFO and operations leads — who want fast, clear, "
        "accurate readouts of what is happening in production right now.\n\n"
        "RULES:\n"
        "- Answer ONLY from the LIVE OPERATIONS SNAPSHOT below. Never invent or estimate "
        "numbers not present in it. If the snapshot doesn't contain the answer, say so plainly.\n"
        "- BE BRIEF. Lead with the direct answer in the first sentence. An executive should get "
        "the point from the first line alone. Most answers should be 2-4 sentences.\n"
        "- Write in plain prose, not markdown. NO bold, NO headers, NO bullet-point lists unless "
        "the answer is genuinely a list of items — and even then keep it tight.\n"
        "- Use plain business language, not database or SQL terms. Say 'batches waiting at "
        "final packout', not 'rows in v_bottleneck_summary'.\n"
        "- Name risks as risks (compliance, stalled batches, expiring product) and say why they "
        "matter in a few words — don't pad with action plans unless asked.\n\n"
        "LIVE OPERATIONS SNAPSHOT (current as of this request):\n"
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