"""
Production endpoints — thin wrappers around BigQuery views.

Endpoints related to live production state: what's actively running,
what's stuck, what needs approval, what's next, and where bottlenecks are.
"""

from fastapi import APIRouter, HTTPException, Query
from api.bq_client import query_view, run_sql
import logging

logger = logging.getLogger(__name__)
router = APIRouter()

# Allowed batch_health values — must match v_active_batches' CASE output exactly.
# Validated because `health` is f-string interpolated into the WHERE clause.
_BATCH_HEALTH = {
    "NEEDS_ATTENTION",
    "PENDING_APPROVAL",
    "IN_PROGRESS",
    "PENDING_START",
    "UNKNOWN",
}

@router.get("/ops/active_production")
def active_production(
    health: str | None = Query(None, description="Filter by batch_health: NEEDS_ATTENTION, PENDING_APPROVAL, IN_PROGRESS, PENDING_START")
):
    """
    Returns currently-active manufacturing batches with run progress.

    Active-set = v_active_batches (real template + at least one non-SUBMITTED
    run + updated within 90 days). Replaces the prior `end_date IS NULL`
    definition, which was semantically wrong (end_date is a scheduling field,
    not a done-marker) and over-counted active batches ~2x with zombies/completed.

    Sorted by health priority (NEEDS_ATTENTION first), then by days_on_current_run.
    """
    if health and health not in _BATCH_HEALTH:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid health. Must be one of: {sorted(_BATCH_HEALTH)}"
        )  
    try:
        where = f"batch_health = '{health}'" if health else None
        rows = query_view("v_active_batches", where=where)
        return {
            "source": "bigquery",
            "view": "v_active_batches",
            "rows": rows,
            "row_count": len(rows)
        }
    except Exception as e:
        logger.error(f"active_production failed: {e}")
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")


@router.get("/ops/bottleneck_summary")
def bottleneck_summary():
    """
    Returns aggregated bottleneck counts by step.

    For each (step_name, step_status) combination across active batches:
    how many batches are waiting, average days waiting, max days waiting,
    and counts over key thresholds (7 days, 30 days).

    Sorted by batches_waiting DESC, then avg_days_waiting DESC.
    """
    try:
        rows = query_view("v_bottleneck_summary")
        return {
            "source": "bigquery",
            "view": "v_bottleneck_summary",
            "rows": rows,
            "row_count": len(rows)
        }
    except Exception as e:
        logger.error(f"bottleneck_summary failed: {e}")
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")


@router.get("/ops/packaging_queue")
def packaging_queue():
    """
    Returns active runs whose names match packaging-stage steps:
    pack, label, fill, tube, gram, portion, bottle.

    Used by the Operations dashboard to show what's currently being
    packaged or labeled. Sorted by run status priority, then days on step.
    """
    try:
        rows = query_view("v_packaging_queue")
        return {
            "source": "bigquery",
            "view": "v_packaging_queue",
            "rows": rows,
            "row_count": len(rows)
        }
    except Exception as e:
        logger.error(f"packaging_queue failed: {e}")
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")


@router.get("/ops/stalled_batches")
def stalled_batches(min_days: int = Query(7, description="Minimum days_on_current_run to qualify as stalled")):
    """
    Returns active batches whose current run has been open for too long.

    A batch is "stalled" if its current run has been in OPEN, ERRORED, or
    PENDING_CONFIGURATION status for >= min_days (default 7).

    Built from v_active_batches with a days_on_current_run filter.
    """
    try:
        sql = f"""
            SELECT *
            FROM `mfny-to-bigquery.canix_raw.v_active_batches`
            WHERE days_on_current_run >= {min_days}
              AND current_run_status IN ('OPEN', 'ERRORED', 'PENDING_CONFIGURATION')
            ORDER BY days_on_current_run DESC
        """
        rows = run_sql(sql)
        return {
            "source": "bigquery",
            "view": "v_active_batches (filtered)",
            "filter": f"days_on_current_run >= {min_days}",
            "rows": rows,
            "row_count": len(rows)
        }
    except Exception as e:
        logger.error(f"stalled_batches failed: {e}")
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")


@router.get("/ops/approval_queue")
def approval_queue():
    """
    Returns runs that are pending approval — submitted by operators but not
    yet approved/finalized in Canix.

    This represents work that's done physically but blocked at the approval
    gate. Important compliance signal — long-pending approvals = data that
    isn't getting submitted to METRC on time.
    """
    try:
        sql = """
            SELECT
                r.id AS run_id,
                r.name AS run_name,
                r.status AS run_status,
                r.start_date,
                r.end_date,
                r.manufacturing_batch_id AS batch_id,
                b.name AS batch_name,
                b.template_name,
                b.current_location,
                DATE_DIFF(CURRENT_DATE(), r.end_date, DAY) AS days_pending_approval
            FROM `mfny-to-bigquery.canix_raw.bronze_manu_batch_runs` r
            JOIN `mfny-to-bigquery.canix_raw.bronze_manu_batches` b
                ON r.manufacturing_batch_id = b.id
            WHERE r.status = 'SUBMITTED_FOR_APPROVAL'
            ORDER BY r.end_date ASC NULLS LAST
        """
        rows = run_sql(sql)
        return {
            "source": "bigquery",
            "view": "bronze_manu_batch_runs (filtered)",
            "filter": "status = 'SUBMITTED_FOR_APPROVAL'",
            "rows": rows,
            "row_count": len(rows)
        }
    except Exception as e:
        logger.error(f"approval_queue failed: {e}")
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")


@router.get("/ops/next_actions")
def next_actions(
    limit: int = Query(50, description="Max rows to return")
):
    """
    Per-active-batch next-action worklist. One row per active batch
    (active-set = v_active_batches), showing the first non-SUBMITTED run and the
    action it needs, prioritized 1=highest. Reads v_next_actions, which ports the
    ops_model 6-rule classification to SQL. Replaces the prior 4-category
    problems-only SQL.
    """
    try:
        rows = run_sql("SELECT * FROM `mfny-to-bigquery.canix_raw.v_next_actions`")
        if limit:
            rows = rows[:limit]
        return {
            "source": "bigquery",
            "view": "v_next_actions",
            "rows": rows,
            "row_count": len(rows)
        }
    except Exception as e:
        logger.error(f"next_actions failed: {e}")
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")