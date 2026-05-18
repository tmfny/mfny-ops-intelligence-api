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


@router.get("/ops/active_production")
def active_production(
    health: str | None = Query(None, description="Filter by batch_health: NEEDS_ATTENTION, PENDING_APPROVAL, IN_PROGRESS, PENDING_START")
):
    """
    Returns currently-active manufacturing batches with run progress.

    Each row represents a batch with end_date IS NULL (still active),
    showing total/completed/open/pending/errored run counts and a
    computed batch_health classification.

    Sorted by health priority (NEEDS_ATTENTION first), then by days_on_current_run.
    """
    try:
        where = f"batch_health = '{health}'" if health else None
        rows = query_view("v_active_production", where=where)
        return {
            "source": "bigquery",
            "view": "v_active_production",
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

    Built from v_active_production with a days_on_current_run filter.
    """
    try:
        sql = f"""
            SELECT *
            FROM `mfny-to-bigquery.canix_raw.v_active_production`
            WHERE days_on_current_run >= {min_days}
              AND current_run_status IN ('OPEN', 'ERRORED', 'PENDING_CONFIGURATION')
            ORDER BY days_on_current_run DESC
        """
        rows = run_sql(sql)
        return {
            "source": "bigquery",
            "view": "v_active_production (filtered)",
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
    Returns prioritized list of items needing operator attention.

    Combines signals from across the data:
    - Errored runs (highest priority)
    - Runs pending approval
    - Compliance-overdue items (physically complete, not submitted in Canix)
    - Stalled batches (days_on_current_run > 7)

    Each row includes priority (1=highest, 4=lowest), batch_name, run_name,
    status, age_hours since the issue arose, and a suggested action.
    """
    try:
        sql = f"""
            WITH errored AS (
                SELECT
                    1 AS priority,
                    b.name AS batch_name,
                    r.name AS run_name,
                    r.status,
                    'Run errored — needs investigation' AS action,
                    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), r.updated_at, HOUR) AS age_hours,
                    r.id AS run_id,
                    r.manufacturing_batch_id AS batch_id
                FROM `mfny-to-bigquery.canix_raw.bronze_manu_batch_runs` r
                JOIN `mfny-to-bigquery.canix_raw.bronze_manu_batches` b
                    ON r.manufacturing_batch_id = b.id
                WHERE r.status = 'ERRORED'
            ),
            pending_approval AS (
                SELECT
                    2 AS priority,
                    b.name AS batch_name,
                    r.name AS run_name,
                    r.status,
                    'Pending approval — review and submit' AS action,
                    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), r.updated_at, HOUR) AS age_hours,
                    r.id AS run_id,
                    r.manufacturing_batch_id AS batch_id
                FROM `mfny-to-bigquery.canix_raw.bronze_manu_batch_runs` r
                JOIN `mfny-to-bigquery.canix_raw.bronze_manu_batches` b
                    ON r.manufacturing_batch_id = b.id
                WHERE r.status = 'SUBMITTED_FOR_APPROVAL'
            ),
            compliance_overdue AS (
                SELECT
                    3 AS priority,
                    batch_name,
                    run_name,
                    run_status AS status,
                    'Physically complete, not submitted in Canix' AS action,
                    days_since_end * 24 AS age_hours,
                    run_id,
                    CAST(NULL AS STRING) AS batch_id
                FROM `mfny-to-bigquery.canix_raw.v_compliance_timing`
                WHERE compliance_risk IN ('OVERDUE', 'AT_RISK')
            ),
            stalled AS (
                SELECT
                    4 AS priority,
                    batch_name,
                    current_run_name AS run_name,
                    current_run_status AS status,
                    'Stalled — no progress in 7+ days' AS action,
                    days_on_current_run * 24 AS age_hours,
                    CAST(NULL AS STRING) AS run_id,
                    batch_id
                FROM `mfny-to-bigquery.canix_raw.v_active_production`
                WHERE days_on_current_run >= 7
                  AND current_run_status IN ('OPEN', 'PENDING_CONFIGURATION')
            )
            SELECT * FROM errored
            UNION ALL SELECT * FROM pending_approval
            UNION ALL SELECT * FROM compliance_overdue
            UNION ALL SELECT * FROM stalled
            ORDER BY priority ASC, age_hours DESC
            LIMIT {limit}
        """
        rows = run_sql(sql)
        return {
            "source": "bigquery",
            "view": "synthesized from multiple views",
            "rows": rows,
            "row_count": len(rows)
        }
    except Exception as e:
        logger.error(f"next_actions failed: {e}")
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")