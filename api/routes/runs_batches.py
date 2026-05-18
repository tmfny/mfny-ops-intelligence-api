"""
Run-level and batch-level lookup endpoints.

These are simpler than the synthesis endpoints in Phase 6 — each one
queries a focused slice of bronze data without complex aggregation.

Used for drill-down navigation: "show me all runs in X status", "show me
the timeline of run Y", "what's the progress of batch Z".
"""

from fastapi import APIRouter, HTTPException, Query
from api.bq_client import run_sql
import logging

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/ops/active_runs")
def active_runs(
    status: str | None = Query(None, description="Filter by run status: OPEN, SUBMITTED, ERRORED, etc."),
    limit: int = Query(200, description="Max rows to return")
):
    """
    Returns runs that are currently active (their parent batch has no end_date).

    Each row is one run with its parent batch context. Use this when you
    need run-level detail rather than batch-level rollups.

    Different from /ops/active_production which aggregates to batch level.
    """
    try:
        where = "b.end_date IS NULL"
        if status:
            where += f" AND r.status = '{status}'"

        sql = f"""
            SELECT
                r.id AS run_id,
                r.name AS run_name,
                r.status AS run_status,
                r.start_date AS run_start_date,
                r.end_date AS run_end_date,
                DATE_DIFF(CURRENT_DATE(), DATE(r.created_at), DAY) AS days_since_run_created,
                b.id AS batch_id,
                b.name AS batch_name,
                b.template_name,
                b.current_location,
                b.start_date AS batch_start_date,
                r.created_at AS run_created_at,
                r.updated_at AS run_updated_at
            FROM `mfny-to-bigquery.canix_raw.bronze_manu_batch_runs` r
            JOIN `mfny-to-bigquery.canix_raw.bronze_manu_batches` b
                ON r.manufacturing_batch_id = b.id
            WHERE {where}
            ORDER BY r.created_at DESC
            LIMIT {limit}
        """
        rows = run_sql(sql)
        return {
            "source": "bigquery",
            "view": "bronze_manu_batch_runs (filtered)",
            "rows": rows,
            "row_count": len(rows),
            "filter": where
        }
    except Exception as e:
        logger.error(f"active_runs failed: {e}")
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")


@router.get("/ops/run_timeline")
def run_timeline(
    run_id: str = Query(..., description="Run ID to look up timeline for")
):
    """
    Returns the timeline of a specific run — created, started, ended,
    last updated. Useful for drill-down inspection.

    Required: ?run_id=<run_id>
    """
    try:
        sql = f"""
            SELECT
                r.id AS run_id,
                r.name AS run_name,
                r.status,
                r.start_date,
                r.end_date,
                r.created_at,
                r.updated_at,
                DATE_DIFF(IFNULL(r.end_date, CURRENT_DATE()), r.start_date, DAY) AS run_duration_days,
                DATE_DIFF(CURRENT_DATE(), DATE(r.updated_at), DAY) AS days_since_last_update,
                b.id AS batch_id,
                b.name AS batch_name,
                b.template_name,
                b.current_location,
                b.start_date AS batch_start_date,
                b.end_date AS batch_end_date
            FROM `mfny-to-bigquery.canix_raw.bronze_manu_batch_runs` r
            JOIN `mfny-to-bigquery.canix_raw.bronze_manu_batches` b
                ON r.manufacturing_batch_id = b.id
            WHERE r.id = '{run_id}'
        """
        rows = run_sql(sql)
        if not rows:
            raise HTTPException(status_code=404, detail=f"Run {run_id} not found")
        return {
            "source": "bigquery",
            "view": "bronze tables (filtered to one run)",
            "rows": rows,
            "row_count": len(rows)
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"run_timeline failed: {e}")
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")


@router.get("/ops/batch_progress")
def batch_progress(
    batch_id: str | None = Query(None, description="Optional: filter to a specific batch_id"),
    active_only: bool = Query(True, description="If true, only batches with no end_date")
):
    """
    Returns batch-level progress: how many runs total, completed, open,
    pending, errored. Aggregated from runs.

    Without ?batch_id=, returns all (active by default — set
    ?active_only=false for completed batches too). With ?batch_id=, returns
    one batch.
    """
    try:
        where = []
        if batch_id:
            where.append(f"b.id = '{batch_id}'")
        if active_only:
            where.append("b.end_date IS NULL")
        where_clause = " AND ".join(where) if where else "1=1"

        sql = f"""
            SELECT
                b.id AS batch_id,
                b.name AS batch_name,
                b.template_name,
                b.current_location,
                b.status AS batch_status,
                b.start_date,
                b.end_date,
                COUNT(DISTINCT r.id) AS total_runs,
                COUNTIF(r.status = 'SUBMITTED') AS runs_completed,
                COUNTIF(r.status = 'OPEN') AS runs_open,
                COUNTIF(r.status = 'PENDING_CONFIGURATION') AS runs_pending_config,
                COUNTIF(r.status = 'SUBMITTED_FOR_APPROVAL') AS runs_pending_approval,
                COUNTIF(r.status = 'ERRORED') AS runs_errored,
                ROUND(
                    SAFE_DIVIDE(
                        COUNTIF(r.status = 'SUBMITTED'),
                        COUNT(DISTINCT r.id)
                    ) * 100,
                    1
                ) AS pct_complete
            FROM `mfny-to-bigquery.canix_raw.bronze_manu_batches` b
            LEFT JOIN `mfny-to-bigquery.canix_raw.bronze_manu_batch_runs` r
                ON r.manufacturing_batch_id = b.id
            WHERE {where_clause}
            GROUP BY
                b.id, b.name, b.template_name, b.current_location,
                b.status, b.start_date, b.end_date
            ORDER BY b.start_date DESC NULLS LAST
        """
        rows = run_sql(sql)
        return {
            "source": "bigquery",
            "view": "computed from bronze tables",
            "rows": rows,
            "row_count": len(rows),
            "filter": where_clause
        }
    except Exception as e:
        logger.error(f"batch_progress failed: {e}")
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")