"""
Velocity, throughput, and yield endpoints.

Tracks how fast we're producing (velocity), how much we're producing
(throughput), and how efficiently we're producing (yield). Used by ops
leadership and lab/extraction (Galen's primary tools).
"""

from fastapi import APIRouter, HTTPException, Query
from api.bq_client import query_view, run_sql
import logging

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/ops/velocity_weekly")
def velocity_weekly(
    weeks: int = Query(12, description="Number of weeks of history to return")
):
    """
    Returns weekly production velocity — runs completed per week, broken
    down by template.

    Each row represents (week_start, template_name) with run count, average
    run duration, and total cost (where available). Used for trend analysis.

    Default: last 12 weeks. Adjust ?weeks= for longer/shorter history.
    """
    try:
        sql = f"""
            SELECT *
            FROM `mfny-to-bigquery.canix_raw.v_production_velocity_weekly`
            WHERE week_start >= DATE_SUB(CURRENT_DATE(), INTERVAL {weeks} WEEK)
            ORDER BY week_start DESC, template_name
        """
        rows = run_sql(sql)
        return {
            "source": "bigquery",
            "view": "v_production_velocity_weekly",
            "rows": rows,
            "row_count": len(rows),
            "filter": f"last {weeks} weeks"
        }
    except Exception as e:
        logger.error(f"velocity_weekly failed: {e}")
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")


@router.get("/ops/biomass_transformation")
def biomass_transformation():
    """
    Returns biomass yield analysis by extraction template and strain.

    For each (template_name, strain) combination: input/output weights,
    yield percentage, run count.

    KNOWN ISSUE: Strain extraction uses regex parsing of run names which
    is fragile. Pre-METRC migration runs may have unreliable strain
    attribution. To be addressed in a later cleanup.
    """
    try:
        rows = query_view("v_biomass_transformation")
        return {
            "source": "bigquery",
            "view": "v_biomass_transformation",
            "rows": rows,
            "row_count": len(rows),
            "known_issues": [
                "Strain extraction uses fragile regex on run names",
                "Pre-METRC migration runs may have unreliable attribution"
            ]
        }
    except Exception as e:
        logger.error(f"biomass_transformation failed: {e}")
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")


@router.get("/ops/production_velocity")
def production_velocity():
    """
    Returns current production velocity — what's actively running right now,
    aggregated by template.

    Different from /ops/velocity_weekly (which is historical). This is a
    snapshot of the current shop floor.

    Returns: template_name, active_run_count, batch_count,
    oldest_run_age_days, newest_run_age_days.
    """
    try:
        sql = """
            SELECT
                b.template_name,
                COUNT(DISTINCT r.id) AS active_run_count,
                COUNT(DISTINCT b.id) AS batch_count,
                MAX(DATE_DIFF(CURRENT_DATE(), DATE(r.created_at), DAY)) AS oldest_run_age_days,
                MIN(DATE_DIFF(CURRENT_DATE(), DATE(r.created_at), DAY)) AS newest_run_age_days
            FROM `mfny-to-bigquery.canix_raw.bronze_manu_batch_runs` r
            JOIN `mfny-to-bigquery.canix_raw.bronze_manu_batches` b
                ON r.manufacturing_batch_id = b.id
            WHERE r.status IN ('OPEN', 'PENDING_CONFIGURATION')
              AND b.end_date IS NULL
            GROUP BY b.template_name
            ORDER BY active_run_count DESC
        """
        rows = run_sql(sql)
        return {
            "source": "bigquery",
            "view": "computed from bronze tables",
            "rows": rows,
            "row_count": len(rows)
        }
    except Exception as e:
        logger.error(f"production_velocity failed: {e}")
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")


@router.get("/ops/throughput")
def throughput(
    days: int = Query(30, description="Time window in days for throughput calculation")
):
    """
    Returns production throughput by template over the given window.

    For each template_name: completed_runs (status SUBMITTED with end_date
    in the window), avg_run_duration_days. Used for "how much are we
    actually shipping per template?"

    Default window: last 30 days.
    """
    try:
        sql = f"""
            SELECT
                b.template_name,
                COUNT(DISTINCT r.id) AS completed_runs_in_window,
                AVG(DATE_DIFF(r.end_date, r.start_date, DAY)) AS avg_run_duration_days,
                MIN(r.end_date) AS earliest_completion,
                MAX(r.end_date) AS latest_completion
            FROM `mfny-to-bigquery.canix_raw.bronze_manu_batch_runs` r
            JOIN `mfny-to-bigquery.canix_raw.bronze_manu_batches` b
                ON r.manufacturing_batch_id = b.id
            WHERE r.status = 'SUBMITTED'
              AND r.end_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
              AND r.end_date IS NOT NULL
            GROUP BY b.template_name
            ORDER BY completed_runs_in_window DESC
        """
        rows = run_sql(sql)
        return {
            "source": "bigquery",
            "view": "computed from bronze tables",
            "rows": rows,
            "row_count": len(rows),
            "window_days": days
        }
    except Exception as e:
        logger.error(f"throughput failed: {e}")
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")


@router.get("/ops/yield_analysis")
def yield_analysis():
    """
    Returns per-template yield analysis based on biomass_transformation view.

    This is a wrapper around v_biomass_transformation that aggregates to the
    template level (rather than template + strain level), giving a single
    yield % per template.

    For more granular (strain-level) yield data, use /ops/biomass_transformation.
    """
    try:
        sql = """
            SELECT
                template_name,
                COUNT(DISTINCT run_id) AS total_runs,
                COUNT(DISTINCT strain_name) AS strain_groups,
                ROUND(SUM(total_input_lbs), 2) AS total_input_lbs,
                ROUND(SUM(total_output_lbs), 2) AS total_output_lbs,
                ROUND(
                    SAFE_DIVIDE(
                        SUM(total_output_lbs),
                        SUM(total_input_lbs)
                    ) * 100,
                    2
                ) AS yield_pct,
                ROUND(AVG(yield_pct), 2) AS avg_yield_pct_per_run,
                MIN(end_date) AS earliest_completion,
                MAX(end_date) AS latest_completion
            FROM `mfny-to-bigquery.canix_raw.v_biomass_transformation`
            WHERE total_input_lbs > 0
            GROUP BY template_name
            ORDER BY total_runs DESC
        """
        rows = run_sql(sql)
        return {
            "source": "bigquery",
            "view": "computed from v_biomass_transformation",
            "rows": rows,
            "row_count": len(rows),
            "known_issues": [
                "Inherits strain attribution issues from v_biomass_transformation",
                "Includes pre-METRC migration data — yields may not be reliable"
            ]
        }
    except Exception as e:
        logger.error(f"yield_analysis failed: {e}")
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")