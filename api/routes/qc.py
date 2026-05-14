"""
QC endpoints — feed the QC page in Retool.

Audience: Emma (Quality Control). Focuses on data integrity and timing
between physical operations and Canix entry.

Endpoints:
  GET /qc/dashboard — Combined response with two zones:
                       - outstanding: runs physically complete but not in Canix
                                       (with WATCH/AT_RISK/OVERDUE/STALE buckets
                                       anchored to NYS 9 NYCRR § 125.8 3-day rule)
                       - recent_activity: counts of runs started/completed across
                                          today / 7-day / 30-day windows
"""

from fastapi import APIRouter, HTTPException
from api.bq_client import query_view
import logging

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/qc/dashboard")
def qc_dashboard():
    """
    Returns data for the QC page in Retool.

    Response combines two views:
      - outstanding (list):  rows from v_compliance_timing — runs where
                              physical work has completed but Canix entry
                              has not. Bucketed by lag:
                                WATCH      (1 day),
                                AT_RISK    (2 days),
                                OVERDUE    (3-60 days),
                                STALE      (60+ days — needs cleanup,
                                            not recovery)
                              Same-day completions (lag = 0) are excluded
                              because they meet the real-time regulatory
                              standard.

                              NOTE: this view only catches runs that exist
                              as shells in Canix. Runs that finish physically
                              without any Canix entry at all are invisible
                              to this view by design.

      - recent_activity (single object): counts of runs_started and
                              runs_completed across today / 7d / 30d windows
                              from v_qc_recent_activity.

    Thresholds anchored to 9 NYCRR § 125.8(a)(8)(i) cultivation 3-day rule
    as the closest numeric analog to NYS's real-time inventory tracking
    requirement for processors.
    """
    try:
        outstanding = query_view("v_compliance_timing")
        recent_activity_rows = query_view("v_qc_recent_activity")

        # Recent activity view returns exactly one row; surface as object
        recent_activity = recent_activity_rows[0] if recent_activity_rows else None

        return {
            "source": "bigquery",
            "view": "v_compliance_timing + v_qc_recent_activity",
            "outstanding": outstanding,
            "outstanding_count": len(outstanding),
            "recent_activity": recent_activity,
        }
    except Exception as e:
        logger.error(f"qc_dashboard failed: {e}")
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")