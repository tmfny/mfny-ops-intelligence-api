"""
Compliance & activity endpoints.

Tracks data hygiene at the Canix layer — what's physically complete but
not yet submitted, what's been recently changed, what's expiring soon.

Primary audience: Quality Control (Emma) and operations leadership
monitoring METRC compliance readiness.
"""

from fastapi import APIRouter, HTTPException, Query
from api.bq_client import query_view
import logging

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/ops/compliance_timing")
def compliance_timing(
    risk: str | None = Query(None, description="Filter by compliance_risk: OVERDUE, AT_RISK, PENDING, OK")
):
    """
    Returns runs that are physically complete but not yet submitted in Canix,
    or otherwise blocking compliance — including errored runs and pending
    approvals.

    Each row has a compliance_risk classification:
    - OVERDUE: physically complete > 7 days, still not submitted
    - AT_RISK: physically complete 3-7 days, still not submitted
    - PENDING: physically complete < 3 days
    - OK: not currently a compliance issue

    Sorted by risk priority (OVERDUE first), then by days_since_end DESC.
    """
    try:
        where = f"compliance_risk = '{risk}'" if risk else None
        rows = query_view("v_compliance_timing", where=where)
        return {
            "source": "bigquery",
            "view": "v_compliance_timing",
            "rows": rows,
            "row_count": len(rows)
        }
    except Exception as e:
        logger.error(f"compliance_timing failed: {e}")
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")


@router.get("/ops/recent_activity")
def recent_activity(
    activity_type: str | None = Query(None, description="Filter by activity_type: PACKAGE CREATED, RUN SUBMITTED, RUN ERRORED, PENDING APPROVAL, RUN UPDATED"),
    limit: int = Query(100, description="Max rows to return")
):
    """
    Returns Canix activity from the last 7 days.

    Combines package creation events and run state changes into a single
    timeline, sorted newest first. Each row shows what happened, when,
    and where.

    Used by ops leadership to get a quick read on whether Canix is being
    actively maintained ("are we entering data?") and by QC to spot
    unusual activity patterns.
    """
    try:
        # query_view doesn't support LIMIT well with our envelope, so we
        # apply it via the view's natural ordering (already DESC by activity_date)
        where = f"activity_type = '{activity_type}'" if activity_type else None
        rows = query_view("v_recent_canix_activity", where=where, limit=limit)
        return {
            "source": "bigquery",
            "view": "v_recent_canix_activity",
            "rows": rows,
            "row_count": len(rows),
            "limit_applied": limit
        }
    except Exception as e:
        logger.error(f"recent_activity failed: {e}")
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")


@router.get("/ops/expiring_skus")
def expiring_skus(
    expiry_status: str | None = Query(None, description="Filter by expiry_status: EXPIRED, CRITICAL, WARNING, OK"),
    limit: int | None = Query(None, description="Max rows to return")
):
    """
    Returns sellable packages with their expiration risk classification:
    - EXPIRED: use_by_date is in the past
    - CRITICAL: expires within 30 days
    - WARNING: expires within 90 days
    - OK: more than 90 days until expiry

    NOTE: The 'quantity' field in this view is currently NULL for all rows
    (a known issue — the underlying view hasn't been rewritten yet). Use
    'weight' for unit counts on 'Each' SKUs. To be addressed in a later cleanup.

    Sorted by expiry urgency (EXPIRED first), then by use_by_date.
    """
    try:
        where = f"expiry_status = '{expiry_status}'" if expiry_status else None
        rows = query_view("v_expiring_skus", where=where, limit=limit)
        return {
            "source": "bigquery",
            "view": "v_expiring_skus",
            "rows": rows,
            "row_count": len(rows),
            "known_issues": ["'quantity' field is NULL — use 'weight' instead"]
        }
    except Exception as e:
        logger.error(f"expiring_skus failed: {e}")
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")