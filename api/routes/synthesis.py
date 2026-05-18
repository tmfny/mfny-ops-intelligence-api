"""
Phase 6 synthesis endpoints.

These endpoints aggregate or synthesize data across multiple bronze tables
or views to surface higher-level operational signals — material pressure,
system alerts, etc.
"""
from fastapi import APIRouter, HTTPException
from api.bq_client import query_view
import logging

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