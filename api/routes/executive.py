"""
Executive endpoints — feed the investor-grade Executive page in Retool.

Currently a single endpoint backed by v_executive_flow, which consolidates
seven hero-node values into a single row. As the Executive page expands,
additional endpoints will live here (production_funnel, cultivation_pipeline,
forward_signals, etc.).
"""

from fastapi import APIRouter, HTTPException
from api.bq_client import query_view
import logging

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/executive/flow")
def executive_flow():
    """
    Returns the consolidated row of values displayed in the Executive page
    hero flow diagram. Always returns exactly one row with twelve columns —
    one per node, with weight/package_count pairs for the material-pressure
    nodes.

    Column reference (by hero node):
      1. cultivation_facility_count       — hardcoded 4
      2. fresh_frozen_lbs / _packages     — from v_material_pressure
      3. cured_flower_lbs / _packages     — from v_material_pressure
      4. active_production_count          — filtered v_active_production
      5. extracted_grams / _packages      — from v_material_pressure
      6. concentrate_ready_grams / _pkgs  — from v_material_pressure
      7. finished_units_shipped /         — bronze_packages where facility_id=4510
         finished_distinct_skus             and status='Transferred'
    """
    try:
        rows = query_view("v_executive_flow")
        return {
            "source": "bigquery",
            "view": "v_executive_flow",
            "rows": rows,
            "row_count": len(rows)
        }
    except Exception as e:
        logger.error(f"executive_flow failed: {e}")
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")
