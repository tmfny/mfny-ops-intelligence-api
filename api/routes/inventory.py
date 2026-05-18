"""
Inventory endpoints — thin wrappers around BigQuery views.

Each endpoint queries a corresponding view in canix_raw and returns the rows
as JSON. The views encapsulate all business logic (status mappings, exclusions,
unit handling, etc.) — these endpoints just pass results through.

If you need to change what an endpoint returns, change the view, not this file.
"""

from fastapi import APIRouter, HTTPException, Query
from api.bq_client import query_view
import logging

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/ops/inventory_health")
def inventory_health(unit: str | None = Query(None, description="Filter to a specific unit_of_measure (Each / Grams / Pounds). Omit for all units.")):
    """
    Returns aggregated inventory health by unit_of_measure.

    Each row represents one unit type with package counts and quantities
    broken down by category: sellable, reserved, non_sellable, wip, staged.

    Returns 3 rows by default (Each, Grams, Pounds). Pass ?unit=Each to filter.
    """
    try:
        where = f"unit_of_measure = '{unit}'" if unit else None
        rows = query_view("v_inventory_health", where=where)
        return {
            "source": "bigquery",
            "view": "v_inventory_health",
            "rows": rows,
            "row_count": len(rows)
        }
    except Exception as e:
        logger.error(f"inventory_health failed: {e}")
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")


@router.get("/ops/sellable_by_sku")
def sellable_by_sku(
    category: str | None = Query(None, description="Filter to a specific item_category"),
    limit: int | None = Query(None, description="Maximum rows to return")
):
    """
    Returns sellable inventory at the SKU level.

    One row per (item_name, subcategory, strain, unit_of_measure) for active
    packages with status 'Available To Sell'. Excludes pre-METRC legacy data.

    For 'Each' SKUs, total_weight_lbs and total_weight_grams are NULL because
    'Each' represents unit count, not mass. Use total_quantity for the count.
    """
    try:
        where = f"item_category = '{category}'" if category else None
        order_by = "item_category, item_name, total_quantity DESC"
        rows = query_view("v_sellable_inventory_by_sku", where=where,
                          order_by=order_by, limit=limit)
        return {
            "source": "bigquery",
            "view": "v_sellable_inventory_by_sku",
            "rows": rows,
            "row_count": len(rows)
        }
    except Exception as e:
        logger.error(f"sellable_by_sku failed: {e}")
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")


@router.get("/ops/biomass_inventory")
def biomass_inventory():
    """
    Returns biomass inventory aggregated by item, strain, and location.

    Includes Wet Whole Plant, Dry Whole Plant, Biomass, Trim, Fresh Frozen,
    Shake, Hemp Biomass categories. Only currently-sellable biomass packages.
    """
    try:
        rows = query_view("v_biomass_inventory")
        return {
            "source": "bigquery",
            "view": "v_biomass_inventory",
            "rows": rows,
            "row_count": len(rows)
        }
    except Exception as e:
        logger.error(f"biomass_inventory failed: {e}")
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")


@router.get("/ops/dead_inventory")
def dead_inventory(reason: str | None = Query(None, description="Filter by dead_reason: pre_metrc_facility_4475, pre_metrc_legacy_category, pre_metrc_packaged_date_still_active")):
    """
    Returns zombie packages — pre-METRC legacy records still flagged as active.

    These need cleanup in Canix. Each row represents one package with full
    detail (id, tag, item_name, status, dates, location, facility) and a
    dead_reason indicating why it's flagged.

    Filter by ?reason= to focus on a specific cleanup category.
    """
    try:
        where = f"dead_reason = '{reason}'" if reason else None
        order_by = "facility_id, days_since_packaged DESC"
        rows = query_view("v_dead_inventory", where=where, order_by=order_by)
        return {
            "source": "bigquery",
            "view": "v_dead_inventory",
            "rows": rows,
            "row_count": len(rows)
        }
    except Exception as e:
        logger.error(f"dead_inventory failed: {e}")
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")