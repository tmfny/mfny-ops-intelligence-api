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
    

@router.get("/ops/kpi_strip")
def kpi_strip():
    """
    Powers the Operations page KPI strip (RIGHT NOW zone).

    Combines two views:
      - v_inventory_health_multi_uom: finished-goods buckets (sellable,
        reserved, non_sellable, in_progress) at facilities 4510 + 4511,
        item_category '% - Each'. Multi-UoM aware.
      - v_pre_production_inventory: upstream bulk material at the processor
        (4511) — biomass + concentrates + kief, all UoMs. Single summary row.

    finished_goods is a list (one row per status_bucket).
    pre_production is a single object (summary), or null if empty.
    """
    try:
        finished_goods = query_view("v_inventory_health_multi_uom")
        pre_production_rows = query_view("v_pre_production_inventory")
        return {
            "source": "bigquery",
            "view": "v_inventory_health_multi_uom + v_pre_production_inventory",
            "finished_goods": finished_goods,
            "pre_production": pre_production_rows[0] if pre_production_rows else None,
            "row_count": len(finished_goods)
        }
    except Exception as e:
        logger.error(f"kpi_strip failed: {e}")
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")


@router.get("/ops/inventory_breakdown")
def inventory_breakdown(
    scope: str | None = Query(None, description="Filter by scope: finished_goods or pre_production"),
    status_bucket: str | None = Query(None, description="Filter by status_bucket: sellable, reserved, non_sellable, in_progress, pre_production")
):
    """
    Powers the KPI tile drill-down modal.

    Returns un-aggregated rows from v_inventory_health_breakdown — one row per
    (scope, status_bucket, canix_status, item_category, unit_of_measure,
    facility_id). This is the detail behind each KPI number.

    Filter with ?scope= and/or ?status_bucket= to drill into one tile.
    """
    try:
        clauses = []
        if scope:
            clauses.append(f"scope = '{scope}'")
        if status_bucket:
            clauses.append(f"status_bucket = '{status_bucket}'")
        where = " AND ".join(clauses) if clauses else None
        order_by = "scope, status_bucket, canix_status, item_category, unit_of_measure"
        rows = query_view("v_inventory_health_breakdown", where=where, order_by=order_by)
        return {
            "source": "bigquery",
            "view": "v_inventory_health_breakdown",
            "rows": rows,
            "row_count": len(rows)
        }
    except Exception as e:
        logger.error(f"inventory_breakdown failed: {e}")
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