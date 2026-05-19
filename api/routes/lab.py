"""
Lab/Extraction endpoints — feed the Lab/Extraction page in Retool.

Audience: Galen (Lab / Extraction). Focuses on biomass and concentrate
inventory at the processor (4511), plus sales velocity for allocation
context.

Endpoints:
  GET /lab/dashboard — Combined response with three zones:
                        - status_strip: 4 tiles (Fresh Frozen, Concentrate,
                                                  Decarb, Active Extraction Runs)
                        - biomass_inventory: rows by (stage, subcategory, strain)
                                              at the processor
                        - sales_by_sku: monthly units/revenue per SKU,
                                         trailing 12 months, deduped by name

  Future zones to consider:
    - Greenhouse Status (pending bronze_plants ingestion)
    - Lab Testing Pipeline (blocked on workflow gap: test_status NULL)
    - Hash Allocation Forecast (pending package lineage extraction)
"""
from fastapi import APIRouter, HTTPException

from api.bq_client import query_view

import logging
logger = logging.getLogger(__name__)

router = APIRouter()


def _compute_biomass_summary(biomass_rows: list) -> dict:
    """
    Roll up the biomass inventory rows into top-level totals per stage group.

    Stage groups in v_lab_biomass_inventory:
      - Biomass Inputs (in Pounds)
      - Live Resin (BHO) (in Grams)
      - Live Rosin (SHO) (in Grams)
    """
    biomass_inputs_lbs = 0.0
    live_resin_bho_g = 0.0
    live_rosin_sho_g = 0.0
    strains = set()

    for row in biomass_rows:
        stage = row.get("stage_group")
        weight = float(row.get("total_weight") or 0)
        strain = row.get("strain_name")

        if stage == "Biomass Inputs":
            biomass_inputs_lbs += weight
        elif stage == "Live Resin (BHO)":
            live_resin_bho_g += weight
        elif stage == "Live Rosin (SHO)":
            live_rosin_sho_g += weight

        if strain and strain != "(no strain specified)":
            strains.add(strain)

    return {
        "biomass_inputs_total_lbs": round(biomass_inputs_lbs, 2),
        "live_resin_bho_total_g": round(live_resin_bho_g, 2),
        "live_rosin_sho_total_g": round(live_rosin_sho_g, 2),
        "distinct_strains_in_pipeline": len(strains),
    }


def _compute_sales_summary(sales_rows: list) -> dict:
    """
    Compute trailing-window aggregates and top-seller highlights from
    the monthly sales-by-SKU rows.

    Rows from v_sales_by_sku_monthly are sorted by (sale_month DESC,
    units_sold DESC) — i.e., most-recent month first, biggest sellers
    first within each month.
    """
    if not sales_rows:
        return {
            "total_units_12mo": 0,
            "total_revenue_12mo": 0.0,
            "distinct_skus": 0,
            "months_covered": 0,
            "best_selling_sku_current_month": None,
            "top_5_skus_current_month": [],
        }

    # Identify the most recent month (rows are already sorted DESC)
    latest_month = sales_rows[0].get("sale_month")

    # All-12mo totals
    total_units = 0.0
    total_revenue = 0.0
    distinct_skus = set()
    months = set()

    # Current-month aggregates (for top-sellers highlight)
    current_month_rows = []

    for row in sales_rows:
        total_units += float(row.get("units_sold") or 0)
        total_revenue += float(row.get("revenue") or 0)
        if row.get("item_name"):
            distinct_skus.add(row["item_name"])
        if row.get("sale_month"):
            months.add(str(row["sale_month"]))
        if row.get("sale_month") == latest_month:
            current_month_rows.append(row)

    # Top sellers in current month, by units_sold
    # (rows are already sorted by units_sold DESC within the month)
    top_5_by_units = current_month_rows[:5]
    best_selling = top_5_by_units[0].get("item_name") if top_5_by_units else None

    return {
        "total_units_12mo": int(total_units),
        "total_revenue_12mo": round(total_revenue, 2),
        "distinct_skus": len(distinct_skus),
        "months_covered": len(months),
        "best_selling_sku_current_month": best_selling,
        "top_5_skus_current_month": [
            {
                "item_name": r.get("item_name"),
                "units_sold": int(r.get("units_sold") or 0),
                "revenue": float(r.get("revenue") or 0),
            }
            for r in top_5_by_units
        ],
    }


@router.get("/lab/dashboard")
def lab_dashboard():
    """
    Returns data for the Lab/Extraction page in Retool.

    Response zones:
      - status_strip (object): 4 tile values (fresh_frozen, concentrate,
                                decarb, active_extraction_runs).
      - biomass_inventory (list): rows per (stage_group, subcategory,
                                  strain) at processor facility 4511.
      - biomass_summary (object): top-level totals per stage group.
      - sales_by_sku (list): trailing 12 months of monthly sales per
                              SKU at facility 4510, deduplicated by
                              item_name (Canix's same-product-different-
                              item_id issue handled here).
      - sales_summary (object): all-12mo totals plus top-5 current-month
                                 highlight.
    """
    try:
        # Zone: Status strip — single-row scalar view
        strip_rows = query_view("v_lab_status_strip")
        if not strip_rows:
            raise HTTPException(status_code=500, detail="v_lab_status_strip returned no rows")
        strip = strip_rows[0]

        status_strip = {
            "fresh_frozen": {
                "weight": float(strip.get("fresh_frozen_lbs") or 0),
                "unit": strip.get("fresh_frozen_unit") or "Pounds",
                "pkg_count": int(strip.get("fresh_frozen_pkg_count") or 0),
            },
            "concentrate": {
                "weight": float(strip.get("concentrate_g") or 0),
                "unit": strip.get("concentrate_unit") or "Grams",
                "pkg_count": int(strip.get("concentrate_pkg_count") or 0),
            },
            "decarb": {
                "weight": float(strip.get("decarb_g") or 0),
                "unit": strip.get("decarb_unit") or "Grams",
                "pkg_count": int(strip.get("decarb_pkg_count") or 0),
            },
            "active_extraction_runs": int(strip.get("active_extraction_run_count") or 0),
        }

        # Zone: Biomass inventory — rows per (stage, subcategory, strain)
        biomass_inventory = query_view("v_lab_biomass_inventory")
        biomass_summary = _compute_biomass_summary(biomass_inventory)

        # Zone: Sales by SKU — monthly rows, trailing 12 months, deduped
        sales_by_sku = query_view("v_sales_by_sku_monthly")
        sales_summary = _compute_sales_summary(sales_by_sku)

        return {
            "source": "bigquery",
            "view": "v_lab_biomass_inventory + v_lab_status_strip + v_sales_by_sku_monthly",
            "status_strip": status_strip,
            "biomass_inventory": biomass_inventory,
            "biomass_summary": biomass_summary,
            "sales_by_sku": sales_by_sku,
            "sales_summary": sales_summary,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"lab_dashboard failed: {e}")
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")
