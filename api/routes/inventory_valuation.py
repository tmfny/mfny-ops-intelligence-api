"""
Operations endpoints backed by BigQuery views.

Lives alongside ops.py (Canix-direct legacy endpoints) but uses the BigQuery
query_view helper, matching the pattern established in executive.py.

Endpoints:
  GET /ops/inventory_valuation — Wholesale book value of sellable inventory at
                                 distribution facility 4510, with summary KPIs
                                 and three pre-aggregated breakdowns
                                 (by category, by strain, by SKU) plus the
                                 raw package-level rows.
"""

from fastapi import APIRouter, HTTPException
from api.bq_client import query_view
import logging

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/ops/inventory_valuation")
def ops_inventory_valuation():
    """
    Returns wholesale book value of sellable inventory at distribution
    facility 4510, drill-down-ready for the CFO's "Available to Sell"
    KPI on the Operations dashboard.

    Response combines five views:
      - summary:     single object with total_packages, total_units,
                     total_book_value, unmatched_packages, distinct_skus,
                     distinct_strains
      - by_category: 6 rows (Concentrates, Edibles, Vapes, Pre-Rolls,
                     Tinctures, Topicals), sorted by book_value DESC
      - by_strain:   N rows of named strains sorted by book_value DESC,
                     with "Unspecified" pushed to the bottom regardless
                     of value (94 packages currently have blank strain_name —
                     known data hygiene issue logged 2026-05-13)
      - by_sku:      ~20 rows (one per item_subcategory) with per-unit
                     wholesale price, sorted by book_value DESC
      - packages:    raw package-level rows for ad-hoc Retool filtering

    All five surfaces derive from v_inventory_valuation, which joins
    bronze_packages (facility 4510, status='Available To Sell', SAMPLES
    excluded) against dim_wholesale_prices on item_subcategory. Pricing
    is CFO-provided 2026-05-12.

    NOTE: weight (renamed unit_count in the view) is the count of
    sellable units in a package, NOT a physical weight. UoM='Each'
    across all sellable inventory at 4510.
    """
    try:
        summary_rows = query_view("v_inventory_valuation_summary")
        by_category  = query_view("v_inventory_valuation_by_category")
        by_strain    = query_view("v_inventory_valuation_by_strain_drillable")
        by_sku       = query_view("v_inventory_valuation_by_sku_drillable")
        packages     = query_view("v_inventory_valuation")

        summary = summary_rows[0] if summary_rows else None

        return {
            "source": "bigquery",
            "view": (
                "v_inventory_valuation_summary + "
                "v_inventory_valuation_by_category + "
                "v_inventory_valuation_by_strain_drillable + "
                "v_inventory_valuation_by_sku_drillable + "
                "v_inventory_valuation"
            ),
            "summary": summary,
            "by_category": by_category,
            "by_strain": by_strain,
            "by_sku": by_sku,
            "packages": packages,
            "package_count": len(packages),
        }
    except Exception as e:
        logger.error(f"ops_inventory_valuation failed: {e}")
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")