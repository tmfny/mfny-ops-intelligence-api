"""
Executive endpoints — feed the investor-grade Executive page in Retool.

Endpoints:
  GET /executive/flow        — Hero subway-map values (single row from v_executive_flow)
  GET /executive/throughput  — Production Story zone: monthly chart rows + KPI summary
                               (combines v_executive_production_throughput and
                                v_executive_production_summary into one response)
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


@router.get("/executive/throughput")
def executive_throughput():
    """
    Returns data for the Production Story zone on the Executive page.

    Response shape combines two views:
      - chart_rows: list of (month, stage, run_count) for the stacked bar chart.
                    Covers Apr 2025 (operational launch) through current month.
      - summary:    single object with KPI tile values (last_month_runs,
                    trailing_3mo_avg, peak_runs) and their period-over-period
                    deltas. Excludes the current partial month for honest M/M
                    comparison.

    Used by:
      - Retool Executive page chart component (chart_rows)
      - Retool KPI tiles for last month / 3-month avg / all-time peak (summary)
    """
    try:
        chart_rows = query_view("v_executive_production_throughput")
        summary_rows = query_view("v_executive_production_summary")

        # Summary view returns exactly one row; surface as a single object.
        summary = summary_rows[0] if summary_rows else None

        return {
            "source": "bigquery",
            "view": "v_executive_production_throughput + v_executive_production_summary",
            "chart_rows": chart_rows,
            "chart_row_count": len(chart_rows),
            "summary": summary
        }
    except Exception as e:
        logger.error(f"executive_throughput failed: {e}")
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")
    
@router.get("/executive/inventory")
def executive_inventory():
    """
    Returns data for the Inventory Health zone on the Executive page.

    Response combines two views:
      - chart_rows: list of (category, total_packages, sellable_packages,
                    transferred_packages) for the inventory-by-category chart.
                    Six rows: Concentrates, Edibles, Vapes, Pre-Rolls,
                    Tinctures, Topicals.
      - summary:    single object with KPI tile values (sellable_now,
                    cumulative_shipped, days_of_inventory, etc.).

    Distribution facility 4510 is the source of truth for finished-goods
    inventory and shipping. Biomass / intermediate categories are excluded
    from this zone.
    """
    try:
        chart_rows = query_view("v_executive_inventory_health")
        summary_rows = query_view("v_executive_inventory_summary")

        summary = summary_rows[0] if summary_rows else None

        return {
            "source": "bigquery",
            "view": "v_executive_inventory_health + v_executive_inventory_summary",
            "chart_rows": chart_rows,
            "chart_row_count": len(chart_rows),
            "summary": summary
        }
    except Exception as e:
        logger.error(f"executive_inventory failed: {e}")
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")