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

from datetime import date, datetime, timedelta


def _to_date(v):
    """BigQuery may hand back a date as a date object or an ISO string. Normalize."""
    if v is None:
        return None
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()
    try:
        return datetime.strptime(str(v)[:10], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def _month(v):
    """Format a date field as 'May 2026'. Returns '' if unparseable."""
    d = _to_date(v)
    return d.strftime("%B %Y") if d else ""


def _prior_month_name(v):
    """Month name of the month before the given date, e.g. 'April'. No view change needed."""
    d = _to_date(v)
    if not d:
        return ""
    # first of this month, minus one day, lands in the prior month
    first = d.replace(day=1)
    prior = first - timedelta(days=1)
    return prior.strftime("%B")


def _pct(v):
    """A SAFE_DIVIDE ratio (e.g. -0.131) as a whole-number percent string '13'. '' if None."""
    if v is None:
        return ""
    return f"{abs(round(float(v) * 100))}"


def _commas(v):
    """Integer-ish value with thousands separators. '' if None."""
    if v is None:
        return ""
    try:
        return f"{int(round(float(v))):,}"
    except (ValueError, TypeError):
        return ""

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

        caption = None
        if summary:
            month = _month(summary.get("last_month_date"))
            prior = _prior_month_name(summary.get("last_month_date"))
            runs = summary.get("last_month_runs")
            pct = summary.get("last_month_pct_change")
            direction = "up" if (pct or 0) >= 0 else "down"
            avg = summary.get("trailing_3mo_avg")
            peak = summary.get("peak_runs")
            peak_month = _month(summary.get("peak_month_date"))
            pct_of_peak = _pct(summary.get("last_month_pct_of_peak"))

            caption = (
                f"{month} completed {_commas(runs)} production runs, "
                f"{direction} {_pct(pct)}% from {prior}. "
                f"The trailing three-month average is {_commas(avg)} runs per month "
                f"— {pct_of_peak}% of the all-time peak of {_commas(peak)} runs in {peak_month}. "
                f"Production dipped Aug\u2013Dec 2025 during a four-month migration to METRC "
                f"compliance tracking; volumes have since recovered toward prior peaks."
            )

        return {
            "source": "bigquery",
            "view": "v_executive_production_throughput + v_executive_production_summary",
            "chart_rows": chart_rows,
            "chart_row_count": len(chart_rows),
            "summary": summary,
            "caption": caption
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

        caption = None
        if summary:
            sellable = summary.get("sellable_now")
            shipped = summary.get("cumulative_shipped")
            cats = len(chart_rows) if chart_rows else 0
            caption = (
                f"{_commas(sellable)} packages are sellable-ready across "
                f"{cats} product categories. {_commas(shipped)} packages have shipped "
                f"cumulatively from distribution."
            )

        return {
            "source": "bigquery",
            "view": "v_executive_inventory_health + v_executive_inventory_summary",
            "chart_rows": chart_rows,
            "chart_row_count": len(chart_rows),
            "summary": summary,
            "caption": caption
        }
    except Exception as e:
        logger.error(f"executive_inventory failed: {e}")
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")
    
@router.get("/executive/biomass")
def executive_biomass():
    """
    Returns data for the Biomass at Processor zone on the Executive page.

    Response combines two views:
      - chart_rows: list of (strain, package_count, total_weight_lbs,
                    avg_days_on_hand) for the strain breakdown chart.
                    Sorted by total_weight_lbs descending.
      - summary:    single object with total_biomass_lbs, total_packages,
                    distinct_strains, top_strain_name, top_strain_lbs.

    Filters: facility 4511 (processor), biomass categories only, active
    packages only, weight > 0. Wet Whole Plant excluded due to unreliable
    weight data.
    """
    try:
        chart_rows = query_view("v_executive_biomass_health")
        summary_rows = query_view("v_executive_biomass_summary")

        summary = summary_rows[0] if summary_rows else None

        caption = None
        if summary:
            total = summary.get("total_biomass_lbs")
            strains = summary.get("distinct_strains")
            pkgs = summary.get("total_packages")
            top_name = summary.get("top_strain_name")
            top_lbs = summary.get("top_strain_lbs")
            caption = (
                f"{_commas(total)} lbs of biomass on hand across {strains} strains "
                f"and {_commas(pkgs)} packages. The largest holding is {top_name} "
                f"at {_commas(top_lbs)} lbs."
            )

        return {
            "source": "bigquery",
            "view": "v_executive_biomass_health + v_executive_biomass_summary",
            "chart_rows": chart_rows,
            "chart_row_count": len(chart_rows),
            "summary": summary,
            "caption": caption
        }
    except Exception as e:
        logger.error(f"executive_biomass failed: {e}")
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")