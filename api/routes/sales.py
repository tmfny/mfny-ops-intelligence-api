"""
Sales endpoints — feed the Sales page in Retool.

Audience: Omar Massoud (Sales). Focuses on revenue tracking, sellable
inventory, expiring SKUs, and sales-rep-by-customer activity for
brand rep assignment decisions.

Notes on data semantics:
  - "Accepted" is MFNY's terminal order status (98% of orders). "Shipped"
    status is rarely used and has $0 paid — likely a transitional status
    that orders briefly pass through. Revenue metrics use status='accepted'
    as the "real order" filter.
  - Payment lag is significant: ~$23M total revenue vs $4.3M paid. Status
    strip surfaces both MTD revenue and MTD paid to make this visible.
  - bronze_packages uses 'weight' as universal quantity field even when
    unit_of_measure = 'Each'. At facility 4510, all sellable packages are
    'Each', so weight × price-per-unit gives the right value.
  - Strain type (indica/sativa/hybrid) intentionally deferred to v2 — no
    reliable data source in Canix today.

Endpoints:
  GET /sales/dashboard — Combined response with six zones:
                          - status_strip: 4 tile values (MTD revenue,
                                          MTD paid, sellable value,
                                          pending fulfillment)
                          - daily_revenue: 30-day daily revenue trend
                          - sellable_by_category: inventory grouped by
                                                   item_category
                          - sales_rep_activity: rep-customer pairs with
                                                 recency and lifetime
                                                 metrics
                          - expiring_skus: top 30 closest-to-expiry with
                                           tier classification
                          - sales_by_sku_monthly: reused from Lab page

  Future zones (deferred to v2):
    - Payment aging buckets ($23M vs $4.3M paid gap warrants visibility)
    - Strain type breakdown (needs strain → type mapping table)
    - Sales rep performance comparison (could be politically tricky)
"""
from fastapi import APIRouter, HTTPException

from api.bq_client import query_view

import logging
logger = logging.getLogger(__name__)

router = APIRouter()


def _classify_expiry_tier(days_until_expiry):
    """
    Bucket days_until_expiry into operational tiers for Retool color coding.
      - critical: <= 30 days
      - warning: 31-60 days
      - watch: 61-90 days
      - ok: 91+ days
    """
    if days_until_expiry is None:
        return "unknown"
    if days_until_expiry <= 30:
        return "critical"
    if days_until_expiry <= 60:
        return "warning"
    if days_until_expiry <= 90:
        return "watch"
    return "ok"


def _compute_expiring_summary(expiring_rows: list) -> dict:
    """
    Roll up expiring SKU rows into tier counts. Useful for the Retool
    zone header — Omar can see "X critical, Y warning, Z watch" at a glance.
    """
    if not expiring_rows:
        return {
            "total_packages": 0,
            "critical_count": 0,
            "warning_count": 0,
            "watch_count": 0,
            "ok_count": 0,
            "soonest_days": None,
        }

    tier_counts = {"critical": 0, "warning": 0, "watch": 0, "ok": 0, "unknown": 0}
    soonest = None

    for row in expiring_rows:
        days = row.get("days_until_expiry")
        tier = _classify_expiry_tier(days)
        tier_counts[tier] = tier_counts.get(tier, 0) + 1
        if days is not None and (soonest is None or days < soonest):
            soonest = days

    return {
        "total_packages": len(expiring_rows),
        "critical_count": tier_counts["critical"],
        "warning_count": tier_counts["warning"],
        "watch_count": tier_counts["watch"],
        "ok_count": tier_counts["ok"],
        "soonest_days": soonest,
    }


def _compute_rep_activity_summary(rep_rows: list) -> dict:
    """
    Roll up sales rep activity into team-level metrics for the Sales page.
    Identifies stale relationships (no activity in 60+ days) and the
    most-active reps by 30-day revenue.
    """
    if not rep_rows:
        return {
            "total_relationships": 0,
            "distinct_reps": 0,
            "distinct_customers": 0,
            "stale_relationships_60d": 0,
            "active_reps_30d": 0,
        }

    distinct_reps = set()
    distinct_customers = set()
    stale_60d = 0
    active_reps_30d = set()

    for row in rep_rows:
        rep = row.get("sales_rep_name")
        customer = row.get("customer_company_name")
        days_since = row.get("days_since_last_order")
        orders_30d = row.get("orders_last_30d") or 0

        if rep:
            distinct_reps.add(rep)
        if customer:
            distinct_customers.add(customer)
        if days_since is not None and days_since > 60:
            stale_60d += 1
        if orders_30d > 0 and rep:
            active_reps_30d.add(rep)

    return {
        "total_relationships": len(rep_rows),
        "distinct_reps": len(distinct_reps),
        "distinct_customers": len(distinct_customers),
        "stale_relationships_60d": stale_60d,
        "active_reps_30d": len(active_reps_30d),
    }


def _compute_inventory_category_summary(category_rows: list) -> dict:
    """
    Roll up inventory-by-category into top-level totals. Used for
    Retool subtitle/header context (e.g. "X categories, $Y total value").
    """
    if not category_rows:
        return {
            "total_categories": 0,
            "total_packages": 0,
            "total_units": 0,
            "total_value": 0.0,
        }

    return {
        "total_categories": len(category_rows),
        "total_packages": sum(int(r.get("package_count") or 0) for r in category_rows),
        "total_units": sum(float(r.get("total_units") or 0) for r in category_rows),
        "total_value": round(sum(float(r.get("estimated_value") or 0) for r in category_rows), 2),
    }


@router.get("/sales/dashboard")
def sales_dashboard():
    """
    Returns data for the Sales page in Retool.

    Response zones:
      - status_strip (object): scalar metrics for the 4 status tiles
      - daily_revenue (list): daily aggregations, last 30 days
      - sellable_by_category (list): inventory breakdown per item_category
      - sellable_summary (object): rolled-up category totals
      - sales_rep_activity (list): rep-customer pairs with recency
      - sales_rep_summary (object): team-level activity rollup
      - expiring_skus (list): top 30 closest-to-expiry with tier field
      - expiring_summary (object): tier counts for header context
      - sales_by_sku_monthly (list): reused from Lab page; trailing 12mo
                                      sales per SKU
    """
    try:
        # Zone 1: Status strip — single-row scalar view
        strip_rows = query_view("v_sales_status_strip")
        if not strip_rows:
            raise HTTPException(status_code=500, detail="v_sales_status_strip returned no rows")
        strip = strip_rows[0]

        status_strip = {
            "mtd_revenue": float(strip.get("mtd_revenue") or 0),
            "mtd_paid": float(strip.get("mtd_paid") or 0),
            "mtd_order_count": int(strip.get("mtd_order_count") or 0),
            "total_sellable_value": float(strip.get("total_sellable_value") or 0),
            "sellable_package_count": int(strip.get("sellable_package_count") or 0),
            "sellable_unit_count": int(strip.get("sellable_unit_count") or 0),
            "pending_order_count": int(strip.get("pending_order_count") or 0),
            "pending_order_value": float(strip.get("pending_order_value") or 0),
        }

        # Zone 2: Daily revenue — 30-day trend
        daily_revenue = query_view("v_sales_daily_revenue")

        # Zone 3: Sellable inventory by category
        sellable_by_category = query_view("v_sellable_inventory_by_category")
        sellable_summary = _compute_inventory_category_summary(sellable_by_category)

        # Zone 4: Sales rep activity — full rep-customer pair list
        sales_rep_activity = query_view("v_sales_rep_activity")
        sales_rep_summary = _compute_rep_activity_summary(sales_rep_activity)

        # Zone 5: Expiring SKUs — top 30 closest-to-expiry with tier
        # The view returns all 149 packages sorted by days_until_expiry ASC.
        # Slice to top 30 and add tier classification for color coding.
        all_expiring = query_view("v_expiring_skus")
        expiring_skus = []
        for row in all_expiring[:30]:
            # Convert to dict if not already (defensive — query_view should
            # already return dicts but no harm)
            row_dict = dict(row) if not isinstance(row, dict) else row.copy()
            row_dict["tier"] = _classify_expiry_tier(row_dict.get("days_until_expiry"))
            expiring_skus.append(row_dict)
        expiring_summary = _compute_expiring_summary(expiring_skus)

        # Zone 6: Sales by SKU monthly — reused from Lab page
        sales_by_sku_monthly = query_view("v_sales_by_sku_monthly")

        return {
            "source": "bigquery",
            "view": (
                "v_sales_status_strip + v_sales_daily_revenue + "
                "v_sellable_inventory_by_category + v_sales_rep_activity + "
                "v_expiring_skus + v_sales_by_sku_monthly"
            ),
            "status_strip": status_strip,
            "daily_revenue": daily_revenue,
            "sellable_by_category": sellable_by_category,
            "sellable_summary": sellable_summary,
            "sales_rep_activity": sales_rep_activity,
            "sales_rep_summary": sales_rep_summary,
            "expiring_skus": expiring_skus,
            "expiring_summary": expiring_summary,
            "sales_by_sku_monthly": sales_by_sku_monthly,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"sales_dashboard failed: {e}")
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")
