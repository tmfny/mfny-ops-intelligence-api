"""
Distribution endpoints — feed the Distribution page in Retool.

Audience: Amber (Inventory / Logistics / Distribution). Focuses on
operational visibility into the order-to-delivery pipeline at the
distribution facility (4510).

Endpoints:
  GET /distribution/dashboard — Combined response with multiple zones:
                                  - status_strip: top-of-page scalars
                                                   (inventory + cutoff state)
                                  - pending_orders: orders in pre-shipment
                                                     statuses, bucketed by
                                                     delivery date urgency
                                  (more zones to come as page builds out:
                                   today's deliveries, ready for transfer)
"""
from datetime import datetime
from zoneinfo import ZoneInfo

from fastapi import APIRouter, HTTPException
from google.cloud import bigquery

from api.bq_client import query_view

import logging
logger = logging.getLogger(__name__)

router = APIRouter()

PROJECT_ID = "mfny-to-bigquery"
DATASET = "canix_raw"

# Distribution facility (per project knowledge).
DISTRIBUTION_FACILITY_ID = "4510"

# Order cutoff time. Orders submitted before this ship same-day;
# orders after ship next business day. America/New_York timezone.
CUTOFF_HOUR_ET = 16  # 4 PM
CUTOFF_MINUTE_ET = 30  # :30


def _compute_cutoff_status() -> dict:
    """
    Returns the current cutoff state as an object.

    Cutoff is 4:30 PM America/New_York. Before that: OPEN.
    After that: CLOSED (orders will ship next business day).
    """
    now_et = datetime.now(ZoneInfo("America/New_York"))
    cutoff_today = now_et.replace(
        hour=CUTOFF_HOUR_ET,
        minute=CUTOFF_MINUTE_ET,
        second=0,
        microsecond=0,
    )

    is_open = now_et < cutoff_today
    return {
        "state": "OPEN" if is_open else "CLOSED",
        "cutoff_label": "4:30 PM ET",
        "current_time_et": now_et.strftime("%I:%M %p ET"),
        "is_open": is_open,
    }


def _fetch_status_strip_inventory() -> dict:
    """
    Returns inventory aggregates for the Distribution Status strip:
      - total_packages: all active packages at 4510
      - available_packages: packages with status='Available To Sell'
      - available_units: sum of weight on available packages
      - total_units: sum of weight on all active packages
    """
    client = bigquery.Client(project=PROJECT_ID)
    query = f"""
        SELECT
          COUNT(*) AS total_packages,
          COUNTIF(status = 'Available To Sell') AS available_packages,
          ROUND(SUM(CASE WHEN status = 'Available To Sell' THEN weight ELSE 0 END), 0) AS available_units,
          ROUND(SUM(weight), 0) AS total_units
        FROM `{PROJECT_ID}.{DATASET}.bronze_packages`
        WHERE facility_id = '{DISTRIBUTION_FACILITY_ID}'
          AND status NOT IN ('Transferred', 'Finished', 'Inactive')
    """
    result = list(client.query(query).result())
    if not result:
        return {
            "total_packages": 0,
            "available_packages": 0,
            "available_units": 0,
            "total_units": 0,
        }
    row = result[0]
    return {
        "total_packages": int(row.total_packages or 0),
        "available_packages": int(row.available_packages or 0),
        "available_units": int(row.available_units or 0),
        "total_units": int(row.total_units or 0),
    }


@router.get("/distribution/dashboard")
def distribution_dashboard():
    """
    Returns data for the Distribution page in Retool.

    Response zones:
      - status_strip (object): top-of-page scalars combining inventory
                                aggregates with cutoff state.
      - pending_orders (list): sales orders at facility 4510 with status
                                IN ('created', 'approved', 'requested'),
                                bucketed by delivery_date urgency.
      - pending_orders_count (int): total pending orders.
      - pending_orders_summary (object): aggregate counts and dollar
                                          values per urgency bucket.
    """
    try:
        pending_orders = query_view("v_distribution_pending_orders")

        # Pending orders summary (computed in-loop to avoid two passes)
        bucket_counts = {"OVERDUE": 0, "TODAY": 0, "UPCOMING": 0, "NO_DATE": 0}
        bucket_values = {"OVERDUE": 0.0, "TODAY": 0.0, "UPCOMING": 0.0, "NO_DATE": 0.0}
        longest_days_overdue = 0

        for row in pending_orders:
            bucket = row.get("urgency_bucket", "NO_DATE")
            if bucket in bucket_counts:
                bucket_counts[bucket] += 1
                bucket_values[bucket] += float(row.get("total_price") or 0)
            days = row.get("days_to_delivery")
            if days is not None and days < 0:
                longest_days_overdue = max(longest_days_overdue, abs(days))

        total_value = sum(bucket_values.values())

        pending_summary = {
            "total_orders": len(pending_orders),
            "overdue_count": bucket_counts["OVERDUE"],
            "today_count": bucket_counts["TODAY"],
            "upcoming_count": bucket_counts["UPCOMING"],
            "no_date_count": bucket_counts["NO_DATE"],
            "overdue_value": round(bucket_values["OVERDUE"], 2),
            "today_value": round(bucket_values["TODAY"], 2),
            "upcoming_value": round(bucket_values["UPCOMING"], 2),
            "no_date_value": round(bucket_values["NO_DATE"], 2),
            "total_value": round(total_value, 2),
            "longest_days_overdue": longest_days_overdue,
        }

        # Status strip: inventory + cutoff
        inventory = _fetch_status_strip_inventory()
        cutoff = _compute_cutoff_status()
        status_strip = {
            **inventory,
            "pending_orders": pending_summary["total_orders"],
            "cutoff": cutoff,
        }

        return {
            "source": "bigquery",
            "view": "v_distribution_pending_orders + bronze_packages",
            "status_strip": status_strip,
            "pending_orders": pending_orders,
            "pending_orders_count": len(pending_orders),
            "pending_orders_summary": pending_summary,
        }
    except Exception as e:
        logger.error(f"distribution_dashboard failed: {e}")
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")
