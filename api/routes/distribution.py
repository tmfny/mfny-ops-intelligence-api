"""
Distribution endpoints — feed the Distribution page in Retool.

Audience: Amber (Inventory / Logistics / Distribution). Focuses on
operational visibility into the order-to-delivery pipeline at the
distribution facility (4510).

Endpoints:
  GET /distribution/dashboard — Combined response with multiple zones:
                                  - pending_orders: orders in pre-shipment
                                                     statuses, bucketed by
                                                     delivery date urgency
                                  (more zones to come as page builds out:
                                   today's deliveries, ready for transfer,
                                   distribution status strip)
"""
from fastapi import APIRouter, HTTPException
from api.bq_client import query_view

import logging
logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/distribution/dashboard")
def distribution_dashboard():
    """
    Returns data for the Distribution page in Retool.

    Response zones:
      - pending_orders (list): rows from v_distribution_pending_orders —
                                sales orders at facility 4510 with status
                                IN ('created', 'approved', 'requested').
                                Bucketed by delivery_date urgency:
                                  OVERDUE   (delivery_date < today),
                                  TODAY     (delivery_date = today),
                                  UPCOMING  (delivery_date > today),
                                  NO_DATE   (delivery_date IS NULL)
                                Sorted: OVERDUE first (oldest first),
                                then TODAY, then UPCOMING, then NO_DATE.
      - pending_orders_count (int): total pending orders.
      - pending_orders_summary (object): aggregate counts and total
                                          dollar value per urgency bucket
                                          plus overall totals.
    """
    try:
        pending_orders = query_view("v_distribution_pending_orders")

        # Build summary aggregates so the hero line and KPI tiles can
        # render without recomputing in Retool.
        bucket_counts = {"OVERDUE": 0, "TODAY": 0, "UPCOMING": 0, "NO_DATE": 0}
        bucket_values = {"OVERDUE": 0.0, "TODAY": 0.0, "UPCOMING": 0.0, "NO_DATE": 0.0}
        longest_days_overdue = 0

        for row in pending_orders:
            bucket = row.get("urgency_bucket", "NO_DATE")
            if bucket in bucket_counts:
                bucket_counts[bucket] += 1
                bucket_values[bucket] += float(row.get("total_price") or 0)

            # Track longest overdue period for the hero summary
            days = row.get("days_to_delivery")
            if days is not None and days < 0:
                longest_days_overdue = max(longest_days_overdue, abs(days))

        total_value = sum(bucket_values.values())

        summary = {
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

        return {
            "source": "bigquery",
            "view": "v_distribution_pending_orders",
            "pending_orders": pending_orders,
            "pending_orders_count": len(pending_orders),
            "pending_orders_summary": summary,
        }
    except Exception as e:
        logger.error(f"distribution_dashboard failed: {e}")
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")
