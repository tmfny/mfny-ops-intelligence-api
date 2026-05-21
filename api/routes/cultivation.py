"""
Cultivation endpoints — feed the Cultivation page in Retool.

Audience: Cultivation team (Ian Dyche identified as primary respondent;
broader team includes leads at the two active cultivation facilities:
4509 (MFNY Ops Cultivation) and 4528 (American Seed & Oil)).

Note on data trust: Canix's cultivation date fields (vegetative_date,
flowering_date) have semantics that don't match their names — averages
don't reconcile with real cannabis cycle biology. harvested_date IS NOT
NULL DOES reliably indicate a plant is harvested (confirmed 2026-05-20).
We've intentionally limited this v1 page to plant counts, phase splits,
greenhouse/strain aggregations, and run-level yield. Date-derived
lifecycle metrics deferred until field meanings clarified with Ian.

Endpoints:
  GET /cultivation/dashboard — Combined response with four zones:
                                - status_strip: 6 tile values (total
                                                          plants, flowering,
                                                          vegetative,
                                                          active greenhouses,
                                                          strains, facilities)
                                - greenhouse_status: rows per (facility,
                                                     greenhouse) — same view
                                                     used on Lab page
                                - strain_mix: rows per strain with phase
                                              split, batch/facility/greenhouse
                                              coverage
                                - yield_efficiency: run-level yield rows
                                                     sliced to last 90 days
                                                     (answers Ian's #1 wish)

  Future zones planned (deferred to v2):
    - Cultivation lifecycle timing (blocked on date-field clarification)
    - Yield efficiency v2: split extraction category into Fresh Freeze vs
      concentrate extraction for cleaner per-stage averages
"""
from datetime import date, timedelta
from fastapi import APIRouter, HTTPException

from api.bq_client import query_view

import logging
logger = logging.getLogger(__name__)

router = APIRouter()

# Yield window — number of days back from today to include in the dashboard.
# Per session 2026-05-21 design: 90-day window for daily check use case.
YIELD_WINDOW_DAYS = 90


def _compute_strain_mix_summary(strain_rows: list) -> dict:
    """
    Roll up strain mix into top-level metrics for the Cultivation page.

    Rows from v_cultivation_strain_mix are already sorted by total_plants DESC.
    """
    if not strain_rows:
        return {
            "total_strains": 0,
            "total_plants": 0,
            "top_strain": None,
            "top_5_strains": [],
            "strains_only_in_vegetative": 0,
            "strains_only_in_flowering": 0,
        }

    total_plants = 0
    only_veg = 0
    only_flower = 0

    for row in strain_rows:
        total_plants += int(row.get("total_plants") or 0)
        pct = float(row.get("pct_in_flower") or 0)
        if pct == 0.0:
            only_veg += 1
        elif pct >= 99.9:
            only_flower += 1

    top_5 = [
        {
            "strain_name": r.get("strain_name"),
            "total_plants": int(r.get("total_plants") or 0),
            "flowering_count": int(r.get("flowering_count") or 0),
            "vegetative_count": int(r.get("vegetative_count") or 0),
            "pct_in_flower": float(r.get("pct_in_flower") or 0),
        }
        for r in strain_rows[:5]
    ]

    return {
        "total_strains": len(strain_rows),
        "total_plants": total_plants,
        "top_strain": strain_rows[0].get("strain_name") if strain_rows else None,
        "top_5_strains": top_5,
        "strains_only_in_vegetative": only_veg,
        "strains_only_in_flowering": only_flower,
    }


def _compute_greenhouse_summary(greenhouse_rows: list) -> dict:
    """
    Roll up greenhouse status rows into top-level totals.

    Same shape as the Lab page's greenhouse summary so both pages render
    consistent metrics from the shared v_greenhouse_status view.

    Treats "Unclassified" rows as not counting toward the greenhouse-count
    metric since they represent Clone Room plants, not greenhouses.
    """
    if not greenhouse_rows:
        return {
            "total_active_plants": 0,
            "total_flowering": 0,
            "total_vegetative": 0,
            "active_greenhouses": 0,
            "active_facilities": 0,
        }

    total_plants = 0
    total_flowering = 0
    total_vegetative = 0
    facilities = set()
    greenhouses = 0

    for row in greenhouse_rows:
        plant_count = int(row.get("plant_count") or 0)
        total_plants += plant_count
        total_flowering += int(row.get("flowering_count") or 0)
        total_vegetative += int(row.get("vegetative_count") or 0)

        if row.get("greenhouse") and row.get("greenhouse") != "Unclassified" and plant_count > 0:
            greenhouses += 1

        if row.get("facility_id"):
            facilities.add(row["facility_id"])

    return {
        "total_active_plants": total_plants,
        "total_flowering": total_flowering,
        "total_vegetative": total_vegetative,
        "active_greenhouses": greenhouses,
        "active_facilities": len(facilities),
    }


def _slice_yield_to_window(yield_rows: list, window_days: int) -> list:
    """
    Filter yield rows to the last N days based on start_date.

    The view returns ALL runs (~1,300 rows). For the dashboard's 90-day
    window we filter here in Python rather than at the view level — this
    keeps the view reusable for future deeper-history exploration without
    requiring new view variants.

    Defensive against missing or malformed start_date values.
    """
    if not yield_rows or window_days <= 0:
        return yield_rows or []

    cutoff = date.today() - timedelta(days=window_days)
    filtered = []

    for row in yield_rows:
        start_date_val = row.get("start_date")
        if start_date_val is None:
            continue
        # BigQuery DATE comes back as either a date object or ISO string
        # depending on the client library. Handle both.
        if isinstance(start_date_val, str):
            try:
                start_date_obj = date.fromisoformat(start_date_val)
            except (ValueError, TypeError):
                continue
        elif isinstance(start_date_val, date):
            start_date_obj = start_date_val
        else:
            continue

        if start_date_obj >= cutoff:
            filtered.append(row)

    return filtered


def _compute_yield_summary(yield_rows: list) -> dict:
    """
    Roll up the windowed yield rows into top-level metrics for the
    Cultivation page's Yield Efficiency zone.

    Surfaces per-category counts and average yield, plus distinct strains
    and the most-recent run date. Powers the zone header so Ian can see
    "X runs in last 90 days, Y strains, last run on date Z" without
    scanning the table.

    Yield-meaningful categories: extraction, pressing, decarb,
    biomass_processing, processing.
    Yield-NOT-meaningful: manufacturing (non-cannabis weight added).
    """
    if not yield_rows:
        return {
            "total_runs": 0,
            "completed_runs": 0,
            "in_progress_runs": 0,
            "distinct_strains": 0,
            "most_recent_run_date": None,
            "categories": {},
        }

    completed = 0
    in_progress = 0
    strains = set()
    most_recent = None

    # Per-category aggregates: count + sum + count_with_yield for averaging
    category_aggs = {}

    for row in yield_rows:
        status = row.get("status")
        if status == "SUBMITTED":
            completed += 1
        elif status in ("OPEN", "PENDING_CONFIGURATION"):
            in_progress += 1

        strain = row.get("strain")
        if strain:
            strains.add(strain)

        start_date_val = row.get("start_date")
        if start_date_val is not None:
            # Normalize to comparable string form
            if isinstance(start_date_val, str):
                date_str = start_date_val
            elif isinstance(start_date_val, date):
                date_str = start_date_val.isoformat()
            else:
                date_str = None
            if date_str and (most_recent is None or date_str > most_recent):
                most_recent = date_str

        category = row.get("run_category", "other")
        if category not in category_aggs:
            category_aggs[category] = {
                "count": 0,
                "yield_sum": 0.0,
                "yield_count": 0,
            }
        category_aggs[category]["count"] += 1

        yield_val = row.get("computed_yield_pct")
        if yield_val is not None:
            category_aggs[category]["yield_sum"] += float(yield_val)
            category_aggs[category]["yield_count"] += 1

    # Compute averages per category
    categories = {}
    for cat, aggs in category_aggs.items():
        avg_yield = None
        if aggs["yield_count"] > 0:
            avg_yield = round(aggs["yield_sum"] / aggs["yield_count"], 1)
        categories[cat] = {
            "run_count": aggs["count"],
            "runs_with_yield": aggs["yield_count"],
            "avg_yield_pct": avg_yield,
        }

    return {
        "total_runs": len(yield_rows),
        "completed_runs": completed,
        "in_progress_runs": in_progress,
        "distinct_strains": len(strains),
        "most_recent_run_date": most_recent,
        "categories": categories,
    }


@router.get("/cultivation/dashboard")
def cultivation_dashboard():
    """
    Returns data for the Cultivation page in Retool.

    Response zones:
      - status_strip (object): 6 tile values (total_active_plants,
                                flowering_count, vegetative_count,
                                active_greenhouses, distinct_strains,
                                active_facilities). Single row from
                                v_cultivation_status_strip.
      - greenhouse_status (list): rows per (facility, greenhouse) with
                                   plant counts, phase split, age, strain
                                   detail. Shared with Lab page.
      - greenhouse_summary (object): rolled-up totals across all
                                      greenhouses.
      - strain_mix (list): rows per strain with phase split, facility/
                            greenhouse/batch coverage, pct_in_flower.
                            Sorted by total_plants DESC.
      - strain_mix_summary (object): rolled-up strain metrics and top 5
                                      highlight.
      - yield_efficiency (list): run-level yield rows, last 90 days only.
                                  Each row includes run_category for
                                  filtering. Manufacturing rows have
                                  computed_yield_pct=NULL (yield not
                                  meaningful — non-cannabis weight added).
      - yield_summary (object): per-category counts and avg yield, plus
                                 strain coverage and most-recent run date.
    """
    try:
        # Zone: Status strip — single-row scalar view
        strip_rows = query_view("v_cultivation_status_strip")
        if not strip_rows:
            raise HTTPException(status_code=500, detail="v_cultivation_status_strip returned no rows")
        strip = strip_rows[0]

        status_strip = {
            "total_active_plants": int(strip.get("total_active_plants") or 0),
            "flowering_count": int(strip.get("flowering_count") or 0),
            "vegetative_count": int(strip.get("vegetative_count") or 0),
            "active_facilities": int(strip.get("active_facilities") or 0),
            "distinct_strains_in_cultivation": int(strip.get("distinct_strains_in_cultivation") or 0),
            "active_greenhouses": int(strip.get("active_greenhouses") or 0),
        }

        # Zone: Greenhouse status — rows per (facility, greenhouse). Same view
        # the Lab page uses, surfaced here as primary content for the
        # cultivation team's audience.
        greenhouse_status = query_view("v_greenhouse_status")
        greenhouse_summary = _compute_greenhouse_summary(greenhouse_status)

        # Zone: Strain mix — rows per strain with phase split and coverage.
        strain_mix = query_view("v_cultivation_strain_mix")
        strain_mix_summary = _compute_strain_mix_summary(strain_mix)

        # Zone: Yield efficiency — run-level rows from v_yield_efficiency_by_run,
        # sliced to YIELD_WINDOW_DAYS in Python so the view stays reusable for
        # deeper-history exploration later. View returns ~1,300 rows total;
        # filtered to ~300-400 in the 90-day window.
        all_yield_rows = query_view("v_yield_efficiency_by_run")
        yield_efficiency = _slice_yield_to_window(all_yield_rows, YIELD_WINDOW_DAYS)
        yield_summary = _compute_yield_summary(yield_efficiency)

        return {
            "source": "bigquery",
            "view": (
                "v_cultivation_status_strip + v_greenhouse_status + "
                "v_cultivation_strain_mix + v_yield_efficiency_by_run"
            ),
            "status_strip": status_strip,
            "greenhouse_status": greenhouse_status,
            "greenhouse_summary": greenhouse_summary,
            "strain_mix": strain_mix,
            "strain_mix_summary": strain_mix_summary,
            "yield_efficiency": yield_efficiency,
            "yield_summary": yield_summary,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"cultivation_dashboard failed: {e}")
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")
