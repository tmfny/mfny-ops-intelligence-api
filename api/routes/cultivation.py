"""
Cultivation endpoints — feed the Cultivation page in Retool.

Audience: Cultivation team (Ian Dyche identified as primary respondent;
broader team includes leads at the two active cultivation facilities:
4509 (MFNY Ops Cultivation) and 4528 (American Seed & Oil)).

Note on data trust: Canix's cultivation date fields (vegetative_date,
flowering_date, harvested_date) have semantics that don't match their
names — averages don't reconcile with real cannabis cycle biology.
We've intentionally limited this v1 page to plant counts, phase splits,
and greenhouse/strain aggregations. Date-derived metrics are deferred
until we can clarify field meanings with Ian or the cultivation leads.

Endpoints:
  GET /cultivation/dashboard — Combined response with three zones:
                                - status_strip: 4 tiles (total plants,
                                                          flowering, vegetative,
                                                          active greenhouses)
                                - greenhouse_status: rows per (facility,
                                                     greenhouse) — same view
                                                     used on Lab page
                                - strain_mix: rows per strain with phase split,
                                              batch/facility/greenhouse coverage

  Future zones planned (deferred to v2):
    - Yield Efficiency: extracted yields vs biomass yields by harvest_date.
      Ian's #1 wished metric. Requires harvest → biomass package → extraction
      run lineage walk. Same data path as the Lab page's hash allocation
      forecast.
"""
from fastapi import APIRouter, HTTPException

from api.bq_client import query_view

import logging
logger = logging.getLogger(__name__)

router = APIRouter()


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


@router.get("/cultivation/dashboard")
def cultivation_dashboard():
    """
    Returns data for the Cultivation page in Retool.

    Response zones:
      - status_strip (object): 4 tile values (total_active_plants,
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

        return {
            "source": "bigquery",
            "view": "v_cultivation_status_strip + v_greenhouse_status + v_cultivation_strain_mix",
            "status_strip": status_strip,
            "greenhouse_status": greenhouse_status,
            "greenhouse_summary": greenhouse_summary,
            "strain_mix": strain_mix,
            "strain_mix_summary": strain_mix_summary,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"cultivation_dashboard failed: {e}")
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")
