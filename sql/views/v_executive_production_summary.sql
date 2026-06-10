CREATE OR REPLACE VIEW `mfny-to-bigquery.canix_raw.v_executive_production_summary` AS
WITH

-- ============================================================
-- Monthly totals (sum across stages)
-- ============================================================
-- Source from the canonical throughput view so definitions stay in sync.
-- Excludes the current partial month to make month-over-month comparisons
-- meaningful for investor audiences.
monthly_totals AS (
  SELECT
    month,
    SUM(run_count) AS total_runs
  FROM `mfny-to-bigquery.canix_raw.v_executive_production_throughput`
  WHERE month < DATE_TRUNC(CURRENT_DATE(), MONTH)
  GROUP BY month
),

-- ============================================================
-- Last month + delta vs prior month
-- ============================================================
last_month AS (
  SELECT
    total_runs AS last_month_runs,
    month AS last_month_date,
    LAG(total_runs) OVER (ORDER BY month) AS prior_month_runs
  FROM monthly_totals
  QUALIFY ROW_NUMBER() OVER (ORDER BY month DESC) = 1
),

-- ============================================================
-- Trailing 3-month average + delta vs prior 3-month average
-- ============================================================
-- "Last 3 complete months" vs "the 3 months before those"
trailing_3mo AS (
  SELECT
    AVG(total_runs) AS trailing_3mo_avg
  FROM (
    SELECT total_runs
    FROM monthly_totals
    ORDER BY month DESC
    LIMIT 3
  )
),

prior_3mo AS (
  SELECT
    AVG(total_runs) AS prior_3mo_avg
  FROM (
    SELECT total_runs
    FROM monthly_totals
    ORDER BY month DESC
    LIMIT 3 OFFSET 3
  )
),

-- ============================================================
-- All-time peak month
-- ============================================================
peak AS (
  SELECT
    total_runs AS peak_runs,
    month AS peak_month_date
  FROM monthly_totals
  QUALIFY ROW_NUMBER() OVER (ORDER BY total_runs DESC) = 1
)

SELECT
  -- Last complete month
  lm.last_month_runs,
  lm.last_month_date,
  lm.prior_month_runs,
  SAFE_DIVIDE(lm.last_month_runs - lm.prior_month_runs, lm.prior_month_runs) AS last_month_pct_change,

  -- Trailing 3-month average
  ROUND(t3.trailing_3mo_avg, 0) AS trailing_3mo_avg,
  ROUND(p3.prior_3mo_avg, 0) AS prior_3mo_avg,
  SAFE_DIVIDE(t3.trailing_3mo_avg - p3.prior_3mo_avg, p3.prior_3mo_avg) AS trailing_3mo_pct_change,

  -- All-time peak
  pk.peak_runs,
  pk.peak_month_date,

  -- Current-vs-peak ratio (how close are we to the all-time high)
  SAFE_DIVIDE(lm.last_month_runs, pk.peak_runs) AS last_month_pct_of_peak

FROM last_month lm
CROSS JOIN trailing_3mo t3
CROSS JOIN prior_3mo p3
CROSS JOIN peak pk
