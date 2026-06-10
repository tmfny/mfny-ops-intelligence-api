CREATE OR REPLACE VIEW `mfny-to-bigquery.canix_raw.v_executive_inventory_summary` AS
WITH

distribution_packages AS (
  SELECT
    status,
    packaged_date
  FROM `mfny-to-bigquery.canix_raw.bronze_packages`
  WHERE facility_id = '4510'
    AND _ingested_at = (
      SELECT MAX(_ingested_at)
      FROM `mfny-to-bigquery.canix_raw.bronze_packages`
    )
),

-- ============================================================
-- Active distribution window
-- ============================================================
-- Days active = days between the earliest transferred package's packaged_date
-- and today. Filter to packages with valid packaged_date (post-2024) to avoid
-- pre-METRC legacy data skewing the denominator.
-- ============================================================
active_window AS (
  SELECT
    DATE_DIFF(
      CURRENT_DATE(),
      DATE(MIN(packaged_date)),
      DAY
    ) AS days_active
  FROM distribution_packages
  WHERE status = 'Transferred'
    AND DATE(packaged_date) >= '2024-01-01'
),

-- ============================================================
-- Sellable + cumulative shipped
-- ============================================================
totals AS (
  SELECT
    SUM(CASE WHEN status = 'Available To Sell' THEN 1 ELSE 0 END) AS sellable_now,
    SUM(CASE WHEN status = 'Transferred' THEN 1 ELSE 0 END) AS cumulative_shipped
  FROM distribution_packages
)

SELECT
  t.sellable_now,
  t.cumulative_shipped,
  a.days_active,
  ROUND(t.cumulative_shipped / NULLIF(a.days_active, 0), 1) AS avg_shipped_per_day,
  ROUND(SAFE_DIVIDE(t.sellable_now, t.cumulative_shipped / NULLIF(a.days_active, 0)), 1) AS days_of_inventory
FROM totals t
CROSS JOIN active_window a
