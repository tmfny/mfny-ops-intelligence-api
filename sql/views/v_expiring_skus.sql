CREATE OR REPLACE VIEW `mfny-to-bigquery.canix_raw.v_expiring_skus` AS
SELECT
  item_name,
  item_category,
  strain_name,
  location_name,
  tag,
  status,
  packaged_date,
  use_by_date,
  DATE_DIFF(use_by_date, CURRENT_DATE(), DAY) as days_until_expiry,
  quantity,
  weight,
  CASE
    WHEN use_by_date <= CURRENT_DATE() THEN 'EXPIRED'
    WHEN use_by_date <= DATE_ADD(CURRENT_DATE(), INTERVAL 30 DAY) THEN 'CRITICAL'
    WHEN use_by_date <= DATE_ADD(CURRENT_DATE(), INTERVAL 90 DAY) THEN 'WARNING'
    ELSE 'OK'
  END as expiry_status
FROM `mfny-to-bigquery.canix_raw.bronze_packages`
WHERE status = 'Available To Sell'
  AND use_by_date IS NOT NULL
  -- Exclude SANDBOX facility (4475) — not real production data
  AND facility_id != '4475'
ORDER BY
  CASE
    WHEN use_by_date <= CURRENT_DATE() THEN 1
    WHEN use_by_date <= DATE_ADD(CURRENT_DATE(), INTERVAL 30 DAY) THEN 2
    WHEN use_by_date <= DATE_ADD(CURRENT_DATE(), INTERVAL 90 DAY) THEN 3
    ELSE 4
  END,
  use_by_date ASC
