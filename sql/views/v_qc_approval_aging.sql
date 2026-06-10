CREATE OR REPLACE VIEW `mfny-to-bigquery.canix_raw.v_qc_approval_aging` AS
WITH run_status_cte AS (
  SELECT
    r.id AS run_id,
    r.name AS run_name,
    r.status AS run_status,
    r.start_date,
    r.end_date,
    r.updated_at,
    r.manufacturing_batch_id,
    b.name AS batch_name,
    b.template_name,
    b.current_location,
    DATE_DIFF(CURRENT_DATE(), r.end_date, DAY) AS days_since_end
  FROM `mfny-to-bigquery.canix_raw.bronze_manu_batch_runs` r
  JOIN `mfny-to-bigquery.canix_raw.bronze_manu_batches` b
    ON r.manufacturing_batch_id = b.id
  WHERE r.status = 'SUBMITTED_FOR_APPROVAL'
    -- Exclude SANDBOX facility (4475) — not real production data
    AND r.facility_id != '4475'
)
SELECT
  run_id,
  run_name,
  run_status,
  batch_name,
  template_name,
  current_location,
  start_date,
  end_date,
  updated_at,
  days_since_end AS days_in_approval_queue,
  CASE
    WHEN days_since_end > 60 THEN 'STALE'
    WHEN days_since_end >= 3 THEN 'OVERDUE'
    WHEN days_since_end = 2 THEN 'AT_RISK'
    WHEN days_since_end = 1 THEN 'WATCH'
    ELSE 'OK'
  END AS approval_risk
FROM run_status_cte
WHERE days_since_end >= 1
ORDER BY
  CASE 
    WHEN days_since_end > 60 THEN 5
    WHEN days_since_end >= 3 THEN 1
    WHEN days_since_end = 2 THEN 2
    WHEN days_since_end = 1 THEN 3
    ELSE 6
  END,
  days_since_end DESC
