CREATE OR REPLACE VIEW `mfny-to-bigquery.canix_raw.v_bottleneck_summary` AS
WITH active_batch_current_step AS (
  SELECT
    b.id AS batch_id,
    r.name AS step_name,
    r.status AS step_status,
    r.start_date AS step_start_date,
    DATE_DIFF(CURRENT_DATE(), r.start_date, DAY) AS days_waiting,
    r.run_order
  FROM `mfny-to-bigquery.canix_raw.bronze_manu_batches` b
  JOIN `mfny-to-bigquery.canix_raw.bronze_manu_batch_runs` r
    ON r.manufacturing_batch_id = b.id
  JOIN `mfny-to-bigquery.canix_raw.v_active_batches` ab
    ON CAST(ab.batch_id AS STRING) = CAST(b.id AS STRING)
  WHERE r.status IN ('OPEN', 'SUBMITTED_FOR_APPROVAL', 'ERRORED', 'PENDING_CONFIGURATION')
    AND r.facility_id != '4475'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY b.id ORDER BY r.run_order) = 1
)
SELECT
  step_name,
  COUNT(*) AS batches_waiting,
  COUNTIF(step_status = 'OPEN') AS open_count,
  COUNTIF(step_status = 'PENDING_CONFIGURATION') AS pending_count,
  COUNTIF(step_status = 'SUBMITTED_FOR_APPROVAL') AS approval_count,
  COUNTIF(step_status = 'ERRORED') AS errored_count,
  ROUND(AVG(CASE WHEN days_waiting >= 0 THEN days_waiting END), 1) AS avg_days_waiting,
  MAX(CASE WHEN days_waiting >= 0 THEN days_waiting END) AS max_days_waiting,
  COUNTIF(days_waiting > 7) AS batches_over_7_days,
  COUNTIF(days_waiting > 30) AS batches_over_30_days
FROM active_batch_current_step
WHERE step_name NOT LIKE 'Split %'
  AND step_name NOT LIKE '%R&D%'
GROUP BY step_name
ORDER BY batches_waiting DESC, avg_days_waiting DESC
