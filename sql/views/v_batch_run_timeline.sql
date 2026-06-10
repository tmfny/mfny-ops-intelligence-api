CREATE OR REPLACE VIEW `mfny-to-bigquery.canix_raw.v_batch_run_timeline` AS
SELECT
  manufacturing_batch_id,
  run_order,
  name AS run_name,
  status,
  start_date,
  end_date,
  CASE
    WHEN start_date IS NULL THEN NULL
    WHEN end_date IS NOT NULL THEN DATE_DIFF(end_date, start_date, DAY)
    ELSE DATE_DIFF(CURRENT_DATE(), start_date, DAY)
  END AS days_on_run
FROM `mfny-to-bigquery.canix_raw.bronze_manu_batch_runs`
WHERE facility_id != '4475'
