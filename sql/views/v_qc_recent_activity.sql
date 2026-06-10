CREATE OR REPLACE VIEW `mfny-to-bigquery.canix_raw.v_qc_recent_activity` AS
WITH active_runs AS (
  SELECT
    r.id AS run_id,
    r.name AS run_name,
    r.status,
    r.start_date,
    r.end_date,
    b.name AS batch_name,
    b.template_name
  FROM `mfny-to-bigquery.canix_raw.bronze_manu_batch_runs` r
  JOIN `mfny-to-bigquery.canix_raw.bronze_manu_batches` b
    ON r.manufacturing_batch_id = b.id
  -- Exclude SANDBOX facility (4475) — not real production data
  WHERE r.facility_id != '4475'
)
SELECT
  COUNTIF(start_date = CURRENT_DATE()) AS runs_started_today,
  COUNTIF(start_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)) AS runs_started_7d,
  COUNTIF(start_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)) AS runs_started_30d,
  
  -- Completed = end_date set AND run actually submitted in Canix
  -- Without the SUBMITTED filter, scheduled-but-still-OPEN runs inflate the count
  COUNTIF(end_date = CURRENT_DATE() AND status = 'SUBMITTED') AS runs_completed_today,
  COUNTIF(end_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) AND status = 'SUBMITTED') AS runs_completed_7d,
  COUNTIF(end_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AND status = 'SUBMITTED') AS runs_completed_30d
FROM active_runs
