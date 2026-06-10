CREATE OR REPLACE VIEW `mfny-to-bigquery.canix_raw.v_qc_daily_activity` AS
WITH date_range AS (
  -- Generate 30 days back to today, inclusive
  SELECT DATE_SUB(CURRENT_DATE(), INTERVAL n DAY) AS activity_date
  FROM UNNEST(GENERATE_ARRAY(0, 29)) AS n
),
active_runs AS (
  SELECT
    r.id AS run_id,
    r.status,
    r.start_date,
    r.end_date
  FROM `mfny-to-bigquery.canix_raw.bronze_manu_batch_runs` r
  WHERE r.facility_id != '4475'
),
started_daily AS (
  SELECT
    start_date AS activity_date,
    COUNT(*) AS runs_started
  FROM active_runs
  WHERE start_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 29 DAY)
    AND start_date <= CURRENT_DATE()
  GROUP BY start_date
),
completed_daily AS (
  SELECT
    end_date AS activity_date,
    COUNT(*) AS runs_completed
  FROM active_runs
  WHERE end_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 29 DAY)
    AND end_date <= CURRENT_DATE()
    AND status = 'SUBMITTED'
  GROUP BY end_date
)
SELECT
  d.activity_date,
  COALESCE(s.runs_started, 0) AS runs_started,
  COALESCE(c.runs_completed, 0) AS runs_completed
FROM date_range d
LEFT JOIN started_daily s ON s.activity_date = d.activity_date
LEFT JOIN completed_daily c ON c.activity_date = d.activity_date
ORDER BY d.activity_date ASC
