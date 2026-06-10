CREATE OR REPLACE VIEW `mfny-to-bigquery.canix_raw.v_compliance_timing` AS
WITH run_status_cte AS (
  SELECT
    r.id AS run_id,
    r.name AS run_name,
    r.status AS run_status,
    r.start_date,
    r.end_date,
    r.manufacturing_batch_id,
    b.name AS batch_name,
    b.template_name,
    b.current_location,
    DATE_DIFF(CURRENT_DATE(), r.end_date, DAY) AS days_since_end,
    DATE_DIFF(CURRENT_DATE(), r.start_date, DAY) AS days_since_start,
    CASE
      WHEN r.status = 'SUBMITTED' THEN 'COMPLETE_IN_CANIX'
      WHEN r.status = 'ERRORED' THEN 'ERRORED_NEEDS_ATTENTION'
      WHEN r.status = 'SUBMITTED_FOR_APPROVAL' THEN 'PENDING_APPROVAL'
      WHEN r.status = 'OPEN' AND r.end_date IS NOT NULL 
        AND r.end_date <= CURRENT_DATE() THEN 'PHYSICALLY_COMPLETE_NOT_SUBMITTED'
      WHEN r.status = 'OPEN' AND r.end_date IS NULL 
        AND r.start_date IS NOT NULL THEN 'IN_PROGRESS'
      WHEN r.status = 'OPEN' AND r.start_date IS NULL THEN 'NOT_STARTED'
      WHEN r.status = 'PENDING_CONFIGURATION' THEN 'NEEDS_CONFIGURATION'
      ELSE 'UNKNOWN'
    END AS canix_entry_status
  FROM `mfny-to-bigquery.canix_raw.bronze_manu_batch_runs` r
  JOIN `mfny-to-bigquery.canix_raw.bronze_manu_batches` b
    ON r.manufacturing_batch_id = b.id
  -- Exclude SANDBOX facility (4475) — not real production data
  WHERE r.facility_id != '4475'
)
SELECT
  run_id,
  run_name,
  run_status,
  canix_entry_status,
  batch_name,
  template_name,
  current_location,
  start_date,
  end_date,
  days_since_end,
  days_since_start,
  CASE
    WHEN canix_entry_status = 'PHYSICALLY_COMPLETE_NOT_SUBMITTED' 
      AND days_since_end > 60 THEN 'STALE'
    WHEN canix_entry_status = 'PHYSICALLY_COMPLETE_NOT_SUBMITTED' 
      AND days_since_end >= 3 THEN 'OVERDUE'
    WHEN canix_entry_status = 'PHYSICALLY_COMPLETE_NOT_SUBMITTED' 
      AND days_since_end = 2 THEN 'AT_RISK'
    WHEN canix_entry_status = 'PHYSICALLY_COMPLETE_NOT_SUBMITTED' 
      AND days_since_end = 1 THEN 'WATCH'
    WHEN canix_entry_status = 'ERRORED_NEEDS_ATTENTION' THEN 'OVERDUE'
    WHEN canix_entry_status = 'PENDING_APPROVAL' THEN 'PENDING_APPROVAL'
    ELSE 'OK'
  END AS compliance_risk
FROM run_status_cte
WHERE 
  canix_entry_status IN (
    'PHYSICALLY_COMPLETE_NOT_SUBMITTED',
    'ERRORED_NEEDS_ATTENTION',
    'PENDING_APPROVAL'
  )
  AND (canix_entry_status != 'PHYSICALLY_COMPLETE_NOT_SUBMITTED' OR days_since_end >= 1)
ORDER BY
  CASE compliance_risk
    WHEN 'OVERDUE' THEN 1
    WHEN 'AT_RISK' THEN 2
    WHEN 'WATCH' THEN 3
    WHEN 'PENDING_APPROVAL' THEN 4
    WHEN 'STALE' THEN 5
    ELSE 6
  END,
  days_since_end DESC
