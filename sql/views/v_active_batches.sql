CREATE OR REPLACE VIEW `mfny-to-bigquery.canix_raw.v_active_batches` AS
WITH run_progress AS (
  SELECT
    manufacturing_batch_id,
    COUNT(*) as total_runs,
    COUNTIF(status = 'SUBMITTED') as completed_runs,
    COUNTIF(status = 'OPEN') as open_runs,
    COUNTIF(status = 'PENDING_CONFIGURATION') as pending_runs,
    COUNTIF(status = 'ERRORED') as errored_runs,
    COUNTIF(status = 'SUBMITTED_FOR_APPROVAL') as approval_runs
  FROM `mfny-to-bigquery.canix_raw.bronze_manu_batch_runs`
  WHERE facility_id != '4475'                     -- exclude deprecated facility
  GROUP BY manufacturing_batch_id
),
current_run AS (
  SELECT DISTINCT
    manufacturing_batch_id,
    FIRST_VALUE(name) OVER (PARTITION BY manufacturing_batch_id ORDER BY run_order) as current_run_name,
    FIRST_VALUE(status) OVER (PARTITION BY manufacturing_batch_id ORDER BY run_order) as current_run_status,
    FIRST_VALUE(start_date) OVER (PARTITION BY manufacturing_batch_id ORDER BY run_order) as current_run_start_date
  FROM `mfny-to-bigquery.canix_raw.bronze_manu_batch_runs`
  WHERE status IN ('OPEN', 'SUBMITTED_FOR_APPROVAL', 'ERRORED', 'PENDING_CONFIGURATION')
    AND facility_id != '4475'
)
SELECT
  b.id as batch_id,
  b.name as batch_name,
  b.template_name,
  b.current_location,
  b.start_date as batch_start_date,
  b.end_date as batch_end_date,
  p.total_runs,
  p.completed_runs,
  p.open_runs,
  p.pending_runs,
  p.errored_runs,
  p.approval_runs,
  ROUND((p.completed_runs / NULLIF(p.total_runs, 0)) * 100, 1) as completion_pct,
  c.current_run_name,
  c.current_run_status,
  c.current_run_start_date,
  DATE_DIFF(CURRENT_DATE(), c.current_run_start_date, DAY) as days_on_current_run,
  CASE
    WHEN p.errored_runs > 0 THEN 'NEEDS_ATTENTION'
    WHEN p.approval_runs > 0 THEN 'PENDING_APPROVAL'
    WHEN p.open_runs > 0 THEN 'IN_PROGRESS'
    WHEN p.pending_runs > 0 THEN 'PENDING_START'
    ELSE 'UNKNOWN'
  END as batch_health
FROM `mfny-to-bigquery.canix_raw.bronze_manu_batches` b
JOIN run_progress p ON b.id = p.manufacturing_batch_id
LEFT JOIN current_run c ON b.id = c.manufacturing_batch_id
-- CORRECTED PREDICATE (replaces the semantically-wrong `b.end_date IS NULL`):
-- active = real template + at least one non-SUBMITTED run + touched within 90 days.
-- `end_date` was a scheduling field (future/past/null on active batches), not a done-marker.
WHERE b.template_name IS NOT NULL
  AND TRIM(b.template_name) != ''
  AND DATE_DIFF(CURRENT_DATE(), DATE(b.updated_at), DAY) <= 90
  AND EXISTS (
    SELECT 1 FROM `mfny-to-bigquery.canix_raw.bronze_manu_batch_runs` r
    WHERE r.manufacturing_batch_id = b.id
      AND r.status != 'SUBMITTED'
  )
ORDER BY
  CASE
    WHEN batch_health = 'NEEDS_ATTENTION' THEN 1
    WHEN batch_health = 'PENDING_APPROVAL' THEN 2
    WHEN batch_health = 'IN_PROGRESS' THEN 3
    WHEN batch_health = 'PENDING_START' THEN 4
    ELSE 5
  END,
  days_on_current_run DESC
