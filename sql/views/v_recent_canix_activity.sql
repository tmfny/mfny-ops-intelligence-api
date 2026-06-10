CREATE OR REPLACE VIEW `mfny-to-bigquery.canix_raw.v_recent_canix_activity` AS
WITH recent_packages AS (
  SELECT
    'PACKAGE CREATED' as activity_type,
    id as record_id,
    item_name as description,
    location_name as location,
    status as record_status,
    created_at as activity_date,
    _ingested_at
  FROM `mfny-to-bigquery.canix_raw.bronze_packages`
  WHERE created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    AND created_at IS NOT NULL
    -- Exclude SANDBOX facility (4475) — not real production data
    AND facility_id != '4475'
),
recent_runs AS (
  SELECT
    CASE
      WHEN r.status = 'SUBMITTED' THEN 'RUN SUBMITTED'
      WHEN r.status = 'ERRORED' THEN 'RUN ERRORED'
      WHEN r.status = 'SUBMITTED_FOR_APPROVAL' THEN 'PENDING APPROVAL'
      ELSE 'RUN UPDATED'
    END as activity_type,
    r.id as record_id,
    r.name as description,
    b.current_location as location,
    r.status as record_status,
    r.updated_at as activity_date,
    r._ingested_at
  FROM `mfny-to-bigquery.canix_raw.bronze_manu_batch_runs` r
  JOIN `mfny-to-bigquery.canix_raw.bronze_manu_batches` b
    ON r.manufacturing_batch_id = b.id
  WHERE r.updated_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    AND r.updated_at IS NOT NULL
    -- Exclude SANDBOX facility (4475) — not real production data
    AND r.facility_id != '4475'
)
SELECT * FROM recent_packages
UNION ALL
SELECT * FROM recent_runs
ORDER BY activity_date DESC
