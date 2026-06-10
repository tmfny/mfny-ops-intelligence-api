CREATE OR REPLACE VIEW `mfny-to-bigquery.canix_raw.v_system_alerts` AS
WITH
errored_runs AS (
  SELECT COUNT(*) AS errored_count
  FROM `mfny-to-bigquery.canix_raw.bronze_manu_batch_runs` r
  JOIN `mfny-to-bigquery.canix_raw.v_active_batches` ab ON r.manufacturing_batch_id = ab.batch_id
  WHERE r.status = 'ERRORED'
    AND r.facility_id != '4475'
),
errored_alert AS (
  SELECT
    CAST('CRITICAL' AS STRING) AS level,
    CAST(1 AS INT64) AS severity_order,
    CONCAT(CAST(errored_count AS STRING), ' run', IF(errored_count = 1, '', 's'),
           ' in ERRORED status — needs investigation') AS message,
    CAST('errored_runs' AS STRING) AS source,
    CAST(NULL AS STRING) AS reference_id
  FROM errored_runs
  WHERE errored_count > 0
),

compliance_items AS (
  SELECT
    r.id AS run_id,
    ab.batch_id AS batch_id,
    ab.batch_name AS batch_name,
    r.name AS run_name,
    DATE_DIFF(CURRENT_DATE(), CAST(r.updated_at AS DATE), DAY) AS age_days
  FROM `mfny-to-bigquery.canix_raw.bronze_manu_batch_runs` r
  JOIN `mfny-to-bigquery.canix_raw.v_active_batches` ab ON r.manufacturing_batch_id = ab.batch_id
  WHERE r.status = 'OPEN'
    AND r.start_date IS NOT NULL
    AND r.end_date IS NOT NULL
    AND DATE_DIFF(CURRENT_DATE(), CAST(r.updated_at AS DATE), DAY) >= 7
    AND ab.batch_name IS NOT NULL
    AND LENGTH(TRIM(ab.batch_name)) > 4
    AND r.facility_id != '4475'
),
compliance_top5 AS (
  SELECT
    CAST('WARNING' AS STRING) AS level,
    CAST(2 AS INT64) AS severity_order,
    CONCAT('Compliance: "', batch_name, '" physically complete, not submitted (',
           CAST(age_days AS STRING), 'd)') AS message,
    CAST('compliance_overdue' AS STRING) AS source,
    CAST(batch_id AS STRING) AS reference_id
  FROM compliance_items
  ORDER BY age_days DESC
  LIMIT 5
),
compliance_overflow AS (
  SELECT
    CAST('WARNING' AS STRING) AS level,
    CAST(2 AS INT64) AS severity_order,
    CONCAT('+ ', CAST(extra_count AS STRING), ' more compliance items overdue') AS message,
    CAST('compliance_overdue_overflow' AS STRING) AS source,
    CAST(NULL AS STRING) AS reference_id
  FROM (
    SELECT GREATEST(COUNT(*) - 5, 0) AS extra_count
    FROM compliance_items
  )
  WHERE extra_count > 0
),

-- stalled batches: per-batch detail rows (top 5 + overflow)
-- migrated to v_active_batches (was v_active_production)
stalled_items AS (
  SELECT
    batch_id,
    batch_name,
    current_run_name,
    days_on_current_run
  FROM `mfny-to-bigquery.canix_raw.v_active_batches`
  WHERE days_on_current_run >= 7
    AND template_name IS NOT NULL
    AND LENGTH(TRIM(template_name)) > 0
    AND template_name LIKE '% %'
    AND batch_name IS NOT NULL
    AND LENGTH(TRIM(batch_name)) > 4
),
stalled_top5 AS (
  SELECT
    CAST('WARNING' AS STRING) AS level,
    CAST(3 AS INT64) AS severity_order,
    CONCAT('Stalled: "', batch_name, '" — ', CAST(days_on_current_run AS STRING),
           'd at "', current_run_name, '"') AS message,
    CAST('stalled_batches' AS STRING) AS source,
    CAST(batch_id AS STRING) AS reference_id
  FROM stalled_items
  ORDER BY days_on_current_run DESC
  LIMIT 5
),
stalled_overflow AS (
  SELECT
    CAST('WARNING' AS STRING) AS level,
    CAST(3 AS INT64) AS severity_order,
    CONCAT('+ ', CAST(extra_count AS STRING), ' more batches stalled 7+ days') AS message,
    CAST('stalled_batches_overflow' AS STRING) AS source,
    CAST(NULL AS STRING) AS reference_id
  FROM (
    SELECT GREATEST(COUNT(*) - 5, 0) AS extra_count
    FROM stalled_items
  )
  WHERE extra_count > 0
),

bottleneck_items AS (
  SELECT
    step_name,
    batches_waiting,
    avg_days_waiting
  FROM `mfny-to-bigquery.canix_raw.v_bottleneck_summary`
  WHERE batches_waiting >= 5
    AND avg_days_waiting IS NOT NULL
    AND avg_days_waiting >= 1
    AND avg_days_waiting <= 60
),
bottleneck_top3 AS (
  SELECT
    CAST('INFO' AS STRING) AS level,
    CAST(4 AS INT64) AS severity_order,
    CONCAT(CAST(batches_waiting AS STRING), ' batches queued at "', step_name,
           '" (avg ', CAST(ROUND(avg_days_waiting, 1) AS STRING), 'd)') AS message,
    CAST('bottleneck' AS STRING) AS source,
    CAST(step_name AS STRING) AS reference_id
  FROM bottleneck_items
  ORDER BY batches_waiting DESC
  LIMIT 3
),
bottleneck_overflow AS (
  SELECT
    CAST('INFO' AS STRING) AS level,
    CAST(4 AS INT64) AS severity_order,
    CONCAT('+ ', CAST(extra_count AS STRING), ' more bottleneck steps') AS message,
    CAST('bottleneck_overflow' AS STRING) AS source,
    CAST(NULL AS STRING) AS reference_id
  FROM (
    SELECT GREATEST(COUNT(*) - 3, 0) AS extra_count
    FROM bottleneck_items
  )
  WHERE extra_count > 0
),

recent_extractions AS (
  SELECT MAX(r.start_date) AS last_extraction_date
  FROM `mfny-to-bigquery.canix_raw.bronze_manu_batch_runs` r
  WHERE r.name IN ('Extraction', 'Wash, Freeze Dry & Sift')
    AND r.start_date IS NOT NULL
    AND r.facility_id != '4475'
),
cold_extraction_alert AS (
  SELECT
    CAST('WARNING' AS STRING) AS level,
    CAST(2 AS INT64) AS severity_order,
    CONCAT('No extraction runs started in last ',
           CAST(DATE_DIFF(CURRENT_DATE(), last_extraction_date, DAY) AS STRING), ' days') AS message,
    CAST('cold_extraction' AS STRING) AS source,
    CAST(NULL AS STRING) AS reference_id
  FROM recent_extractions
  WHERE DATE_DIFF(CURRENT_DATE(), last_extraction_date, DAY) >= 7
),

approval_aging AS (
  SELECT
    COUNT(*) AS aging_count,
    MAX(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), r.updated_at, HOUR)) AS max_age_hours
  FROM `mfny-to-bigquery.canix_raw.bronze_manu_batch_runs` r
  JOIN `mfny-to-bigquery.canix_raw.v_active_batches` ab ON r.manufacturing_batch_id = ab.batch_id
  WHERE r.status = 'SUBMITTED_FOR_APPROVAL'
    AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), r.updated_at, HOUR) >= 24
    AND r.facility_id != '4475'
),
approval_alert AS (
  SELECT
    CAST('WARNING' AS STRING) AS level,
    CAST(2 AS INT64) AS severity_order,
    CONCAT(CAST(aging_count AS STRING), ' run', IF(aging_count = 1, '', 's'),
           ' pending approval > 24 hours (oldest ', CAST(max_age_hours AS STRING), 'h)') AS message,
    CAST('approval_aging' AS STRING) AS source,
    CAST(NULL AS STRING) AS reference_id
  FROM approval_aging
  WHERE aging_count > 0
),

all_alerts AS (
  SELECT * FROM errored_alert
  UNION ALL SELECT * FROM compliance_top5
  UNION ALL SELECT * FROM compliance_overflow
  UNION ALL SELECT * FROM stalled_top5
  UNION ALL SELECT * FROM stalled_overflow
  UNION ALL SELECT * FROM bottleneck_top3
  UNION ALL SELECT * FROM bottleneck_overflow
  UNION ALL SELECT * FROM cold_extraction_alert
  UNION ALL SELECT * FROM approval_alert
),

all_normal AS (
  SELECT
    CAST('OK' AS STRING) AS level,
    CAST(99 AS INT64) AS severity_order,
    CAST('All systems normal — no alerts to surface' AS STRING) AS message,
    CAST('system_status' AS STRING) AS source,
    CAST(NULL AS STRING) AS reference_id
  FROM UNNEST([1]) AS dummy
  WHERE NOT EXISTS (SELECT 1 FROM all_alerts)
)

SELECT level, severity_order, message, source, reference_id
FROM all_alerts
UNION ALL
SELECT level, severity_order, message, source, reference_id
FROM all_normal
ORDER BY severity_order, message
