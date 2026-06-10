CREATE OR REPLACE VIEW `mfny-to-bigquery.canix_raw.v_packaging_queue` AS
SELECT
  b.id AS batch_id,
  b.name AS batch_name,
  b.template_name,
  b.current_location,
  r.id AS run_id,
  r.name AS run_name,
  r.status AS run_status,
  r.start_date,
  r.end_date,
  DATE_DIFF(CURRENT_DATE(), r.start_date, DAY) AS days_on_step,
  CASE
    WHEN LOWER(r.name) LIKE "%pack%" THEN "PACKAGING"
    WHEN LOWER(r.name) LIKE "%label%" THEN "LABELING"
    WHEN LOWER(r.name) LIKE "%fill%" THEN "FILLING"
    WHEN LOWER(r.name) LIKE "%tube%" THEN "TUBING"
    WHEN LOWER(r.name) LIKE "%gram%" THEN "GRAMMING"
    WHEN LOWER(r.name) LIKE "%portion%" THEN "PORTIONING"
    WHEN LOWER(r.name) LIKE "%bottle%" THEN "BOTTLING"
    ELSE "OTHER"
  END AS step_type
FROM `mfny-to-bigquery.canix_raw.bronze_manu_batch_runs` r
JOIN `mfny-to-bigquery.canix_raw.bronze_manu_batches` b
  ON r.manufacturing_batch_id = b.id
-- MIGRATED: was `b.end_date IS NULL` (scheduling field, wrongly used as active-batch marker;
-- hid 2 live packaging runs). Now routes active-set decision through v_active_batches.
WHERE b.id IN (SELECT batch_id FROM `mfny-to-bigquery.canix_raw.v_active_batches`)
  AND r.status IN ("OPEN", "SUBMITTED_FOR_APPROVAL", "PENDING_CONFIGURATION")
  AND (r.end_date IS NULL OR r.end_date <= CURRENT_DATE())
  AND (
    LOWER(r.name) LIKE "%pack%"
    OR LOWER(r.name) LIKE "%label%"
    OR LOWER(r.name) LIKE "%fill%"
    OR LOWER(r.name) LIKE "%tube%"
    OR LOWER(r.name) LIKE "%gram%"
    OR LOWER(r.name) LIKE "%portion%"
    OR LOWER(r.name) LIKE "%bottle%"
  )
  AND r.start_date IS NOT NULL
  AND r.facility_id != "4475"
  AND NOT (
    REGEXP_CONTAINS(LOWER(b.name), r"\btest\b")
    OR b.name LIKE "~%"
    OR LOWER(b.name) LIKE "%bom test%"
    OR b.name IN ("sfvadfv", "BHO RAINBOW BELTS", "510 test 3")
    OR b.name = ""
    OR b.name IS NULL
  )
ORDER BY
  CASE r.status
    WHEN "SUBMITTED_FOR_APPROVAL" THEN 1
    WHEN "OPEN" THEN 2
    ELSE 3
  END,
  days_on_step DESC
