CREATE OR REPLACE VIEW `mfny-to-bigquery.canix_raw.v_executive_flow` AS
WITH
cultivation AS (
  SELECT 4 AS cultivation_facility_count
),
material AS (
  SELECT
    SUM(CASE WHEN bucket_id = 'fresh_frozen_awaiting_extraction' THEN total_weight ELSE 0 END) AS fresh_frozen_lbs,
    SUM(CASE WHEN bucket_id = 'fresh_frozen_awaiting_extraction' THEN package_count ELSE 0 END) AS fresh_frozen_packages,
    SUM(CASE WHEN bucket_id = 'cured_flower_awaiting_production' THEN total_weight ELSE 0 END) AS cured_flower_lbs,
    SUM(CASE WHEN bucket_id = 'cured_flower_awaiting_production' THEN package_count ELSE 0 END) AS cured_flower_packages,
    SUM(CASE WHEN bucket_id = 'extracted_awaiting_decarb' THEN total_weight ELSE 0 END) AS extracted_grams,
    SUM(CASE WHEN bucket_id = 'extracted_awaiting_decarb' THEN package_count ELSE 0 END) AS extracted_packages,
    SUM(CASE WHEN bucket_id = 'concentrate_ready_for_production' THEN total_weight ELSE 0 END) AS concentrate_ready_grams,
    SUM(CASE WHEN bucket_id = 'concentrate_ready_for_production' THEN package_count ELSE 0 END) AS concentrate_ready_packages
  FROM `mfny-to-bigquery.canix_raw.v_material_pressure`
),
-- ============================================================
-- NODE 4: Active Production ("In Production" on the Executive hero)
-- ============================================================
-- "Active" = batches currently in progress, using the canonical
-- lifecycle state on v_active_batches:
--   - batch_health = 'IN_PROGRESS'  (the canonical "running now" state)
--   - name guards exclude operator test/draft batches from the
--     investor-facing number (no '%test%', no leading '~')
--
-- History: this CTE previously used a 7-condition hand-rolled filter
-- (current_run_status OPEN + completion_pct + template + days +
-- name LIKEs). That filter produced the same count as IN_PROGRESS but
-- was unexplainable and duplicated logic already canonicalized in
-- v_active_batches. Replaced 2026-06-10 with the canonical predicate.
-- The Operations page counts a broader "active" set (running + queued,
-- i.e. includes PENDING_START); the hero intentionally shows only
-- running batches, hence a smaller number than Operations by design.
production AS (
  SELECT COUNT(*) AS active_production_count
  FROM `mfny-to-bigquery.canix_raw.v_active_batches`
  WHERE batch_health = 'IN_PROGRESS'
    AND LOWER(batch_name) NOT LIKE '%test%'
    AND batch_name NOT LIKE '~%'
),
finished AS (
  SELECT
    COUNT(*) AS finished_units_shipped,
    COUNT(DISTINCT item_name) AS finished_distinct_skus
  FROM `mfny-to-bigquery.canix_raw.bronze_packages`
  WHERE facility_id = '4510'
    AND status = 'Transferred'
    AND _ingested_at = (
      SELECT MAX(_ingested_at)
      FROM `mfny-to-bigquery.canix_raw.bronze_packages`
    )
)
SELECT
  c.cultivation_facility_count,
  m.fresh_frozen_lbs,
  m.fresh_frozen_packages,
  m.cured_flower_lbs,
  m.cured_flower_packages,
  p.active_production_count,
  m.extracted_grams,
  m.extracted_packages,
  m.concentrate_ready_grams,
  m.concentrate_ready_packages,
  f.finished_units_shipped,
  f.finished_distinct_skus
FROM cultivation c
CROSS JOIN material m
CROSS JOIN production p
CROSS JOIN finished f
