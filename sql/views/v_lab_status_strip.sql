CREATE OR REPLACE VIEW `mfny-to-bigquery.canix_raw.v_lab_status_strip` AS
WITH
fresh_frozen AS (
  -- Tile 1: Fresh Frozen biomass on hand at processor (in Pounds)
  SELECT
    ROUND(SUM(total_weight), 2) AS fresh_frozen_lbs,
    SUM(package_count) AS fresh_frozen_pkg_count
  FROM `mfny-to-bigquery.canix_raw.v_lab_biomass_inventory`
  WHERE subcategory = 'Fresh Frozen'
),

concentrate AS (
  -- Tile 2: Concentrate on hand (in Grams)
  -- Broad definition: all post-extraction intermediates that aren't decarbed yet.
  -- Includes BHO outputs (Pre-Decarb, Badder, Pre-Roll Concentrate) and SHO outputs
  -- (Live Rosin Badder, Badder Grade Dry Sift).
  SELECT
    ROUND(SUM(total_weight), 2) AS concentrate_g,
    SUM(package_count) AS concentrate_pkg_count
  FROM `mfny-to-bigquery.canix_raw.v_lab_biomass_inventory`
  WHERE subcategory IN (
    'Live Resin Pre-Decarb',
    'Live Resin Badder',
    'Live Resin Pre-Roll Concentrate',
    'Live Rosin Badder',
    'Badder Grade Dry Sift'
  )
),

decarb AS (
  -- Tile 3: Decarb ready for production (in Grams)
  -- All three Decarb subcategories across both BHO and SHO paths.
  SELECT
    ROUND(SUM(total_weight), 2) AS decarb_g,
    SUM(package_count) AS decarb_pkg_count
  FROM `mfny-to-bigquery.canix_raw.v_lab_biomass_inventory`
  WHERE subcategory IN (
    'Live Resin Decarb',
    'Portioned Live Resin Decarb',
    'Live Rosin Decarb'
  )
),

active_extraction_runs AS (
  -- Tile 4: Active extraction runs at processor.
  -- "Extraction" = BHO Extraction, SHO Extraction, or legacy "BHO" template_name.
  -- "Active" = run status OPEN or PENDING_CONFIGURATION (per project knowledge,
  -- the only active-state statuses observed in MFNY's data).
  --
  -- bronze_manu_batches has template_name. bronze_manu_batch_runs has status and facility_id.
  -- JOIN required because template lives on the batch, not the run.
  -- SANDBOX exclusion via the run's facility_id (the batch table has no facility_id).
  SELECT COUNT(*) AS active_extraction_run_count
  FROM `mfny-to-bigquery.canix_raw.bronze_manu_batch_runs` r
  INNER JOIN `mfny-to-bigquery.canix_raw.bronze_manu_batches` b
    ON r.manufacturing_batch_id = b.id
  WHERE r.status IN ('OPEN', 'PENDING_CONFIGURATION')
    AND r.facility_id != '4475'  -- exclude SANDBOX
    AND JSON_EXTRACT_SCALAR(b._raw_json, '$.template_name') IN ('BHO Extraction', 'SHO Extraction', 'BHO')
)

SELECT
  ff.fresh_frozen_lbs,
  ff.fresh_frozen_pkg_count,
  'Pounds' AS fresh_frozen_unit,
  c.concentrate_g,
  c.concentrate_pkg_count,
  'Grams' AS concentrate_unit,
  d.decarb_g,
  d.decarb_pkg_count,
  'Grams' AS decarb_unit,
  aer.active_extraction_run_count
FROM fresh_frozen ff
CROSS JOIN concentrate c
CROSS JOIN decarb d
CROSS JOIN active_extraction_runs aer
