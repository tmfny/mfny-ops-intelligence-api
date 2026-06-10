CREATE OR REPLACE VIEW `mfny-to-bigquery.canix_raw.v_executive_production_throughput` AS
WITH

completed_runs AS (
  SELECT
    name,
    LOWER(name) AS name_lower,
    DATE_TRUNC(DATE(end_date), MONTH) AS month_completed
  FROM `mfny-to-bigquery.canix_raw.bronze_manu_batch_runs`
  WHERE status = 'SUBMITTED'
    AND end_date IS NOT NULL
    AND DATE(end_date) <= CURRENT_DATE()
    AND DATE(end_date) >= '2025-04-01'
    AND DATE_TRUNC(DATE(end_date), MONTH) < DATE_TRUNC(CURRENT_DATE(), MONTH)
    AND LOWER(TRIM(name)) NOT LIKE '%add testing output%'
    -- Exclude SANDBOX facility (4475) — not real production data
    AND facility_id != '4475'
),

staged AS (
  SELECT
    month_completed,
    name AS original_name,
    CASE
      WHEN name_lower = 'fresh freeze flower' THEN 'Biomass Prep'
      WHEN name_lower = 'sort, sift & grind flower' THEN 'Biomass Prep'
      WHEN name_lower = 'cure & package' THEN 'Biomass Prep'
      WHEN name_lower = 'buck & trim cured flower' THEN 'Biomass Prep'
      WHEN name_lower = 'curing, bucking & trimming biomass' THEN 'Biomass Prep'
      WHEN name_lower = 'cure biomass' THEN 'Biomass Prep'

      WHEN name_lower = 'extraction' THEN 'Extraction'
      WHEN name_lower = 'bho extraction' THEN 'Extraction'
      WHEN name_lower = 'wash, freeze dry & sift' THEN 'Extraction'
      WHEN name_lower LIKE '%press%' AND name_lower NOT LIKE '%re-prep%' AND name_lower NOT LIKE '%reprep%' THEN 'Extraction'

      WHEN name_lower LIKE 'decarboxylate%' THEN 'Decarb'
      WHEN name_lower LIKE 'decarboxylation%' THEN 'Decarb'
      WHEN name_lower LIKE 're-prep%for decarb%' THEN 'Decarb'
      WHEN name_lower LIKE 're-prep%decarb%' THEN 'Decarb'
      WHEN name_lower LIKE 'combine & whip rosin%' THEN 'Decarb'

      WHEN name_lower = 'gram badder' THEN 'Concentrate Portioning'
      WHEN name_lower = 'gramming badder' THEN 'Concentrate Portioning'
      WHEN name_lower = 'portion concentrate' THEN 'Concentrate Portioning'
      WHEN name_lower = 'portion' THEN 'Concentrate Portioning'

      WHEN name_lower = 'produce & demold' THEN 'Product Manufacturing'
      WHEN name_lower LIKE 'fill 510 carts%' THEN 'Product Manufacturing'
      WHEN name_lower = 'fill vape pens' THEN 'Product Manufacturing'
      WHEN name_lower LIKE 'infuse flower, create & tube%' THEN 'Product Manufacturing'
      WHEN name_lower LIKE 'make%tincture%' THEN 'Product Manufacturing'
      WHEN name_lower LIKE 'make%intimacy oil%' THEN 'Product Manufacturing'
      WHEN name_lower LIKE 'fill for testing%' THEN 'Product Manufacturing'
      WHEN name_lower LIKE '%pre-roll%' THEN 'Product Manufacturing'
      WHEN name_lower LIKE '%gummies%' OR name_lower LIKE '%gummy%' THEN 'Product Manufacturing'
      WHEN name_lower LIKE '%510%' THEN 'Product Manufacturing'

      WHEN name_lower LIKE '%final packout%' THEN 'Packaging'
      WHEN name_lower = 'clean & tube' THEN 'Packaging'

      ELSE 'UNCLASSIFIED'
    END AS stage
  FROM completed_runs
)

SELECT
  month,
  stage,
  run_count
FROM (
  SELECT
    month_completed AS month,
    stage,
    COUNT(*) AS run_count
  FROM staged
  WHERE stage != 'UNCLASSIFIED'
  GROUP BY month_completed, stage
)
ORDER BY month, stage
