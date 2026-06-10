CREATE OR REPLACE VIEW `mfny-to-bigquery.canix_raw.v_lab_biomass_inventory` AS
WITH stage_mapping AS (
  SELECT subcategory, stage_group, stage_order, subcategory_order FROM UNNEST([
    STRUCT('Fresh Frozen' AS subcategory, 'Biomass Inputs' AS stage_group, 1 AS stage_order, 1 AS subcategory_order),
    STRUCT('Cured Flower', 'Biomass Inputs', 1, 2),
    STRUCT('Machine Trim', 'Biomass Inputs', 1, 3),
    STRUCT('Live Resin Pre-Decarb', 'Live Resin (BHO)', 2, 1),
    STRUCT('Live Resin Decarb', 'Live Resin (BHO)', 2, 2),
    STRUCT('Portioned Live Resin Decarb', 'Live Resin (BHO)', 2, 3),
    STRUCT('Live Resin Badder', 'Live Resin (BHO)', 2, 4),
    STRUCT('Live Resin Pre-Roll Concentrate', 'Live Resin (BHO)', 2, 5),
    STRUCT('Badder Grade Dry Sift', 'Live Rosin (SHO)', 3, 1),
    STRUCT('Live Rosin Badder', 'Live Rosin (SHO)', 3, 2),
    STRUCT('Live Rosin Decarb', 'Live Rosin (SHO)', 3, 3)
  ])
),
processor_packages AS (
  SELECT id, item_subcategory, strain_name, weight, unit_of_measure, item_category
  FROM `mfny-to-bigquery.canix_raw.bronze_packages`
  WHERE facility_id = '4511' AND is_active = TRUE
),
classified AS (
  SELECT
    p.id, p.weight, p.unit_of_measure,
    COALESCE(p.strain_name, '(no strain specified)') AS strain_name,
    CASE WHEN sm.stage_group IS NOT NULL THEN sm.stage_group
         WHEN p.item_category = 'Buds' AND p.item_subcategory IS NULL THEN 'Biomass Inputs'
         ELSE NULL END AS stage_group,
    CASE WHEN sm.stage_order IS NOT NULL THEN sm.stage_order
         WHEN p.item_category = 'Buds' AND p.item_subcategory IS NULL THEN 1
         ELSE NULL END AS stage_order,
    CASE WHEN sm.subcategory IS NOT NULL THEN sm.subcategory
         WHEN p.item_category = 'Buds' AND p.item_subcategory IS NULL THEN 'Unclassified Biomass'
         ELSE NULL END AS subcategory,
    CASE WHEN sm.subcategory_order IS NOT NULL THEN sm.subcategory_order
         WHEN p.item_category = 'Buds' AND p.item_subcategory IS NULL THEN 99
         ELSE NULL END AS subcategory_order
  FROM processor_packages p
  LEFT JOIN stage_mapping sm ON p.item_subcategory = sm.subcategory
)
SELECT
  stage_group, stage_order, subcategory, subcategory_order, strain_name,
  COUNT(*) AS package_count,
  ROUND(SUM(weight), 2) AS total_weight,
  MAX(unit_of_measure) AS weight_unit
FROM classified
WHERE stage_group IS NOT NULL
GROUP BY stage_group, stage_order, subcategory, subcategory_order, strain_name
ORDER BY stage_order, subcategory_order, total_weight DESC
