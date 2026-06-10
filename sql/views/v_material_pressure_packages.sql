CREATE OR REPLACE VIEW `mfny-to-bigquery.canix_raw.v_material_pressure_packages` AS
WITH base AS (
  SELECT
    id AS package_id,
    tag,                          -- METRC tag (Emma's ask)
    item_name,
    strain_name,
    item_subcategory,
    unit_of_measure,
    CAST(weight AS FLOAT64) AS weight,   -- stored as STRING; cast for reliable math
    location_name,
    source_batch_id,              -- provenance
    packaged_date,
    DATE_DIFF(CURRENT_DATE(), packaged_date, DAY) AS days_old
  FROM `mfny-to-bigquery.canix_raw.bronze_packages`
  WHERE is_active = TRUE
    AND facility_id = '4511'
    AND status = 'In Progress'
    AND CAST(weight AS FLOAT64) > 0
)
SELECT
  CASE
    WHEN unit_of_measure = 'Pounds' AND item_subcategory = 'Fresh Frozen'
      THEN 'fresh_frozen_awaiting_extraction'
    WHEN unit_of_measure = 'Pounds' AND item_subcategory = 'Cured Flower'
      THEN 'cured_flower_awaiting_production'
    WHEN unit_of_measure = 'Grams' AND item_subcategory = 'Live Resin Pre-Decarb'
      THEN 'extracted_awaiting_decarb'
    WHEN unit_of_measure = 'Grams' AND item_subcategory IN ('Live Resin Decarb', 'Live Rosin Decarb')
      THEN 'concentrate_ready_for_production'
  END AS bucket_id,
  package_id,
  tag,
  item_name,
  strain_name,
  item_subcategory,
  unit_of_measure,
  weight,
  location_name,
  source_batch_id,
  packaged_date,
  days_old
FROM base
WHERE
  (unit_of_measure = 'Pounds' AND item_subcategory IN ('Fresh Frozen', 'Cured Flower'))
  OR (unit_of_measure = 'Grams' AND item_subcategory IN ('Live Resin Pre-Decarb', 'Live Resin Decarb', 'Live Rosin Decarb'))
