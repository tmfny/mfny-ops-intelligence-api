CREATE OR REPLACE VIEW `mfny-to-bigquery.canix_raw.v_material_pressure` AS
WITH base AS (
  SELECT
    item_subcategory,
    unit_of_measure,
    weight,
    packaged_date,
    DATE_DIFF(CURRENT_DATE(), packaged_date, DAY) AS days_old
  FROM `mfny-to-bigquery.canix_raw.bronze_packages`
  WHERE is_active = TRUE
    AND facility_id = '4511'
    AND status = 'In Progress'
    AND weight > 0
),
pressure_buckets AS (
  -- KPI 1: Fresh Frozen Awaiting Extraction
  SELECT
    'fresh_frozen_awaiting_extraction' AS bucket_id,
    'Fresh Frozen Awaiting Extraction' AS label,
    'Pounds' AS unit,
    1 AS sort_order
  UNION ALL
  SELECT 'cured_flower_awaiting_production', 'Cured Flower Awaiting Production', 'Pounds', 2
  UNION ALL
  SELECT 'extracted_awaiting_decarb', 'Extracted Awaiting Decarb', 'Grams', 3
  UNION ALL
  SELECT 'concentrate_ready_for_production', 'Concentrate Ready for Production', 'Grams', 4
)
SELECT
  pb.bucket_id,
  pb.label,
  pb.unit,
  pb.sort_order,
  COALESCE(stats.package_count, 0) AS package_count,
  COALESCE(stats.total_weight, 0.0) AS total_weight,
  COALESCE(stats.aging_count, 0) AS aging_count,
  COALESCE(stats.oldest_days, 0) AS oldest_days
FROM pressure_buckets pb
LEFT JOIN (
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
    COUNT(*) AS package_count,
    SUM(weight) AS total_weight,
    COUNTIF(days_old > 90) AS aging_count,
    MAX(days_old) AS oldest_days
  FROM base
  WHERE
    (unit_of_measure = 'Pounds' AND item_subcategory IN ('Fresh Frozen', 'Cured Flower'))
    OR (unit_of_measure = 'Grams' AND item_subcategory IN ('Live Resin Pre-Decarb', 'Live Resin Decarb', 'Live Rosin Decarb'))
  GROUP BY bucket_id
) stats ON stats.bucket_id = pb.bucket_id
ORDER BY pb.sort_order
