CREATE OR REPLACE VIEW `mfny-to-bigquery.canix_raw.v_dead_inventory` AS
SELECT
  id,
  tag,
  item_name,
  item_category,
  item_subcategory,
  strain_name,
  status,
  is_active,
  weight,
  unit_of_measure,
  packaged_date,
  use_by_date,
  DATE_DIFF(CURRENT_DATE(), packaged_date, DAY)  AS days_since_packaged,
  DATE_DIFF(CURRENT_DATE(), DATE(updated_at), DAY) AS days_since_canix_update,
  room_name,
  location_name,
  facility_id,
  source_batch_id,
  created_at,
  updated_at,
  CASE
    WHEN facility_id = '4475' THEN 'pre_metrc_facility_4475'
    WHEN item_category IN (
      'Edible (count)', 'Edible (weight)',
      'Vape Cart (count)',
      'Combined (count)', 'Combined (weight)',
      'Concentrate (count)', 'Concentrate (weight)',
      'Tincture (count)', 'Extract (weight)',
      'Seeds (count)'
    ) THEN 'pre_metrc_legacy_category'
    WHEN packaged_date < '2025-12-01' THEN 'pre_metrc_packaged_date_still_active'
    ELSE 'other'
  END AS dead_reason
FROM `mfny-to-bigquery.canix_raw.bronze_packages`
WHERE is_active = TRUE
  AND (
    facility_id = '4475'
    OR item_category IN (
      'Edible (count)', 'Edible (weight)',
      'Vape Cart (count)',
      'Combined (count)', 'Combined (weight)',
      'Concentrate (count)', 'Concentrate (weight)',
      'Tincture (count)', 'Extract (weight)',
      'Seeds (count)'
    )
    OR (packaged_date < '2025-12-01' AND packaged_date IS NOT NULL)
  )
ORDER BY
  facility_id,
  days_since_packaged DESC
