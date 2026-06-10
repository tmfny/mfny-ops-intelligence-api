CREATE OR REPLACE VIEW `mfny-to-bigquery.canix_raw.v_executive_biomass_health` AS
WITH
active_biomass AS (
  SELECT
    strain_name,
    item_category,
    CASE
      WHEN unit_of_measure = 'Pounds' THEN weight
      WHEN unit_of_measure = 'Grams'  THEN weight / 453.592
      ELSE NULL
    END AS weight_lbs,
    packaged_date
  FROM `mfny-to-bigquery.canix_raw.bronze_packages`
  WHERE facility_id = '4511'
    AND item_category IN (
      'Bud/Flower - Bulk',
      'Shake/Trim (by strain) - Bulk',
      'Infused Flower - Bulk'
    )
    AND status NOT IN ('Transferred', 'Inactive')
    AND CASE
          WHEN unit_of_measure = 'Pounds' THEN weight
          WHEN unit_of_measure = 'Grams'  THEN weight / 453.592
          ELSE NULL
        END > 0
    AND _ingested_at = (
      SELECT MAX(_ingested_at)
      FROM `mfny-to-bigquery.canix_raw.bronze_packages`
    )
)
SELECT
  COALESCE(NULLIF(TRIM(strain_name), ''), 'Unspecified') AS strain,
  COUNT(*) AS package_count,
  ROUND(SUM(weight_lbs), 1) AS total_weight_lbs,
  ROUND(AVG(DATE_DIFF(CURRENT_DATE(), packaged_date, DAY)), 0) AS avg_days_on_hand
FROM active_biomass
GROUP BY strain
ORDER BY total_weight_lbs DESC
