CREATE OR REPLACE VIEW `mfny-to-bigquery.canix_raw.v_executive_biomass_summary` AS
WITH
active_biomass AS (
  SELECT
    strain_name,
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
),
totals AS (
  SELECT
    ROUND(SUM(weight_lbs), 0) AS total_biomass_lbs,
    COUNT(*) AS total_packages,
    COUNT(DISTINCT 
      COALESCE(NULLIF(TRIM(strain_name), ''), 'Unspecified')
    ) AS distinct_strains
  FROM active_biomass
),
top_strain AS (
  SELECT
    COALESCE(NULLIF(TRIM(strain_name), ''), 'Unspecified') AS top_strain_name,
    ROUND(SUM(weight_lbs), 0) AS top_strain_lbs
  FROM active_biomass
  GROUP BY top_strain_name
  ORDER BY top_strain_lbs DESC
  LIMIT 1
)
SELECT
  t.total_biomass_lbs,
  t.total_packages,
  t.distinct_strains,
  ts.top_strain_name,
  ts.top_strain_lbs
FROM totals t
CROSS JOIN top_strain ts
