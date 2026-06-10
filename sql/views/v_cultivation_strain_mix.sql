CREATE OR REPLACE VIEW `mfny-to-bigquery.canix_raw.v_cultivation_strain_mix` AS
SELECT
  strain_name,
  COUNT(*) AS total_plants,
  COUNTIF(growth_phase = 'Flowering') AS flowering_count,
  COUNTIF(growth_phase = 'Vegetative') AS vegetative_count,
  COUNT(DISTINCT facility_id) AS facilities,
  COUNT(DISTINCT plant_batch_id) AS distinct_batches,
  COUNT(DISTINCT 
    REGEXP_EXTRACT(UPPER(location_name), r'^(GH\d+)')
  ) AS greenhouses_present,
  ROUND(100.0 * COUNTIF(growth_phase = 'Flowering') / COUNT(*), 1) AS pct_in_flower
FROM `mfny-to-bigquery.canix_raw.bronze_plants`
WHERE facility_id != '4475'
  AND strain_name IS NOT NULL
  AND strain_name != ''
  AND harvested_date IS NULL  -- exclude harvested-but-not-destroyed plants
GROUP BY strain_name
HAVING total_plants > 0  -- defensive: drop empty strains (redundant given filters above)
ORDER BY total_plants DESC
