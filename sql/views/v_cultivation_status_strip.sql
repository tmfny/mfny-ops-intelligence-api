CREATE OR REPLACE VIEW `mfny-to-bigquery.canix_raw.v_cultivation_status_strip` AS
SELECT
  COUNT(*) AS total_active_plants,
  COUNTIF(growth_phase = 'Flowering') AS flowering_count,
  COUNTIF(growth_phase = 'Vegetative') AS vegetative_count,
  COUNT(DISTINCT facility_id) AS active_facilities,
  COUNT(DISTINCT strain_name) AS distinct_strains_in_cultivation,
  COUNT(DISTINCT 
    REGEXP_EXTRACT(UPPER(location_name), r'^(GH\d+)')
  ) AS active_greenhouses
FROM `mfny-to-bigquery.canix_raw.bronze_plants`
WHERE facility_id != '4475'  -- SANDBOX exclusion
  AND strain_name IS NOT NULL
  AND strain_name != ''
  AND harvested_date IS NULL
