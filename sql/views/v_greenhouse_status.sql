CREATE OR REPLACE VIEW `mfny-to-bigquery.canix_raw.v_greenhouse_status` AS
WITH facility_mapping AS (
  SELECT facility_id, facility_name FROM UNNEST([
    STRUCT('4509' AS facility_id, 'MFNY Ops Cultivation' AS facility_name),
    STRUCT('4528', 'American Seed & Oil'),
    STRUCT('4699', 'KickFly'),
    STRUCT('4675', 'RAGA Growers')
  ])
),

plants_classified AS (
  -- Extract greenhouse label from location_name (e.g. "GH3 - Bay 8 - Row 2" → "GH3").
  -- UPPER() handles inconsistent casing ("Gh6" vs "GH6").
  -- Special locations (Clone Room, Mother Room) are labeled explicitly.
  -- Anything else falls into "Unclassified".
  --
  -- CRITICAL FILTER: Canix's API reports "Flowering" growth_phase for plants
  -- whose harvested_date is populated (i.e. already harvested but not destroyed).
  -- We filter these out so "active" means truly still growing.
  SELECT
    p.facility_id,
    CASE
      WHEN REGEXP_EXTRACT(UPPER(p.location_name), r'^(GH\d+)') IS NOT NULL
        THEN REGEXP_EXTRACT(UPPER(p.location_name), r'^(GH\d+)')
      WHEN UPPER(p.location_name) LIKE '%CLONE%' THEN 'Clone Room'
      WHEN UPPER(p.location_name) LIKE '%MOTHER%' THEN 'Mother Room'
      ELSE 'Unclassified'
    END AS greenhouse,
    p.growth_phase,
    p.strain_name,
    p.age_in_days,
    p.flowering_date,
    p.vegetative_date,
    p.planted_date
  FROM `mfny-to-bigquery.canix_raw.bronze_plants` p
  WHERE p.facility_id != '4475'  -- SANDBOX exclusion
    AND p.harvested_date IS NULL  -- exclude harvested-but-not-destroyed plants
),

strain_ranks AS (
  SELECT
    facility_id,
    greenhouse,
    strain_name,
    COUNT(*) AS strain_plant_count,
    ROW_NUMBER() OVER (
      PARTITION BY facility_id, greenhouse
      ORDER BY COUNT(*) DESC
    ) AS strain_rank
  FROM plants_classified
  WHERE strain_name IS NOT NULL AND strain_name != ''
  GROUP BY facility_id, greenhouse, strain_name
),

top_strains AS (
  SELECT
    facility_id,
    greenhouse,
    STRING_AGG(
      CONCAT(strain_name, ' (', CAST(strain_plant_count AS STRING), ')'),
      ', '
      ORDER BY strain_plant_count DESC
    ) AS top_strains_display
  FROM strain_ranks
  WHERE strain_rank <= 3
  GROUP BY facility_id, greenhouse
),

aggregated AS (
  SELECT
    facility_id,
    greenhouse,
    COUNT(*) AS plant_count,
    COUNTIF(growth_phase = 'Vegetative') AS vegetative_count,
    COUNTIF(growth_phase = 'Flowering') AS flowering_count,
    ROUND(AVG(age_in_days), 1) AS avg_age_days,
    ROUND(
      AVG(IF(growth_phase = 'Flowering',
             DATE_DIFF(CURRENT_DATE(), DATE(flowering_date), DAY),
             NULL)),
      1
    ) AS avg_days_in_flower,
    MIN(IF(growth_phase = 'Flowering', DATE(flowering_date), NULL)) AS oldest_flowering_date,
    MAX(DATE(planted_date)) AS youngest_planted_date,
    COUNT(DISTINCT NULLIF(strain_name, '')) AS distinct_strains
  FROM plants_classified
  GROUP BY facility_id, greenhouse
)

SELECT
  a.facility_id,
  COALESCE(fm.facility_name, CONCAT('Unknown (', a.facility_id, ')')) AS facility_name,
  a.greenhouse,
  a.plant_count,
  a.vegetative_count,
  a.flowering_count,
  a.avg_age_days,
  a.avg_days_in_flower,
  a.oldest_flowering_date,
  a.youngest_planted_date,
  a.distinct_strains,
  COALESCE(ts.top_strains_display, '(no strain data)') AS top_strains
FROM aggregated a
LEFT JOIN facility_mapping fm ON a.facility_id = fm.facility_id
LEFT JOIN top_strains ts ON a.facility_id = ts.facility_id AND a.greenhouse = ts.greenhouse
WHERE a.plant_count > 0
ORDER BY facility_name, greenhouse
