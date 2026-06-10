CREATE OR REPLACE VIEW `mfny-to-bigquery.canix_raw.v_biomass_transformation` AS
WITH parsed AS (
  SELECT
    b.name as batch_name,
    b.template_name,
    r.id as run_id,
    r.name as run_name,
    r.status,
    r.start_date,
    r.end_date,
    (
      SELECT ROUND(SUM(CAST(JSON_VALUE(inp, '$.quantity') AS FLOAT64)), 2)
      FROM UNNEST(JSON_QUERY_ARRAY(r.cannabis_inputs)) AS inp
    ) as total_input_lbs,
    (
      SELECT ROUND(SUM(CAST(JSON_VALUE(out, '$.quantity') AS FLOAT64)), 2)
      FROM UNNEST(JSON_QUERY_ARRAY(r.cannabis_outputs)) AS out
    ) as total_output_lbs,
    REGEXP_EXTRACT(b.name, r'CBT (.+?) Grow') as strain_name,
    REGEXP_EXTRACT(b.name, r'Grow (\d+)') as grow_number
  FROM `mfny-to-bigquery.canix_raw.bronze_manu_batch_runs` r
  JOIN `mfny-to-bigquery.canix_raw.bronze_manu_batches` b
    ON r.manufacturing_batch_id = b.id
  WHERE b.template_name IN ('Cure, Buck & Trim Biomass', 'Buck & Trim Cured Biomass')
    AND r.cannabis_inputs IS NOT NULL
    AND r.cannabis_inputs != '[]'
    -- Defensive: only completed runs (yield/days_since_completion otherwise misleading)
    AND r.status = 'SUBMITTED'
    -- Exclude SANDBOX facility (4475) — not real production data
    AND r.facility_id != '4475'
)
SELECT
  batch_name,
  template_name,
  run_id,
  run_name,
  status,
  start_date,
  end_date,
  strain_name,
  grow_number,
  total_input_lbs,
  total_output_lbs,
  ROUND((total_output_lbs / NULLIF(total_input_lbs, 0)) * 100, 1) as yield_pct,
  DATE_DIFF(CURRENT_DATE(), end_date, DAY) as days_since_completion
FROM parsed
WHERE total_input_lbs IS NOT NULL
ORDER BY end_date DESC
