CREATE OR REPLACE VIEW `mfny-to-bigquery.canix_raw.v_yield_efficiency_by_run` AS
WITH run_input_totals AS (
  SELECT
    r.id AS run_id,
    SUM(
      CAST(JSON_EXTRACT_SCALAR(input, '$.quantity') AS FLOAT64) *
      CASE
        WHEN JSON_EXTRACT_SCALAR(input, '$.weight_unit') = 'Pounds' THEN 453.592
        WHEN JSON_EXTRACT_SCALAR(input, '$.weight_unit') = 'Kilograms' THEN 1000.0
        WHEN JSON_EXTRACT_SCALAR(input, '$.weight_unit') = 'Grams' THEN 1.0
        ELSE 1.0
      END
    ) AS input_grams,
    LOGICAL_OR(JSON_EXTRACT_SCALAR(input, '$.weight_unit') = 'Each') AS input_has_each,
    COUNT(*) AS input_package_count
  FROM `mfny-to-bigquery.canix_raw.bronze_manu_batch_runs` r,
    UNNEST(JSON_EXTRACT_ARRAY(r.cannabis_inputs)) AS input
  GROUP BY r.id
),

run_output_totals AS (
  SELECT
    r.id AS run_id,
    SUM(
      CAST(JSON_EXTRACT_SCALAR(output, '$.quantity') AS FLOAT64) *
      CASE
        WHEN JSON_EXTRACT_SCALAR(output, '$.weight_unit') = 'Pounds' THEN 453.592
        WHEN JSON_EXTRACT_SCALAR(output, '$.weight_unit') = 'Kilograms' THEN 1000.0
        WHEN JSON_EXTRACT_SCALAR(output, '$.weight_unit') = 'Grams' THEN 1.0
        ELSE 1.0
      END
    ) AS output_grams,
    LOGICAL_OR(JSON_EXTRACT_SCALAR(output, '$.weight_unit') = 'Each') AS output_has_each,
    COUNT(*) AS output_package_count
  FROM `mfny-to-bigquery.canix_raw.bronze_manu_batch_runs` r,
    UNNEST(JSON_EXTRACT_ARRAY(r.cannabis_outputs)) AS output
  GROUP BY r.id
),

run_strain_context AS (
  SELECT
    r.id AS run_id,
    p.strain_name AS strain,
    p.item_name AS input_item_name,
    JSON_EXTRACT_SCALAR(p._raw_json, '$.source_harvests') AS source_harvest
  FROM `mfny-to-bigquery.canix_raw.bronze_manu_batch_runs` r
  LEFT JOIN `mfny-to-bigquery.canix_raw.bronze_packages` p
    ON CAST(JSON_EXTRACT_SCALAR(r.cannabis_inputs, '$[0].package_id') AS STRING) = p.id
),

categorized AS (
  -- Classify each run by name pattern. Order matters — more specific
  -- patterns first so they win when multiple keywords could match.
  --
  -- Packaging runs are excluded entirely via WHERE clause below since
  -- their yield is always ~100% and adds no signal.
  SELECT
    r.id AS run_id,
    r.name AS run_name,
    r.status,
    r.start_date,
    r.end_date,
    r.yield AS canix_yield_pct,
    CASE
      -- Pressing first (so "Press" doesn't fall into extraction)
      WHEN r.name = 'Press' OR r.name LIKE 'Press %' THEN 'pressing'
      -- Biomass processing (post-harvest handling of raw flower)
      WHEN r.name LIKE '%Buck%Trim%' THEN 'biomass_processing'
      WHEN r.name LIKE '%Curing%Bucking%' THEN 'biomass_processing'
      WHEN r.name LIKE '%Sort%Sift%Grind%' THEN 'biomass_processing'
      WHEN r.name = 'Cure Biomass' THEN 'biomass_processing'
      WHEN r.name LIKE 'Freeze Dry %' AND r.name NOT LIKE '%Sift%' THEN 'biomass_processing'
      -- Extraction-stage transformations (biomass to concentrate)
      WHEN r.name LIKE '%Fresh Freeze%' THEN 'extraction'
      WHEN r.name LIKE '%Extraction%' THEN 'extraction'
      WHEN r.name LIKE '%Wash%' AND r.name LIKE '%Sift%' THEN 'extraction'
      -- Decarb (concentrate to decarbed)
      WHEN r.name LIKE '%Decarb%' THEN 'decarb'
      -- Combining/portioning (concentrate manipulation, near-100% yield)
      WHEN r.name LIKE '%Combine%' THEN 'processing'
      WHEN r.name LIKE '%Portion%' THEN 'processing'
      WHEN r.name LIKE '%Re-Prep%' THEN 'processing'
      WHEN r.name LIKE 'Gram %' THEN 'processing'
      WHEN r.name LIKE 'Gramming %' THEN 'processing'
      -- Product manufacturing (adds non-cannabis weight; yield not meaningful)
      WHEN r.name LIKE '%Produce%' THEN 'manufacturing'
      WHEN r.name LIKE '%Demold%' THEN 'manufacturing'
      WHEN r.name LIKE 'Fill%' THEN 'manufacturing'
      WHEN r.name LIKE '%Bottle%' THEN 'manufacturing'
      WHEN r.name LIKE '%Make and%' THEN 'manufacturing'
      WHEN r.name LIKE '%Infuse%' THEN 'manufacturing'
      WHEN r.name LIKE '%Cure %Bag%' THEN 'manufacturing'
      ELSE 'other'
    END AS run_category
  FROM `mfny-to-bigquery.canix_raw.bronze_manu_batch_runs` r
  WHERE
    -- Filter out pure packaging runs entirely (always ~100% yield)
    r.name NOT IN (
      'QC and Clean',
      'Clean & Tube',
      'Label & Final Packout',
      'Cure & Package',
      'Box, Label & Final Packout',
      'Clean, Label & Final Packout',
      'Clean, Seal, Label & Final Packout',
      'Final Packout',
      'Label Bags'
    )
    -- Also filter out parameterized packaging variants
    AND r.name NOT LIKE 'Clean, Label & Final Packout%'
    AND r.name NOT LIKE 'Clean, Seal, Label & Final Packout%'
)

SELECT
  c.run_id,
  c.run_name,
  c.run_category,
  c.status,
  c.start_date,
  c.end_date,
  ctx.strain,
  ctx.input_item_name,
  NULLIF(ctx.source_harvest, '') AS source_harvest,
  ROUND(COALESCE(inp.input_grams, 0), 2) AS input_grams,
  CASE 
    WHEN c.status IN ('OPEN', 'PENDING_CONFIGURATION') THEN NULL
    ELSE ROUND(COALESCE(out.output_grams, 0), 2)
  END AS output_grams,
  COALESCE(inp.input_package_count, 0) AS input_package_count,
  COALESCE(out.output_package_count, 0) AS output_package_count,
  ROUND(c.canix_yield_pct, 2) AS canix_yield_pct,
  CASE
    WHEN c.status IN ('OPEN', 'PENDING_CONFIGURATION') THEN NULL
    -- Manufacturing: yield meaningless (non-cannabis weight added)
    WHEN c.run_category = 'manufacturing' THEN NULL
    -- "Each" units don't divide cleanly with grams
    WHEN inp.input_has_each OR out.output_has_each THEN NULL
    WHEN COALESCE(inp.input_grams, 0) = 0 THEN NULL
    ELSE ROUND(100.0 * COALESCE(out.output_grams, 0) / inp.input_grams, 2)
  END AS computed_yield_pct
FROM categorized c
LEFT JOIN run_input_totals inp ON c.run_id = inp.run_id
LEFT JOIN run_output_totals out ON c.run_id = out.run_id
LEFT JOIN run_strain_context ctx ON c.run_id = ctx.run_id
ORDER BY c.start_date DESC, c.run_id DESC
