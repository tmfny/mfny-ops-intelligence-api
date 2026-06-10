CREATE OR REPLACE VIEW `mfny-to-bigquery.canix_raw.v_next_actions` AS
WITH
run_qty AS (
  SELECT
    r.id AS run_id,
    r.manufacturing_batch_id AS batch_id,
    r.name AS run_name,
    r.status,
    r.run_order,
    r.created_at,
    r.start_date,                          -- NEW: started vs not-started signal
    COALESCE((
      SELECT SUM(CAST(JSON_EXTRACT_SCALAR(elem, '$.quantity') AS FLOAT64))
      FROM UNNEST(JSON_EXTRACT_ARRAY(r.cannabis_inputs)) AS elem
    ), 0) AS input_qty,
    COALESCE((
      SELECT SUM(CAST(JSON_EXTRACT_SCALAR(elem, '$.quantity') AS FLOAT64))
      FROM UNNEST(JSON_EXTRACT_ARRAY(r.cannabis_outputs)) AS elem
    ), 0) AS output_qty
  FROM `mfny-to-bigquery.canix_raw.bronze_manu_batch_runs` r
  WHERE r.facility_id != '4475'
),
active_runs AS (
  SELECT q.*
  FROM run_qty q
  JOIN `mfny-to-bigquery.canix_raw.v_active_batches` ab
    ON CAST(q.batch_id AS STRING) = CAST(ab.batch_id AS STRING)
),
first_open AS (
  SELECT *
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY batch_id ORDER BY run_order) AS rn
    FROM active_runs
    WHERE status != 'SUBMITTED'
  )
  WHERE rn = 1
),
classified AS (
  SELECT
    fo.batch_id,
    fo.run_id,
    b.name AS batch_name,
    fo.run_name,
    fo.status,
    fo.run_order,
    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), fo.created_at, HOUR) AS age_hours,
    fo.input_qty,
    fo.output_qty,
    -- Priority + action: first matching rule wins (most specific first).
    -- start_date populated = run has actually been started.
    CASE
      WHEN fo.status = 'SUBMITTED_FOR_APPROVAL'                              THEN 1
      WHEN fo.status = 'ERRORED'                                            THEN 1
      WHEN fo.status = 'PENDING_CONFIGURATION'                              THEN 3
      WHEN fo.status = 'OPEN' AND fo.start_date IS NULL                     THEN 2
      WHEN fo.status = 'OPEN' AND fo.input_qty = 0                          THEN 2
      WHEN fo.status = 'OPEN' AND fo.output_qty = 0                         THEN 2
      WHEN fo.status = 'OPEN' AND fo.output_qty > 0                         THEN 2
      ELSE 4
    END AS priority,
    CASE
      WHEN fo.status = 'SUBMITTED_FOR_APPROVAL'                              THEN 'Approve run'
      WHEN fo.status = 'ERRORED'                                            THEN 'Investigate errored run'
      WHEN fo.status = 'PENDING_CONFIGURATION'                              THEN 'Set up next run'
      WHEN fo.status = 'OPEN' AND fo.start_date IS NULL                     THEN 'Start run'
      WHEN fo.status = 'OPEN' AND fo.input_qty = 0                          THEN 'Investigate missing material'
      WHEN fo.status = 'OPEN' AND fo.output_qty = 0                         THEN 'In progress — continue'
      WHEN fo.status = 'OPEN' AND fo.output_qty > 0                         THEN 'Finalize / submit run'
      ELSE 'Advance production'
    END AS action
  FROM first_open fo
  JOIN `mfny-to-bigquery.canix_raw.bronze_manu_batches` b
    ON fo.batch_id = b.id
)
SELECT
  priority,
  batch_name,
  run_name,
  status,
  action,
  age_hours,
  CAST(run_id AS STRING) AS run_id,
  CAST(batch_id AS STRING) AS batch_id
FROM classified
ORDER BY priority ASC, age_hours DESC
