CREATE OR REPLACE VIEW `mfny-to-bigquery.canix_raw.v_production_velocity_weekly` AS
WITH weekly_runs AS (
  SELECT
    DATE_TRUNC(r.end_date, WEEK(MONDAY)) as week_start,
    b.template_name,
    COUNT(*) as runs_completed,
    COUNT(DISTINCT r.manufacturing_batch_id) as batches_completed,
    ROUND(SUM(r.total_cannabis_costs), 2) as total_cannabis_costs,
    ROUND(SUM(r.total_labor_costs), 2) as total_labor_costs,
    ROUND(SUM(r.total_nci_costs), 2) as total_nci_costs,
    ROUND(SUM(r.total_cannabis_costs + r.total_labor_costs + r.total_nci_costs), 2) as total_costs
  FROM `mfny-to-bigquery.canix_raw.bronze_manu_batch_runs` r
  JOIN `mfny-to-bigquery.canix_raw.bronze_manu_batches` b
    ON r.manufacturing_batch_id = b.id
  WHERE r.status = 'SUBMITTED'
    AND r.end_date IS NOT NULL
    AND r.end_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 26 WEEK)
    -- Exclude SANDBOX facility (4475) — not real production data
    AND r.facility_id != '4475'
  GROUP BY week_start, b.template_name
)
SELECT
  week_start,
  template_name,
  runs_completed,
  batches_completed,
  total_cannabis_costs,
  total_labor_costs,
  total_nci_costs,
  total_costs,
  SUM(runs_completed) OVER (
    PARTITION BY template_name
    ORDER BY week_start
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) as cumulative_runs,
  ROUND(AVG(runs_completed) OVER (
    PARTITION BY template_name
    ORDER BY week_start
    ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
  ), 1) as rolling_4wk_avg
FROM weekly_runs
ORDER BY week_start DESC, runs_completed DESC
