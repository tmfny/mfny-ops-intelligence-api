CREATE OR REPLACE VIEW `mfny-to-bigquery.canix_raw.v_inventory_health_breakdown` AS
WITH

finished_goods AS (
  SELECT
    'finished_goods'                                                    AS scope,
    CASE
      WHEN status = 'Available To Sell' THEN 'sellable'
      WHEN status = 'Allocated'         THEN 'reserved'
      WHEN status IN (
        'Retention',
        'In Quarantine',
        'In Transit',
        'To be sent for testing',
        'Restricted - Sent as Field Samples'
      ) THEN 'non_sellable'
      WHEN status = 'In Progress'       THEN 'in_progress'
      ELSE 'other'
    END                                                                 AS status_bucket,
    status                                                              AS canix_status,
    item_category,
    unit_of_measure,
    facility_id,
    weight
  FROM `mfny-to-bigquery.canix_raw.bronze_packages`
  WHERE is_active = TRUE
    AND weight > 0
    AND facility_id IN ('4510', '4511')
    AND item_category LIKE '% - Each'
),

pre_production AS (
  SELECT
    'pre_production'                                                    AS scope,
    'pre_production'                                                    AS status_bucket,
    status                                                              AS canix_status,
    item_category,
    unit_of_measure,
    facility_id,
    weight
  FROM `mfny-to-bigquery.canix_raw.bronze_packages`
  WHERE is_active = TRUE
    AND weight > 0
    AND facility_id = '4511'
    AND status NOT IN ('Transferred', 'Inactive')
    AND (
      item_category LIKE '% - Bulk'
      OR item_category = 'Kief - Bulk'
    )
),

combined AS (
  SELECT * FROM finished_goods
  UNION ALL
  SELECT * FROM pre_production
)

SELECT
  scope,
  status_bucket,
  canix_status,
  item_category,
  unit_of_measure,
  facility_id,
  COUNT(*)                       AS package_count,
  ROUND(SUM(weight), 2)          AS total_weight,
  CURRENT_TIMESTAMP()            AS _view_built_at
FROM combined
WHERE status_bucket != 'other'
GROUP BY scope, status_bucket, canix_status, item_category, unit_of_measure, facility_id
ORDER BY scope, status_bucket, canix_status, item_category, unit_of_measure, facility_id
