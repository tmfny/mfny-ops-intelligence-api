CREATE OR REPLACE VIEW `mfny-to-bigquery.canix_raw.v_inventory_health_multi_uom` AS
WITH

scoped AS (
  SELECT
    status,
    item_category,
    unit_of_measure,
    weight,
    facility_id,
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
    END AS status_bucket
  FROM `mfny-to-bigquery.canix_raw.bronze_packages`
  WHERE is_active = TRUE
    AND weight > 0
    AND facility_id IN ('4510', '4511')
    AND item_category LIKE '% - Each'
)

SELECT
  status_bucket,
  COUNT(*) AS total_packages,

  COUNTIF(unit_of_measure = 'Each')                                    AS each_packages,
  ROUND(SUM(IF(unit_of_measure = 'Each',   weight, 0)), 2)             AS each_quantity,

  COUNTIF(unit_of_measure = 'Grams')                                   AS grams_packages,
  ROUND(SUM(IF(unit_of_measure = 'Grams',  weight, 0)), 2)             AS grams_quantity,

  COUNTIF(unit_of_measure = 'Pounds')                                  AS pounds_packages,
  ROUND(SUM(IF(unit_of_measure = 'Pounds', weight, 0)), 2)             AS pounds_quantity,

  CURRENT_TIMESTAMP() AS _view_built_at
FROM scoped
WHERE status_bucket != 'other'
GROUP BY status_bucket
ORDER BY
  CASE status_bucket
    WHEN 'sellable'     THEN 1
    WHEN 'reserved'     THEN 2
    WHEN 'non_sellable' THEN 3
    WHEN 'in_progress'  THEN 4
  END
