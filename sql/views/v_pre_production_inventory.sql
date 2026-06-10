CREATE OR REPLACE VIEW `mfny-to-bigquery.canix_raw.v_pre_production_inventory` AS
WITH

scoped AS (
  SELECT
    item_category,
    unit_of_measure,
    weight,
    strain_name,
    packaged_date
  FROM `mfny-to-bigquery.canix_raw.bronze_packages`
  WHERE is_active = TRUE
    AND weight > 0
    AND facility_id = '4511'
    AND status NOT IN ('Transferred', 'Inactive')
    AND (
      item_category LIKE '% - Bulk'
      OR item_category = 'Kief - Bulk'
    )
    AND _ingested_at = (
      SELECT MAX(_ingested_at)
      FROM `mfny-to-bigquery.canix_raw.bronze_packages`
    )
)

SELECT
  COUNT(*)                                                              AS total_packages,

  COUNTIF(unit_of_measure = 'Grams')                                    AS grams_packages,
  ROUND(SUM(IF(unit_of_measure = 'Grams',  weight, 0)), 2)              AS grams_quantity,

  COUNTIF(unit_of_measure = 'Pounds')                                   AS pounds_packages,
  ROUND(SUM(IF(unit_of_measure = 'Pounds', weight, 0)), 2)              AS pounds_quantity,

  COUNT(DISTINCT
    COALESCE(NULLIF(TRIM(strain_name), ''), 'Unspecified')
  )                                                                     AS distinct_strains,

  COUNT(DISTINCT item_category)                                         AS distinct_categories,

  ROUND(AVG(DATE_DIFF(CURRENT_DATE(), packaged_date, DAY)), 0)          AS avg_days_on_hand,

  CURRENT_TIMESTAMP()                                                   AS _view_built_at
FROM scoped
