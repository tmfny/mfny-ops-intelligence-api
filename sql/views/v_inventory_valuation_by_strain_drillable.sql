CREATE OR REPLACE VIEW `mfny-to-bigquery.canix_raw.v_inventory_valuation_by_strain_drillable` AS
WITH coerced AS (
  SELECT
    CASE
      WHEN strain_name IS NULL OR strain_name = '' THEN 'Unspecified'
      ELSE strain_name
    END AS strain_name,
    display_category,
    unit_count,
    book_value
  FROM `mfny-to-bigquery.canix_raw.v_inventory_valuation`
)
SELECT
  strain_name,
  display_category,
  COUNT(*)                                        AS package_count,
  CAST(ROUND(SUM(unit_count), 0) AS INT64)        AS total_units,
  ROUND(SUM(book_value), 2)                       AS book_value
FROM coerced
GROUP BY strain_name, display_category
ORDER BY
  CASE WHEN strain_name = 'Unspecified' THEN 1 ELSE 0 END,
  book_value DESC
