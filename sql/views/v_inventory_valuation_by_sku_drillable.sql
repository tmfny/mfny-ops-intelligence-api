CREATE OR REPLACE VIEW `mfny-to-bigquery.canix_raw.v_inventory_valuation_by_sku_drillable` AS
WITH coerced AS (
  SELECT
    item_subcategory,
    display_category,
    CASE
      WHEN strain_name IS NULL OR strain_name = '' THEN 'Unspecified'
      ELSE strain_name
    END AS strain_name,
    wholesale_price_per_unit,
    unit_count,
    book_value
  FROM `mfny-to-bigquery.canix_raw.v_inventory_valuation`
)
SELECT
  item_subcategory,
  ANY_VALUE(display_category)                     AS display_category,
  strain_name,
  ANY_VALUE(wholesale_price_per_unit)             AS wholesale_price_per_unit,
  COUNT(*)                                        AS package_count,
  CAST(ROUND(SUM(unit_count), 0) AS INT64)        AS total_units,
  ROUND(SUM(book_value), 2)                       AS book_value
FROM coerced
GROUP BY item_subcategory, strain_name
ORDER BY book_value DESC
