CREATE OR REPLACE VIEW `mfny-to-bigquery.canix_raw.v_inventory_valuation_by_sku` AS
SELECT
  item_subcategory,
  ANY_VALUE(display_category)                     AS display_category,
  ANY_VALUE(wholesale_price_per_unit)             AS wholesale_price_per_unit,
  COUNT(*)                                        AS package_count,
  CAST(ROUND(SUM(unit_count), 0) AS INT64)        AS total_units,
  ROUND(SUM(book_value), 2)                       AS book_value
FROM `mfny-to-bigquery.canix_raw.v_inventory_valuation`
GROUP BY item_subcategory
ORDER BY book_value DESC
