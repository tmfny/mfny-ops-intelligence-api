CREATE OR REPLACE VIEW `mfny-to-bigquery.canix_raw.v_inventory_valuation_summary` AS
SELECT
  COUNT(*)                                        AS total_packages,
  CAST(ROUND(SUM(unit_count), 0) AS INT64)        AS total_units,
  ROUND(SUM(book_value), 2)                       AS total_book_value,
  COUNTIF(pricing_status = 'unmatched')           AS unmatched_packages,
  COUNT(DISTINCT item_subcategory)                AS distinct_skus,
  COUNT(DISTINCT NULLIF(strain_name, ''))         AS distinct_strains
FROM `mfny-to-bigquery.canix_raw.v_inventory_valuation`
