CREATE OR REPLACE VIEW `mfny-to-bigquery.canix_raw.v_inventory_valuation` AS
WITH

sellable_packages AS (
  SELECT
    id                AS package_id,
    tag               AS package_tag,
    facility_id,
    location_name,
    item_name,
    item_subcategory,
    item_category,
    strain_name,
    weight            AS unit_count,
    packaged_date,
    status
  FROM `mfny-to-bigquery.canix_raw.bronze_packages`
  WHERE facility_id = '4510'
    AND status = 'Available To Sell'
    AND _ingested_at = (
      SELECT MAX(_ingested_at)
      FROM `mfny-to-bigquery.canix_raw.bronze_packages`
    )
    AND NOT REGEXP_CONTAINS(item_subcategory, r'(?i)samples')
),

priced AS (
  SELECT
    sp.*,
    p.wholesale_price_per_unit,
    CASE
      WHEN p.wholesale_price_per_unit IS NULL THEN 'unmatched'
      ELSE 'priced'
    END AS pricing_status,
    SAFE_MULTIPLY(sp.unit_count, p.wholesale_price_per_unit) AS book_value
  FROM sellable_packages sp
  LEFT JOIN `mfny-to-bigquery.canix_raw.dim_wholesale_prices` p
    ON sp.item_subcategory = p.item_subcategory
)

SELECT
  package_id,
  package_tag,
  facility_id,
  location_name,
  item_name,
  item_subcategory,
  item_category,
  CASE
    WHEN item_category = 'Concentrate/Extract - Each' THEN 'Concentrates'
    WHEN item_category = 'Infused Edible - Each'      THEN 'Edibles'
    WHEN item_category = 'Vape Cartridge - Each'      THEN 'Vapes'
    WHEN item_category = 'Infused Pre-Roll - Each'    THEN 'Pre-Rolls'
    WHEN item_category = 'Tincture - Each'            THEN 'Tinctures'
    WHEN item_category = 'Topical - Each'             THEN 'Topicals'
    ELSE 'Other'
  END AS display_category,
  -- CHANGED: fall back to strain parsed from item_name when the
  -- structured strain_name is empty. Real strain_name always wins.
  -- Parse = everything in item_name before "Live Resin"/"Live Rosin".
  CASE
    WHEN strain_name IS NOT NULL AND TRIM(strain_name) != '' THEN strain_name
    WHEN REGEXP_CONTAINS(item_name, r'(?i)\bLive (Resin|Rosin)\b')
      THEN TRIM(REGEXP_EXTRACT(item_name, r'(?i)^(.*?)\s+Live (?:Resin|Rosin)\b'))
    ELSE strain_name  -- leave as-is (empty); downstream maps to 'Unspecified'
  END AS strain_name,
  unit_count,
  wholesale_price_per_unit,
  book_value,
  pricing_status,
  packaged_date,
  status
FROM priced
