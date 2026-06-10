CREATE OR REPLACE VIEW `mfny-to-bigquery.canix_raw.v_executive_inventory_health` AS
WITH

-- ============================================================
-- Distribution facility (4510) snapshot — current finished goods
-- ============================================================
distribution_packages AS (
  SELECT
    item_category,
    status
  FROM `mfny-to-bigquery.canix_raw.bronze_packages`
  WHERE facility_id = '4510'
    AND _ingested_at = (
      SELECT MAX(_ingested_at)
      FROM `mfny-to-bigquery.canix_raw.bronze_packages`
    )
),

-- ============================================================
-- Map raw item_category to clean display categories
-- ============================================================
-- Display categories: Concentrates, Edibles, Vapes, Pre-Rolls, Tinctures, Topicals.
-- Excludes biomass/intermediate categories — those don't live at 4510 anyway.
-- ============================================================
categorized AS (
  SELECT
    CASE
      WHEN item_category = 'Concentrate/Extract - Each' THEN 'Concentrates'
      WHEN item_category = 'Infused Edible - Each'      THEN 'Edibles'
      WHEN item_category = 'Vape Cartridge - Each'      THEN 'Vapes'
      WHEN item_category = 'Infused Pre-Roll - Each'    THEN 'Pre-Rolls'
      WHEN item_category = 'Tincture - Each'            THEN 'Tinctures'
      WHEN item_category = 'Topical - Each'             THEN 'Topicals'
      ELSE 'Other'
    END AS category,
    status
  FROM distribution_packages
),

-- ============================================================
-- Per-category breakdown
-- ============================================================
by_category AS (
  SELECT
    category,
    COUNT(*) AS total_packages,
    SUM(CASE WHEN status = 'Available To Sell' THEN 1 ELSE 0 END) AS sellable_packages,
    SUM(CASE WHEN status = 'Transferred' THEN 1 ELSE 0 END) AS transferred_packages
  FROM categorized
  WHERE category != 'Other'
  GROUP BY category
)

SELECT
  category,
  total_packages,
  sellable_packages,
  transferred_packages
FROM by_category
ORDER BY 
  CASE category
    WHEN 'Concentrates' THEN 1
    WHEN 'Edibles' THEN 2
    WHEN 'Vapes' THEN 3
    WHEN 'Pre-Rolls' THEN 4
    WHEN 'Tinctures' THEN 5
    WHEN 'Topicals' THEN 6
    ELSE 99
  END
