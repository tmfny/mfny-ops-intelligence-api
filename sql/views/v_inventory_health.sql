CREATE OR REPLACE VIEW `mfny-to-bigquery.canix_raw.v_inventory_health` AS
WITH active_classified AS (
  SELECT
    id,
    weight,
    unit_of_measure,
    status,
    CASE
      WHEN status = 'Available To Sell' THEN 'sellable'
      WHEN status IN ('Allocated', 'Retention') THEN 'reserved'
      WHEN status IN ('In Quarantine', 'In Transit', 'Incoming',
                      'To be sent for testing', 'To be Sent for Testing',
                      'Restricted - Sent as Field Samples') THEN 'non_sellable'
      WHEN status = 'In Progress' THEN 'wip'
      -- Inventory-organization statuses (status reverts when package enters a run)
      WHEN status IN ('Pre-Roll/Gummies', 'Vape/510s', 'Badder') THEN 'staged'
      ELSE 'other'
    END AS category
  FROM `mfny-to-bigquery.canix_raw.bronze_packages`
  WHERE is_active = TRUE
    AND weight IS NOT NULL
    AND weight > 0
    -- Exclude deprecated pre-METRC facility (see v_dead_inventory)
    AND facility_id != '4475'
    -- Exclude pre-METRC legacy item_categories (see v_dead_inventory)
    AND item_category NOT IN (
      'Edible (count)',
      'Edible (weight)',
      'Vape Cart (count)',
      'Combined (count)',
      'Combined (weight)',
      'Concentrate (count)',
      'Concentrate (weight)',
      'Tincture (count)',
      'Extract (weight)',
      'Seeds (count)'
    )
)
SELECT
  unit_of_measure,
  COUNT(*)                                              AS total_packages,
  COUNTIF(category = 'sellable')                        AS sellable_packages,
  COUNTIF(category = 'reserved')                        AS reserved_packages,
  COUNTIF(category = 'non_sellable')                    AS non_sellable_packages,
  COUNTIF(category = 'wip')                             AS wip_packages,
  COUNTIF(category = 'staged')                          AS staged_packages,
  COUNTIF(category = 'other')                           AS other_packages,
  ROUND(SUM(weight), 2)                                              AS total_quantity,
  ROUND(SUM(IF(category = 'sellable', weight, 0)), 2)                AS sellable_quantity,
  ROUND(SUM(IF(category = 'reserved', weight, 0)), 2)                AS reserved_quantity,
  ROUND(SUM(IF(category = 'non_sellable', weight, 0)), 2)            AS non_sellable_quantity,
  ROUND(SUM(IF(category = 'wip', weight, 0)), 2)                     AS wip_quantity,
  ROUND(SUM(IF(category = 'staged', weight, 0)), 2)                  AS staged_quantity,
  ROUND(SUM(IF(category = 'other', weight, 0)), 2)                   AS other_quantity,
  ROUND(SAFE_DIVIDE(
    SUM(IF(category = 'sellable', weight, 0)),
    SUM(weight)
  ) * 100, 1)                                                        AS pct_sellable
FROM active_classified
GROUP BY unit_of_measure
ORDER BY
  CASE unit_of_measure
    WHEN 'Each' THEN 1
    WHEN 'Grams' THEN 2
    WHEN 'Pounds' THEN 3
    ELSE 4
  END
