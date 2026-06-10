CREATE OR REPLACE VIEW `mfny-to-bigquery.canix_raw.v_distribution_today_deliveries` AS
WITH parsed AS (
  SELECT
    id,
    name AS transfer_name,
    manifest_number,
    sales_order_id,
    sales_order_name,
    is_active,
    updated_at,
    
    -- First destination's departure time (raw ISO string)
    -- Canix stores this with a 'Z' suffix but values are actually ET
    -- See handoff doc for context
    JSON_VALUE(destinations_json, '$[0].departure_time') AS departure_time_str,
    
    -- First destination's other fields
    JSON_VALUE(destinations_json, '$[0].destination_facility') AS destination_license,
    JSON_VALUE(destinations_json, '$[0].transfer_type') AS transfer_type,
    
    -- Package count in the first destination
    ARRAY_LENGTH(JSON_QUERY_ARRAY(destinations_json, '$[0].contents')) AS package_count,
    
    -- Sum of shipped_weight across all packages in the first destination
    (
      SELECT COALESCE(SUM(CAST(JSON_VALUE(content, '$.shipped_weight') AS FLOAT64)), 0)
      FROM UNNEST(JSON_QUERY_ARRAY(destinations_json, '$[0].contents')) AS content
    ) AS total_units_shipped,
    
    -- Determine if this is a samples-only transfer based on name convention
    LOWER(name) LIKE '%sample%' AS is_samples
    
  FROM `mfny-to-bigquery.canix_raw.bronze_transfers`
  WHERE facility_id = '4510'
    AND is_active = TRUE
)
SELECT
  id,
  transfer_name,
  manifest_number,
  sales_order_id,
  sales_order_name,
  destination_license,
  transfer_type,
  package_count,
  CAST(total_units_shipped AS INT64) AS total_units_shipped,
  is_samples,
  is_active,
  updated_at,
  
  -- Parse the ISO string into a DATETIME (no timezone interpretation)
  -- This preserves the literal hour/minute values stored by Canix
  PARSE_DATETIME('%Y-%m-%dT%H:%M:%E*SZ', departure_time_str) AS departure_datetime_et,
  
  -- Date component for filtering
  DATE(PARSE_DATETIME('%Y-%m-%dT%H:%M:%E*SZ', departure_time_str)) AS departure_date_et,
  
  -- Formatted time for display, e.g. "8:30 AM" or "9:00 AM"
  FORMAT_DATETIME(
    '%l:%M %p',
    PARSE_DATETIME('%Y-%m-%dT%H:%M:%E*SZ', departure_time_str)
  ) AS departure_time_et_display
  
FROM parsed
WHERE
  -- Filter to today using ET — same as how the data is stored
  DATE(PARSE_DATETIME('%Y-%m-%dT%H:%M:%E*SZ', departure_time_str)) = CURRENT_DATE('America/New_York')

ORDER BY departure_datetime_et ASC
