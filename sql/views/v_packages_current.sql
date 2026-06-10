CREATE OR REPLACE VIEW `mfny-to-bigquery.canix_raw.v_packages_current` AS
SELECT
  id,
  tag,
  item_name,
  item_category,
  strain_name,
  quantity,
  initial_quantity,
  current_quantity,
  weight,
  unit_of_measure,
  status,
  CASE
    WHEN status NOT IN (
      'Available To Sell',
      'In Progress',
      'In Transit',
      'In Quarantine',
      'Allocated',
      'Retention',
      'To be sent for testing',
      'To be Sent for Testing',
      'Incoming',
      'Restricted - Sent as Field Samples',
      'Transferred',
      'Finished',
      'Inactive'
    ) THEN 'CORRUPTED_STATUS'
    ELSE status
  END as normalized_status,
  CASE
    WHEN status NOT IN (
      'Available To Sell',
      'In Progress',
      'In Transit',
      'In Quarantine',
      'Allocated',
      'Retention',
      'To be sent for testing',
      'To be Sent for Testing',
      'Incoming',
      'Restricted - Sent as Field Samples',
      'Transferred',
      'Finished',
      'Inactive'
    ) THEN TRUE
    ELSE FALSE
  END as has_corrupted_status,
  package_type,
  is_active,
  packaged_date,
  use_by_date,
  harvest_date,
  created_at,
  updated_at,
  source_batch_id,
  source_package_ids,
  room_id,
  room_name,
  location_name,
  _ingested_at
FROM `mfny-to-bigquery.canix_raw.bronze_packages`
WHERE status NOT IN ('Transferred', 'Finished', 'Inactive')
  -- Exclude SANDBOX facility (4475) — not real production data
  AND facility_id != '4475'
