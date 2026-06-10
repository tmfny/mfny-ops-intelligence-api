CREATE OR REPLACE VIEW `mfny-to-bigquery.canix_raw.v_expiring_skus_90d` AS
SELECT *
FROM `mfny-to-bigquery.canix_raw.v_expiring_skus`
WHERE days_until_expiry IS NOT NULL
  AND days_until_expiry <= 90
ORDER BY days_until_expiry ASC
