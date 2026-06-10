# BigQuery View Definitions — `mfny-to-bigquery.canix_raw`

These `.sql` files are exported snapshots of the **live BigQuery views** in the
`mfny-to-bigquery.canix_raw` dataset, captured **2026-06-10** from
`INFORMATION_SCHEMA.VIEWS`. There are 53 views (`v_*`).

Going forward, **these files are the source of truth** for view definitions.
Changes should be made here in version control first, then applied to BigQuery.

## File format

Each file is named `<view_name>.sql` and contains a directly re-appliable statement:

```sql
CREATE OR REPLACE VIEW `mfny-to-bigquery.canix_raw.<view_name>` AS
<the view's SELECT body, preserved exactly as stored in BigQuery>
```

## Workflow: edit, then apply

1. Edit the `.sql` file for the view you want to change.
2. Apply it to BigQuery by running the file:

   ```sh
   bq query --use_legacy_sql=false < sql/views/<view_name>.sql
   ```

3. Commit the change so version control stays in sync with BigQuery.

## Notes

- These were exported read-only — no `CREATE OR REPLACE` was run against
  BigQuery during the export.
- The bodies are preserved exactly as BigQuery stored them (BigQuery normalizes
  SQL on save, so the text may differ from whatever originally created the view).
