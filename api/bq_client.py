"""
BigQuery client for the MFNY Operations API.

Provides a single, cached BigQuery client and helper functions for querying
views in the canix_raw dataset. All endpoints that need data should use these
functions rather than instantiating their own BigQuery clients.

Authentication:
- In Cloud Run: uses the attached service account automatically
- Locally: uses application default credentials
  (run `gcloud auth application-default login` to set up)
"""

from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError
import logging
import time
from typing import Any

logger = logging.getLogger(__name__)

# BigQuery project and dataset references
PROJECT = "mfny-to-bigquery"
DATASET = "canix_raw"

# Singleton client — one connection, reused across all queries
_client: bigquery.Client | None = None


def get_client() -> bigquery.Client:
    """Return a cached BigQuery client, creating it on first call."""
    global _client
    if _client is None:
        _client = bigquery.Client(project=PROJECT)
    return _client


def query_view(view_name: str, where: str | None = None,
               order_by: str | None = None, limit: int | None = None) -> list[dict[str, Any]]:
    """
    Query a view in canix_raw and return rows as a list of dicts.

    Args:
        view_name: Name of the view (e.g., "v_inventory_health")
        where: Optional WHERE clause (without the WHERE keyword)
        order_by: Optional ORDER BY clause (without the ORDER BY keyword)
        limit: Optional row limit

    Returns:
        List of dicts, one per row. Empty list if no rows. Raises on query error.
    """
    sql = f"SELECT * FROM `{PROJECT}.{DATASET}.{view_name}`"
    if where:
        sql += f" WHERE {where}"
    if order_by:
        sql += f" ORDER BY {order_by}"
    if limit:
        sql += f" LIMIT {limit}"

    return run_sql(sql)


def run_sql(sql: str) -> list[dict[str, Any]]:
    """
    Run an arbitrary SQL query and return rows as a list of dicts.

    Use this when you need a query that doesn't fit query_view's pattern
    (joins, custom SELECT, aggregations across views, etc.).

    All datetime/date values are converted to ISO strings for JSON serialization.
    """
    client = get_client()
    start = time.time()
    try:
        query_job = client.query(sql)
        rows = [dict(row) for row in query_job.result()]
    except GoogleAPIError as e:
        logger.error(f"BigQuery query failed: {e}\nSQL: {sql}")
        raise

    elapsed_ms = int((time.time() - start) * 1000)
    logger.info(f"BigQuery query returned {len(rows)} rows in {elapsed_ms}ms")

    # Convert non-JSON-serializable types
    for row in rows:
        for key, value in row.items():
            if hasattr(value, 'isoformat'):  # datetime, date, time
                row[key] = value.isoformat()

    return rows


def health_check() -> dict[str, Any]:
    """
    Quick BigQuery connectivity test. Returns status info.
    Useful for debugging deployment issues.
    """
    try:
        client = get_client()
        # Trivial query that exercises the connection
        result = list(client.query("SELECT 1 AS ok").result())
        return {
            "bigquery": "ok",
            "project": PROJECT,
            "dataset": DATASET,
            "test_result": dict(result[0]) if result else None
        }
    except Exception as e:
        return {
            "bigquery": "error",
            "error": str(e)
        }