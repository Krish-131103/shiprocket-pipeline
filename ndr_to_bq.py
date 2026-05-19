"""
Shiprocket NDR → Google BigQuery Pipeline
------------------------------------------
Fetches all NDR (Non-Delivery Report) data from the Shiprocket API
and loads it into a BigQuery table (shiprocket.ndr_data).

The ndr_data table joins to the orders table on AWB number.
NDR fields are then surfaced in the orders_enriched view.

.env file must contain the same values as shiprocket_to_bq.py:
  SHIPROCKET_TOKEN        – Bearer token from POST /auth/login
  GOOGLE_PROJECT_ID       – GCP project ID
  BQ_DATASET              – BigQuery dataset name
  GOOGLE_CREDENTIALS_PATH – Path to bq_credentials.json

Run manually:   python ndr_to_bq.py
Auto trigger:   GitHub Actions (daily_ndr_sync.yml)
"""

import os
import time
import logging
from typing import Optional

import requests
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config  (mirrors shiprocket_to_bq.py exactly)
# ---------------------------------------------------------------------------
load_dotenv()

SHIPROCKET_TOKEN        = os.getenv("SHIPROCKET_TOKEN")
GOOGLE_PROJECT_ID       = os.getenv("GOOGLE_PROJECT_ID")
BQ_DATASET              = os.getenv("BQ_DATASET")
GOOGLE_CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH", "bq_credentials.json")

NDR_TABLE    = "ndr_data"
NDR_URL      = "https://apiv2.shiprocket.in/v1/external/ndr/all"
PER_PAGE     = 100
MAX_RETRIES  = 3
RETRY_WAIT   = 5
PAGE_LIMIT   = 100    # same Shiprocket hard cap as orders endpoint


# ---------------------------------------------------------------------------
# BigQuery schema for ndr_data table
# ---------------------------------------------------------------------------

NDR_SCHEMA = [
    # ── Identifiers ───────────────────────────────────────────────────────
    bigquery.SchemaField("ndr_id",              "INTEGER"),
    bigquery.SchemaField("awb",                 "STRING"),
    bigquery.SchemaField("shipment_id",         "INTEGER"),
    bigquery.SchemaField("channel_order_id",    "STRING"),
    bigquery.SchemaField("channel_name",        "STRING"),

    # ── Order / product info ──────────────────────────────────────────────
    bigquery.SchemaField("courier",             "STRING"),
    bigquery.SchemaField("customer_name",       "STRING"),
    bigquery.SchemaField("customer_city",       "STRING"),
    bigquery.SchemaField("customer_state",      "STRING"),
    bigquery.SchemaField("customer_pincode",    "STRING"),
    bigquery.SchemaField("payment_method",      "STRING"),
    bigquery.SchemaField("product_name",        "STRING"),
    bigquery.SchemaField("product_price",       "STRING"),
    bigquery.SchemaField("channel_sku",         "STRING"),
    bigquery.SchemaField("master_sku",          "STRING"),
    bigquery.SchemaField("product_qty",         "STRING"),

    # ── NDR summary ───────────────────────────────────────────────────────
    bigquery.SchemaField("status",              "STRING"),
    bigquery.SchemaField("reason",              "STRING"),
    bigquery.SchemaField("total_ndr_attempts",  "INTEGER"),
    bigquery.SchemaField("ndr_raised_at",       "STRING"),
    bigquery.SchemaField("created_at",          "STRING"),
    bigquery.SchemaField("escalation_status",   "STRING"),
    bigquery.SchemaField("delivered_date",      "STRING"),
    bigquery.SchemaField("cancellation_reason", "STRING"),

    # ── NDR attempt 1 (from history array) ───────────────────────────────
    bigquery.SchemaField("ndr_1_date",          "STRING"),
    bigquery.SchemaField("ndr_1_reason",        "STRING"),
    bigquery.SchemaField("ndr_1_comment",       "STRING"),
    bigquery.SchemaField("ndr_1_sms_response",  "STRING"),

    # ── NDR attempt 2 ─────────────────────────────────────────────────────
    bigquery.SchemaField("ndr_2_date",          "STRING"),
    bigquery.SchemaField("ndr_2_reason",        "STRING"),
    bigquery.SchemaField("ndr_2_comment",       "STRING"),
    bigquery.SchemaField("ndr_2_sms_response",  "STRING"),

    # ── NDR attempt 3 ─────────────────────────────────────────────────────
    bigquery.SchemaField("ndr_3_date",          "STRING"),
    bigquery.SchemaField("ndr_3_reason",        "STRING"),
    bigquery.SchemaField("ndr_3_comment",       "STRING"),
    bigquery.SchemaField("ndr_3_sms_response",  "STRING"),

    # ── Latest attempt (derived from history) ─────────────────────────────
    bigquery.SchemaField("latest_ndr_date",     "STRING"),
    bigquery.SchemaField("latest_ndr_reason",   "STRING"),

    # ── Raw history JSON ──────────────────────────────────────────────────
    bigquery.SchemaField("history_json",        "STRING"),
]


# ---------------------------------------------------------------------------
# Fetch helpers  (same pattern as shiprocket_to_bq.py)
# ---------------------------------------------------------------------------

def get_headers() -> dict:
    return {
        "Authorization": f"Bearer {SHIPROCKET_TOKEN}",
        "Content-Type": "application/json",
    }


def fetch_ndr_page(page: int, from_date: str = None,
                   to_date: str = None) -> dict | None:
    """
    Fetch one page of NDR records, optionally filtered by date range.
    Returns None on 422 (no more pages). Retries on transient errors.
    """
    params = f"page={page}&per_page={PER_PAGE}"
    if from_date:
        params += f"&from={from_date}&to={to_date}"
    url = f"{NDR_URL}?{params}"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=get_headers(), timeout=30)

            if resp.status_code == 422:
                log.info("NDR page %d returned 422 (no more pages).", page)
                return None

            resp.raise_for_status()
            return resp.json()

        except requests.exceptions.HTTPError as e:
            log.warning("HTTP error on NDR page %d attempt %d: %s",
                        page, attempt, e)
        except requests.exceptions.RequestException as e:
            log.warning("Request error on NDR page %d attempt %d: %s",
                        page, attempt, e)

        if attempt < MAX_RETRIES:
            time.sleep(RETRY_WAIT)

    raise RuntimeError(
        f"Failed to fetch NDR page {page} after {MAX_RETRIES} attempts")


def fetch_ndr_window(from_date: str, to_date: str) -> list[dict]:
    """
    Fetch all NDR records for a single date window.
    Uses the same daily-window strategy as the orders pipeline to
    stay under Shiprocket's 100-page hard cap.
    """
    log.info("  NDR window %s → %s", from_date, to_date)
    first = fetch_ndr_page(1, from_date, to_date)
    if not first:
        return []

    # NDR response: {"data": [...], "meta": {"pagination": {...}}}
    records     = first.get("data", [])
    pagination  = first.get("meta", {}).get("pagination", {})
    total_pages = min(pagination.get("total_pages", 1), PAGE_LIMIT)
    total_count = pagination.get("total", len(records))
    log.info("  → %s NDR records, %d pages", total_count, total_pages)

    all_records = list(records)

    for page in range(2, total_pages + 1):
        data = fetch_ndr_page(page, from_date, to_date)
        if data is None:
            break
        recs = data.get("data", [])
        if not recs:
            log.info("  Empty NDR page %d – stopping.", page)
            break
        all_records.extend(recs)
        time.sleep(0.2)

    return all_records


# ---------------------------------------------------------------------------
# Flattening logic
# ---------------------------------------------------------------------------

def safe_str(val) -> Optional[str]:
    if val is None or val == "" or val == "0000-00-00 00:00:00":
        return None
    return str(val)


def safe_int(val) -> Optional[int]:
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def get_history_attempt(history: list, attempt_num: int) -> tuple:
    """
    Extract date, reason, comment and sms_response for a specific
    NDR attempt number from the history array.
    history items have: ndr_attempt, ndr_reason, comment,
                        sms_response, ndr_raised_at
    Returns (date, reason, comment, sms_response) — all str or None.
    """
    # Filter by ndr_attempt number
    matches = [h for h in history if h.get("ndr_attempt") == attempt_num]
    if not matches:
        return None, None, None, None
    h = matches[0]
    return (
        safe_str(h.get("ndr_raised_at")),
        safe_str(h.get("ndr_reason")),
        safe_str(h.get("comment")),
        safe_str(h.get("sms_response")),
    )


def flatten_ndr(record: dict) -> dict:
    """
    Flatten one NDR record into a single BQ row.

    Real API response structure (confirmed from live data):
      id, shipment_id, channel_order_id, channel_name,
      customer_*, payment_*, status, status_code,
      reason, attempts (int — total count),
      ndr_raised_at, courier, awb_code,
      channel_sku, master_sku, product_qty,
      escalation_status, product_name, product_price,
      shipment_channel_id,
      history (list of attempt objects),
      delivered_date, cancellation_reason
    """
    import json

    history = record.get("history") or []

    # Sort history by ndr_attempt ascending to ensure correct ordering
    try:
        history = sorted(history, key=lambda x: x.get("ndr_attempt", 0))
    except Exception:
        pass

    d1, r1, c1, s1 = get_history_attempt(history, 1)
    d2, r2, c2, s2 = get_history_attempt(history, 2)
    d3, r3, c3, s3 = get_history_attempt(history, 3)

    # Latest = last item in sorted history
    latest        = history[-1] if history else {}
    latest_date   = safe_str(latest.get("ndr_raised_at"))
    latest_reason = safe_str(latest.get("ndr_reason"))

    return {
        # ── Identifiers ───────────────────────────────────────────────────
        "ndr_id":             safe_int(record.get("id")),
        "awb":                safe_str(record.get("awb_code")),
        "shipment_id":        safe_int(record.get("shipment_id")),
        "channel_order_id":   safe_str(record.get("channel_order_id")),
        "channel_name":       safe_str(record.get("channel_name")),

        # ── Order / product info ──────────────────────────────────────────
        "courier":            safe_str(record.get("courier")),
        "customer_name":      safe_str(record.get("customer_name")),
        "customer_city":      safe_str(record.get("customer_city")),
        "customer_state":     safe_str(record.get("customer_state")),
        "customer_pincode":   safe_str(record.get("customer_pincode")),
        "payment_method":     safe_str(record.get("payment_method")),
        "product_name":       safe_str(record.get("product_name")),
        "product_price":      safe_str(record.get("product_price")),
        "channel_sku":        safe_str(record.get("channel_sku")),
        "master_sku":         safe_str(record.get("master_sku")),
        "product_qty":        safe_str(record.get("product_qty")),

        # ── NDR summary ───────────────────────────────────────────────────
        "status":             safe_str(record.get("status")),
        "reason":             safe_str(record.get("reason")),
        "total_ndr_attempts": safe_int(record.get("attempts")),
        "ndr_raised_at":      safe_str(record.get("ndr_raised_at")),
        "created_at":         safe_str(record.get("created_at")),
        "escalation_status":  safe_str(record.get("escalation_status")),
        "delivered_date":     safe_str(record.get("delivered_date")),
        "cancellation_reason":safe_str(record.get("cancellation_reason")),

        # ── Attempt 1 ─────────────────────────────────────────────────────
        "ndr_1_date":         d1,
        "ndr_1_reason":       r1,
        "ndr_1_comment":      c1,
        "ndr_1_sms_response": s1,

        # ── Attempt 2 ─────────────────────────────────────────────────────
        "ndr_2_date":         d2,
        "ndr_2_reason":       r2,
        "ndr_2_comment":      c2,
        "ndr_2_sms_response": s2,

        # ── Attempt 3 ─────────────────────────────────────────────────────
        "ndr_3_date":         d3,
        "ndr_3_reason":       r3,
        "ndr_3_comment":      c3,
        "ndr_3_sms_response": s3,

        # ── Latest ────────────────────────────────────────────────────────
        "latest_ndr_date":    latest_date,
        "latest_ndr_reason":  latest_reason,

        # ── Raw history JSON ──────────────────────────────────────────────
        "history_json":       json.dumps(history, ensure_ascii=False)
                              if history else None,
    }


# ---------------------------------------------------------------------------
# BigQuery helpers  (mirrors shiprocket_to_bq.py)
# ---------------------------------------------------------------------------

def get_bq_client() -> bigquery.Client:
    credentials = service_account.Credentials.from_service_account_file(
        GOOGLE_CREDENTIALS_PATH,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    return bigquery.Client(project=GOOGLE_PROJECT_ID, credentials=credentials)


def get_latest_ndr_date(client: bigquery.Client) -> "date | None":
    """
    Returns the most recent latest_ndr_date already in the ndr_data table.
    Returns None if the table is empty or doesn't exist yet.
    """
    table_ref = f"{GOOGLE_PROJECT_ID}.{BQ_DATASET}.{NDR_TABLE}"
    query = f"""
        SELECT MAX(
            SAFE.PARSE_DATE('%Y-%m-%d',
                REGEXP_EXTRACT(latest_ndr_date, r'^(\\d{{4}}-\\d{{2}}-\\d{{2}})')
            )
        ) AS latest_date
        FROM `{table_ref}`
    """
    try:
        result = list(client.query(query).result())
        return result[0].latest_date
    except Exception as e:
        log.info("Could not read latest NDR date (table may be empty): %s", e)
        return None


def coerce_ndr_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Apply correct dtypes before BQ upload."""
    int_cols = [f.name for f in NDR_SCHEMA if f.field_type == "INTEGER"]
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    return df


def upload_ndr_to_bq(client: bigquery.Client, df: pd.DataFrame,
                     mode: str = "WRITE_TRUNCATE") -> None:
    table_ref = f"{GOOGLE_PROJECT_ID}.{BQ_DATASET}.{NDR_TABLE}"
    log.info("Uploading %d NDR rows to %s (mode: %s) …",
             len(df), table_ref, mode)

    disposition = (bigquery.WriteDisposition.WRITE_TRUNCATE
                   if mode == "WRITE_TRUNCATE"
                   else bigquery.WriteDisposition.WRITE_APPEND)

    job_config = bigquery.LoadJobConfig(
        schema=NDR_SCHEMA,
        write_disposition=disposition,
    )

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()

    if job.errors:
        raise RuntimeError(f"BigQuery NDR load errors: {job.errors}")

    log.info("NDR upload complete. Rows in table: %d",
             client.get_table(table_ref).num_rows)


def delete_ndr_dates(client: bigquery.Client, date_strs: list[str]) -> None:
    """Delete NDR rows for specific dates before re-inserting."""
    table_ref = f"{GOOGLE_PROJECT_ID}.{BQ_DATASET}.{NDR_TABLE}"
    for date_str in date_strs:
        query = f"""
            DELETE FROM `{table_ref}`
            WHERE REGEXP_EXTRACT(latest_ndr_date, r'^(\\d{{4}}-\\d{{2}}-\\d{{2}})')
                  = '{date_str}'
        """
        client.query(query).result()
        log.info("  Deleted NDR rows for %s.", date_str)


def dedup_ndr_table(client: bigquery.Client) -> None:
    """Keep only the most recent record per AWB."""
    table_ref = f"{GOOGLE_PROJECT_ID}.{BQ_DATASET}.{NDR_TABLE}"
    query = f"""
        CREATE OR REPLACE TABLE `{table_ref}` AS
        SELECT * EXCEPT(_row_num)
        FROM (
          SELECT *,
            ROW_NUMBER() OVER (
              PARTITION BY awb
              ORDER BY total_ndr_attempts DESC, ndr_raised_at DESC
            ) AS _row_num
          FROM `{table_ref}`
        )
        WHERE _row_num = 1
    """
    log.info("Deduplicating NDR table …")
    client.query(query).result()
    log.info("NDR dedup complete. Rows: %d",
             client.get_table(table_ref).num_rows)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    from datetime import date, timedelta

    # ── Validate env vars ─────────────────────────────────────────────────
    missing = [v for v in ("SHIPROCKET_TOKEN", "GOOGLE_PROJECT_ID", "BQ_DATASET")
               if not os.getenv(v)]
    if missing:
        raise EnvironmentError(f"Missing required env vars: {missing}")

    # ── Connect to BigQuery ───────────────────────────────────────────────
    log.info("Authenticating with BigQuery …")
    client = get_bq_client()

    # ── Decide: backfill or incremental? ──────────────────────────────────
    latest_date   = get_latest_ndr_date(client)
    today         = date.today()
    BACKFILL_FROM = date(2026, 5, 1)   # ← same as orders backfill start

    if latest_date is None:
        log.info("NDR table empty. Running full backfill from %s …",
                 BACKFILL_FROM)
        fetch_start     = BACKFILL_FROM
        upload_mode     = "WRITE_TRUNCATE"
        days_to_refresh = []
    else:
        fetch_start     = latest_date - timedelta(days=3)  # 3-day lookback
        upload_mode     = "WRITE_APPEND"                   # for NDR (status changes more)
        days_to_refresh = [
            (fetch_start + timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range((today - fetch_start).days + 1)
        ]
        log.info("Incremental NDR mode. Re-fetching from %s …", fetch_start)

    # ── Fetch NDR records day by day ──────────────────────────────────────
    all_ndrs = {}   # awb → record  (deduplicates by AWB)
    current  = fetch_start

    while current <= today:
        day_str     = current.strftime("%Y-%m-%d")
        day_records = fetch_ndr_window(day_str, day_str)

        new = 0
        for r in day_records:
            awb = r.get("awb_code")   # confirmed field name from API
            if awb and awb not in all_ndrs:
                all_ndrs[awb] = r
                new += 1

        if new:
            log.info("  %s → %d new NDR records (total: %d)",
                     day_str, new, len(all_ndrs))
        current += timedelta(days=1)

    log.info("Fetched %d unique NDR records in total.", len(all_ndrs))

    if not all_ndrs:
        log.info("No NDR records found. Nothing to upload.")
        return

    # ── Flatten & coerce ──────────────────────────────────────────────────
    log.info("Flattening NDR records …")
    rows = [flatten_ndr(r) for r in all_ndrs.values()]
    df   = coerce_ndr_dataframe(pd.DataFrame(rows))
    log.info("NDR DataFrame shape: %s", df.shape)

    # ── Delete stale rows before re-inserting ─────────────────────────────
    if upload_mode == "WRITE_APPEND" and days_to_refresh:
        log.info("Removing stale NDR rows for refreshed days …")
        delete_ndr_dates(client, days_to_refresh)

    # ── Upload ────────────────────────────────────────────────────────────
    upload_ndr_to_bq(client, df, mode=upload_mode)

    # ── Deduplicate ───────────────────────────────────────────────────────
    dedup_ndr_table(client)
    log.info("NDR pipeline finished successfully.")


if __name__ == "__main__":
    main()