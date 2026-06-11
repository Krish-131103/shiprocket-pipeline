"""
Shiprocket → Google BigQuery Pipeline
--------------------------------------
Fetches all orders from the Shiprocket API (paginated) and loads them into
a BigQuery table using WRITE_TRUNCATE (full replace on every run).

.env file must contain:
  SHIPROCKET_TOKEN        – Bearer token from POST /auth/login
  GOOGLE_PROJECT_ID       – GCP project ID
  BQ_DATASET              – BigQuery dataset name
  BQ_TABLE                – BigQuery table name
  GOOGLE_CREDENTIALS_PATH – Path to bq_credentials.json (relative or absolute)
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
# Config
# ---------------------------------------------------------------------------
load_dotenv()

SHIPROCKET_TOKEN        = os.getenv("SHIPROCKET_TOKEN")
GOOGLE_PROJECT_ID       = os.getenv("GOOGLE_PROJECT_ID")
BQ_DATASET              = os.getenv("BQ_DATASET")
BQ_TABLE                = os.getenv("BQ_TABLE")
GOOGLE_CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH", "bq_credentials.json")

BASE_URL    = "https://apiv2.shiprocket.in/v1/external/orders"
PER_PAGE    = 100          # max Shiprocket allows per page
MAX_RETRIES = 3
RETRY_WAIT  = 5            # seconds between retries
PAGE_LIMIT  = 100          # Shiprocket hard cap: 422 beyond page 100


# ---------------------------------------------------------------------------
# Fetch helpers
# ---------------------------------------------------------------------------

def get_headers() -> dict:
    return {
        "Authorization": f"Bearer {SHIPROCKET_TOKEN}",
        "Content-Type": "application/json",
    }


def fetch_page(page: int, from_date: str = None, to_date: str = None) -> dict | None:
    """
    Fetch one page of orders, optionally filtered by date range.
    Returns None (instead of raising) on 422, which signals end-of-pages
    for this date window.
    Retries up to MAX_RETRIES on transient errors.
    """
    params = f"page={page}&per_page={PER_PAGE}"
    if from_date:
        params += f"&from={from_date}&to={to_date}"
    url = f"{BASE_URL}?{params}"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=get_headers(), timeout=30)

            # 422 = Shiprocket's way of saying "no more pages here"
            if resp.status_code == 422:
                log.info("Page %d returned 422 (no more pages). Stopping window.", page)
                return None

            resp.raise_for_status()
            return resp.json()

        except requests.exceptions.HTTPError as e:
            log.warning("HTTP error on page %d attempt %d: %s", page, attempt, e)
        except requests.exceptions.RequestException as e:
            log.warning("Request error on page %d attempt %d: %s", page, attempt, e)

        if attempt < MAX_RETRIES:
            time.sleep(RETRY_WAIT)

    raise RuntimeError(f"Failed to fetch page {page} after {MAX_RETRIES} attempts")


def fetch_window(from_date: str, to_date: str) -> list[dict]:
    """
    Fetch all orders in a single date window (from_date → to_date).
    Respects the 100-page hard cap by stopping at PAGE_LIMIT or on 422.
    Dates must be in YYYY-MM-DD format.
    """
    log.info("  Window %s → %s", from_date, to_date)
    first = fetch_page(1, from_date, to_date)
    if not first:
        return []

    pagination  = first.get("meta", {}).get("pagination", {})
    total_pages = min(pagination.get("total_pages", 1), PAGE_LIMIT)
    total_count = pagination.get("total", "?")
    log.info("  → %s orders, %d pages", total_count, total_pages)

    orders = list(first.get("data", []))

    for page in range(2, total_pages + 1):
        data = fetch_page(page, from_date, to_date)
        if data is None:          # 422 hit early
            break
        page_orders = data.get("data", [])
        if not page_orders:
            log.info("  Empty page %d – stopping window early.", page)
            break
        orders.extend(page_orders)
        time.sleep(0.2)

    return orders





# ---------------------------------------------------------------------------
# Flattening logic (derived from the real API response structure)
# ---------------------------------------------------------------------------

def safe_str(val) -> Optional[str]:
    if val is None or val == "" or val == "0000-00-00 00:00:00":
        return None
    return str(val)


def flatten_order(order: dict) -> dict:
    """
    Flatten one order dict into a single row suitable for BigQuery.
    Nested arrays (products, shipments) are serialised to JSON strings.
    Zone, warehouse, weight_category and rto fields are computed in the
    orders_enriched VIEW in BigQuery — not here.
    """
    # -- first shipment (most orders have exactly one) ----------------------
    shipments = order.get("shipments") or []
    s = shipments[0] if shipments else {}

    # -- first product & all products ---------------------------------------
    products = order.get("products") or []

    # -- others sub-object --------------------------------------------------
    others = order.get("others") or {}

    # -- pickup address -----------------------------------------------------
    pa = order.get("pickup_address_detail") or {}

    row = {
        # ── Order identifiers ─────────────────────────────────────────────
        "order_id":             order.get("id"),
        "channel_order_id":     safe_str(order.get("channel_order_id")),
        "invoice_no":           safe_str(order.get("invoice_no")),
        "channel_id":           order.get("channel_id"),
        "channel_name":         safe_str(order.get("channel_name")),
        "base_channel_code":    safe_str(order.get("base_channel_code")),

        # ── Customer ──────────────────────────────────────────────────────
        "customer_name":        safe_str(order.get("customer_name")),
        "customer_email":       safe_str(order.get("customer_email")),
        "customer_phone":       safe_str(order.get("customer_phone")),
        "customer_address":     safe_str(order.get("customer_address")),
        "customer_address_2":   safe_str(order.get("customer_address_2")),
        "customer_city":        safe_str(order.get("customer_city")),
        "customer_state":       safe_str(order.get("customer_state")),
        "customer_pincode":     safe_str(order.get("customer_pincode")),
        "customer_country":     safe_str(order.get("customer_country")),
        "customer_latitude":    safe_str(order.get("customer_latitude")),
        "customer_longitude":   safe_str(order.get("customer_longitude")),
        "customer_alternate_phone": safe_str(order.get("customer_alternate_phone")),

        # ── Order financials ──────────────────────────────────────────────
        "total":                _to_float(order.get("total")),
        "tax":                  _to_float(order.get("tax")),
        "discount":             _to_float(order.get("discount")),
        "other_charges":        _to_float(order.get("other_charges")),
        "giftwrap_charges":     _to_float(order.get("giftwrap_charges")),
        "payment_method":       safe_str(order.get("payment_method")),
        "payment_status":       safe_str(order.get("payment_status")),
        "cod":                  order.get("cod"),

        # ── Order status & logistics ───────────────────────────────────────
        "status":               safe_str(order.get("status")),
        "status_code":          order.get("status_code"),
        "master_status":        safe_str(order.get("master_status")),
        "sla":                  safe_str(order.get("sla")),
        "shipping_method":      safe_str(order.get("shipping_method")),
        "ship_type":            order.get("ship_type"),
        "rto_risk":             safe_str(order.get("rto_risk")),
        "expedited":            order.get("expedited"),
        "zone":                 safe_str(order.get("zone")),
        "is_international":     order.get("is_international"),
        "is_return":            order.get("is_return"),
        "is_b2b":               order.get("is_b2b"),

        # ── Dates ─────────────────────────────────────────────────────────
        "channel_created_at":   safe_str(order.get("channel_created_at")),
        "created_at":           safe_str(order.get("created_at")),
        "updated_at":           safe_str(order.get("updated_at")),
        "etd_date":             safe_str(order.get("etd_date")),
        "delivered_date":       safe_str(order.get("delivered_date")),
        "out_for_delivery_date":safe_str(order.get("out_for_delivery_date")),
        "picked_up_date":       safe_str(order.get("picked_up_date")),

        # ── Pickup location ───────────────────────────────────────────────
        "pickup_location":      safe_str(order.get("pickup_location")),
        "pickup_city":          safe_str(pa.get("city")),
        "pickup_state":         safe_str(pa.get("state")),
        "pickup_pincode":       safe_str(pa.get("pin_code")),

        # ── Product summary ───────────────────────────────────────────────
        "product_quantity":     order.get("product_quantity"),

        # ── Shipment (first / primary) ────────────────────────────────────
        "shipment_id":          s.get("id"),
        "awb":                  safe_str(s.get("awb")),
        "courier":              safe_str(s.get("courier")),
        "courier_id":           s.get("courier_id"),
        "weight_kg":            _to_float(s.get("weight")),
        "dimensions":           safe_str(s.get("dimensions")),
        "pickup_scheduled_date":safe_str(s.get("pickup_scheduled_date")),
        "shipped_date":         safe_str(s.get("shipped_date")),
        "shipment_status":      s.get("status"),
        "shipment_total":       _to_float(s.get("total")),
        "shipment_cost":        _to_float(s.get("cost")),
        "etd":                  safe_str(s.get("etd")),
        "saral_etd":            safe_str(s.get("saral_etd")),
        "pickup_id":            safe_str(s.get("pickup_id")),
        "awb_assign_date":      safe_str(s.get("awb_assign_date")),
        "volumetric_weight":    _to_float(s.get("volumetric_weight")),

        # ── AWB charges ───────────────────────────────────────────────────
        "freight_charges":      _to_float((order.get("awb_data") or {}).get("charges", {}).get("freight_charges")),
        "applied_weight":       _to_float((order.get("awb_data") or {}).get("charges", {}).get("applied_weight")),

        # ── Others / billing ──────────────────────────────────────────────
        "billing_name":         safe_str(others.get("billing_name")),
        "billing_city":         safe_str(others.get("billing_city")),
        "billing_state":        safe_str(others.get("billing_state")),
        "billing_pincode":      safe_str(others.get("billing_pincode")),
        "billing_country":      safe_str(others.get("billing_country")),
        "billing_email":        safe_str(others.get("billing_email")),
        "invoice_number":       safe_str(others.get("invoice_number")),
        "package_count":        others.get("package_count"),

        # ── Delivery flags ────────────────────────────────────────────────
        "delivery_delayed":     order.get("delivery_delayed"),
        "delay_reason":         safe_str(order.get("delay_reason")),
        "is_delayed":           safe_str(order.get("is_delayed")),
        "is_early":             safe_str(order.get("is_early")),

        # ── Activities (comma-joined) ──────────────────────────────────────
        "activities":           ",".join(order.get("activities") or []),

        # ── Nested arrays as JSON strings ──────────────────────────────────
        "products_json":        _to_json(products),
        "shipments_json":       _to_json(shipments),
    }
    return row


def _to_float(val) -> Optional[float]:
    try:
        f = float(val)
        return None if f == 0.0 and val in (None, "", 0) else f
    except (TypeError, ValueError):
        return None


def _to_json(val) -> Optional[str]:
    import json
    if not val:
        return None
    try:
        return json.dumps(val, ensure_ascii=False)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# BigQuery schema  (matches every column produced by flatten_order)
# ---------------------------------------------------------------------------

BQ_SCHEMA = [
    bigquery.SchemaField("order_id",              "INTEGER"),
    bigquery.SchemaField("channel_order_id",       "STRING"),
    bigquery.SchemaField("invoice_no",             "STRING"),
    bigquery.SchemaField("channel_id",             "INTEGER"),
    bigquery.SchemaField("channel_name",           "STRING"),
    bigquery.SchemaField("base_channel_code",      "STRING"),

    bigquery.SchemaField("customer_name",          "STRING"),
    bigquery.SchemaField("customer_email",         "STRING"),
    bigquery.SchemaField("customer_phone",         "STRING"),
    bigquery.SchemaField("customer_address",       "STRING"),
    bigquery.SchemaField("customer_address_2",     "STRING"),
    bigquery.SchemaField("customer_city",          "STRING"),
    bigquery.SchemaField("customer_state",         "STRING"),
    bigquery.SchemaField("customer_pincode",       "STRING"),
    bigquery.SchemaField("customer_country",       "STRING"),
    bigquery.SchemaField("customer_latitude",      "STRING"),
    bigquery.SchemaField("customer_longitude",     "STRING"),
    bigquery.SchemaField("customer_alternate_phone","STRING"),

    bigquery.SchemaField("total",                  "FLOAT"),
    bigquery.SchemaField("tax",                    "FLOAT"),
    bigquery.SchemaField("discount",               "FLOAT"),
    bigquery.SchemaField("other_charges",          "FLOAT"),
    bigquery.SchemaField("giftwrap_charges",       "FLOAT"),
    bigquery.SchemaField("payment_method",         "STRING"),
    bigquery.SchemaField("payment_status",         "STRING"),
    bigquery.SchemaField("cod",                    "INTEGER"),

    bigquery.SchemaField("status",                 "STRING"),
    bigquery.SchemaField("status_code",            "INTEGER"),
    bigquery.SchemaField("master_status",          "STRING"),
    bigquery.SchemaField("sla",                    "STRING"),
    bigquery.SchemaField("shipping_method",        "STRING"),
    bigquery.SchemaField("ship_type",              "INTEGER"),
    bigquery.SchemaField("rto_risk",               "STRING"),
    bigquery.SchemaField("expedited",              "INTEGER"),
    bigquery.SchemaField("zone",                   "STRING"),
    bigquery.SchemaField("is_international",       "INTEGER"),
    bigquery.SchemaField("is_return",              "INTEGER"),
    bigquery.SchemaField("is_b2b",                 "BOOLEAN"),

    bigquery.SchemaField("channel_created_at",     "STRING"),
    bigquery.SchemaField("created_at",             "STRING"),
    bigquery.SchemaField("updated_at",             "STRING"),
    bigquery.SchemaField("etd_date",               "STRING"),
    bigquery.SchemaField("delivered_date",         "STRING"),
    bigquery.SchemaField("out_for_delivery_date",  "STRING"),
    bigquery.SchemaField("picked_up_date",         "STRING"),

    bigquery.SchemaField("pickup_location",        "STRING"),
    bigquery.SchemaField("pickup_city",            "STRING"),
    bigquery.SchemaField("pickup_state",           "STRING"),
    bigquery.SchemaField("pickup_pincode",         "STRING"),

    bigquery.SchemaField("product_quantity",       "INTEGER"),

    bigquery.SchemaField("shipment_id",            "INTEGER"),
    bigquery.SchemaField("awb",                    "STRING"),
    bigquery.SchemaField("courier",                "STRING"),
    bigquery.SchemaField("courier_id",             "INTEGER"),
    bigquery.SchemaField("weight_kg",              "FLOAT"),
    bigquery.SchemaField("dimensions",             "STRING"),
    bigquery.SchemaField("pickup_scheduled_date",  "STRING"),
    bigquery.SchemaField("shipped_date",           "STRING"),
    bigquery.SchemaField("shipment_status",        "INTEGER"),
    bigquery.SchemaField("shipment_total",         "FLOAT"),
    bigquery.SchemaField("shipment_cost",          "FLOAT"),
    bigquery.SchemaField("etd",                    "STRING"),
    bigquery.SchemaField("saral_etd",              "STRING"),
    bigquery.SchemaField("pickup_id",              "STRING"),
    bigquery.SchemaField("awb_assign_date",        "STRING"),
    bigquery.SchemaField("volumetric_weight",      "FLOAT"),

    bigquery.SchemaField("freight_charges",        "FLOAT"),
    bigquery.SchemaField("applied_weight",         "FLOAT"),

    bigquery.SchemaField("billing_name",           "STRING"),
    bigquery.SchemaField("billing_city",           "STRING"),
    bigquery.SchemaField("billing_state",          "STRING"),
    bigquery.SchemaField("billing_pincode",        "STRING"),
    bigquery.SchemaField("billing_country",        "STRING"),
    bigquery.SchemaField("billing_email",          "STRING"),
    bigquery.SchemaField("invoice_number",         "STRING"),
    bigquery.SchemaField("package_count",          "INTEGER"),

    bigquery.SchemaField("delivery_delayed",       "BOOLEAN"),
    bigquery.SchemaField("delay_reason",           "STRING"),
    bigquery.SchemaField("is_delayed",             "STRING"),
    bigquery.SchemaField("is_early",               "STRING"),

    bigquery.SchemaField("activities",             "STRING"),
    bigquery.SchemaField("products_json",          "STRING"),
    bigquery.SchemaField("shipments_json",         "STRING"),
]


# ---------------------------------------------------------------------------
# BigQuery helpers
# ---------------------------------------------------------------------------

def get_bq_client() -> bigquery.Client:
    credentials = service_account.Credentials.from_service_account_file(
        GOOGLE_CREDENTIALS_PATH,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    return bigquery.Client(project=GOOGLE_PROJECT_ID, credentials=credentials)


def get_latest_date_in_bq(client: bigquery.Client) -> "date | None":
    """
    Returns the most recent channel_created_at date already in the BQ table.
    Returns None if the table is empty or doesn't exist yet.
    channel_created_at format: '15 May 2026, 12:21 PM'
    """
    from datetime import datetime
    table_ref = f"{GOOGLE_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
    query = f"""
        SELECT MAX(
            PARSE_DATE('%d %b %Y',
                REGEXP_EXTRACT(channel_created_at, r'^(\\d{{1,2}} \\w+ \\d{{4}})')
            )
        ) AS latest_date
        FROM `{table_ref}`
    """
    try:
        result = list(client.query(query).result())
        latest = result[0].latest_date
        return latest  # already a datetime.date object from BQ
    except Exception as e:
        log.info("Could not read latest date from BQ (table may be empty): %s", e)
        return None


def coerce_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Apply correct dtypes to all columns before BQ upload."""
    int_cols   = [f.name for f in BQ_SCHEMA if f.field_type == "INTEGER"]
    float_cols = [f.name for f in BQ_SCHEMA if f.field_type == "FLOAT"]
    bool_cols  = [f.name for f in BQ_SCHEMA if f.field_type == "BOOLEAN"]

    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in bool_cols:
        if col in df.columns:
            df[col] = df[col].map(lambda x: bool(x) if x is not None else None)
    return df


def upload_to_bq(client: bigquery.Client, df: pd.DataFrame,
                 mode: str = "WRITE_TRUNCATE") -> None:
    table_ref = f"{GOOGLE_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
    log.info("Uploading %d rows to %s (mode: %s) …", len(df), table_ref, mode)

    disposition = (bigquery.WriteDisposition.WRITE_TRUNCATE
                   if mode == "WRITE_TRUNCATE"
                   else bigquery.WriteDisposition.WRITE_APPEND)

    job_config = bigquery.LoadJobConfig(
        schema=BQ_SCHEMA,
        write_disposition=disposition,
    )

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()

    if job.errors:
        raise RuntimeError(f"BigQuery load errors: {job.errors}")

    log.info("Upload complete. Rows in table: %d", client.get_table(table_ref).num_rows)


def delete_date_from_bq(client: bigquery.Client, date_str: str) -> None:
    """
    Delete all rows for a specific channel_created_at date before re-appending.
    This prevents duplicates when re-fetching recent days whose statuses changed.
    date_str format: 'YYYY-MM-DD'
    """
    table_ref = f"{GOOGLE_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
    query = f"""
        DELETE FROM `{table_ref}`
        WHERE PARSE_DATE('%d %b %Y',
            REGEXP_EXTRACT(channel_created_at, r'^(\\d{{1,2}} \\w+ \\d{{4}})')
        ) = DATE('{date_str}')
    """
    client.query(query).result()
    log.info("  Deleted existing rows for %s before re-fetch.", date_str)



def dedup_raw_table(client: bigquery.Client) -> None:
    """
    Remove duplicate order_id rows from the raw orders table in-place.
    Keeps the most recently assigned row (by awb_assign_date DESC).
    Runs after every upload to keep the raw table clean.
    """
    table_ref = f"{GOOGLE_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
    query = f"""
        CREATE OR REPLACE TABLE `{table_ref}` AS
        SELECT * EXCEPT(_row_num)
        FROM (
          SELECT *,
            ROW_NUMBER() OVER (
              PARTITION BY order_id
              ORDER BY awb_assign_date DESC, created_at DESC
            ) AS _row_num
          FROM `{table_ref}`
        )
        WHERE _row_num = 1
    """
    log.info("Deduplicating raw orders table …")
    client.query(query).result()
    log.info("Deduplication complete. Rows in table: %d",
             client.get_table(table_ref).num_rows)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    from datetime import date, timedelta

    # ── Validate env vars ─────────────────────────────────────────────────
    missing = [v for v in ("SHIPROCKET_TOKEN", "GOOGLE_PROJECT_ID", "BQ_DATASET", "BQ_TABLE")
               if not os.getenv(v)]
    if missing:
        raise EnvironmentError(f"Missing required env vars: {missing}")

    # ── Connect to BigQuery ───────────────────────────────────────────────
    log.info("Authenticating with BigQuery …")
    client = get_bq_client()

    # ── Decide: backfill or incremental? ──────────────────────────────────
    latest_date = get_latest_date_in_bq(client)
    today       = date.today()

    # BACKFILL MODE: table is empty or we force a full reload
    # Change BACKFILL_FROM to your actual first order date
    BACKFILL_FROM = date(2026, 5, 1)   # ← adjust this to your go-live date

    if latest_date is None:
        # Table is empty → full backfill
        log.info("Table is empty. Running full backfill from %s …", BACKFILL_FROM)
        fetch_start = BACKFILL_FROM
        upload_mode = "WRITE_TRUNCATE"
        days_to_refresh = []   # no status refresh needed on first load

    else:
        # Table has data → incremental mode
        # Fetch yesterday + today (yesterday's statuses may have changed)
        fetch_start      = latest_date - timedelta(days=40)  # 15-day lookback for status updates
        upload_mode      = "WRITE_APPEND"
        days_to_refresh  = [
            (fetch_start + timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range((today - fetch_start).days + 1)
        ]
        log.info("Incremental mode. Re-fetching from %s to catch status updates …", fetch_start)

    # ── Fetch orders ──────────────────────────────────────────────────────
    all_orders = {}
    current = fetch_start
    while current <= today:
        day_str       = current.strftime("%Y-%m-%d")
        window_orders = fetch_window(day_str, day_str)
        new = 0
        for o in window_orders:
            oid = o.get("id")
            if oid and oid not in all_orders:
                all_orders[oid] = o
                new += 1
        if new:
            log.info("  %s → %d new orders (running total: %d)", day_str, new, len(all_orders))
        current += timedelta(days=1)

    log.info("Fetched %d unique orders in total.", len(all_orders))

    if not all_orders:
        log.info("No new orders found. Nothing to upload.")
        return

    # ── Flatten & coerce ──────────────────────────────────────────────────
    log.info("Flattening orders …")
    rows = [flatten_order(o) for o in all_orders.values()]
    df   = coerce_dataframe(pd.DataFrame(rows))
    log.info("DataFrame shape: %s", df.shape)

    # ── For incremental: delete the days we're about to re-insert ─────────
    if upload_mode == "WRITE_APPEND":
        log.info("Removing stale rows for refreshed days before re-inserting …")
        for day_str in days_to_refresh:
            delete_date_from_bq(client, day_str)

    # ── Upload ────────────────────────────────────────────────────────────
    upload_to_bq(client, df, mode=upload_mode)

    # ── Deduplicate raw table after every upload ──────────────────────────
    dedup_raw_table(client)
    log.info("Pipeline finished successfully.")


if __name__ == "__main__":
    main()