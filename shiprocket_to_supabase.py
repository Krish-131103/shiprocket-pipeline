"""
Shiprocket → Supabase (PostgreSQL) Pipeline
--------------------------------------------
Fetches all orders from the Shiprocket API (paginated) and loads them into
a Supabase PostgreSQL table. Runs a full replace (DROP + recreate) on every run.

.env file must contain:
  SHIPROCKET_TOKEN    – Bearer token from POST /auth/login
  SUPABASE_HOST       – e.g. db.xxxxxxxxxxxx.supabase.co
  SUPABASE_PORT       – 5432
  SUPABASE_DB         – postgres
  SUPABASE_USER       – postgres
  SUPABASE_PASSWORD   – your database password
  SUPABASE_TABLE      – e.g. shiprocket_orders
"""

import os
import time
import json
import logging
from typing import Optional

import requests
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

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

SHIPROCKET_TOKEN = os.getenv("SHIPROCKET_TOKEN")
SUPABASE_HOST    = os.getenv("SUPABASE_HOST")
SUPABASE_PORT    = os.getenv("SUPABASE_PORT", "5432")
SUPABASE_DB      = os.getenv("SUPABASE_DB", "postgres")
SUPABASE_USER    = os.getenv("SUPABASE_USER", "postgres")
SUPABASE_PASSWORD= os.getenv("SUPABASE_PASSWORD")
SUPABASE_TABLE   = os.getenv("SUPABASE_TABLE", "shiprocket_orders")

BASE_URL    = "https://apiv2.shiprocket.in/v1/external/orders"
PER_PAGE    = 100
MAX_RETRIES = 3
RETRY_WAIT  = 5  # seconds


# ---------------------------------------------------------------------------
# Fetch helpers
# ---------------------------------------------------------------------------

def get_headers() -> dict:
    return {
        "Authorization": f"Bearer {SHIPROCKET_TOKEN}",
        "Content-Type": "application/json",
    }


def fetch_page(page: int) -> dict:
    url = f"{BASE_URL}?page={page}&per_page={PER_PAGE}"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=get_headers(), timeout=30)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError as e:
            log.warning("HTTP error on page %d attempt %d: %s", page, attempt, e)
        except requests.exceptions.RequestException as e:
            log.warning("Request error on page %d attempt %d: %s", page, attempt, e)
        if attempt < MAX_RETRIES:
            time.sleep(RETRY_WAIT)
    raise RuntimeError(f"Failed to fetch page {page} after {MAX_RETRIES} attempts")


def fetch_all_orders() -> list:
    log.info("Starting Shiprocket order fetch ...")
    first       = fetch_page(1)
    pagination  = first.get("meta", {}).get("pagination", {})
    total_pages = pagination.get("total_pages", 1)
    total_count = pagination.get("total", "?")

    log.info("Total orders: %s | Total pages: %d", total_count, total_pages)
    all_orders = list(first.get("data", []))

    for page in range(2, total_pages + 1):
        log.info("Fetching page %d / %d ...", page, total_pages)
        data = fetch_page(page)
        orders = data.get("data", [])
        if not orders:
            log.info("Empty page %d – stopping early.", page)
            break
        all_orders.extend(orders)
        time.sleep(0.2)

    log.info("Fetched %d orders total.", len(all_orders))
    return all_orders


# ---------------------------------------------------------------------------
# Flattening logic
# ---------------------------------------------------------------------------

def safe_str(val) -> Optional[str]:
    if val is None or val == "" or val == "0000-00-00 00:00:00":
        return None
    return str(val)


def to_float(val) -> Optional[float]:
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def to_json(val) -> Optional[str]:
    if not val:
        return None
    try:
        return json.dumps(val, ensure_ascii=False)
    except Exception:
        return None


def flatten_order(order: dict) -> dict:
    shipments = order.get("shipments") or []
    s         = shipments[0] if shipments else {}
    products  = order.get("products") or []
    others    = order.get("others") or {}
    pa        = order.get("pickup_address_detail") or {}
    charges   = (order.get("awb_data") or {}).get("charges", {})

    return {
        # Order identifiers
        "order_id":               order.get("id"),
        "channel_order_id":       safe_str(order.get("channel_order_id")),
        "invoice_no":             safe_str(order.get("invoice_no")),
        "channel_id":             order.get("channel_id"),
        "channel_name":           safe_str(order.get("channel_name")),
        "base_channel_code":      safe_str(order.get("base_channel_code")),

        # Customer
        "customer_name":          safe_str(order.get("customer_name")),
        "customer_email":         safe_str(order.get("customer_email")),
        "customer_phone":         safe_str(order.get("customer_phone")),
        "customer_address":       safe_str(order.get("customer_address")),
        "customer_address_2":     safe_str(order.get("customer_address_2")),
        "customer_city":          safe_str(order.get("customer_city")),
        "customer_state":         safe_str(order.get("customer_state")),
        "customer_pincode":       safe_str(order.get("customer_pincode")),
        "customer_country":       safe_str(order.get("customer_country")),
        "customer_latitude":      safe_str(order.get("customer_latitude")),
        "customer_longitude":     safe_str(order.get("customer_longitude")),
        "customer_alternate_phone": safe_str(order.get("customer_alternate_phone")),

        # Financials
        "total":                  to_float(order.get("total")),
        "tax":                    to_float(order.get("tax")),
        "discount":               to_float(order.get("discount")),
        "other_charges":          to_float(order.get("other_charges")),
        "giftwrap_charges":       to_float(order.get("giftwrap_charges")),
        "payment_method":         safe_str(order.get("payment_method")),
        "payment_status":         safe_str(order.get("payment_status")),
        "cod":                    order.get("cod"),

        # Status & logistics
        "status":                 safe_str(order.get("status")),
        "status_code":            order.get("status_code"),
        "master_status":          safe_str(order.get("master_status")),
        "sla":                    safe_str(order.get("sla")),
        "shipping_method":        safe_str(order.get("shipping_method")),
        "ship_type":              order.get("ship_type"),
        "rto_risk":               safe_str(order.get("rto_risk")),
        "expedited":              order.get("expedited"),
        "zone":                   safe_str(order.get("zone")),
        "is_international":       order.get("is_international"),
        "is_return":              order.get("is_return"),
        "is_b2b":                 bool(order.get("is_b2b")),

        # Dates (stored as text — cast in SQL if needed)
        "channel_created_at":     safe_str(order.get("channel_created_at")),
        "created_at":             safe_str(order.get("created_at")),
        "updated_at":             safe_str(order.get("updated_at")),
        "etd_date":               safe_str(order.get("etd_date")),
        "delivered_date":         safe_str(order.get("delivered_date")),
        "out_for_delivery_date":  safe_str(order.get("out_for_delivery_date")),
        "picked_up_date":         safe_str(order.get("picked_up_date")),

        # Pickup location
        "pickup_location":        safe_str(order.get("pickup_location")),
        "pickup_city":            safe_str(pa.get("city")),
        "pickup_state":           safe_str(pa.get("state")),
        "pickup_pincode":         safe_str(pa.get("pin_code")),

        # Product summary
        "product_quantity":       order.get("product_quantity"),

        # Primary shipment
        "shipment_id":            s.get("id"),
        "awb":                    safe_str(s.get("awb")),
        "courier":                safe_str(s.get("courier")),
        "courier_id":             s.get("courier_id"),
        "weight_kg":              to_float(s.get("weight")),
        "dimensions":             safe_str(s.get("dimensions")),
        "pickup_scheduled_date":  safe_str(s.get("pickup_scheduled_date")),
        "shipped_date":           safe_str(s.get("shipped_date")),
        "shipment_status":        s.get("status"),
        "shipment_total":         to_float(s.get("total")),
        "shipment_cost":          to_float(s.get("cost")),
        "etd":                    safe_str(s.get("etd")),
        "saral_etd":              safe_str(s.get("saral_etd")),
        "pickup_id":              safe_str(s.get("pickup_id")),
        "awb_assign_date":        safe_str(s.get("awb_assign_date")),
        "volumetric_weight":      to_float(s.get("volumetric_weight")),

        # AWB charges
        "freight_charges":        to_float(charges.get("freight_charges")),
        "applied_weight":         to_float(charges.get("applied_weight")),

        # Billing (from others{})
        "billing_name":           safe_str(others.get("billing_name")),
        "billing_city":           safe_str(others.get("billing_city")),
        "billing_state":          safe_str(others.get("billing_state")),
        "billing_pincode":        safe_str(others.get("billing_pincode")),
        "billing_country":        safe_str(others.get("billing_country")),
        "billing_email":          safe_str(others.get("billing_email")),
        "invoice_number":         safe_str(others.get("invoice_number")),
        "package_count":          others.get("package_count"),

        # Delivery flags
        "delivery_delayed":       bool(order.get("delivery_delayed")),
        "delay_reason":           safe_str(order.get("delay_reason")),
        "is_delayed":             safe_str(order.get("is_delayed")),
        "is_early":               safe_str(order.get("is_early")),

        # Activities
        "activities":             ",".join(order.get("activities") or []),

        # Nested arrays as JSONB strings
        "products_json":          to_json(products),
        "shipments_json":         to_json(shipments),
    }


# ---------------------------------------------------------------------------
# Supabase upload
# ---------------------------------------------------------------------------

def get_engine():def get_engine():
    from urllib.parse import quote_plus
    password = quote_plus(SUPABASE_PASSWORD)
    db_name  = quote_plus(SUPABASE_DB)          # handles the space in "LMxSR AUTOMATION"
    url = (
        f"postgresql+psycopg2://{SUPABASE_USER}:{password}"
        f"@{SUPABASE_HOST}:{SUPABASE_PORT}/{db_name}"
        f"?sslmode=require"
    )
    return create_engine(url)


def upload_to_supabase(df: pd.DataFrame) -> None:
    log.info("Connecting to Supabase ...")
    engine = get_engine()

    with engine.connect() as conn:
        # Drop table if it exists so we get a clean slate (WRITE_TRUNCATE equivalent)
        log.info("Dropping existing table '%s' if it exists ...", SUPABASE_TABLE)
        conn.execute(text(f'DROP TABLE IF EXISTS "{SUPABASE_TABLE}"'))
        conn.commit()

    log.info("Writing %d rows to table '%s' ...", len(df), SUPABASE_TABLE)

    # pandas to_sql creates the table automatically from the DataFrame schema
    df.to_sql(
        name=SUPABASE_TABLE,
        con=engine,
        if_exists="replace",   # replace = drop + recreate (safe for full refresh)
        index=False,
        chunksize=500,         # insert in batches of 500 rows
        method="multi",        # faster bulk insert
    )

    # Verify
    with engine.connect() as conn:
        result = conn.execute(text(f'SELECT COUNT(*) FROM "{SUPABASE_TABLE}"'))
        count = result.scalar()
    log.info("Upload complete. Rows in Supabase table: %d", count)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    # Validate env vars
    required = ["SHIPROCKET_TOKEN", "SUPABASE_HOST", "SUPABASE_PASSWORD"]
    missing  = [v for v in required if not os.getenv(v)]
    if missing:
        raise EnvironmentError(f"Missing required env vars: {missing}")

    # 1. Fetch all orders from Shiprocket
    raw_orders = fetch_all_orders()

    # 2. Flatten each order into a single dict
    log.info("Flattening orders ...")
    rows = [flatten_order(o) for o in raw_orders]

    # 3. Build DataFrame
    df = pd.DataFrame(rows)
    log.info("DataFrame shape: %s", df.shape)

    # 4. Upload to Supabase
    upload_to_supabase(df)
    log.info("Pipeline finished successfully.")


if __name__ == "__main__":
    main()