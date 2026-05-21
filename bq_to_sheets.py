"""
BigQuery → Google Sheets Sync
------------------------------
Fetches order data for the past 15 days from BigQuery (orders_enriched view)
and writes it into a worksheet named 'Last 15 Days Orders' in the specified
Google Sheet.

Layout:
  A1        = Last updated timestamp
  B1:L1     = Headers (11 columns)
  B2:L...   = Data rows
  M onwards = Safe zone for manual formulas/notes (never cleared by script)

Run manually:         python bq_to_sheets.py
Automated trigger:    GitHub Actions (daily_sheets_sync.yml)

.env file must contain:
  GOOGLE_PROJECT_ID       - GCP project ID
  BQ_DATASET              - BigQuery dataset name
  GOOGLE_CREDENTIALS_PATH - Path to bq_credentials.json
  GOOGLE_SHEET_ID         - The long ID from your Google Sheet URL
"""

import os
import logging
from datetime import datetime

import gspread
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

GOOGLE_PROJECT_ID       = os.getenv("GOOGLE_PROJECT_ID")
BQ_DATASET              = os.getenv("BQ_DATASET")
GOOGLE_CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH", "bq_credentials.json")
GOOGLE_SHEET_ID         = os.getenv("GOOGLE_SHEET_ID")

WORKSHEET_NAME = "Last 15 Days Orders"
LOOKBACK_DAYS  = 15

# Data occupies columns A:L (12 columns).
# Anything in column M onwards is never touched by this script.
DATA_RANGE     = "A1:L50000"
LAST_DATA_COL  = "L"
NUM_COLS       = 11   # B through L = 11 data columns

# ---------------------------------------------------------------------------
# BigQuery query
# ---------------------------------------------------------------------------

QUERY = f"""
SELECT
  CAST(channel_order_id AS STRING)     AS order_id,
  awb,
  channel_created_at                   AS created_at,
  status,
  COALESCE(delivered_date, '')         AS delivered_date,
  customer_pincode                     AS address_pincode,
  warehouse,
  courier                              AS delivery_partner,
  COALESCE(ndr_1_date, '')             AS ndr_1_date,
  COALESCE(ndr_1_reason, '')           AS ndr_1_remark,
  COALESCE(ndr_2_date, '')             AS ndr_2_date
FROM `{GOOGLE_PROJECT_ID}.{BQ_DATASET}.orders_enriched`
WHERE
  PARSE_DATE(
    '%d %b %Y',
    REGEXP_EXTRACT(channel_created_at, r'^(\\d{{1,2}} \\w+ \\d{{4}})')
  ) >= DATE_SUB(CURRENT_DATE(), INTERVAL {LOOKBACK_DAYS} DAY)
ORDER BY channel_created_at DESC
"""


# ---------------------------------------------------------------------------
# Fetch from BigQuery
# ---------------------------------------------------------------------------

def fetch_from_bq() -> pd.DataFrame:
    log.info("Connecting to BigQuery ...")
    credentials = service_account.Credentials.from_service_account_file(
        GOOGLE_CREDENTIALS_PATH,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    client = bigquery.Client(project=GOOGLE_PROJECT_ID, credentials=credentials)

    log.info("Running query for last %d days ...", LOOKBACK_DAYS)
    df = client.query(QUERY).to_dataframe()
    log.info("Fetched %d rows from BigQuery.", len(df))
    return df


# ---------------------------------------------------------------------------
# Write to Google Sheets
# ---------------------------------------------------------------------------

def write_to_sheets(df: pd.DataFrame) -> None:
    log.info("Authenticating with Google Sheets ...")
    credentials = service_account.Credentials.from_service_account_file(
        GOOGLE_CREDENTIALS_PATH,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )

    gc    = gspread.authorize(credentials)
    sheet = gc.open_by_key(GOOGLE_SHEET_ID)

    # Get or create the worksheet
    try:
        ws = sheet.worksheet(WORKSHEET_NAME)
        log.info("Found existing worksheet '%s'.", WORKSHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        log.info("Creating new worksheet '%s' ...", WORKSHEET_NAME)
        ws = sheet.add_worksheet(title=WORKSHEET_NAME, rows=50000, cols=26)

    # ── Step 1: Clear ONLY the data range (A:L) ───────────────────────────
    # Columns M onwards are NEVER cleared — your formulas/manual work
    # in those columns will persist across script runs.
    log.info("Clearing data range %s ...", DATA_RANGE)
    ws.batch_clear([DATA_RANGE])

    # ── Step 2: Write timestamp in A1 ─────────────────────────────────────
    timestamp = datetime.now().strftime("%d %b %Y, %I:%M %p")
    ws.update("A1", [[f"Last updated: {timestamp}"]])

    # Format A1 — black text, white background, default font size
    ws.format("A1", {
        "textFormat": {
            "bold": False,
            "italic": False,
            "fontSize": 10,
            "foregroundColor": {"red": 0.0, "green": 0.0, "blue": 0.0}
        },
        "backgroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0}
    })

    # ── Step 3: Write headers in B1:L1 ────────────────────────────────────
    headers = [
        "Order ID",
        "AWB",
        "Created At",
        "Status",
        "Delivered Date",
        "Address Pincode",
        "Warehouse",
        "Delivery Partner",
        "NDR 1 Date",
        "NDR 1 Remark",
        "NDR 2 Date",
    ]
    ws.update("B1", [headers])

    # Format headers — bold + blue background + white text
    ws.format(f"B1:{LAST_DATA_COL}1", {
        "textFormat": {
            "bold": True,
            "fontSize": 11,
            "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0}
        },
        "backgroundColor":     {"red": 0.22, "green": 0.46, "blue": 0.69},
        "horizontalAlignment": "CENTER",
    })

    # ── Step 4: Write data rows starting from B2 ──────────────────────────
    df = df.fillna("").astype(str)
    data_rows = df.values.tolist()

    if data_rows:
        log.info("Writing %d data rows starting at B2 ...", len(data_rows))
        ws.update("B2", data_rows, value_input_option="RAW")
    else:
        log.warning("No data rows to write.")

    # ── Step 5: Freeze header row ─────────────────────────────────────────
    sheet.batch_update({
        "requests": [{
            "updateSheetProperties": {
                "properties": {
                    "sheetId": ws.id,
                    "gridProperties": {"frozenRowCount": 1}
                },
                "fields": "gridProperties.frozenRowCount"
            }
        }]
    })

    log.info("Sheet updated: https://docs.google.com/spreadsheets/d/%s",
             GOOGLE_SHEET_ID)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    missing = [v for v in ("GOOGLE_PROJECT_ID", "BQ_DATASET", "GOOGLE_SHEET_ID")
               if not os.getenv(v)]
    if missing:
        raise EnvironmentError(f"Missing required env vars: {missing}")

    df = fetch_from_bq()

    if df.empty:
        log.warning("No data returned from BigQuery. Sheet not updated.")
        return

    write_to_sheets(df)
    log.info("Done.")


if __name__ == "__main__":
    main()