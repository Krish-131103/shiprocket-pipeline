"""
BigQuery → Google Sheets Sync
------------------------------
Fetches order data for the past 15 days from BigQuery (orders_enriched view)
and writes it into a worksheet named 'Last 15 Days Orders' in the specified
Google Sheet.

Run manually:         python bq_to_sheets.py
Automated trigger:    Google Apps Script (see instructions below)

.env file must contain the same values as shiprocket_to_bq.py:
  GOOGLE_PROJECT_ID       – GCP project ID
  BQ_DATASET              – BigQuery dataset name
  GOOGLE_CREDENTIALS_PATH – Path to bq_credentials.json
  GOOGLE_SHEET_ID         – The long ID from your Google Sheet URL
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

WORKSHEET_NAME          = "Last 15 Days Orders"
LOOKBACK_DAYS           = 15

# ---------------------------------------------------------------------------
# BigQuery query
# ---------------------------------------------------------------------------

QUERY = f"""
SELECT
  CAST(channel_order_id AS STRING)        AS order_id,
  awb,
  channel_created_at              AS created_at,
  status,
  COALESCE(delivered_date, '')    AS delivered_date,
  customer_pincode                AS address_pincode,
  warehouse                       AS warehouse,
  courier                           AS Delivery_Partner
  FROM `{GOOGLE_PROJECT_ID}.{BQ_DATASET}.orders_enriched`
WHERE
  PARSE_DATE(
    '%d %b %Y',
    REGEXP_EXTRACT(channel_created_at, r'^(\\d{{1,2}} \\w+ \\d{{4}})')
  ) >= DATE_SUB(CURRENT_DATE(), INTERVAL {LOOKBACK_DAYS} DAY)
ORDER BY channel_created_at DESC
"""


def fetch_from_bq() -> pd.DataFrame:
    log.info("Connecting to BigQuery …")
    credentials = service_account.Credentials.from_service_account_file(
        GOOGLE_CREDENTIALS_PATH,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    client = bigquery.Client(project=GOOGLE_PROJECT_ID, credentials=credentials)

    log.info("Running query for last %d days …", LOOKBACK_DAYS)
    df = client.query(QUERY).to_dataframe()
    log.info("Fetched %d rows from BigQuery.", len(df))
    return df


def write_to_sheets(df: pd.DataFrame) -> None:
    log.info("Authenticating with Google Sheets …")
    credentials = service_account.Credentials.from_service_account_file(
        GOOGLE_CREDENTIALS_PATH,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )

    gc     = gspread.authorize(credentials)
    sheet  = gc.open_by_key(GOOGLE_SHEET_ID)

    # Get or create the worksheet
    try:
        ws = sheet.worksheet(WORKSHEET_NAME)
        log.info("Found existing worksheet '%s'. Clearing it …", WORKSHEET_NAME)
        ws.clear()
    except gspread.exceptions.WorksheetNotFound:
        log.info("Creating new worksheet '%s' …", WORKSHEET_NAME)
        ws = sheet.add_worksheet(title=WORKSHEET_NAME, rows=50000, cols=20)

    # Convert all values to strings (Sheets doesn't like NaN or int64)
    df = df.fillna("").astype(str)

    # Build data: header row + data rows
    headers = ["Order ID", "AWB", "Created At", "Status", "Delivered Date", "Address Pincode", "Warehouse", "Delivery Partner"]
    data_rows = df.values.tolist()
    all_rows  = [headers] + data_rows

    log.info("Writing %d rows to sheet …", len(data_rows))
    ws.update(all_rows, value_input_option="RAW")

    # Format header row: bold + background color
    ws.format("A1:H1", {
        "textFormat":      {"bold": True, "fontSize": 11},
        "backgroundColor": {"red": 0.22, "green": 0.46, "blue": 0.69},
        "horizontalAlignment": "CENTER",
    })
    # Format header text color to white
    ws.format("A1:F1", {
        "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
    })

    # Freeze header row
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

    # Add last updated timestamp in cell H1
    timestamp = datetime.now().strftime("%d %b %Y, %I:%M %p")
    ws.update("O1", [[f"Last updated: {timestamp}"]])

    log.info("✅ Sheet updated successfully: https://docs.google.com/spreadsheets/d/%s", GOOGLE_SHEET_ID)


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