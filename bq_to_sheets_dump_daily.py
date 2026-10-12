"""
BigQuery → Google Sheets DUMP Daily Incremental Sync
------------------------------------------------------
Runs AFTER the initial full rebuild (bq_to_sheets_monthly.py).
On each daily run:
  1. For ALL months in the sheet: fills in any blank AWBs that BQ now has
  2. For CURRENT month only: appends any new orders not yet in the sheet

This script never clears anything. Safe to run daily.

Env vars required:
  GOOGLE_PROJECT_ID        - GCP project ID
  BQ_DATASET               - BigQuery dataset (default: shiprocket)
  DUMP_SHEET_ID            - Target Google Sheet ID
  GOOGLE_CREDENTIALS_PATH  - (local) path to bq_credentials.json
  GCP_CREDENTIALS_JSON     - (GitHub Actions) credentials JSON string
"""

import os
import json
import logging
from datetime import date
from calendar import month_name

import gspread
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

load_dotenv()

GOOGLE_PROJECT_ID       = os.getenv("GOOGLE_PROJECT_ID")
BQ_DATASET              = os.getenv("BQ_DATASET", "shiprocket")
GOOGLE_CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH", "bq_credentials.json")
GCP_CREDENTIALS_JSON    = os.getenv("GCP_CREDENTIALS_JSON")
DUMP_SHEET_ID           = os.getenv("DUMP_SHEET_ID")

WORKSHEET_NAME = "DUMP"
HEADERS        = ["Created Date", "Tracking Number", "Facility"]

WAREHOUSE_MAP = {
    "GGN": "Gurgaon D2C",
    "MUM": "Mumbai D2C",
    "BLR": "Bangalore D2C",
}

PIPELINE_START_YEAR  = 2026
PIPELINE_START_MONTH = 5

DATA_COLS   = 3
GAP_COLS    = 1
BLOCK_WIDTH = DATA_COLS + GAP_COLS


def month_index(year, month):
    return (year - PIPELINE_START_YEAR) * 12 + (month - PIPELINE_START_MONTH)

def col_index_for_month(year, month):
    return month_index(year, month) * BLOCK_WIDTH

def col_letter(col_idx):
    result = ""
    col_idx += 1
    while col_idx:
        col_idx, remainder = divmod(col_idx - 1, 26)
        result = chr(65 + remainder) + result
    return result

def month_range_iter(sy, sm, ey, em):
    y, m = sy, sm
    while (y, m) <= (ey, em):
        yield y, m
        m += 1
        if m > 12:
            m = 1
            y += 1

def get_credentials(scopes):
    if GCP_CREDENTIALS_JSON:
        return service_account.Credentials.from_service_account_info(
            json.loads(GCP_CREDENTIALS_JSON), scopes=scopes)
    return service_account.Credentials.from_service_account_file(
        GOOGLE_CREDENTIALS_PATH, scopes=scopes)

def get_worksheet(spreadsheet):
    try:
        ws = spreadsheet.worksheet(WORKSHEET_NAME)
        log.info("Worksheet '%s' found.", WORKSHEET_NAME)
        return ws
    except gspread.exceptions.WorksheetNotFound:
        raise RuntimeError(f"Worksheet '{WORKSHEET_NAME}' not found.")

def ensure_headers(ws, spreadsheet, year, month):
    c_start     = col_index_for_month(year, month)
    header_cell = f"{col_letter(c_start)}1"
    existing    = ws.acell(header_cell).value
    if existing and existing.strip():
        return
    c_end = c_start + DATA_COLS - 1
    ws.update(f"{col_letter(c_start)}1:{col_letter(c_end)}1", [HEADERS])
    spreadsheet.batch_update({"requests": [{
        "repeatCell": {
            "range": {
                "sheetId": ws.id,
                "startRowIndex": 0, "endRowIndex": 1,
                "startColumnIndex": c_start, "endColumnIndex": c_end + 1,
            },
            "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
            "fields": "userEnteredFormat.textFormat.bold",
        }
    }]})
    log.info("Headers written for %s %d.", month_name[month], year)

def fetch_bq_orders(year, month):
    start = date(year, month, 1)
    end   = date(year + (1 if month == 12 else 0), month % 12 + 1, 1)
    query = f"""
        SELECT
            CAST(channel_order_id AS STRING) AS channel_order_id,
            FORMAT_DATE('%d-%m-%Y',
                COALESCE(
                    SAFE.PARSE_DATE('%d-%m-%Y', channel_created_at),
                    CAST(SAFE.PARSE_DATETIME('%d %b %Y, %I:%M %p', channel_created_at) AS DATE)
                )
            ) AS channel_created_at,
            COALESCE(CAST(awb AS STRING), '') AS awb_code,
            CAST(warehouse AS STRING)         AS warehouse
        FROM `{GOOGLE_PROJECT_ID}.{BQ_DATASET}.orders_enriched`
        WHERE
            COALESCE(
                SAFE.PARSE_DATE('%d-%m-%Y', channel_created_at),
                CAST(SAFE.PARSE_DATETIME('%d %b %Y, %I:%M %p', channel_created_at) AS DATE)
            ) >= DATE '{start}'
            AND
            COALESCE(
                SAFE.PARSE_DATE('%d-%m-%Y', channel_created_at),
                CAST(SAFE.PARSE_DATETIME('%d %b %Y, %I:%M %p', channel_created_at) AS DATE)
            ) < DATE '{end}'
        ORDER BY
            COALESCE(
                SAFE.PARSE_DATE('%d-%m-%Y', channel_created_at),
                CAST(SAFE.PARSE_DATETIME('%d %b %Y, %I:%M %p', channel_created_at) AS DATE)
            ) ASC
    """
    creds  = get_credentials(["https://www.googleapis.com/auth/cloud-platform"])
    client = bigquery.Client(project=GOOGLE_PROJECT_ID, credentials=creds)
    df     = client.query(query).to_dataframe()
    df["warehouse"] = df["warehouse"].map(WAREHOUSE_MAP).fillna(df["warehouse"])
    df = df.fillna("").astype(str)
    log.info("BQ returned %d rows for %s %d.", len(df), month_name[month], year)
    return df

def get_block_state(ws, year, month):
    """Returns existing_awbs (set), next_row (int), blank_awb_rows (dict row->date)."""
    c_start  = col_index_for_month(year, month)
    awb_col  = ws.col_values(c_start + 2)
    date_col = ws.col_values(c_start + 1)

    existing_awbs = set(v for v in awb_col[1:] if v.strip())
    next_row      = len(date_col) + 1 if len(date_col) > 1 else 2

    blank_awb_rows = {}
    for i in range(1, len(date_col)):
        awb = awb_col[i] if i < len(awb_col) else ""
        if not awb.strip() and i < len(date_col) and date_col[i].strip():
            blank_awb_rows[i + 1] = date_col[i]

    return existing_awbs, next_row, blank_awb_rows

def update_blank_awbs(ws, df, blank_awb_rows, year, month):
    if not blank_awb_rows:
        return
    bq_date_awb = {}
    for _, row in df.iterrows():
        if row["awb_code"].strip():
            d = row["channel_created_at"]
            bq_date_awb.setdefault(d, []).append(row["awb_code"])

    c_start  = col_index_for_month(year, month)
    awb_col  = col_letter(c_start + 1)
    updates  = []
    for row_num, date_val in blank_awb_rows.items():
        if bq_date_awb.get(date_val):
            updates.append({
                "range":  f"{awb_col}{row_num}",
                "values": [[bq_date_awb[date_val].pop(0)]]
            })
    if updates:
        ws.batch_update(updates, value_input_option="USER_ENTERED")
        log.info("Filled %d blank AWBs for %s %d.", len(updates), month_name[month], year)

def append_new_rows(ws, df, existing_awbs, next_row, year, month):
    new_rows = [
        [row["channel_created_at"], row["awb_code"], row["warehouse"]]
        for _, row in df.iterrows()
        if row["awb_code"] not in existing_awbs
        and row["awb_code"].strip()  # only append rows with an AWB
    ]
    # Also append NULL AWB rows that aren't yet in sheet
    # (matched by date since we have no order_id in sheet)
    if not new_rows:
        log.info("No new rows to append for %s %d.", month_name[month], year)
        return
    c_start    = col_index_for_month(year, month)
    start_cell = f"{col_letter(c_start)}{next_row}"
    ws.update(start_cell, new_rows, value_input_option="USER_ENTERED")
    log.info("Appended %d new rows for %s %d.", len(new_rows), month_name[month], year)


def main():
    missing = [v for v in ("GOOGLE_PROJECT_ID", "BQ_DATASET", "DUMP_SHEET_ID")
               if not os.getenv(v)]
    if missing:
        raise EnvironmentError(f"Missing env vars: {missing}")

    today = date.today()

    gc          = gspread.authorize(get_credentials([
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]))
    spreadsheet = gc.open_by_key(DUMP_SHEET_ID)
    ws          = get_worksheet(spreadsheet)

    # Ensure headers for current month (in case new month started)
    ensure_headers(ws, spreadsheet, today.year, today.month)

    # On 1st of month: pre-write next month headers
    if today.day == 1:
        nm = today.month % 12 + 1
        ny = today.year + (1 if today.month == 12 else 0)
        ensure_headers(ws, spreadsheet, ny, nm)

    # ── Pass 1: Fill blank AWBs for ALL months ────────────────────────────────
    log.info("Pass 1: Filling blank AWBs across all months ...")
    for y, m in month_range_iter(
        PIPELINE_START_YEAR, PIPELINE_START_MONTH,
        today.year, today.month
    ):
        _, _, blank_awb_rows = get_block_state(ws, y, m)
        if blank_awb_rows:
            df = fetch_bq_orders(y, m)
            update_blank_awbs(ws, df, blank_awb_rows, y, m)

    # ── Pass 2: Append new orders for CURRENT month only ─────────────────────
    log.info("Pass 2: Appending new orders for %s %d ...",
             month_name[today.month], today.year)
    df = fetch_bq_orders(today.year, today.month)
    if not df.empty:
        existing_awbs, next_row, _ = get_block_state(ws, today.year, today.month)
        append_new_rows(ws, df, existing_awbs, next_row, today.year, today.month)

    log.info("Daily sync complete.")


if __name__ == "__main__":
    main()