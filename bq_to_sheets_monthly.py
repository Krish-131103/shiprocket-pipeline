"""
BigQuery → Google Sheets DUMP Sync
-------------------------------------
Fetches order data for the CURRENT MONTH from BigQuery (orders_enriched view)
and appends NEW rows into the "DUMP" worksheet.

Layout (single worksheet, horizontal expansion per month):
  May  → cols A-C | gap D | June → cols E-G | gap H | July → cols I-K | gap L | ...
  Each block = 3 data cols + 1 gap col = 4 cols per month

Per month block:
  Row 1 : Headers  (Created Date | Tracking Number | Facility)
  Row 2+: Data     (channel_created_at | awb | warehouse)

Rules:
  - NEVER clears or overwrites anything already in the sheet
  - Appends only NEW rows, deduped by AWB within the block
  - Warehouse names mapped: GGN→Gurgaon D2C, MUM→Mumbai D2C, BLR→Bangalore D2C
  - First run for a month: writes headers in row 1, data from row 2
  - Subsequent runs: reads existing AWBs, appends only what's missing
  - On the 1st of each month: pre-writes headers for next month's block

Run locally:       python bq_to_sheets_dump.py
GitHub Actions:    daily_dump_sheets_sync.yml

Env vars required:
  GOOGLE_PROJECT_ID        - GCP project ID
  BQ_DATASET               - BigQuery dataset name  (default: shiprocket)
  DUMP_SHEET_ID            - Google Sheet ID for the DUMP sheet
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
BQ_DATASET              = os.getenv("BQ_DATASET", "shiprocket")
GOOGLE_CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH", "bq_credentials.json")
GCP_CREDENTIALS_JSON    = os.getenv("GCP_CREDENTIALS_JSON")
DUMP_SHEET_ID           = os.getenv("DUMP_SHEET_ID")

WORKSHEET_NAME = "DUMP"

HEADERS = ["Created Date", "Tracking Number", "Facility"]

WAREHOUSE_MAP = {
    "GGN": "Gurgaon D2C",
    "MUM": "Mumbai D2C",
    "BLR": "Bangalore D2C",
}

# ---------------------------------------------------------------------------
# Column layout
# ---------------------------------------------------------------------------
# Each month = 3 data cols + 1 gap col = 4 cols per block
# May 2026 = index 0 → starts at col 0 (A)
# June 2026 = index 1 → starts at col 4 (E)
# July 2026 = index 2 → starts at col 8 (I) ... and so on

PIPELINE_START_YEAR  = 2026
PIPELINE_START_MONTH = 5   # May

DATA_COLS   = 3
GAP_COLS    = 1
BLOCK_WIDTH = DATA_COLS + GAP_COLS  # 4 cols per month


def month_index(year: int, month: int) -> int:
    """0-based index relative to pipeline start (May 2026 = 0)."""
    return (year - PIPELINE_START_YEAR) * 12 + (month - PIPELINE_START_MONTH)


def col_index_for_month(year: int, month: int) -> int:
    """0-based column index of the first data column for a given month."""
    return month_index(year, month) * BLOCK_WIDTH


def col_letter(col_idx: int) -> str:
    """Converts 0-based column index to A1-notation. 0=A, 25=Z, 26=AA …"""
    result = ""
    col_idx += 1  # switch to 1-based
    while col_idx:
        col_idx, remainder = divmod(col_idx - 1, 26)
        result = chr(65 + remainder) + result
    return result


def block_range(year: int, month: int, row: int) -> str:
    """
    Returns A1-notation for the 3 data columns of a month block at a given row.
    e.g. block_range(2026, 5, 1) → "A1:C1"
         block_range(2026, 6, 2) → "E2:G2"
    """
    c_start = col_index_for_month(year, month)
    c_end   = c_start + DATA_COLS - 1
    return f"{col_letter(c_start)}{row}:{col_letter(c_end)}{row}"


# ---------------------------------------------------------------------------
# Credentials
# ---------------------------------------------------------------------------

def get_credentials(scopes: list) -> service_account.Credentials:
    if GCP_CREDENTIALS_JSON:
        log.info("Loading credentials from GCP_CREDENTIALS_JSON env var.")
        info = json.loads(GCP_CREDENTIALS_JSON)
        return service_account.Credentials.from_service_account_info(info, scopes=scopes)
    log.info("Loading credentials from file: %s", GOOGLE_CREDENTIALS_PATH)
    return service_account.Credentials.from_service_account_file(
        GOOGLE_CREDENTIALS_PATH, scopes=scopes
    )


# ---------------------------------------------------------------------------
# Worksheet helper
# ---------------------------------------------------------------------------

def get_worksheet(spreadsheet: gspread.Spreadsheet) -> gspread.Worksheet:
    """Returns the 'DUMP' worksheet. Raises clearly if not found."""
    try:
        ws = spreadsheet.worksheet(WORKSHEET_NAME)
        log.info("Worksheet '%s' found.", WORKSHEET_NAME)
        return ws
    except gspread.exceptions.WorksheetNotFound:
        raise RuntimeError(
            f"Worksheet '{WORKSHEET_NAME}' not found in sheet {DUMP_SHEET_ID}. "
            "Please create it manually first."
        )


# ---------------------------------------------------------------------------
# Headers
# ---------------------------------------------------------------------------

def ensure_headers(ws: gspread.Worksheet,
                   spreadsheet: gspread.Spreadsheet,
                   year: int, month: int) -> None:
    """
    Writes headers in row 1 of the month's block ONLY if that cell is empty.
    Headers are written exactly once and never overwritten.
    """
    c_start     = col_index_for_month(year, month)
    header_cell = f"{col_letter(c_start)}1"
    existing    = ws.acell(header_cell).value

    if existing:
        log.info("Headers already present for %s %d at %s — skipping.",
                 month_name[month], year, header_cell)
        return

    log.info("Writing headers for %s %d starting at %s ...",
             month_name[month], year, header_cell)
    ws.update(block_range(year, month, 1), [HEADERS])

    # Bold headers
    c_end = c_start + DATA_COLS - 1
    spreadsheet.batch_update({
        "requests": [{
            "repeatCell": {
                "range": {
                    "sheetId":           ws.id,
                    "startRowIndex":     0,
                    "endRowIndex":       1,
                    "startColumnIndex":  c_start,
                    "endColumnIndex":    c_end + 1,
                },
                "cell": {
                    "userEnteredFormat": {"textFormat": {"bold": True}}
                },
                "fields": "userEnteredFormat.textFormat.bold",
            }
        }]
    })
    log.info("Headers written for %s %d.", month_name[month], year)


# ---------------------------------------------------------------------------
# BigQuery fetch
# ---------------------------------------------------------------------------

def fetch_bq_orders(year: int, month: int) -> pd.DataFrame:
    """
    Fetches channel_created_at (date only), awb, warehouse for the given month.
    Handles both date formats present in the data:
      - "15 May 2026, 12:21 PM"
      - "04-05-2026"
    """
    start = date(year, month, 1)
    end   = date(year + (1 if month == 12 else 0), month % 12 + 1, 1)

    query = f"""
        SELECT
            FORMAT_DATE('%d-%m-%Y',
                COALESCE(
                    SAFE.PARSE_DATE('%d-%m-%Y', channel_created_at),
                    CAST(SAFE.PARSE_DATETIME('%d %b %Y, %I:%M %p', channel_created_at) AS DATE)
                )
            ) AS channel_created_at,
            CAST(awb       AS STRING) AS awb_code,
            CAST(warehouse AS STRING) AS warehouse
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

    log.info("Querying BigQuery for %s %d ...", month_name[month], year)
    creds  = get_credentials(["https://www.googleapis.com/auth/cloud-platform"])
    client = bigquery.Client(project=GOOGLE_PROJECT_ID, credentials=creds)
    df     = client.query(query).to_dataframe()
    log.info("BigQuery returned %d rows.", len(df))

    # Map warehouse codes to full names
    df["warehouse"] = df["warehouse"].map(WAREHOUSE_MAP).fillna(df["warehouse"])

    return df


# ---------------------------------------------------------------------------
# Incremental append
# ---------------------------------------------------------------------------

def get_existing_awbs(ws: gspread.Worksheet, year: int, month: int) -> set:
    """
    Reads the AWB column (col 2 of the block) and returns
    the set of already-present AWB values (skips header row).
    """
    c_start  = col_index_for_month(year, month)
    col_vals = ws.col_values(c_start + 2)   # col 2 of block (awb), 1-indexed
    existing = set(col_vals[1:])             # skip header
    existing.discard("")
    log.info("%s %d: sheet has %d existing AWBs.",
             month_name[month], year, len(existing))
    return existing


def append_new_rows(ws: gspread.Worksheet,
                    df: pd.DataFrame,
                    existing_awbs: set,
                    year: int,
                    month: int) -> None:
    """
    Appends rows whose AWB is not already in the sheet.
    Writes ONLY the 3 data columns per block.
    Never touches any other column.
    """
    df = df.fillna("").astype(str)

    new_rows = [
        [row["channel_created_at"], row["awb_code"], row["warehouse"]]
        for _, row in df.iterrows()
        if row["awb_code"] not in existing_awbs
    ]

    if not new_rows:
        log.info("No new orders to append for %s %d.", month_name[month], year)
        return

    log.info("Appending %d new rows for %s %d ...",
             len(new_rows), month_name[month], year)

    # Find next empty row using first column of this month's block
    c_start  = col_index_for_month(year, month)
    col_vals = ws.col_values(c_start + 1)   # 1-indexed, first col of block
    next_row = len(col_vals) + 1            # 1-indexed row number

    start_cell = f"{col_letter(c_start)}{next_row}"
    ws.update(start_cell, new_rows, value_input_option="USER_ENTERED")
    log.info("Done. Rows written starting at %s.", start_cell)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    missing_vars = [v for v in ("GOOGLE_PROJECT_ID", "BQ_DATASET", "DUMP_SHEET_ID")
                    if not os.getenv(v)]
    if missing_vars:
        raise EnvironmentError(f"Missing required env vars: {missing_vars}")

    today = date.today()

    # ── Google Sheets client ─────────────────────────────────────────────────
    gc          = gspread.authorize(get_credentials([
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]))
    spreadsheet = gc.open_by_key(DUMP_SHEET_ID)
    ws          = get_worksheet(spreadsheet)

    # ── 1. Ensure headers exist for current month ────────────────────────────
    ensure_headers(ws, spreadsheet, today.year, today.month)

    # ── 2. Fetch this month's orders from BigQuery ───────────────────────────
    df = fetch_bq_orders(today.year, today.month)

    if df.empty:
        log.warning("No orders returned from BQ for %s %d — nothing appended.",
                    month_name[today.month], today.year)
        return

    # ── 3. Incremental append (deduped by AWB) ────────────────────────────────
    existing_awbs = get_existing_awbs(ws, today.year, today.month)
    append_new_rows(ws, df, existing_awbs, today.year, today.month)

    # ── 4. On the 1st: pre-write headers for next month's block ──────────────
    if today.day == 1:
        next_month = today.month % 12 + 1
        next_year  = today.year + (1 if today.month == 12 else 0)
        log.info("1st of month — pre-writing headers for %s %d ...",
                 month_name[next_month], next_year)
        ensure_headers(ws, spreadsheet, next_year, next_month)

    log.info("All done.")


if __name__ == "__main__":
    main()