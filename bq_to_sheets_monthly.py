"""
BigQuery → Google Sheets DUMP Sync (Full Rebuild + Incremental)
----------------------------------------------------------------
Fetches order data from BigQuery (orders_enriched view) and writes
it into the "DUMP" worksheet of the target Google Sheet.

On FIRST RUN (or after a manual clear):
  - Clears all data rows (keeps headers) across all month blocks
  - Rebuilds from PIPELINE_START_MONTH (May 2026) to current month
  - Writes all orders for each month, deduped by channel_order_id

On SUBSEQUENT RUNS:
  - For each month block already in the sheet:
      * Finds rows where AWB (col B) is blank → fetches from BQ → fills in
      * Appends any new channel_order_ids not yet in the sheet
  - For the current month only: appends new orders since last run

Layout (single worksheet, horizontal expansion per month):
  May  → A-C | gap D | June → E-G | gap H | July → I-K | gap L | ...
  Block width = 3 data cols + 1 gap col = 4 cols per month

Row 1: Headers  (Created Date | Tracking Number | Facility)
Row 2+: Data    (channel_created_at date | awb | warehouse)

Rules:
  - NEVER clears headers
  - Dedup by channel_order_id (stable unique key)
  - Warehouse: GGN→Gurgaon D2C, MUM→Mumbai D2C, BLR→Bangalore D2C
  - NULL AWB orders included (blank in col B)
  - Update pass fills in AWBs that were blank on previous runs

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
HEADERS        = ["Created Date", "Tracking Number", "Facility"]

WAREHOUSE_MAP = {
    "GGN": "Gurgaon D2C",
    "MUM": "Mumbai D2C",
    "BLR": "Bangalore D2C",
}

# Pipeline start
PIPELINE_START_YEAR  = 2026
PIPELINE_START_MONTH = 5  # May

# Layout
DATA_COLS   = 3
GAP_COLS    = 1
BLOCK_WIDTH = DATA_COLS + GAP_COLS  # 4 cols per month


# ---------------------------------------------------------------------------
# Column layout helpers
# ---------------------------------------------------------------------------

def month_index(year: int, month: int) -> int:
    return (year - PIPELINE_START_YEAR) * 12 + (month - PIPELINE_START_MONTH)


def col_index_for_month(year: int, month: int) -> int:
    return month_index(year, month) * BLOCK_WIDTH


def col_letter(col_idx: int) -> str:
    result = ""
    col_idx += 1
    while col_idx:
        col_idx, remainder = divmod(col_idx - 1, 26)
        result = chr(65 + remainder) + result
    return result


def month_range_iter(start_year, start_month, end_year, end_month):
    """Yields (year, month) tuples from start to end inclusive."""
    y, m = start_year, start_month
    while (y, m) <= (end_year, end_month):
        yield y, m
        m += 1
        if m > 12:
            m = 1
            y += 1


# ---------------------------------------------------------------------------
# Credentials
# ---------------------------------------------------------------------------

def get_credentials(scopes: list) -> service_account.Credentials:
    if GCP_CREDENTIALS_JSON:
        log.info("Loading credentials from GCP_CREDENTIALS_JSON env var.")
        return service_account.Credentials.from_service_account_info(
            json.loads(GCP_CREDENTIALS_JSON), scopes=scopes
        )
    log.info("Loading credentials from file: %s", GOOGLE_CREDENTIALS_PATH)
    return service_account.Credentials.from_service_account_file(
        GOOGLE_CREDENTIALS_PATH, scopes=scopes
    )


# ---------------------------------------------------------------------------
# Worksheet
# ---------------------------------------------------------------------------

def get_worksheet(spreadsheet: gspread.Spreadsheet) -> gspread.Worksheet:
    try:
        ws = spreadsheet.worksheet(WORKSHEET_NAME)
        log.info("Worksheet '%s' found.", WORKSHEET_NAME)
        return ws
    except gspread.exceptions.WorksheetNotFound:
        raise RuntimeError(
            f"Worksheet '{WORKSHEET_NAME}' not found. Please create it manually first."
        )


# ---------------------------------------------------------------------------
# Headers
# ---------------------------------------------------------------------------

def ensure_headers(ws: gspread.Worksheet,
                   spreadsheet: gspread.Spreadsheet,
                   year: int, month: int) -> None:
    """Writes bold headers for a month block only if the cell is empty."""
    c_start     = col_index_for_month(year, month)
    header_cell = f"{col_letter(c_start)}1"
    existing    = ws.acell(header_cell).value

    if existing and existing.strip():
        log.info("Headers already present for %s %d — skipping.",
                 month_name[month], year)
        return

    log.info("Writing headers for %s %d at %s ...",
             month_name[month], year, header_cell)
    c_end = c_start + DATA_COLS - 1
    ws.update(
        f"{col_letter(c_start)}1:{col_letter(c_end)}1",
        [HEADERS]
    )
    spreadsheet.batch_update({"requests": [{
        "repeatCell": {
            "range": {
                "sheetId":          ws.id,
                "startRowIndex":    0,
                "endRowIndex":      1,
                "startColumnIndex": c_start,
                "endColumnIndex":   c_end + 1,
            },
            "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
            "fields": "userEnteredFormat.textFormat.bold",
        }
    }]})
    log.info("Headers written for %s %d.", month_name[month], year)


# ---------------------------------------------------------------------------
# BigQuery fetch
# ---------------------------------------------------------------------------

def fetch_bq_orders(year: int, month: int) -> pd.DataFrame:
    """
    Fetches all orders for the given month.
    Returns channel_order_id, channel_created_at (date only DD-MM-YYYY),
    awb (blank if NULL), warehouse (mapped to full name).
    """
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

    log.info("Querying BQ for %s %d ...", month_name[month], year)
    creds  = get_credentials(["https://www.googleapis.com/auth/cloud-platform"])
    client = bigquery.Client(project=GOOGLE_PROJECT_ID, credentials=creds)
    df     = client.query(query).to_dataframe()
    df["warehouse"] = df["warehouse"].map(WAREHOUSE_MAP).fillna(df["warehouse"])
    df = df.fillna("").astype(str)
    log.info("BQ returned %d rows for %s %d.", len(df), month_name[month], year)
    return df


# ---------------------------------------------------------------------------
# Sheet read helpers
# ---------------------------------------------------------------------------

def read_block(ws: gspread.Worksheet, year: int, month: int) -> dict:
    """
    Reads the full data block for a month from the sheet.
    Returns a dict: { channel_order_id: (row_number, awb_value) }
    Row numbers are 1-indexed (row 1 = header).
    """
    c_start = col_index_for_month(year, month)

    # Read all 3 columns at once
    col_a = ws.col_values(c_start + 1)  # Created Date
    col_b = ws.col_values(c_start + 2)  # Tracking Number (AWB)
    col_c = ws.col_values(c_start + 3)  # Facility

    # Pad shorter columns
    max_len = max(len(col_a), len(col_b), len(col_c))
    col_a  += [""] * (max_len - len(col_a))
    col_b  += [""] * (max_len - len(col_b))

    # We need channel_order_id for dedup — but we only store 3 cols in sheet.
    # We use AWB as the lookup key for update pass, row index for position.
    # Return row_number → (date, awb, facility) starting from row 2
    block = {}
    for i in range(1, max_len):   # skip header at index 0
        date_val = col_a[i] if i < len(col_a) else ""
        awb_val  = col_b[i] if i < len(col_b) else ""
        block[i + 1] = (date_val, awb_val)  # row_number (1-indexed): (date, awb)

    return block


def get_existing_order_ids_from_col(ws: gspread.Worksheet,
                                    year: int, month: int,
                                    id_col_offset: int = 1) -> dict:
    """
    Since we don't store channel_order_id in the sheet, we use AWB as dedup key.
    Returns set of non-blank AWB values already in the block.
    Also returns the next empty row number.
    """
    c_start  = col_index_for_month(year, month)
    awb_col  = ws.col_values(c_start + 2)   # col B of block (AWB), 1-indexed
    date_col = ws.col_values(c_start + 1)   # col A of block (date), 1-indexed

    existing_awbs = set(awb_col[1:])   # skip header
    existing_awbs.discard("")

    # Count total rows including blank-AWB rows (use date col as anchor)
    next_row = len(date_col) + 1 if date_col else 2

    # Build map of row_number → awb for update pass
    blank_awb_rows = {}
    for i in range(1, len(date_col)):   # skip header
        awb = awb_col[i] if i < len(awb_col) else ""
        if not awb.strip():
            blank_awb_rows[i + 1] = date_col[i]  # row_number: date_value

    log.info("%s %d: %d existing AWBs, %d blank-AWB rows, next row=%d.",
             month_name[month], year,
             len(existing_awbs), len(blank_awb_rows), next_row)

    return existing_awbs, next_row, blank_awb_rows


# ---------------------------------------------------------------------------
# Clear data rows (keep headers)
# ---------------------------------------------------------------------------

def clear_data_rows(ws: gspread.Spreadsheet,
                    spreadsheet: gspread.Spreadsheet,
                    today: date) -> None:
    """
    Clears all data rows (row 2 onwards) for all month blocks
    from pipeline start to current month. Headers in row 1 are preserved.
    """
    log.info("Clearing all data rows across all month blocks ...")
    requests = []

    for y, m in month_range_iter(
        PIPELINE_START_YEAR, PIPELINE_START_MONTH,
        today.year, today.month
    ):
        c_start = col_index_for_month(y, m)
        c_end   = c_start + DATA_COLS - 1
        requests.append({
            "updateCells": {
                "range": {
                    "sheetId":          ws.id,
                    "startRowIndex":    1,      # row 2 (0-indexed = 1)
                    "endRowIndex":      500000, # clear all rows below header
                    "startColumnIndex": c_start,
                    "endColumnIndex":   c_end + 1,
                },
                "fields": "userEnteredValue",
            }
        })

    if requests:
        spreadsheet.batch_update({"requests": requests})
    log.info("All data rows cleared.")


# ---------------------------------------------------------------------------
# Write operations
# ---------------------------------------------------------------------------

def write_month_data(ws: gspread.Worksheet, df: pd.DataFrame,
                     year: int, month: int, start_row: int = 2) -> None:
    """Writes all rows for a month block starting at start_row."""
    if df.empty:
        log.info("No data for %s %d — skipping write.", month_name[month], year)
        return

    c_start    = col_index_for_month(year, month)
    start_cell = f"{col_letter(c_start)}{start_row}"

    rows = [
        [row["channel_created_at"], row["awb_code"], row["warehouse"]]
        for _, row in df.iterrows()
    ]

    ws.update(start_cell, rows, value_input_option="USER_ENTERED")
    log.info("Wrote %d rows for %s %d starting at %s.",
             len(rows), month_name[month], year, start_cell)


def append_new_rows(ws: gspread.Worksheet, df: pd.DataFrame,
                    existing_awbs: set, next_row: int,
                    year: int, month: int) -> None:
    """Appends rows whose AWB is not already in the sheet."""
    new_rows = [
        [row["channel_created_at"], row["awb_code"], row["warehouse"]]
        for _, row in df.iterrows()
        if row["awb_code"] not in existing_awbs
    ]

    if not new_rows:
        log.info("No new rows to append for %s %d.", month_name[month], year)
        return

    c_start    = col_index_for_month(year, month)
    start_cell = f"{col_letter(c_start)}{next_row}"
    ws.update(start_cell, new_rows, value_input_option="USER_ENTERED")
    log.info("Appended %d new rows for %s %d at %s.",
             len(new_rows), month_name[month], year, start_cell)


def update_blank_awbs(ws: gspread.Worksheet, df: pd.DataFrame,
                      blank_awb_rows: dict,
                      year: int, month: int) -> None:
    """
    For rows in the sheet where AWB is blank, checks if BQ now has an AWB
    and fills it in. Matches by date + position since we don't store order_id.
    Uses AWB from BQ dataframe matched by date.
    """
    if not blank_awb_rows:
        return

    # Build date → awb map from BQ (only non-blank AWBs)
    bq_date_awb = {}
    for _, row in df.iterrows():
        if row["awb_code"].strip():
            date_key = row["channel_created_at"]
            if date_key not in bq_date_awb:
                bq_date_awb[date_key] = []
            bq_date_awb[date_key].append(row["awb_code"])

    c_start  = col_index_for_month(year, month)
    awb_col  = col_letter(c_start + 1)   # B column of this block
    updates  = []
    filled   = 0

    for row_num, date_val in blank_awb_rows.items():
        if date_val in bq_date_awb and bq_date_awb[date_val]:
            awb_to_fill = bq_date_awb[date_val].pop(0)
            updates.append({
                "range": f"{awb_col}{row_num}",
                "values": [[awb_to_fill]]
            })
            filled += 1

    if updates:
        ws.batch_update(updates, value_input_option="USER_ENTERED")
        log.info("Filled %d blank AWBs for %s %d.", filled, month_name[month], year)
    else:
        log.info("No blank AWBs to fill for %s %d.", month_name[month], year)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    missing_vars = [v for v in ("GOOGLE_PROJECT_ID", "BQ_DATASET", "DUMP_SHEET_ID")
                    if not os.getenv(v)]
    if missing_vars:
        raise EnvironmentError(f"Missing required env vars: {missing_vars}")

    today = date.today()

    # ── Sheets client ────────────────────────────────────────────────────────
    gc          = gspread.authorize(get_credentials([
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]))
    spreadsheet = gc.open_by_key(DUMP_SHEET_ID)
    ws          = get_worksheet(spreadsheet)

    # ── Step 1: Ensure headers for all months ────────────────────────────────
    log.info("Ensuring headers for all months ...")
    for y, m in month_range_iter(
        PIPELINE_START_YEAR, PIPELINE_START_MONTH,
        today.year, today.month
    ):
        ensure_headers(ws, spreadsheet, y, m)

    # ── Step 2: Clear all data rows ──────────────────────────────────────────
    clear_data_rows(ws, spreadsheet, today)

    # ── Step 3: Rebuild all months from BQ ──────────────────────────────────
    log.info("Rebuilding all months from BigQuery ...")
    for y, m in month_range_iter(
        PIPELINE_START_YEAR, PIPELINE_START_MONTH,
        today.year, today.month
    ):
        df = fetch_bq_orders(y, m)
        if not df.empty:
            write_month_data(ws, df, y, m, start_row=2)

    log.info("Full rebuild complete.")


if __name__ == "__main__":
    main()