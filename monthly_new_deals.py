# automation_deals/monthly_new_deals.py
import os, json, random
from datetime import datetime, timedelta
from itertools import product

import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe, get_as_dataframe
from google.oauth2.service_account import Credentials  # ✅ service account auth

# ---------- AUTH (Service Account from env) ----------
# Expect env var GCP_SA_JSON to contain the full JSON string of the service account key
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]
creds_info = json.loads(os.environ["GCP_SA_JSON"])          # ✅ read from secret
creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
gc = gspread.authorize(creds)

# ---------- CONFIG ----------
SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1xBQg2Fpe7P9ff4jtSDl63g_8rxSz5GxSc1hMIopq4a4/edit#gid=0"
TARGET_SHEET = "workbook"

# ---------- HELPERS ----------
def read_all_sheets_as_dfs(spreadsheet):
    """Return {sheet_title: DataFrame} for all worksheets."""
    sheet_dict = {}
    for sheet in spreadsheet.worksheets():
        data = sheet.get_all_records()
        df = pd.DataFrame(data)
        sheet_dict[sheet.title] = df
    return sheet_dict

def append_df_to_gsheet_by_url(spreadsheet_url, sheet_name, new_df, unique_key=None):
    """Append (or create) a worksheet and write DataFrame, optionally deduping by unique_key."""
    spreadsheet = gc.open_by_url(spreadsheet_url)
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
        existing_df = get_as_dataframe(worksheet).dropna(how='all')
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        if unique_key:
            combined_df.drop_duplicates(subset=unique_key, keep='last', inplace=True)
        worksheet.clear()
    except gspread.exceptions.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows="1000", cols="20")
        combined_df = new_df
    set_with_dataframe(worksheet, combined_df)
    print(f"✅ Data exported to sheet '{sheet_name}' at: {spreadsheet_url}")

def generate_full_deal_invoice_data(df_dict, num_days=30, alpha="ksk", max_records=100):
    # Figure out start_date from existing 'workbook' sheet if present
    if "workbook" in df_dict and not df_dict["workbook"].empty and "closedate" in df_dict["workbook"].columns:
        df_wb = df_dict["workbook"].copy()
        # convert closedate to datetime if it isn't already
        df_wb["closedate"] = pd.to_datetime(df_wb["closedate"], errors="coerce")
        start_date = df_wb["closedate"].max() + timedelta(days=1)
    else:
        start_date = datetime.today()

    # Build combinations from 'deals' sheet
    if "deals" not in df_dict:
        raise ValueError("Sheet 'deals' not found—required for combinations.")
    deals_df = df_dict["deals"]
    for col in ["dealstage", "Product Id", "deal_name", "account_executive"]:
        if col not in deals_df.columns:
            raise ValueError(f"Column '{col}' missing in 'deals' sheet.")
    all_combinations = list(product(
        deals_df["dealstage"].dropna().unique(),
        deals_df["Product Id"].dropna().unique(),
        deals_df["deal_name"].dropna().unique(),
        deals_df["account_executive"].dropna().unique()
    ))

    data = []
    record_count = 0
    for i in range(num_days):
        if record_count >= max_records:
            break
        closedate = start_date + timedelta(days=i)
        deal_date_str = closedate.strftime("%Y%m%d")

        n_samples = random.randint(5, min(20, len(all_combinations)))
        sampled_combinations = random.sample(all_combinations, n_samples)

        for (stage, product, deal, ae) in sampled_combinations:
            if record_count >= max_records:
                break
            deal_id = f"{alpha}{deal_date_str}{random.randint(1000, 9999)}"
            num_invoices = random.randint(1, 2)

            for inv_num in range(1, num_invoices + 1):
                if record_count >= max_records:
                    break
                invoice_id = f"{deal_id}_INV{inv_num}"
                num_lines = random.randint(1, 3)

                for line_number in range(1, num_lines + 1):
                    if record_count >= max_records:
                        break
                    invoice_date = closedate + timedelta(days=30)
                    paid_on_date = closedate + timedelta(days=60)
                    unique_line_id = f"{invoice_id}line{line_number}"

                    data.append({
                        "closedate": closedate.date(),
                        "dealstage": stage,
                        "deal_id": deal_id,
                        "Product Id": product,
                        "deal_name": deal,
                        "account_executive": ae,
                        "invoice_id": invoice_id,
                        "line_item_id": int(line_number),
                        "invoice line item amount": random.randint(1000, 99999),
                        "invoice date": invoice_date.date(),
                        "paid on date": paid_on_date.date(),
                        "unique_identifier": unique_line_id,
                    })
                    record_count += 1

    return pd.DataFrame(data)

# ---------- MAIN ----------
def main():
    spreadsheet = gc.open_by_url(SPREADSHEET_URL)
    df_dict = read_all_sheets_as_dfs(spreadsheet)
    new_df = generate_full_deal_invoice_data(df_dict)
    append_df_to_gsheet_by_url(
        spreadsheet_url=SPREADSHEET_URL,
        sheet_name=TARGET_SHEET,
        new_df=new_df,
        # unique_key="deal_id",  # optional de-dup
    )

if __name__ == "__main__":
    main()
