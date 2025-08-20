from google.colab import auth
auth.authenticate_user()
import pandas as pd
import random
import gspread
from google.auth import default
from gspread_dataframe import set_with_dataframe

from datetime import datetime, timedelta
from itertools import product



creds, _ = default()

gc = gspread.authorize(creds)

spreadsheet = gc.open_by_url("https://docs.google.com/spreadsheets/d/1xBQg2Fpe7P9ff4jtSDl63g_8rxSz5GxSc1hMIopq4a4/edit?gid=1793053955#gid=1793053955")
def read_all_sheets_as_dfs(spreadsheet):
    """
    Reads all worksheets in a spreadsheet and returns a dictionary with sheet titles as keys
    and DataFrames as values.
    """
    sheet_dict = {}
    worksheets = spreadsheet.worksheets()

    for sheet in worksheets:
        data = sheet.get_all_records()
        df = pd.DataFrame(data)
        sheet_dict[sheet.title] = df

    return sheet_dict


df_dict = read_all_sheets_as_dfs(spreadsheet)

def generate_full_deal_invoice_data(num_days=30, alpha="ksk", max_records=100,df_dict=dict()):

    if len(df_dict)!=0:
      df = df_dict['workbook'].copy()
      start_date = pd.to_datetime(df['closedate']).max()+timedelta(days=1)

    else :
        start_date = datetime.today()

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

                # Random number of lines per invoice: 1–3
                num_lines = random.randint(1, 3)

                for line_number in range(1, num_lines + 1):
                    if record_count >= max_records:
                        break

                    invoice_date = closedate + timedelta(days=30)
                    paid_on_date = closedate + timedelta(days=60)
                    unique_line_id = f"{invoice_id}line{line_number}"

                    data.append({
                        'closedate': closedate.date(),
                        'dealstage': stage,
                        'deal_id': deal_id,
                        'Product Id': product,
                        'deal_name': deal,
                        'account_executive': ae,
                        'invoice_id': invoice_id,
                        'line_item_id': line_number,  # INTEGER
                        'invoice line item amount': random.randint(1000, 99999),
                        'invoice date': invoice_date.date(),
                        'paid on date': paid_on_date.date(),
                        'unique_identifier': unique_line_id
                    })

                    record_count += 1

    return pd.DataFrame(data)
  def append_df_to_gsheet_by_url(spreadsheet_url, sheet_name, new_df, unique_key=None):
    """
    Appends a DataFrame to a specific worksheet in a Google Sheet opened by URL.

    If the sheet exists, appends to it (and optionally removes duplicates based on unique_key).
    If the sheet does not exist, it creates one and writes the data.

    Args:
        spreadsheet_url (str): Full Google Sheet URL
        sheet_name (str): Name of the worksheet (tab) to write to
        new_df (pd.DataFrame): DataFrame to append
        unique_key (str, optional): Column name to deduplicate on (e.g., 'deal_id')

    Returns:
        None
    """
    import gspread
    from gspread_dataframe import get_as_dataframe, set_with_dataframe
    from google.colab import auth
    from google.auth import default

    # Authenticate if needed
    auth.authenticate_user()
    creds, _ = default()
    gc = gspread.authorize(creds)

    # Open spreadsheet
    spreadsheet = gc.open_by_url(spreadsheet_url)

    try:
        worksheet = spreadsheet.worksheet(sheet_name)
        existing_df = get_as_dataframe(worksheet).dropna(how='all')

        # Combine and optionally drop duplicates
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        if unique_key:
            combined_df.drop_duplicates(subset=unique_key, keep='last', inplace=True)

        worksheet.clear()

    except gspread.exceptions.WorksheetNotFound:
        # If worksheet doesn't exist, create it
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows="1000", cols="20")
        combined_df = new_df

    # Write to sheet
    set_with_dataframe(worksheet, combined_df)
    print(f"✅ Data exported to sheet '{sheet_name}' at: {spreadsheet_url}")
# Define all_combinations based on the data in df_dict
deals_df = df_dict['deals']
all_combinations = list(product(
    deals_df['dealstage'].unique(),
    deals_df['Product Id'].unique(),
    deals_df['deal_name'].unique(),
    deals_df['account_executive'].unique()
))
append_df_to_gsheet_by_url(
    spreadsheet_url="https://docs.google.com/spreadsheets/d/1xBQg2Fpe7P9ff4jtSDl63g_8rxSz5GxSc1hMIopq4a4/edit#gid=0",
    sheet_name="workbook",
    new_df=generate_full_deal_invoice_data()
    #unique_key="deal_id"  # Optional: deduplicates on deal_id
)
