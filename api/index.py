# api/index.py
import os # Ensure os is imported at the top
from flask import Flask, request, render_template, redirect, url_for, send_file, flash, session
from werkzeug.utils import secure_filename
import pandas as pd
from datetime import datetime
import warnings
import numpy as np
import glob
import shutil
import tempfile
import re

warnings.filterwarnings('ignore')

# This will be your main application instance.
# We'll import everything from your original app.py into this file
# or structure it such that your app.py itself becomes the 'api/index.py'.
# For simplicity, let's assume your main Flask app definition is
# what you want to expose.

# --- Start of your app.py content ---

# --- ADD THESE LINES HERE ---
# Explicitly tell Flask where to find static and templates
# os.path.dirname(__file__) gives the directory of the current file (api/index.py)
# The templates and static folders are at the project root,
# so we need to go up one level (..)
template_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'templates')
static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'static')
# --- END ADDITION ---

# --- MODIFY THIS LINE ---
# Original: app = Flask(__name__)
app = Flask(__name__, template_folder=template_dir, static_folder=static_dir)
# --- END MODIFICATION ---

# IMPORTANT: DO NOT expose your secret key directly in code for production.
# Use environment variables for Vercel.
app.secret_key = os.environ.get('FLASK_SECRET_KEY', 'default_secret_key_for_dev_only')

# Vercel's serverless functions have a writable /tmp directory
# which is cleaned up after each invocation.
# We will not create fixed UPLOAD_FOLDER or DOWNLOAD_FOLDER
# as these should be managed within the temporary directory for each request.
# For temporary file operations, we will rely on tempfile module.

# Define consolidated_columns globally
CONSOLIDATED_OUTPUT_COLUMNS = [
    'Barcode', 'Processor', 'Channel', 'Category', 'Company code', 'Region',
    'Vendor number', 'Vendor Name', 'Status', 'Received Date', 'Re-Open Date',
    'Allocation Date', 'Clarification Date', 'Completion Date', 'Requester',
    'Remarks', 'Aging', 'Today'
]

def format_date_to_mdyyyy(date_series):
    """
    Formats a pandas Series of dates to MM/DD/YYYY string format.
    Handles potential mixed types and NaT values.
    """
    datetime_series = pd.to_datetime(date_series, errors='coerce')
    formatted_series = datetime_series.apply(
        lambda x: f"{x.month}/{x.day}/{x.year}" if pd.notna(x) else ''
    )
    return formatted_series

def clean_column_names(df):
    """
    Cleans DataFrame column names by:
    1. Lowercasing all characters.
    2. Replacing spaces with underscores.
    3. Removing special characters (keeping only alphanumeric and underscores).
    4. Removing leading/trailing underscores.
    """
    new_columns = []
    for col in df.columns:
        col = str(col).strip().lower()
        col = re.sub(r'\s+', '_', col)
        col = re.sub(r'[^a-z0-9_]', '', col)
        col = col.strip('_')
        new_columns.append(col)
    df.columns = new_columns
    return df

def consolidate_data_process(df_pisa, df_esm, df_pm7, consolidated_output_file_path):
    """
    Reads PISA, ESM, and PM7 Excel files (now passed as DFs), filters PISA, consolidates data,
    and saves it to a new Excel file.
    """
    print("Starting data consolidation process...")
    print("All input DataFrames loaded successfully!")

    # Always work with copies to avoid modifying original DFs passed in
    df_pisa = clean_column_names(df_pisa.copy())
    df_esm = clean_column_names(df_esm.copy())
    df_pm7 = clean_column_names(df_pm7.copy())

    allowed_pisa_users = ["Goswami Sonali", "Patil Jayapal Gowd", "Ranganath Chilamakuri","Sridhar Divya","Sunitha S","Varunkumar N"]
    if 'assigned_user' in df_pisa.columns:
        original_pisa_count = len(df_pisa)
        df_pisa_filtered = df_pisa[df_pisa['assigned_user'].isin(allowed_pisa_users)].copy()
        print(f"\nPISA file filtered. Original records: {original_pisa_count}, Records after filter: {len(df_pisa_filtered)}")
    else:
        print("\nWarning: 'assigned_user' column not found in PISA file (after cleaning). No filter applied.")
        df_pisa_filtered = df_pisa.copy()

    all_consolidated_rows = []
    today_date = datetime.now()

    # --- PISA Processing ---
    if 'document_id' not in df_pisa_filtered.columns:
        print("Error: 'document_id' column not found in PISA file (after cleaning). Skipping PISA processing.")
    else:
        # Rename 'document_id' to 'Barcode' within the PISA DataFrame for consistency during merge
        df_pisa_filtered['barcode'] = df_pisa_filtered['document_id'].astype(str)
        
        for index, row in df_pisa_filtered.iterrows():
            new_row = {
                'Barcode': row['barcode'], # Use the newly created 'barcode' column
                'Company code': row.get('company_code'),
                'Vendor number': row.get('vendor_number'),
                'Received Date': row.get('received_date'),
                'Completion Date': None, 'Status': None , 'Today': today_date, 'Channel': 'PISA',
                'Vendor Name': row.get('vendor'),
                'Re-Open Date': None, 'Allocation Date': None,
                'Requester': None, 'Clarification Date': None, 'Aging': None, 'Remarks': None,
                'Region': None, # Keep Region here, it will be populated later
                'Processor': None, 'Category': None # Added these
            }
            all_consolidated_rows.append(new_row)
        print(f"Collected {len(df_pisa_filtered)} rows from PISA.")

    # --- ESM Processing ---
    if 'number' not in df_esm.columns:
        print("Error: 'number' column not found in ESM file (after cleaning). Skipping ESM processing.")
    else:
        # Rename 'number' to 'Barcode' within the ESM DataFrame for consistency during merge
        df_esm['barcode'] = df_esm['number'].astype(str)

        for index, row in df_esm.iterrows():
            new_row = {
                'Barcode': row['barcode'], # Use the newly created 'barcode' column
                'Received Date': row.get('received_date'),
                'Status': row.get('state'),
                'Requester': row.get('opened_by'),
                'Completion Date': row.get('closed') if pd.notna(row.get('closed')) else None,
                'Re-Open Date': row.get('updated') if (row.get('state') or '').lower() == 'reopened' else None,
                'Today': today_date, 'Remarks': row.get('short_description'),
                'Channel': 'ESM', 'Company code': None,'Vendor Name': None,
                'Vendor number': None, 'Allocation Date': None,
                'Clarification Date': None, 'Aging': None,
                'Region': None, # Keep Region here, it will be populated later
                'Processor': None, 'Category': None # Added these
            }
            all_consolidated_rows.append(new_row)
        print(f"Collected {len(df_esm)} rows from ESM.")

    # --- PM7 Processing ---
    if 'barcode' not in df_pm7.columns:
        print("Error: 'barcode' column not found in PM7 file (after cleaning). Skipping PM7 processing.")
    else:
        # PM7 already has 'barcode', so just ensure it's string
        df_pm7['barcode'] = df_pm7['barcode'].astype(str)

        for index, row in df_pm7.iterrows():
            new_row = {
                'Barcode': row['barcode'], # Already named 'barcode'
                'Vendor Name': row.get('vendor_name'),
                'Vendor number': row.get('vendor_number'),
                'Received Date': row.get('received_date'),
                'Status': row.get('task'),
                'Today': today_date,
                'Channel': 'PM7',
                'Company code': row.get('co_code'),
                'Re-Open Date': None,
                'Allocation Date': None, 'Completion Date': None, 'Requester': None,
                'Clarification Date': None, 'Aging': None, 'Remarks': None,
                'Region': None, # Keep Region here, it will be populated later
                'Processor': None, 'Category': None # Added these
            }
            all_consolidated_rows.append(new_row)
        print(f"Collected {len(df_pm7)} rows from PM7.")

    if not all_consolidated_rows:
        return False, "No data collected for consolidation."

    df_consolidated = pd.DataFrame(all_consolidated_rows)
   
    # Ensure all columns from CONSOLIDATED_OUTPUT_COLUMNS are present
    # and fill missing ones with None
    for col in CONSOLIDATED_OUTPUT_COLUMNS:
        if col not in df_consolidated.columns:
            df_consolidated[col] = None
   
    # Reorder to match CONSOLIDATED_OUTPUT_COLUMNS
    df_consolidated = df_consolidated[CONSOLIDATED_OUTPUT_COLUMNS]

    date_cols_to_process = ['Received Date', 'Re-Open Date', 'Allocation Date', 'Completion Date', 'Clarification Date', 'Today']
    for col in df_consolidated.columns:
        if col in date_cols_to_process:
            df_consolidated[col] = format_date_to_mdyyyy(df_consolidated[col])
        else:
            if df_consolidated[col].dtype == 'object':
                df_consolidated[col] = df_consolidated[col].fillna('')
            elif col in ['Barcode', 'Company code', 'Vendor number']: # Ensure these are treated as strings
                df_consolidated[col] = df_consolidated[col].astype(str).replace('nan', '')

    try:
        df_consolidated.to_excel(consolidated_output_file_path, index=False)
        print(f"Consolidated file saved to: {consolidated_output_file_path}")
    except Exception as e:
        return False, f"Error saving consolidated file: {e}"
    print("--- Consolidated Data Process Complete ---")
    return True, df_consolidated

def process_central_file_step2_update_existing(consolidated_df, central_file_input_path): # Removed output_path
    """
    Step 2: Updates status of *existing* central file records based on consolidated data.
    """
    print(f"\n--- Starting Central File Status Processing (Step 2: Update Existing Barcodes) ---")
 
    try:
        converters = {'Barcode': str, 'Vendor number': str, 'Company code': str}
        df_central = pd.read_excel(central_file_input_path, converters=converters, keep_default_na=False)
        df_central_cleaned = clean_column_names(df_central.copy())
 
        print("Consolidated (DF) and Central (file) loaded successfully for Step 2!")
    except Exception as e:
        return False, f"Error loading Consolidated (DF) or Central (file) for processing (Step 2): {e}"
 
    if 'Barcode' not in consolidated_df.columns:
        return False, "Error: 'Barcode' column not found in the consolidated file. Cannot proceed with central file processing (Step 2)."
    if 'barcode' not in df_central_cleaned.columns or 'status' not in df_central_cleaned.columns:
        return False, "Error: 'barcode' or 'status' column not found in the central file after cleaning. Cannot update status (Step 2)."
 
    consolidated_df['Barcode'] = consolidated_df['Barcode'].astype(str)
    df_central_cleaned['barcode'] = df_central_cleaned['barcode'].astype(str)
 
    df_central_cleaned['Barcode_compare'] = df_central_cleaned['barcode']
 
    consolidated_barcodes_set = set(consolidated_df['Barcode'].unique())
    print(f"Found {len(consolidated_barcodes_set)} unique barcodes in the consolidated file for Step 2.")
 
    def transform_status_if_barcode_exists(row):
        central_barcode = str(row['Barcode_compare'])
        original_central_status = row['status']
 
        if central_barcode in consolidated_barcodes_set:
            if pd.isna(original_central_status) or \
               (isinstance(original_central_status, str) and original_central_status.strip().lower() in ['', 'n/a', 'na', 'none']):
                return original_central_status
 
            status_str = str(original_central_status).strip().lower()
            if status_str == 'new':
                return 'Untouched'
            elif status_str == 'completed':
                return 'Reopen'
            elif status_str == 'n/a':
                return 'New'
            else:
                return original_central_status
        else:
            return original_central_status
 
    df_central_cleaned['status'] = df_central_cleaned.apply(transform_status_if_barcode_exists, axis=1)
    df_central_cleaned = df_central_cleaned.drop(columns=['Barcode_compare'])
 
    print(f"Updated 'status' column in central file for Step 2 for {len(df_central_cleaned)} records.")
 
    try:
        # We will use CONSOLIDATED_OUTPUT_COLUMNS to rename, but will take all existing columns first
        # to prevent dropping any original columns before reordering at the end of step 3
        # Ensure that any non-standard columns from the central file are also carried over
        existing_cols = df_central_cleaned.columns.tolist()
       
        common_cols_map = {
            'barcode': 'Barcode', 'channel': 'Channel', 'company_code': 'Company code',
            'vendor_name': 'Vendor Name', 'vendor_number': 'Vendor number',
            'received_date': 'Received Date', 're_open_date': 'Re-Open Date',
            'allocation_date': 'Allocation Date', 'completion_date': 'Completion Date',
            'requester': 'Requester', 'clarification_date': 'Clarification Date',
            'aging': 'Aging', 'today': 'Today', 'status': 'Status', 'remarks': 'Remarks',
            'region': 'Region', 'processor': 'Processor', 'category': 'Category' # Added these
        }
 
        cols_to_rename = {k: v for k, v in common_cols_map.items() if k in df_central_cleaned.columns}
        df_central_cleaned.rename(columns=cols_to_rename, inplace=True)
 
        date_cols_in_central_file = [
            'Received Date', 'Re-Open Date', 'Allocation Date',
            'Completion Date', 'Clarification Date', 'Today'
        ]
        for col in df_central_cleaned.columns:
            if col in date_cols_in_central_file:
                df_central_cleaned[col] = format_date_to_mdyyyy(df_central_cleaned[col])
            elif df_central_cleaned[col].dtype == 'object': # Apply to all object columns except region, company code etc
                df_central_cleaned[col] = df_central_cleaned[col].fillna('')
            elif col in ['Barcode', 'Vendor number']: # Company code handled later for truncation
                df_central_cleaned[col] = df_central_cleaned[col].astype(str).replace('nan', '')
            # If 'Company code' is here, it will be processed later in step 3 for truncation
            # No specific handling for 'Company code' at this stage of type conversion unless it needs general string conversion
            if col == 'Company code':
                 df_central_cleaned[col] = df_central_cleaned[col].astype(str).replace('nan', '')
 
        for col in CONSOLIDATED_OUTPUT_COLUMNS:
            if col not in df_central_cleaned.columns:
                df_central_cleaned[col] = None # Add missing columns from desired output to central
 
       
    except Exception as e:
        return False, f"Error processing central file (after Step 2): {e}"
    print(f"--- Central File Status Processing (Step 2) Complete ---")
    return True, df_central_cleaned
 
 
def process_central_file_step3_final_merge_and_needs_review(consolidated_df, updated_existing_central_df, final_central_output_file_path, df_pisa_original, df_esm_original, df_pm7_original, region_mapping_df):
    """
    Step 3: Handles barcodes present only in consolidated (adds them as new)
            and barcodes present only in central (marks them as 'Needs Review' if not 'Completed').
            Also performs region mapping and final column reordering.
    """
    print(f"\n--- Starting Central File Status Processing (Step 3: Final Merge & Needs Review) ---")
 
    df_pisa_lookup = clean_column_names(df_pisa_original.copy())
    df_esm_lookup = clean_column_names(df_esm_original.copy())
    df_pm7_lookup = clean_column_names(df_pm7_original.copy())
 
    df_pisa_indexed = pd.DataFrame()
    if 'document_id' in df_pisa_lookup.columns:
        df_pisa_lookup['document_id'] = df_pisa_lookup['document_id'].astype(str)
        df_pisa_indexed = df_pisa_lookup.set_index('document_id')
    else:
        print("Warning: 'document_id' column not found in PISA lookup. Cannot perform PISA lookups.")
 
    df_esm_indexed = pd.DataFrame()
    if 'number' in df_esm_lookup.columns:
        df_esm_lookup['number'] = df_esm_lookup['number'].astype(str)
        df_esm_indexed = df_esm_lookup.set_index('number')
    else:
        print("Warning: 'number' column not found in ESM lookup. Cannot perform ESM lookups.")
 
    df_pm7_indexed = pd.DataFrame()
    if 'barcode' in df_pm7_lookup.columns:
        df_pm7_lookup['barcode'] = df_pm7_lookup['barcode'].astype(str)
        df_pm7_indexed = df_pm7_lookup.set_index('barcode')
    else:
        print("Warning: 'barcode' column not found in PM7 lookup. Cannot perform PM7 lookups.")
 
    if 'Barcode' not in consolidated_df.columns:
        return False, "Error: 'Barcode' column not found in the consolidated file. Cannot proceed with final central file processing (Step 3)."
    if 'Barcode' not in updated_existing_central_df.columns or 'Status' not in updated_existing_central_df.columns:
        return False, "Error: 'Barcode' or 'Status' column not found in the updated central file. Cannot update status (Step 3)."
 
    consolidated_barcodes_set = set(consolidated_df['Barcode'].unique())
    central_barcodes_set = set(updated_existing_central_df['Barcode'].unique())
   
    barcodes_to_add = consolidated_barcodes_set - central_barcodes_set
    print(f"Found {len(barcodes_to_add)} new barcodes in consolidated file to add to central.")
 
    df_new_records_from_consolidated = consolidated_df[consolidated_df['Barcode'].isin(barcodes_to_add)].copy()
 
    all_new_central_rows_data = []
 
    for index, row_consolidated in df_new_records_from_consolidated.iterrows():
        barcode = row_consolidated['Barcode']
        channel = row_consolidated['Channel']
 
        vendor_name = row_consolidated.get('Vendor Name')
        vendor_number = row_consolidated.get('Vendor number')
        company_code = row_consolidated.get('Company code')
        received_date = row_consolidated.get('Received Date')
        processor = row_consolidated.get('Processor')
        category = row_consolidated.get('Category')
 
        if channel == 'PISA' and not df_pisa_indexed.empty and barcode in df_pisa_indexed.index:
            pisa_row = df_pisa_indexed.loc[barcode]
            vendor_name = pisa_row.get('vendor') if pisa_row.get('vendor') else vendor_name
            vendor_number = pisa_row.get('vendor_number') if pisa_row.get('vendor_number') else vendor_number
            company_code = pisa_row.get('company_code') if pisa_row.get('company_code') else company_code # Use 'cocd' from PISA for company code
           
        elif channel == 'ESM' and not df_esm_indexed.empty and barcode in df_esm_indexed.index:
            esm_row = df_esm_indexed.loc[barcode]
            company_code = esm_row.get('company_code') if esm_row.get('company_code') else company_code
            category = esm_row.get('subcategory') if esm_row.get('subcategory') else category
 
        elif channel == 'PM7' and not df_pm7_indexed.empty and barcode in df_pm7_indexed.index:
            pm7_row = df_pm7_indexed.loc[barcode]
            vendor_name = pm7_row.get('vendor_name') if pm7_row.get('vendor_name') else vendor_name
            vendor_number = pm7_row.get('vendor_number') if pm7_row.get('vendor_number') else vendor_number
            company_code = pm7_row.get('company_code') if pm7_row.get('company_code') else company_code # Use 'co_code' from PM7 for company code
 
        new_central_row_data = row_consolidated.to_dict()
        new_central_row_data['Vendor Name'] = vendor_name if vendor_name is not None else ''
        new_central_row_data['Vendor number'] = vendor_number if vendor_number is not None else ''
        new_central_row_data['Company code'] = company_code if company_code is not None else ''
        new_central_row_data['Received Date'] = received_date
        new_central_row_data['Status'] = 'New'
        # Use strftime("%m/%d/%Y") for consistency across OS, Vercel is Linux
        new_central_row_data['Allocation Date'] = datetime.now().strftime("%m/%d/%Y")
        new_central_row_data['Processor'] = processor if processor is not None else '' # Update Processor
        new_central_row_data['Category'] = category if category is not None else ''   # Update Category
 
        all_new_central_rows_data.append(new_central_row_data)
 
    if all_new_central_rows_data:
        df_new_central_rows = pd.DataFrame(all_new_central_rows_data)
        # Ensure new rows have all expected columns from CONSOLIDATED_OUTPUT_COLUMNS
        for col in CONSOLIDATED_OUTPUT_COLUMNS:
            if col not in df_new_central_rows.columns:
                df_new_central_rows[col] = None
        df_new_central_rows = df_new_central_rows[CONSOLIDATED_OUTPUT_COLUMNS] # Reorder immediately
    else:
        df_new_central_rows = pd.DataFrame(columns=CONSOLIDATED_OUTPUT_COLUMNS) # Initialize with correct columns
 
    for col in df_new_central_rows.columns:
        if df_new_central_rows[col].dtype == 'object':
            df_new_central_rows[col] = df_new_central_rows[col].fillna('')
        elif col in ['Barcode', 'Company code', 'Vendor number']:
            df_new_central_rows[col] = df_new_central_rows[col].astype(str).replace('nan', '')
 
    barcodes_for_needs_review = central_barcodes_set - consolidated_barcodes_set
    print(f"Found {len(barcodes_for_needs_review)} barcodes in central not in consolidated.")
 
    df_final_central = updated_existing_central_df.copy()
 
    needs_review_barcode_mask = df_final_central['Barcode'].isin(barcodes_for_needs_review)
    is_not_completed_status_mask = ~df_final_central['Status'].astype(str).str.strip().str.lower().eq('completed')
    final_needs_review_condition = needs_review_barcode_mask & is_not_completed_status_mask
 
    df_final_central.loc[final_needs_review_condition, 'Status'] = 'Needs Review'
    print(f"Updated {final_needs_review_condition.sum()} records to 'Needs Review' where status was not 'Completed'.")
 
    # Before concat, ensure df_final_central (updated_existing_central_df) also has all CONSOLIDATED_OUTPUT_COLUMNS
    for col in CONSOLIDATED_OUTPUT_COLUMNS:
        if col not in df_final_central.columns:
            df_final_central[col] = None
    df_final_central = df_final_central[CONSOLIDATED_OUTPUT_COLUMNS] # Reorder to maintain consistency before concat
 
    df_final_central = pd.concat([df_final_central, df_new_central_rows], ignore_index=True)
 
    # --- NEW REGION MAPPING LOGIC ---
    print("\n--- Applying Region Mapping ---")
    if region_mapping_df is None or region_mapping_df.empty:
        print("Warning: Region mapping file not provided or is empty. Region column will not be populated.")
        df_final_central['Region'] = df_final_central['Region'].fillna('') # Ensure it's empty string if no mapping
    else:
        # Clean column names for the mapping DataFrame
        region_mapping_df = clean_column_names(region_mapping_df.copy())
        # Ensure mapping columns exist
        if 'r3_coco' not in region_mapping_df.columns or 'region' not in region_mapping_df.columns:
            print("Error: Region mapping file must contain 'r3_coco' and 'region' columns after cleaning. Skipping region mapping.")
            df_final_central['Region'] = df_final_central['Region'].fillna('')
        else:
            # Create a mapping dictionary: first 4 chars of R/3 CoCo -> Region
            region_map = {}
            for idx, row in region_mapping_df.iterrows():
                # Make sure to use the correct cleaned column name here: 'r3_coco'
                coco_key = str(row['r3_coco']).strip().upper()
                if coco_key: # Ensure it's not empty
                    # Use the first 4 characters for the key, as specified for the mapping
                    region_map[coco_key[:4]] = str(row['region']).strip()
 
            print(f"Loaded {len(region_map)} unique R/3 CoCo -> Region mappings.")
 
            # Apply mapping to the 'Region' column of df_final_central
            if 'Company code' in df_final_central.columns:
                # First, ensure 'Company code' is string type and contains only the first 4 characters
                df_final_central['Company code'] = df_final_central['Company code'].astype(str).str.strip().str.upper().str[:4]
 
                # Now, map directly using the modified 'Company code' column
                df_final_central['Region'] = df_final_central['Company code'].map(region_map).fillna(df_final_central['Region'])
                df_final_central['Region'] = df_final_central['Region'].fillna('') # Then fill any remaining NA with empty string
 
                print("Region mapping applied successfully and 'Company code' truncated to 4 characters.")
            else:
                print("Warning: 'Company code' column not found in final central DataFrame. Cannot apply region mapping.")
                df_final_central['Region'] = df_final_central['Region'].fillna('')
 
    date_cols_in_central_file = [
        'Received Date', 'Re-Open Date', 'Allocation Date',
        'Completion Date', 'Clarification Date', 'Today'
    ]
    for col in df_final_central.columns:
        if col in date_cols_in_central_file:
            df_final_central[col] = format_date_to_mdyyyy(df_final_central[col])
        elif df_final_central[col].dtype == 'object': # Apply to all object columns
            df_final_central[col] = df_final_central[col].fillna('')
        elif col in ['Barcode', 'Vendor number']: # Company code handled earlier
            df_final_central[col] = df_final_central[col].astype(str).replace('nan', '')
 
    # Final reordering of columns to the exact desired specification
    # Ensure all columns in CONSOLIDATED_OUTPUT_COLUMNS are present in df_final_central
    # If any are missing, add them with empty strings.
    for col in CONSOLIDATED_OUTPUT_COLUMNS:
        if col not in df_final_central.columns:
            df_final_central[col] = ''
           
    df_final_central = df_final_central[CONSOLIDATED_OUTPUT_COLUMNS]
   
    try:
        df_final_central.to_excel(final_central_output_file_path, index=False)
        print(f"Final central file (after Step 3) saved to: {final_central_output_file_path}")
        print(f"Total rows in final central file (after Step 3): {len(df_final_central)}")
    except Exception as e:
        return False, f"Error saving final central file (after Step 3): {e}"
    print(f"--- Central File Status Processing (Step 3) Complete ---")
    return True, "Central file processing (Step 3) successful"
 
 
@app.route('/', methods=['GET'])
def index():
    return render_template('index.html')
 
@app.route('/process', methods=['POST'])
def process_files():
    # Use /tmp as the temporary directory for Vercel functions
    temp_dir = tempfile.mkdtemp(dir='/tmp')
 
    # Ensure session variables are cleared at the start of a new process
    session.pop('consolidated_output_path', None)
    session.pop('central_output_path', None)
    session.pop('temp_dir', None)
 
    session['temp_dir'] = temp_dir
 
    # Define the fixed path for the mapping file
    # This path is relative to the *root of the deployed project*
    # For Vercel, the app.py (now api/index.py) will be at the root of its bundle
    # so the company_code_region_mapping.xlsx needs to be relative to the PROJECT ROOT,
    # which means it's one level up from api/index.py if api is at the root.
    # Assuming company_code_region_mapping.xlsx is in the project root:
    REGION_MAPPING_FILE_PATH = os.path.join(os.getcwd(), 'company_code_region_mapping.xlsx')
 
    try:
        uploaded_files = {}
        file_keys = ['pisa_file', 'esm_file', 'pm7_file', 'central_file']
        for key in file_keys:
            if key not in request.files:
                flash(f'Missing file: "{key}". All four files are required.', 'error')
                return redirect(url_for('index'))
            file = request.files[key]
            if file.filename == '':
                flash(f'No selected file for "{key}". All four files are required.', 'error')
                return redirect(url_for('index'))
            if file and file.filename.endswith('.xlsx'):
                filename = secure_filename(file.filename)
                file_path = os.path.join(temp_dir, filename)
                file.save(file_path)
                uploaded_files[key] = file_path
                flash(f'File "{filename}" uploaded successfully.', 'info')
            else:
                flash(f'Invalid file type for "{key}". Please upload an .xlsx file.', 'error')
                return redirect(url_for('index'))
 
        pisa_file_path = uploaded_files['pisa_file']
        esm_file_path = uploaded_files['esm_file']
        pm7_file_path = uploaded_files['pm7_file']
        initial_central_file_input_path = uploaded_files['central_file']
 
        df_pisa_original = None
        df_esm_original = None
        df_pm7_original = None
        df_region_mapping = None
 
        try:
            df_pisa_original = pd.read_excel(pisa_file_path)
            df_esm_original = pd.read_excel(esm_file_path)
            df_pm7_original = pd.read_excel(pm7_file_path)
 
            if os.path.exists(REGION_MAPPING_FILE_PATH):
                df_region_mapping = pd.read_excel(REGION_MAPPING_FILE_PATH)
                print(f"Successfully loaded region mapping file from: {REGION_MAPPING_FILE_PATH}")
            else:
                flash(f"Error: Region mapping file not found at {REGION_MAPPING_FILE_PATH}. Region column will be empty.", 'warning')
                df_region_mapping = pd.DataFrame(columns=['R/3 CoCo', 'Region'])
 
        except Exception as e:
            flash(f"Error loading one or more input Excel files or the region mapping file: {e}. Please ensure all files are valid .xlsx formats and the mapping file exists.", 'error')
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
            session.pop('temp_dir', None)
            return redirect(url_for('index'))
 
 
        today_str = datetime.now().strftime("%d_%m_%Y_%H%M%S")
 
        # --- Step 1: Consolidate Data ---
        consolidated_output_filename = f'ConsolidatedData_{today_str}.xlsx'
        consolidated_output_file_path = os.path.join(temp_dir, consolidated_output_filename)
        success, result = consolidate_data_process(
            df_pisa_original, df_esm_original, df_pm7_original, consolidated_output_file_path
        )
 
        if not success:
            flash(f'Consolidation Error: {result}', 'error')
            if os.path.exists(temp_dir): shutil.rmtree(temp_dir)
            session.pop('temp_dir', None)
            return redirect(url_for('index'))
        df_consolidated = result
        flash('Data consolidation from the sources completed successfully!', 'success')
        # We store this in session, as the download_file route needs it to verify the path,
        # but it will NOT be passed to the render_template explicitly.
        session['consolidated_output_path'] = consolidated_output_file_path
 
        # --- Step 2: Update existing central file records based on consolidation ---
        success, result_df = process_central_file_step2_update_existing(
            df_consolidated, initial_central_file_input_path
        )
        if not success:
            flash(f'Central File Processing (Step 2) Error: {result_df}', 'error')
            if os.path.exists(temp_dir): shutil.rmtree(temp_dir)
            session.pop('temp_dir', None)
            return redirect(url_for('index'))
        df_central_updated_existing = result_df
 
        # --- Step 3: Final Merge (Add new barcodes, mark 'Needs Review', and apply Region Mapping) ---
        final_central_output_filename = f'CentralFile_FinalOutput_{today_str}.xlsx'
        final_central_output_file_path = os.path.join(temp_dir, final_central_output_filename)
        success, message = process_central_file_step3_final_merge_and_needs_review(
            df_consolidated, df_central_updated_existing, final_central_output_file_path,
            df_pisa_original, df_esm_original, df_pm7_original, df_region_mapping
        )
        if not success:
            flash(f'Central File Processing (Step 3) Error: {message}', 'error')
            if os.path.exists(temp_dir): shutil.rmtree(temp_dir)
            session.pop('temp_dir', None)
            return redirect(url_for('index'))
        flash('Central file finalized successfully!', 'success')
        session['central_output_path'] = final_central_output_file_path
 
        return render_template('index.html',
                                central_download_link=url_for('download_file', filename=os.path.basename(final_central_output_file_path))
                              )
 
    except Exception as e:
        flash(f'An unhandled error occurred during processing: {e}', 'error')
        import traceback
        traceback.print_exc()
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        session.pop('temp_dir', None)
        return redirect(url_for('index'))
    finally:
        # Ensure cleanup happens even if there's an exception (but after flash messages)
        pass # The cleanup_session route handles this when the user is done.
             # For Vercel, the /tmp directory is cleared for each new function invocation.
             # However, explicit cleanup after sending the file is good practice for local/stateful servers.
 
 
@app.route('/download/<filename>', methods=['GET'])
def download_file(filename):
    file_path_in_temp = None
    temp_dir = session.get('temp_dir')
 
    print(f"DEBUG: Download requested for filename: {filename}")
    print(f"DEBUG: Session temp_dir: {temp_dir}")
    print(f"DEBUG: Consolidated output path in session: {session.get('consolidated_output_path')}")
    print(f"DEBUG: Central output path in session: {session.get('central_output_path')}")
 
    if not temp_dir:
        print("DEBUG: temp_dir not found in session.")
        flash('File not found for download or session expired. Please re-run the process.', 'error')
        return redirect(url_for('index'))
 
    # This part of download_file route still needs to be able to find either file
    # in case someone tries to download the consolidated file by direct URL.
    # But crucially, the link to it won't be displayed on the page.
    if session.get('consolidated_output_path') and os.path.basename(session['consolidated_output_path']) == filename:
        file_path_in_temp = session['consolidated_output_path']
        print(f"DEBUG: Matched consolidated file. Full path: {file_path_in_temp}")
    elif session.get('central_output_path') and os.path.basename(session['central_output_path']) == filename:
        file_path_in_temp = session['central_output_path']
        print(f"DEBUG: Matched final central file. Full path: {file_path_in_temp}")
    else:
        print(f"DEBUG: Filename '{filename}' not found or session data missing/expired. Full path attempted: {file_path_in_temp}")
        flash('File not found for download or session expired. Please re-run the process.', 'error')
        return redirect(url_for('index'))
 
    if file_path_in_temp and os.path.exists(file_path_in_temp):
        print(f"DEBUG: File '{file_path_in_temp}' exists. Attempting to send.")
        try:
            response = send_file(
                file_path_in_temp,
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                as_attachment=True,
                download_name=filename
            )
            return response
        except Exception as e:
            print(f"ERROR: Exception while sending file '{file_path_in_temp}': {e}")
            flash(f'Error providing download: {e}. Please try again.', 'error')
            return redirect(url_for('index'))
    else:
        print(f"DEBUG: File '{filename}' not found or session data missing/expired. Full path attempted: {file_path_in_temp}")
        flash('File not found for download or session expired. Please re-run the process.', 'error')
        return redirect(url_for('index'))
 
@app.route('/cleanup_session', methods=['GET'])
def cleanup_session():
    temp_dir = session.get('temp_dir')
    if temp_dir and os.path.exists(temp_dir):
        try:
            shutil.rmtree(temp_dir)
            print(f"DEBUG: Cleaned up temporary directory: {temp_dir}")
            flash('Temporary files cleaned up.', 'info')
        except OSError as e:
            print(f"ERROR: Error removing temporary directory {temp_dir}: {e}")
            flash(f'Error cleaning up temporary files: {e}', 'error')
    session.pop('temp_dir', None)
    session.pop('consolidated_output_path', None)
    session.pop('central_output_path', None)
    return redirect(url_for('index'))
 
# This block is for local development only and will not run on Vercel
if __name__ == '__main__':
    app.run(debug=True)

# --- End of your app.py content --- 
