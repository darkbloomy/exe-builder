import numpy as np
import pandas as pd
from datetime import datetime as dt
import re
import sys
import os

def validate_file_names(last_week_file, current_week_file):
    """
    Validate that file names follow the pattern 'Sick unapproved_Week xx'
    """
    pattern = r'^Sick unapproved_Week \d+(?:\+\d+)?\.xlsx$'
    
    if not re.match(pattern, last_week_file):
        print(f"Error: Last week file name '{last_week_file}' does not match required pattern.")
        print("Please unify file name to 'Sick unapproved_Week xx.xlsx'")
        return False
    
    if not re.match(pattern, current_week_file):
        print(f"Error: Current week file name '{current_week_file}' does not match required pattern.")
        print("Please unify file name to 'Sick unapproved_Week xx.xlsx'")
        return False
    
    return True

def parse_dates(date_str):
    """Parse dates from various formats"""
    for fmt in ('%Y-%m-%d', '%d.%m.%Y', '%m-%d-%Y', '%m/%d/%Y', '%d/%m/%Y'):
        try:
            return pd.to_datetime(date_str, format=fmt)
        except ValueError:
            continue
    return pd.NaT

def decision(row):
    """Apply business logic for categorization"""
    today = pd.Timestamp.today().normalize()
    
    # Ensure date columns are in datetime format
    row['EndDate'] = pd.to_datetime(row['EndDate'], errors='coerce')
    row['AU bis'] = pd.to_datetime(row['AU bis'], errors='coerce')
    row['Änderung möglich bis'] = pd.to_datetime(row['Änderung möglich bis'], errors='coerce')
    row['StartDate'] = pd.to_datetime(row['StartDate'], errors='coerce')
    row['AU seit'] = pd.to_datetime(row['AU seit'], errors='coerce')
    
    if row['Status Übernahme Fehlzeit'] in ('Fehlzeit bereits vorhanden', 'Ende der AU in passender Fehlzeit korrigiert', 'Ende der AU in vorheriger Fehlzeit korrigiert'):
        if row['StartDate'] >= row['AU seit'] and row['EndDate'] <= row['AU bis']:
            return 'DE GIG Sick Leave eAU-Approved'
        elif pd.isnull(row['AU seit']) and row['EndDate'] <= row['AU bis']: 
            return 'DE GIG Sick Leave eAU-Approved'
        elif row['StartDate'] < row['AU seit']:
            return 'DE GIG Sick Leave eAU-Rejected'
        elif row['EndDate'] > row['AU bis']:
            return 'DE GIG Sick Leave eAU-Rejected'
        else:
            return 'To Verify'
    elif pd.isnull(row['Status Übernahme Fehlzeit']) and row['Meldung KK/DATEV'] == 'AU':
        if row['StartDate'] >= row['AU seit'] and row['EndDate'] <= row['AU bis']:
            return 'DE GIG Sick Leave eAU-Approved'
        elif pd.isnull(row['AU seit']) and row['EndDate'] <= row['AU bis']: 
            return 'DE GIG Sick Leave eAU-Approved'
        elif row['StartDate'] < row['AU seit']:
            return 'DE GIG Sick Leave eAU-Rejected'
        elif row['EndDate'] > row['AU bis']:
            return 'DE GIG Sick Leave eAU-Rejected'
        else:
            return 'Pending Reply'
    elif row['Status Übernahme Fehlzeit'] == 'keine AU':
        if row['Änderung möglich bis'] >= today:
            return 'Pending Reply'
        elif row['Änderung möglich bis'] < today:
            return 'DE GIG Sick Leave eAU-Rejected'
        elif pd.isnull(row['Änderung möglich bis']):
            if row['Meldung KK/DATEV'] == 'AU' or pd.isnull(row['Meldung KK/DATEV']):
                return 'DE GIG Sick Leave eAU-Rejected'
            elif row['Meldung KK/DATEV'] != 'AU':
                return 'To Verify'
    elif row['Status Übernahme Fehlzeit'] == 'AU nicht übernommen (zeitl. Überschneidung)':
        if pd.isnull(row['AU seit']) and row['EndDate'] <= row['AU bis']: 
            return 'DE GIG Sick Leave eAU-Approved'
        elif pd.notnull(row['AU seit']) and row['StartDate'] >= row['AU seit'] and row['EndDate'] <= row['AU bis']:
            return 'DE GIG Sick Leave eAU-Approved'
        elif row['EndDate'] > row['AU bis']:
            return 'DE GIG Sick Leave eAU-Rejected'
        else:
            return 'To Verify'
    elif row['Status Übernahme Fehlzeit'] in ('AU in Fehlzeit übernommen', 'AU nicht übernommen (nicht eAU-relevanter Grund)', 'Folgebescheinigung ohne Erstbescheinigung'):
        return 'To Verify'
    elif pd.isnull(row['Status Übernahme Fehlzeit']) or row['Status Übernahme Fehlzeit'] =='':
        if row['Meldung KK/DATEV'] in ('stat. Aufenthalt', 'anderer Nachweis liegt vor', 'unzuständige KK', 'Fehler'):
            return 'To Verify'
        else:
            return 'Pending Reply'
    elif row['Meldung KK/DATEV'] != 'AU':
        return 'To Verify'
    elif pd.isnull(row['AU seit']) and row['EndDate'] <= row['AU bis']: 
        return 'DE GIG Sick Leave eAU-Approved'
    elif pd.notnull(row['AU seit']) and row['StartDate'] >= row['AU seit'] and row['EndDate'] <= row['AU bis']:
        return 'DE GIG Sick Leave eAU-Approved'
    elif row['EndDate'] > row['AU bis']:
        return 'DE GIG Sick Leave eAU-Rejected'
    return 'Unknown'

def convert_dates_to_text(df, date_columns):
    """Convert specified date columns to text format with apostrophes"""
    for col in date_columns:
        if col in df.columns:
            df[col] = "'"+ df[col]
    return df
def has_approved_and_cancelled(group):
    statuses = set(group['Status'].str.lower())
    return 'approved' in statuses and 'cancelled' in statuses

def main():
    print("Sick Leave Data Processor")
    print("=" * 40)
    
    # Get file names from user input
    last_week_file_name = input("Enter last week file name (e.g., 'Sick unapproved_Week 26.xlsx'): ").strip()
    current_week_file_name = input("Enter current week file name (e.g., 'Sick unapproved_Week 27.xlsx'): ").strip()
    
    print("Starting sick leave processing...")
    
    # Validate file names
    if not validate_file_names(last_week_file_name, current_week_file_name):
        sys.exit(1)
    
    # Check if files exist
    if not os.path.exists(last_week_file_name):
        print(f"Error: File '{last_week_file_name}' not found.")
        sys.exit(1)
    
    if not os.path.exists(current_week_file_name):
        print(f"Error: File '{current_week_file_name}' not found.")
        sys.exit(1)
    
    print("File name validation passed.")
    
    # Extract week numbers
    match_l = re.search(r'Week ([\d\+]+)', last_week_file_name)
    week_number_l = match_l.group(1) if match_l else None
    
    match_c = re.search(r'Week ([\d\+]+)', current_week_file_name)
    week_number_c = match_c.group(1) if match_c else None
    
    print(f"Processing Week {week_number_l} (last) and Week {week_number_c} (current)")
    
    # Read Excel files
    try:
        last_week = pd.ExcelFile(last_week_file_name)
        current_week = pd.ExcelFile(current_week_file_name)
    except Exception as e:
        print(f"Error reading Excel files: {e}")
        sys.exit(1)
    
    # Process last week data
    print("Processing last week data...")
    df1 = pd.read_excel(last_week, sheet_name='Pending Reply')
    print(f"null start_dates before date parse:{df1['StartDate'].isnull().sum()}")
    
    # Remove apostrophes from dates
    for col in ['StartDate', 'EndDate', 'SubmitDate']:
        df1[col] = pd.to_datetime(df1[col].astype(str).str.replace("'", ""), format='%d/%m/%Y')
    print(f"null start_dates after date parse:{df1['StartDate'].isnull().sum()}")
    
    # Process current week data
    print("Processing current week data...")
    df_input = pd.read_excel(current_week, sheet_name='Input')
    df_output = pd.read_excel(current_week, sheet_name='Output')
    
    # Delete cancelled/approved from input
    dup_ids = df_input['RequestID'][df_input['RequestID'].duplicated(keep=False)]
    df_dups = df_input[df_input['RequestID'].isin(dup_ids)]
    result = df_dups.groupby('RequestID').filter(has_approved_and_cancelled)
    df_input = df_input.drop(result.index)
    df_input = df_input[~df_input['Status'].str.upper().isin(['CANCELAPPROVED', 'CANCELLED','Cancelled'])]
    
    # Clean output data
    persnr_row_index = df_output[df_output.iloc[:, 0] == 'PersNr.'].index[0]
    df_output = df_output.iloc[persnr_row_index:].reset_index(drop=True)
    df_output.columns = df_output.iloc[0]
    df_output = df_output[1:]
    
    # Parse dates in output
    date_cols = ['eAU Abfragedatum', 'AU seit', 'AU bis', 'Änderung möglich bis', 'abgefragt am']
    for col in date_cols:
        if col in df_output.columns:
            df_output[col] = df_output[col].apply(parse_dates).dt.strftime('%Y-%m-%d')
    
    # Trim spaces
    df_output['Status Übernahme Fehlzeit'] = df_output['Status Übernahme Fehlzeit'].str.strip()
    df_output['Meldung KK/DATEV'] = df_output['Meldung KK/DATEV'].str.strip()
    
    # Convert last week dates
    for col in ['StartDate', 'EndDate', 'SubmitDate']:
        df1[col] = df1[col].apply(parse_dates).dt.strftime('%Y-%m-%d')
    
    # Drop duplicates in output
    df_output['identifier'] = df_output['Betriebl. PersNr.'].astype(str) + df_output['eAU Abfragedatum']
    df_output_sorted = df_output.sort_values(
        by=['identifier', 'AU bis', 'abgefragt am'],
        ascending=[True, False, False],
        na_position='last'
    )
    df_output_deduplicated = df_output_sorted.drop_duplicates(subset='identifier', keep='first')

    # Add source and origin columns
    df_input['Source'] = 'Current Week'
    df_input['Origin'] = 'Week ' + str(week_number_c)
    
    df1['Source'] = 'Last Week'
    
    # Select relevant columns
    df_input = df_input[['PayGroup', 'EmployeeID', 'EmployeeName', 'StartDate',
                        'EndDate', 'SubmitDate', 'RequestID', 'LeaveType', 'Status', 'Source', 'Origin']]
    
    df1_copy = df1[['PayGroup', 'EmployeeID', 'EmployeeName', 'StartDate',
                   'EndDate', 'SubmitDate', 'RequestID', 'LeaveType', 'Status', 'Source', 'Origin']]
    
    # Combine data
    print("Combining data...")
    df_input_combined = pd.concat([df_input, df1_copy], axis=0, ignore_index=True)
    
    # Ensure consistent date formats
    for col in ['StartDate', 'EndDate', 'SubmitDate']:
        df_input_combined[col] = df_input_combined[col].apply(parse_dates).dt.strftime('%Y-%m-%d')
    
    # Create identifier
    df_input_combined['identifier'] = df_input_combined['EmployeeID'].astype(str) + df_input_combined['StartDate']
    
    # Select output columns
    df_output_copy = df_output_deduplicated[['identifier','Betriebl. PersNr.', 'eAU Abfragedatum', 'AU seit', 'AU bis', 
                                           'Änderung möglich bis', 'Status Übernahme Fehlzeit', 'Meldung KK/DATEV']]
    
    # Merge data
    print("Merging data...")
    df_input_merged = pd.merge(df_input_combined, df_output_copy, left_on='identifier', right_on='identifier', how='left')
    # df_input_merged = pd.concat([
    #     df_input_merged1,
    #     df_input_merged2
    # ], ignore_index=True)
    df_input_merged = df_input_merged.drop_duplicates()
    
    # Apply business logic
    print("Applying business logic...")
    df_input_merged['English reply'] = df_input_merged.apply(decision, axis=1)
    df_input_merged['English reply'] = df_input_merged['English reply'].replace({'Unknown':'To Verify'})
    
    # Format dates
    date_columns_to_format = ['StartDate', 'EndDate', 'SubmitDate', 'AU seit', 'AU bis', 'Änderung möglich bis']
    for col in date_columns_to_format:
        if col in df_input_merged.columns:
            df_input_merged[col] = pd.to_datetime(df_input_merged[col]).dt.strftime('%d/%m/%Y')
    
    # Update source names
    df_input_merged['Source'] = df_input_merged['Source'].replace({
        'Current Week': 'Week ' + week_number_c, 
        'Last Week': 'Week ' + week_number_l
    })
    
    # Rename columns
    df_input_merged.rename(columns={'Status Übernahme Fehlzeit':'IIPAY Reply'}, inplace=True)
    
    # Select final columns
    df_input_merged = df_input_merged[['PayGroup', 'EmployeeID', 'EmployeeName', 'StartDate',
                                     'EndDate', 'SubmitDate', 'RequestID', 'LeaveType', 'Status', 'Origin', 
                                     'IIPAY Reply', 'English reply', 'AU seit','AU bis',
                                     'Änderung möglich bis', 'Meldung KK/DATEV', 'identifier']]
    
    # Convert dates to text format
    date_columns = ['StartDate', 'EndDate', 'SubmitDate', 'AU seit', 'AU bis', 'Änderung möglich bis']
    df_input_merged = convert_dates_to_text(df_input_merged, date_columns)
    
    # Add duplication indicator
    df_input_merged['RequestID_Duplication'] = df_input_merged['RequestID'].map(
        df_input_merged['RequestID'].value_counts()
    ).apply(lambda x: True if x > 1 else False)
    
    # Create output file
    print("Creating output file...")
    output_filename = f'Sick_Leave_Processing_Week_{week_number_l}_{week_number_c}.xlsx'
    
    with pd.ExcelWriter(output_filename, engine='openpyxl') as writer:
        # Track Upload tab
        track_upload = df_input_merged[df_input_merged['English reply'].isin([
            'DE GIG Sick Leave eAU-Approved', 'DE GIG Sick Leave eAU-Rejected'
        ])]
        track_upload.to_excel(writer, sheet_name='Track Upload', index=False)
        
        # To Verify tab
        katja_verify = df_input_merged[df_input_merged['English reply'].isin(['To Verify'])]
        katja_verify.to_excel(writer, sheet_name='To Verify', index=False)
        
        # Pending Reply tab
        pending_reply = df_input_merged[df_input_merged['English reply'].isin(['Pending Reply'])]
        pending_reply.to_excel(writer, sheet_name='Pending Reply', index=False)
        
        # Summary of Pending Reply tab
        summary_pending_reply = pending_reply['Origin'].value_counts().reset_index()
        summary_pending_reply.columns = ['Origin', 'Count']
        summary_pending_reply.to_excel(writer, sheet_name='Summary_of_Pending_Reply', index=False)
    
    print(f"Processing completed successfully!")
    print(f"Output file: {output_filename}")
    print(f"Track Upload: {len(track_upload)} records")
    print(f"To Verify: {len(katja_verify)} records")
    print(f"Pending Reply: {len(pending_reply)} records")
    print(f"Summary of Pending Reply: {len(summary_pending_reply)} unique origins")

if __name__ == "__main__":
    main()