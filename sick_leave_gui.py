import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import threading
import queue
import os
import sys
import re
import traceback

import numpy as np
import pandas as pd
from datetime import datetime as dt


# ── Business logic (unchanged from V5) ──────────────────────────────────────

def validate_file_names(last_week_file, current_week_file):
    pattern = r'^Sick unapproved_Week \d+(?:\+\d+)?\.xlsx$'
    errors = []
    if not re.match(pattern, os.path.basename(last_week_file)):
        errors.append(
            f"Last week file '{os.path.basename(last_week_file)}' does not match "
            f"required pattern 'Sick unapproved_Week xx.xlsx'."
        )
    if not re.match(pattern, os.path.basename(current_week_file)):
        errors.append(
            f"Current week file '{os.path.basename(current_week_file)}' does not match "
            f"required pattern 'Sick unapproved_Week xx.xlsx'."
        )
    return errors


def parse_dates(date_str):
    for fmt in ('%Y-%m-%d', '%d.%m.%Y', '%m-%d-%Y', '%m/%d/%Y', '%d/%m/%Y'):
        try:
            return pd.to_datetime(date_str, format=fmt)
        except ValueError:
            continue
    return pd.NaT


def decision(row):
    today = pd.Timestamp.today().normalize()
    row['EndDate'] = pd.to_datetime(row['EndDate'], errors='coerce')
    row['AU bis'] = pd.to_datetime(row['AU bis'], errors='coerce')
    row['Änderung möglich bis'] = pd.to_datetime(row['Änderung möglich bis'], errors='coerce')
    row['StartDate'] = pd.to_datetime(row['StartDate'], errors='coerce')
    row['AU seit'] = pd.to_datetime(row['AU seit'], errors='coerce')

    if row['Status Übernahme Fehlzeit'] in (
        'Fehlzeit bereits vorhanden',
        'Ende der AU in passender Fehlzeit korrigiert',
        'Ende der AU in vorheriger Fehlzeit korrigiert',
    ):
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
    elif row['Status Übernahme Fehlzeit'] in (
        'AU in Fehlzeit übernommen',
        'AU nicht übernommen (nicht eAU-relevanter Grund)',
        'Folgebescheinigung ohne Erstbescheinigung',
    ):
        return 'To Verify'
    elif pd.isnull(row['Status Übernahme Fehlzeit']) or row['Status Übernahme Fehlzeit'] == '':
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
    for col in date_columns:
        if col in df.columns:
            df[col] = "'" + df[col]
    return df


def has_approved_and_cancelled(group):
    statuses = set(group['Status'].str.lower())
    return 'approved' in statuses and 'cancelled' in statuses


def process_files(last_week_path, current_week_path, log_callback):
    """Run the full processing pipeline. Returns (output_path, summary_text)."""
    last_week_file_name = os.path.basename(last_week_path)
    current_week_file_name = os.path.basename(current_week_path)

    # Validate
    errors = validate_file_names(last_week_path, current_week_path)
    if errors:
        raise ValueError("\n".join(errors))

    # Extract week numbers
    match_l = re.search(r'Week ([\d\+]+)', last_week_file_name)
    week_number_l = match_l.group(1) if match_l else "unknown"
    match_c = re.search(r'Week ([\d\+]+)', current_week_file_name)
    week_number_c = match_c.group(1) if match_c else "unknown"

    log_callback(f"Processing Week {week_number_l} (last) → Week {week_number_c} (current)")

    # Read Excel files
    last_week = pd.ExcelFile(last_week_path)
    current_week = pd.ExcelFile(current_week_path)

    # Process last week
    log_callback("Reading last week 'Pending Reply' sheet...")
    df1 = pd.read_excel(last_week, sheet_name='Pending Reply')
    for col in ['StartDate', 'EndDate', 'SubmitDate']:
        df1[col] = pd.to_datetime(df1[col].astype(str).str.replace("'", ""), format='%d/%m/%Y')

    # Process current week
    log_callback("Reading current week sheets...")
    df_input = pd.read_excel(current_week, sheet_name='Input')
    df_output = pd.read_excel(current_week, sheet_name='Output')

    # Delete cancelled/approved from input
    dup_ids = df_input['RequestID'][df_input['RequestID'].duplicated(keep=False)]
    df_dups = df_input[df_input['RequestID'].isin(dup_ids)]
    result = df_dups.groupby('RequestID').filter(has_approved_and_cancelled)
    df_input = df_input.drop(result.index)
    df_input = df_input[~df_input['Status'].str.upper().isin(['CANCELAPPROVED', 'CANCELLED', 'Cancelled'])]

    # Clean output
    persnr_row_index = df_output[df_output.iloc[:, 0] == 'PersNr.'].index[0]
    df_output = df_output.iloc[persnr_row_index:].reset_index(drop=True)
    df_output.columns = df_output.iloc[0]
    df_output = df_output[1:]

    date_cols = ['eAU Abfragedatum', 'AU seit', 'AU bis', 'Änderung möglich bis', 'abgefragt am']
    for col in date_cols:
        if col in df_output.columns:
            df_output[col] = df_output[col].apply(parse_dates).dt.strftime('%Y-%m-%d')

    df_output['Status Übernahme Fehlzeit'] = df_output['Status Übernahme Fehlzeit'].str.strip()
    df_output['Meldung KK/DATEV'] = df_output['Meldung KK/DATEV'].str.strip()

    for col in ['StartDate', 'EndDate', 'SubmitDate']:
        df1[col] = df1[col].apply(parse_dates).dt.strftime('%Y-%m-%d')

    df_output['identifier'] = df_output['Betriebl. PersNr.'].astype(str) + df_output['eAU Abfragedatum']
    df_output_sorted = df_output.sort_values(
        by=['identifier', 'AU bis', 'abgefragt am'],
        ascending=[True, False, False],
        na_position='last',
    )
    df_output_deduplicated = df_output_sorted.drop_duplicates(subset='identifier', keep='first')

    df_input['Source'] = 'Current Week'
    df_input['Origin'] = 'Week ' + str(week_number_c)
    df1['Source'] = 'Last Week'

    df_input = df_input[['PayGroup', 'EmployeeID', 'EmployeeName', 'StartDate',
                          'EndDate', 'SubmitDate', 'RequestID', 'LeaveType', 'Status', 'Source', 'Origin']]
    df1_copy = df1[['PayGroup', 'EmployeeID', 'EmployeeName', 'StartDate',
                     'EndDate', 'SubmitDate', 'RequestID', 'LeaveType', 'Status', 'Source', 'Origin']]

    log_callback("Combining and merging data...")
    df_input_combined = pd.concat([df_input, df1_copy], axis=0, ignore_index=True)

    for col in ['StartDate', 'EndDate', 'SubmitDate']:
        df_input_combined[col] = df_input_combined[col].apply(parse_dates).dt.strftime('%Y-%m-%d')

    df_input_combined['identifier'] = df_input_combined['EmployeeID'].astype(str) + df_input_combined['StartDate']

    df_output_copy = df_output_deduplicated[['identifier', 'Betriebl. PersNr.', 'eAU Abfragedatum', 'AU seit', 'AU bis',
                                              'Änderung möglich bis', 'Status Übernahme Fehlzeit', 'Meldung KK/DATEV']]

    df_input_merged = pd.merge(df_input_combined, df_output_copy, on='identifier', how='left')
    df_input_merged = df_input_merged.drop_duplicates()

    log_callback("Applying business logic...")
    df_input_merged['English reply'] = df_input_merged.apply(decision, axis=1)
    df_input_merged['English reply'] = df_input_merged['English reply'].replace({'Unknown': 'To Verify'})

    date_columns_to_format = ['StartDate', 'EndDate', 'SubmitDate', 'AU seit', 'AU bis', 'Änderung möglich bis']
    for col in date_columns_to_format:
        if col in df_input_merged.columns:
            df_input_merged[col] = pd.to_datetime(df_input_merged[col]).dt.strftime('%d/%m/%Y')

    df_input_merged['Source'] = df_input_merged['Source'].replace({
        'Current Week': 'Week ' + week_number_c,
        'Last Week': 'Week ' + week_number_l,
    })

    df_input_merged.rename(columns={'Status Übernahme Fehlzeit': 'IIPAY Reply'}, inplace=True)

    df_input_merged = df_input_merged[['PayGroup', 'EmployeeID', 'EmployeeName', 'StartDate',
                                       'EndDate', 'SubmitDate', 'RequestID', 'LeaveType', 'Status', 'Origin',
                                       'IIPAY Reply', 'English reply', 'AU seit', 'AU bis',
                                       'Änderung möglich bis', 'Meldung KK/DATEV', 'identifier']]

    date_columns = ['StartDate', 'EndDate', 'SubmitDate', 'AU seit', 'AU bis', 'Änderung möglich bis']
    df_input_merged = convert_dates_to_text(df_input_merged, date_columns)

    df_input_merged['RequestID_Duplication'] = df_input_merged['RequestID'].map(
        df_input_merged['RequestID'].value_counts()
    ).apply(lambda x: True if x > 1 else False)

    # Write output next to the current week file
    output_dir = os.path.dirname(current_week_path)
    output_filename = f'Sick_Leave_Processing_Week_{week_number_l}_{week_number_c}.xlsx'
    output_path = os.path.join(output_dir, output_filename)

    log_callback("Writing output file...")
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        track_upload = df_input_merged[df_input_merged['English reply'].isin([
            'DE GIG Sick Leave eAU-Approved', 'DE GIG Sick Leave eAU-Rejected',
        ])]
        track_upload.to_excel(writer, sheet_name='Track Upload', index=False)

        katja_verify = df_input_merged[df_input_merged['English reply'].isin(['To Verify'])]
        katja_verify.to_excel(writer, sheet_name='To Verify', index=False)

        pending_reply = df_input_merged[df_input_merged['English reply'].isin(['Pending Reply'])]
        pending_reply.to_excel(writer, sheet_name='Pending Reply', index=False)

        summary_pending_reply = pending_reply['Origin'].value_counts().reset_index()
        summary_pending_reply.columns = ['Origin', 'Count']
        summary_pending_reply.to_excel(writer, sheet_name='Summary_of_Pending_Reply', index=False)

    summary = (
        f"Processing completed!\n\n"
        f"Output: {output_filename}\n"
        f"Track Upload: {len(track_upload)} records\n"
        f"To Verify: {len(katja_verify)} records\n"
        f"Pending Reply: {len(pending_reply)} records\n"
        f"Summary origins: {len(summary_pending_reply)}"
    )
    return output_path, summary


# ── GUI ──────────────────────────────────────────────────────────────────────

class SickLeaveApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Sick Leave Processor")
        self.root.resizable(False, False)

        # Centre window
        win_w, win_h = 600, 480
        sx = (root.winfo_screenwidth() - win_w) // 2
        sy = (root.winfo_screenheight() - win_h) // 2
        root.geometry(f"{win_w}x{win_h}+{sx}+{sy}")

        self.last_week_path = tk.StringVar()
        self.current_week_path = tk.StringVar()

        # Thread-safe message queue for communication between worker and UI
        self.msg_queue = queue.Queue()

        self._build_ui()

    # ── UI construction ──────────────────────────────────────────────────

    def _build_ui(self):
        pad = dict(padx=12, pady=6)

        # Title
        title = tk.Label(self.root, text="Sick Leave Data Processor", font=("Helvetica", 16, "bold"))
        title.pack(pady=(18, 4))
        subtitle = tk.Label(self.root, text="Select the two Excel files, then click Process.", font=("Helvetica", 10))
        subtitle.pack(pady=(0, 12))

        # ── Last week file ───────────────────────────────────────────────
        frame1 = tk.LabelFrame(self.root, text="Last Week File", padx=8, pady=8)
        frame1.pack(fill="x", **pad)

        entry1 = tk.Entry(frame1, textvariable=self.last_week_path, width=55)
        entry1.pack(side="left", fill="x", expand=True)
        tk.Button(frame1, text="Browse…", command=self._browse_last_week).pack(side="right", padx=(6, 0))

        # ── Current week file ────────────────────────────────────────────
        frame2 = tk.LabelFrame(self.root, text="Current Week File", padx=8, pady=8)
        frame2.pack(fill="x", **pad)

        entry2 = tk.Entry(frame2, textvariable=self.current_week_path, width=55)
        entry2.pack(side="left", fill="x", expand=True)
        tk.Button(frame2, text="Browse…", command=self._browse_current_week).pack(side="right", padx=(6, 0))

        # ── Process button ───────────────────────────────────────────────
        self.process_btn = tk.Button(
            self.root, text="Process", font=("Helvetica", 12, "bold"),
            bg="#4CAF50", fg="white", width=20, command=self._on_process,
        )
        self.process_btn.pack(pady=12)

        # ── Progress bar ─────────────────────────────────────────────────
        self.progress = ttk.Progressbar(self.root, mode='indeterminate', length=400)
        self.progress.pack(pady=(0, 4))

        # ── Log area ─────────────────────────────────────────────────────
        log_frame = tk.LabelFrame(self.root, text="Log", padx=8, pady=8)
        log_frame.pack(fill="both", expand=True, **pad)

        self.log_text = tk.Text(log_frame, height=8, state="disabled", wrap="word", font=("Courier", 10))
        scrollbar = tk.Scrollbar(log_frame, command=self.log_text.yview)
        self.log_text.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side="right", fill="y")
        self.log_text.pack(fill="both", expand=True)

    # ── Helpers ──────────────────────────────────────────────────────────

    def _browse_last_week(self):
        path = filedialog.askopenfilename(
            title="Select Last Week File",
            filetypes=[("Excel files", "*.xlsx")],
        )
        if path:
            self.last_week_path.set(path)

    def _browse_current_week(self):
        path = filedialog.askopenfilename(
            title="Select Current Week File",
            filetypes=[("Excel files", "*.xlsx")],
        )
        if path:
            self.current_week_path.set(path)

    def _log(self, message):
        self.log_text.configure(state="normal")
        self.log_text.insert("end", message + "\n")
        self.log_text.see("end")
        self.log_text.configure(state="disabled")

    def _enqueue_log(self, message):
        """Thread-safe: put log message on queue for main thread to pick up."""
        self.msg_queue.put(("log", message))

    def _poll_queue(self):
        """Called periodically on the main thread to drain the message queue."""
        try:
            while True:
                msg_type, payload = self.msg_queue.get_nowait()
                if msg_type == "log":
                    self._log(payload)
                elif msg_type == "success":
                    summary, output_path = payload
                    self.progress.stop()
                    self.process_btn.configure(state="normal")
                    self._log(summary)
                    messagebox.showinfo("Done", summary)
                    return  # stop polling
                elif msg_type == "error":
                    short_msg, full_tb = payload
                    self.progress.stop()
                    self.process_btn.configure(state="normal")
                    self._log(f"ERROR: {short_msg}\n{full_tb}")
                    messagebox.showerror("Error", f"Processing failed:\n\n{short_msg}")
                    return  # stop polling
        except queue.Empty:
            pass
        # Keep polling every 100ms while processing is active
        self.root.after(100, self._poll_queue)

    def _on_process(self):
        lw = self.last_week_path.get().strip()
        cw = self.current_week_path.get().strip()

        if not lw or not cw:
            messagebox.showwarning("Missing files", "Please select both files before processing.")
            return

        if not os.path.isfile(lw):
            messagebox.showerror("File not found", f"Last week file not found:\n{lw}")
            return
        if not os.path.isfile(cw):
            messagebox.showerror("File not found", f"Current week file not found:\n{cw}")
            return

        # Disable button & start spinner
        self.process_btn.configure(state="disabled")
        self.progress.start(15)
        self._log("Starting processing…")

        # Start polling the message queue from the main thread
        self.root.after(100, self._poll_queue)

        # Run in background thread so the UI stays responsive
        thread = threading.Thread(target=self._run_processing, args=(lw, cw), daemon=True)
        thread.start()

    def _run_processing(self, lw, cw):
        try:
            output_path, summary = process_files(lw, cw, self._enqueue_log)
            self.msg_queue.put(("success", (summary, output_path)))
        except Exception as e:
            tb = traceback.format_exc()
            self.msg_queue.put(("error", (str(e), tb)))


def main():
    root = tk.Tk()
    app = SickLeaveApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
