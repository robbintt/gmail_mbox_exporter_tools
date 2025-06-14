import mailbox
import os
import sys
import email.utils
from datetime import datetime, timezone
import hashlib
import random
import time
import re
import sqlite3

# We reuse the helper functions from our main script to ensure consistent logic
# Ensure your main script is named 'process_to_text.py'
from process_to_text import parse_date, get_email_body

# --- Configuration ---
MBOX_FILE_PATH = 'All mail Including Spam and Trash.mbox'
DB_FILE = 'email_index.db'
SAMPLE_PERCENTAGE = 0.01  # 1%

def get_db_data_and_thread_years(db_file):
    """
    Fetches all data from the database and calculates the correct "thread year" for each thread.
    Returns two dictionaries: one with all message data, and one mapping thread_id to its final year.
    """
    print(f"-> Loading data and calculating thread years from '{db_file}'...")
    if not os.path.exists(db_file): return None, None

    conn = sqlite3.connect(db_file)
    cur = conn.cursor()
    
    cur.execute("SELECT thread_id, MAX(date_unix) FROM emails GROUP BY thread_id")
    thread_latest_date = {row[0]: row[1] for row in cur.fetchall()}
    thread_year_map = {
        thread_id: datetime.fromtimestamp(ts, timezone.utc).year
        for thread_id, ts in thread_latest_date.items()
    }
    
    cur.execute("SELECT message_id, thread_id, from_str, to_str, cc_str, subject_str, gmail_labels, body FROM emails")
    db_messages = {
        row[0]: { 'thread_id': row[1], 'From': row[2], 'To': row[3], 'Cc': row[4],
                  'Subject': row[5], 'Labels': row[6], 'body': row[7] }
        for row in cur.fetchall()
    }
    conn.close()
    
    print(f"   ...loaded {len(db_messages)} records.")
    return db_messages, thread_year_map

def run_single_audit(msg_id, db_record, thread_year_map, mbox_dict):
    """Performs an end-to-end audit for a single message and returns (bool, reason)."""
    
    # === Part 1: Find the correct year and original Mbox message ===
    thread_id = db_record['thread_id']
    correct_year = thread_year_map.get(thread_id)
    if not correct_year:
        return False, "Could not determine correct thread year from database."

    original_message = mbox_dict.get(msg_id)
    if not original_message:
        return False, "Message-ID found in DB, but not found during Mbox pre-scan."

    # === Part 2: Verify Text Content ===
    text_file_path = os.path.join('yearly_text_archives', f'{correct_year}.txt')
    try:
        with open(text_file_path, 'r', encoding='utf-8') as f:
            full_text_content = f.read()
    except FileNotFoundError:
        return False, f"Text file not found: {text_file_path}"
        
    if msg_id not in full_text_content:
        return False, f"Message-ID not found in text file '{text_file_path}'."
    
    # A simple check to see if the body text (normalized) is present
    original_text_body = get_email_body(original_message).strip().replace('\r\n', '\n')
    if original_text_body and original_text_body not in full_text_content.replace('\r\n', '\n'):
         return False, "Plain text body mismatch between Mbox and text file."

    # === Part 3: Verify Attachments ===
    for part in original_message.walk():
        if "attachment" in str(part.get('Content-Disposition')):
            filename = part.get_filename()
            if filename:
                attachment_path = os.path.join('attachments_by_year', str(correct_year), filename)
                
                if not os.path.exists(attachment_path):
                    # Handle cases where duplicate filenames were renamed (e.g., file_1.pdf)
                    # This is a simplified check for the first duplicate.
                    name, ext = os.path.splitext(filename)
                    dup_path = os.path.join('attachments_by_year', str(correct_year), f"{name}_1{ext}")
                    if not os.path.exists(dup_path):
                        return False, f"Attachment not found: {filename}"
                    else:
                        attachment_path = dup_path

                try:
                    original_payload = part.get_payload(decode=True)
                    with open(attachment_path, 'rb') as f_extracted:
                        extracted_payload = f_extracted.read()
                    
                    if hashlib.sha256(original_payload).hexdigest() != hashlib.sha256(extracted_payload).hexdigest():
                        return False, f"Checksum mismatch for attachment: {filename}"
                except Exception as e:
                    return False, f"Error checking attachment {filename}: {e}"

    return True, "OK"

def main():
    db_data, thread_years = get_db_data_and_thread_years(DB_FILE)
    if db_data is None: return

    print(f"-> Pre-loading Mbox file into memory for fast lookups...")
    start_time = time.time()
    mbox_dict = {msg.get('Message-ID'): msg for msg in mailbox.mbox(MBOX_FILE_PATH) if msg.get('Message-ID')}
    duration = time.time() - start_time
    print(f"   ...loaded {len(mbox_dict)} messages in {duration:.2f} seconds.")

    total_message_count = len(db_data)
    sample_size = max(1, int(total_message_count * SAMPLE_PERCENTAGE))
    
    ids_to_audit = random.sample(list(db_data.keys()), sample_size)
    
    print(f"\n--- Auditing {sample_size} random messages... ---")
    passed_count, failed_count = 0, 0

    for i, msg_id in enumerate(ids_to_audit):
        db_record = db_data.get(msg_id)
        if not db_record: continue
            
        is_ok, reason = run_single_audit(msg_id, db_record, thread_years, mbox_dict)
        
        if is_ok:
            passed_count += 1
            print('.', end='', flush=True)
        else:
            failed_count += 1
            # This is the new verbose failure output
            print(f"\nF -> FAILURE on {msg_id}:\n   {reason}")

    print("\n\n--- Final Audit Complete ---")
    print(f"Result on {sample_size} message sample: {passed} PASSED, {failed_count} FAILED.")
    if failed == 0:
        print("✅ Archive integrity confirmed with high confidence.")
    else:
        print("❌ Issues detected. See failure reasons above.")

if __name__ == "__main__":
    if not os.path.exists('process_to_text.py'):
        print("Error: The main script 'process_to_text.py' was not found.")
    else:
        main()
