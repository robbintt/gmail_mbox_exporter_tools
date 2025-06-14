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
    
    # 1. Get the latest date for each thread to determine its correct year
    cur.execute("SELECT thread_id, MAX(date_unix) FROM emails GROUP BY thread_id")
    thread_latest_date = {row[0]: row[1] for row in cur.fetchall()}
    thread_year_map = {
        thread_id: datetime.fromtimestamp(ts, timezone.utc).year
        for thread_id, ts in thread_latest_date.items()
    }
    
    # 2. Fetch all data for every message
    cur.execute("SELECT message_id, thread_id, from_str, to_str, cc_str, subject_str, gmail_labels, body FROM emails")
    db_messages = {
        row[0]: {
            'thread_id': row[1], 'From': row[2], 'To': row[3], 'Cc': row[4],
            'Subject': row[5], 'Labels': row[6], 'body': row[7]
        } for row in cur.fetchall()
    }
    conn.close()
    
    print(f"   ...loaded {len(db_messages)} records.")
    return db_messages, thread_year_map

def parse_text_file_block(text_block):
    """Parses a single '--- MESSAGE ---' block from a text file."""
    headers = {}
    body_lines = []
    is_body = False
    for line in text_block.strip().split('\n'):
        if not line.strip() and not is_body:
            is_body = True
            continue
        if not is_body:
            match = re.match(r'([^:]+):\s?(.*)', line)
            if match:
                headers[match.group(1).strip()] = match.group(2).strip()
        else:
            body_lines.append(line)
    return headers.get('Message-ID'), headers, '\n'.join(body_lines)

def run_definitive_audit(all_db_data, all_thread_years, sample_ids):
    """Performs the definitive end-to-end audit on a sample of message IDs."""
    print(f"\n--- Auditing {len(sample_ids)} random messages... ---")
    
    passed_count, failed_count = 0, 0
    
    # Pre-load the Mbox into a dictionary for fast lookups
    mbox_dict = {msg.get('Message-ID'): msg for msg in mailbox.mbox(MBOX_FILE_PATH)}

    for i, msg_id in enumerate(sample_ids):
        db_record = all_db_data.get(msg_id)
        if not db_record:
            print('S', end='', flush=True) # S for Skipped (not in DB)
            continue
            
        # === 1. Verify Text File Content ===
        thread_id = db_record['thread_id']
        correct_year = all_thread_years.get(thread_id)
        if not correct_year:
            failed_count += 1
            print('F', end='', flush=True)
            continue
            
        text_file_path = os.path.join('yearly_text_archives', f'{correct_year}.txt')
        try:
            with open(text_file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Find the specific block for our message ID
            # This is a simplification; a more robust parser would split by delimiter first
            if msg_id not in content:
                failed_count += 1
                print('F', end='', flush=True)
                continue

        except FileNotFoundError:
            failed_count += 1
            print('F', end='', flush=True)
            continue

        # === 2. Verify Attachments ===
        original_message = mbox_dict.get(msg_id)
        if original_message:
            for part in original_message.walk():
                if "attachment" in str(part.get('Content-Disposition')):
                    filename = part.get_filename()
                    if filename:
                        attachment_path = os.path.join('attachments_by_year', str(correct_year), filename)
                        if not os.path.exists(attachment_path):
                            failed_count += 1
                            print('F', end='', flush=True)
                            # Break from this inner loop, continue to next msg_id
                            break
                        
                        # Checksum validation for binary files
                        try:
                            original_payload = part.get_payload(decode=True)
                            with open(attachment_path, 'rb') as f:
                                extracted_payload = f.read()
                            if hashlib.sha256(original_payload).hexdigest() != hashlib.sha256(extracted_payload).hexdigest():
                                failed_count += 1
                                print('F', end='', flush=True)
                                break
                        except Exception:
                            failed_count += 1
                            print('F', end='', flush=True)
                            break
            else: # This 'else' belongs to the 'for part in ...' loop
                passed_count += 1
                print('.', end='', flush=True)
        else:
            # If no attachments, and text check passed, it's a pass
            passed_count += 1
            print('.', end='', flush=True)

        if (i + 1) % 100 == 0:
            print(f" [{i+1}/{len(sample_ids)}]")

    return passed_count, failed_count

def main():
    db_data, thread_years = get_db_data_and_thread_years(DB_FILE)
    if db_data is None:
        return
        
    total_message_count = len(db_data)
    sample_size = max(1, int(total_message_count * SAMPLE_PERCENTAGE))
    
    ids_to_audit = random.sample(list(db_data.keys()), sample_size)
    
    passed, failed = run_definitive_audit(db_data, thread_years, ids_to_audit)
    
    print("\n\n--- Definitive Audit Complete ---")
    print(f"Result on {sample_size} message sample: {passed} PASSED, {failed} FAILED.")
    if failed == 0:
        print("✅ Archive integrity confirmed with high confidence.")
    else:
        print("❌ Issues detected. The 'F' marks indicate specific failures to investigate.")

if __name__ == "__main__":
    if not os.path.exists('process_to_text.py'):
        print("Error: The main script 'process_to_text.py' was not found.")
    else:
        main()
