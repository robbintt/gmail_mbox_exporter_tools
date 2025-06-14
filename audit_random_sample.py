import mailbox
import os
import sys
import email.utils
from datetime import datetime
import hashlib
import random
import time

# We reuse the helper functions from our main script to ensure consistent logic
from process_to_text import parse_date, get_email_body

# --- Configuration ---
MBOX_FILE_PATH = 'All mail Including Spam and Trash.mbox'
SAMPLE_PERCENTAGE = 0.01  # 1%

def get_all_ingestible_messages(mbox_file_path):
    """
    Parses the Mbox file once and returns a dictionary of all messages
    that meet the ingest criteria, keyed by Message-ID.
    """
    print(f"-> Pre-scanning Mbox file to identify all valid messages...")
    print("   (This may take several minutes but only happens once)")
    start_time = time.time()
    
    ingestible_messages = {}
    try:
        mbox = mailbox.mbox(mbox_file_path)
        for message in mbox:
            # Replicate the exact filtering logic from the ingest script
            msg_id = message.get('Message-ID')
            if not msg_id:
                continue

            if not parse_date(message.get('Date')):
                continue
            
            # If it passes filters, add the full message object to our dictionary
            ingestible_messages[msg_id] = message

    except FileNotFoundError:
        return None
    
    duration = time.time() - start_time
    print(f"   -> Found {len(ingestible_messages)} ingestible messages in {duration:.2f} seconds.\n")
    return ingestible_messages


# In audit_random_sample.py, replace the entire run_single_audit function.

def run_single_audit(target_msg_id, original_message):
    """Performs an end-to-end audit for a single message object."""
    # === Part 1: Extract details from the original message ===
    date_tuple = email.utils.parsedate_tz(original_message.get('Date'))
    year_str = str(datetime.fromtimestamp(email.utils.mktime_tz(date_tuple)).year)
    
    # Get the original body and normalize its line endings immediately.
    original_text_body = get_email_body(original_message).strip().replace('\r\n', '\n')
    
    original_attachments = {}
    for part in original_message.walk():
        if "attachment" in str(part.get('Content-Disposition')):
            filename = part.get_filename()
            if filename:
                payload = part.get_payload(decode=True)
                checksum = hashlib.sha256(payload).hexdigest()
                original_attachments[filename] = checksum

    # === Part 2: Verify Text Content ===
    text_file_path = os.path.join('yearly_text_archives', f'{year_str}.txt')
    try:
        with open(text_file_path, 'r', encoding='utf-8') as f:
            # Read and normalize the output file's content as well.
            full_text_content = f.read().replace('\r\n', '\n')
    except FileNotFoundError:
        return False, f"Text file not found: {text_file_path}"
        
    if target_msg_id not in full_text_content:
        return False, "Message-ID not found in text file."
        
    # Check if the normalized body is present in the normalized content.
    if original_text_body and original_text_body not in full_text_content:
        return False, "Plain text body from Mbox not found in text file."

    # === Part 3: Verify Attachments ===
    for filename, original_checksum in original_attachments.items():
        attachment_path = os.path.join('attachments_by_year', year_str, filename)
        
        # Check for numbered duplicates if the original doesn't exist
        # This handles the duplicate filename logic from the main script.
        if not os.path.exists(attachment_path):
            found_duplicate = False
            for i in range(1, 21): # Safety break after 20 duplicates
                name, ext = os.path.splitext(filename)
                dup_path = os.path.join('attachments_by_year', year_str, f"{name}_{i}{ext}")
                if os.path.exists(dup_path):
                    # If we find a numbered duplicate, we'll check its checksum
                    try:
                        with open(dup_path, 'rb') as f_dup:
                            dup_checksum = hashlib.sha256(f_dup.read()).hexdigest()
                        if dup_checksum == original_checksum:
                            attachment_path = dup_path
                            found_duplicate = True
                            break
                    except IOError:
                        continue
            if not found_duplicate:
                 return False, f"Attachment file not found: {filename}"
        
        with open(attachment_path, 'rb') as f:
            extracted_file_checksum = hashlib.sha256(f.read()).hexdigest()
        
        if original_checksum != extracted_file_checksum:
            return False, f"Checksum mismatch for attachment: {filename}"
            
    return True, "OK"


def main():
    # Pre-scan the entire Mbox to get our universe of valid messages
    all_messages = get_all_ingestible_messages(MBOX_FILE_PATH)
    if all_messages is None:
        print(f"ERROR: Mbox file not found at '{MBOX_FILE_PATH}'")
        return

    total_message_count = len(all_messages)
    sample_size = max(1, int(total_message_count * SAMPLE_PERCENTAGE))

    print(f"--- Starting Random Sample Audit ---")
    print(f"Auditing {sample_size} of {total_message_count} messages ({SAMPLE_PERCENTAGE:.2%})...\n")

    # Select the random sample of IDs to test
    ids_to_audit = random.sample(list(all_messages.keys()), sample_size)
    
    passed_count = 0
    failed_count = 0
    
    # Perform the audit for each message in the sample
    for i, msg_id in enumerate(ids_to_audit):
        original_message = all_messages[msg_id]
        is_ok, reason = run_single_audit(msg_id, original_message)
        
        if is_ok:
            passed_count += 1
            print('.', end='', flush=True) # Print a dot for success
        else:
            failed_count += 1
            print('F', end='', flush=True) # Print an F for failure
            print(f"\n -> FAILURE on {msg_id}: {reason}")

        if (i + 1) % 100 == 0:
            print(f" [{i+1}/{sample_size}]")

    print("\n\n--- Audit Complete ---")
    print(f"Result: {passed_count} PASSED, {failed_count} FAILED.")
    if failed_count == 0:
        print("✅ High confidence in archive integrity.")
    else:
        print("❌ Issues detected in the archive.")


if __name__ == "__main__":
    if not os.path.exists('process_to_text.py'):
        print("Error: The main script 'process_to_text.py' was not found.")
        print("Please ensure it is in the same directory to import its helper functions.")
    else:
        main()
