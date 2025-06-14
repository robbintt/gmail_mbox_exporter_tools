import mailbox
import email.utils
import sqlite3
import os
import time
import argparse
from datetime import datetime, timezone

# --- Configuration ---
SOURCE_MBOX_FILE = 'All mail Including Spam and Trash.mbox'
DB_FILE = 'email_index.db'
OUTPUT_DIR = 'yearly_text_archives'

# --- Helper Functions ---

def get_thread_id(msg):
    """Finds a stable identifier for a conversation thread."""
    references = msg.get('References', '').split()
    if references:
        return references[0]
    in_reply_to = msg.get('In-Reply-To')
    if in_reply_to:
        return in_reply_to
    return msg.get('Message-ID')

def parse_date(date_string):
    """Parses a date string into a timezone-aware datetime object."""
    if not date_string: return None
    try:
        return email.utils.parsedate_to_datetime(date_string)
    except (ValueError, TypeError):
        try:
            dt_naive = datetime.strptime(date_string, "%d-%b-%Y %H:%M:%S")
            return dt_naive.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            return None

def get_email_body(msg):
    """Extracts the text body from an email message."""
    body = ""
    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            content_disposition = str(part.get("Content-Disposition"))
            if content_type == "text/plain" and "attachment" not in content_disposition:
                try:
                    charset = part.get_content_charset() or 'utf-8'
                    payload = part.get_payload(decode=True)
                    body += payload.decode(charset, 'replace')
                except (UnicodeDecodeError, AttributeError, LookupError) as e:
                    print(e)
                    continue
    else:
        try:
            charset = msg.get_content_charset() or 'utf-8'
            payload = msg.get_payload(decode=True)
            body = payload.decode(charset, 'replace')
        except (UnicodeDecodeError, AttributeError, LookupError) as e:
            print(e)
            body = ""
    return body.strip()

def format_message(msg_row):
    """Formats a database row into a lightweight, parsable text block."""
    msg_id, thread_id, date_str, from_str, to_str, cc_str, subject_str, gmail_labels, body_str = msg_row
    
    # Only show optional lines if they have content
    cc_line = f"Cc: {cc_str}\n" if cc_str else ""
    labels_line = f"Labels: {gmail_labels}\n" if gmail_labels else ""

    return (
        f"--- MESSAGE ---\n"
        f"Message-ID: {msg_id}\n"
        f"Thread-ID: {thread_id}\n"
        f"Date: {date_str}\n"
        f"From: {from_str}\n"
        f"To: {to_str}\n"
        f"{cc_line}"
        f"Subject: {subject_str}\n"
        f"{labels_line}"
        f"\n" # Blank line separating headers from body
        f"{body_str}\n"
    )

# --- Main Script Phases ---

def phase_one_ingest():
    """Phase 1: Read Mbox and incrementally ingest emails into the SQLite DB."""
    print("--- Phase 1: Ingesting emails into database ---")
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    
    # ADDED 'cc_str' column to the table schema
    cur.execute("""
        CREATE TABLE IF NOT EXISTS emails (
            message_id TEXT PRIMARY KEY,
            thread_id TEXT NOT NULL,
            date_unix INTEGER NOT NULL,
            date_str TEXT,
            from_str TEXT,
            to_str TEXT,
            cc_str TEXT,
            subject_str TEXT,
            gmail_labels TEXT,
            body TEXT
        )
    """)
    conn.commit()

    processed_count = 0
    skipped_count = 0
    start_time = time.time()
    
    try:
        print("-> Opening Mbox file...", flush=True)
        mbox = mailbox.mbox(SOURCE_MBOX_FILE)
        print("-> Starting email processing. Press Ctrl+C to stop and save progress.", flush=True)

        for i, message in enumerate(mbox):
            msg_id = message.get('Message-ID')
            if not msg_id: continue

            cur.execute("SELECT 1 FROM emails WHERE message_id = ?", (msg_id,))
            if cur.fetchone():
                skipped_count += 1
                continue

            dt = parse_date(message.get('Date'))
            if not dt: continue
            
            # ADDED 'Cc' to the extracted data
            email_data = (
                msg_id, get_thread_id(message), int(dt.timestamp()),
                str(message.get('Date')), str(message.get('From')),
                str(message.get('To')), str(message.get('Cc', '')),
                str(message.get('Subject')), str(message.get('X-Gmail-Labels', '')),
                get_email_body(message)
            )
            
            # UPDATED INSERT statement for the new column
            cur.execute("""
                INSERT INTO emails (message_id, thread_id, date_unix, date_str, from_str, to_str, cc_str, subject_str, gmail_labels, body) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, email_data)
            
            processed_count += 1

            if (processed_count > 0) and (processed_count % 250 == 0):
                conn.commit()
                print(f"  ...processed {processed_count} new emails | Saved to DB", flush=True)

    except KeyboardInterrupt:
        print("\n-> KeyboardInterrupt detected. Saving progress...")
    finally:
        print("-> Committing final transaction...")
        conn.commit()
        conn.close()
        duration = time.time() - start_time
        print("\n--- Ingestion Paused/Finished ---")
        print(f"Processed {processed_count} new emails in this session.")
        print(f"Total duration: {duration:.2f} seconds.")

def phase_two_write():
    """Phase 2: Query the DB, sort, and write the final text files."""
    print("\n--- Phase 2: Writing sorted, parsable text files from database ---")
    if not os.path.exists(DB_FILE):
        print(f"Error: Database file '{DB_FILE}' not found. Run --ingest first.")
        return

    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    print("  -> Determining conversation sort order...")
    cur.execute("SELECT thread_id, MAX(date_unix) FROM emails GROUP BY thread_id")
    thread_sort_keys = {thread_id: max_date for thread_id, max_date in cur.fetchall()}
    
    cur.execute("SELECT DISTINCT strftime('%Y', date_unix, 'unixepoch') FROM emails")
    years = [row[0] for row in cur.fetchall()]
    print(f"  -> Found data for years: {', '.join(sorted(years))}")

    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    for year in sorted(years):
        print(f"  -> Processing {year}...")
        output_file_path = os.path.join(OUTPUT_DIR, f"{year}.txt")
        
        items_for_year = []
        for thread_id, sort_key in thread_sort_keys.items():
            if datetime.fromtimestamp(sort_key, timezone.utc).year == int(year):
                items_for_year.append((sort_key, thread_id))

        items_for_year.sort(key=lambda x: x[0], reverse=True)
        
        with open(output_file_path, 'w', encoding='utf-8') as f:
            for _, thread_id in items_for_year:
                # UPDATED SELECT query to fetch 'cc_str'
                cur.execute("""
                    SELECT message_id, thread_id, date_str, from_str, to_str, cc_str, subject_str, gmail_labels, body 
                    FROM emails WHERE thread_id = ? ORDER BY date_unix DESC
                """, (thread_id,))
                
                messages_in_thread = cur.fetchall()

                for i, msg_row in enumerate(messages_in_thread):
                    f.write(format_message(msg_row))
                    if i < len(messages_in_thread) - 1:
                        f.write("\n")
                
                f.write("\n\n")
        
        print(f"     -> Successfully wrote '{output_file_path}'")

    conn.close()
    print("\n--- Writing Complete ---")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process a Google Takeout Mbox file into sorted, yearly text files.")
    parser.add_argument('--ingest', action='store_true', help="Run Phase 1: Ingest emails from Mbox into the SQLite database.")
    parser.add_argument('--write', action='store_true', help="Run Phase 2: Write sorted text files from the database.")
    
    args = parser.parse_args()

    if not args.ingest and not args.write:
        parser.print_help()
    
    if args.ingest:
        phase_one_ingest()
    
    if args.write:
        phase_two_write()
