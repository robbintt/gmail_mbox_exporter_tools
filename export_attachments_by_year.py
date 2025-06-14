import mailbox
import os
import email.utils
from datetime import datetime
import sys

def extract_attachments(mbox_file_path):
    """
    Extracts attachments from an Mbox file and organizes them into folders by year.
    """
    # Create a main directory for all attachments
    output_parent_dir = "attachments_by_year"
    if not os.path.exists(output_parent_dir):
        os.makedirs(output_parent_dir)

    print(f"-> Opening Mbox file: {mbox_file_path}")
    mbox = mailbox.mbox(mbox_file_path)
    
    total_messages = 0
    attachments_extracted = 0

    print("-> Starting extraction process...")
    for i, message in enumerate(mbox):
        total_messages += 1

        # Get the email's date
        date_tuple = email.utils.parsedate_tz(message.get('Date'))
        if date_tuple:
            # Create a datetime object to easily get the year
            local_date = datetime.fromtimestamp(email.utils.mktime_tz(date_tuple))
            year_str = str(local_date.year)
        else:
            # Fallback for messages with no valid date
            year_str = "unknown_year"
        
        # Check each part of the message for attachments
        if message.is_multipart():
            for part in message.walk():
                # Check the Content-Disposition header to see if it's an attachment
                content_disposition = str(part.get('Content-Disposition'))
                if "attachment" in content_disposition:
                    # It's an attachment, get the filename
                    filename = part.get_filename()
                    if filename:
                        # Create the year-specific directory
                        year_dir = os.path.join(output_parent_dir, year_str)
                        if not os.path.exists(year_dir):
                            os.makedirs(year_dir)
                        
                        # Create the full file path
                        filepath = os.path.join(year_dir, filename)
                        
                        # Handle duplicate filenames
                        counter = 1
                        while os.path.exists(filepath):
                            name, ext = os.path.splitext(filename)
                            filepath = os.path.join(year_dir, f"{name}_{counter}{ext}")
                            counter += 1
                        
                        # Decode the attachment payload and save it
                        try:
                            with open(filepath, 'wb') as f:
                                f.write(part.get_payload(decode=True))
                            attachments_extracted += 1
                            print(f"   - Saved: {filepath}")
                        except Exception as e:
                            print(f"   - ERROR saving {filename}: {e}")

        if (i + 1) % 1000 == 0:
            print(f"  ...scanned {i+1} messages...")

    print("\n--- Extraction Complete ---")
    print(f"Total messages scanned: {total_messages}")
    print(f"Total attachments extracted: {attachments_extracted}")


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python extract_attachments_by_year.py <path_to_your_mbox_file>")
        sys.exit(1)
    
    mbox_path = sys.argv[1]
    if not os.path.exists(mbox_path):
        print(f"Error: Mbox file not found at '{mbox_path}'")
        sys.exit(1)
        
    extract_attachments(mbox_path)
