import os
import hashlib

def get_file_hash(file_path):
    """Calculates the SHA256 hash of a file."""
    hasher = hashlib.sha256()
    with open(file_path, 'rb') as f:
        buf = f.read()
        hasher.update(buf)
    return hasher.hexdigest()

def clean_and_deduplicate(base_dir):
    """
    Removes .ics files and deduplicates files within each year's directory.
    """
    for year in range(2008, 2026):
        year_dir = os.path.join(base_dir, str(year))

        if not os.path.isdir(year_dir):
            continue

        print(f"Processing directory: {year_dir}")

        # First, remove all .ics files
        for filename in os.listdir(year_dir):
            if filename.lower().endswith('.ics'):
                file_path = os.path.join(year_dir, filename)
                try:
                    os.remove(file_path)
                    print(f"  Deleted .ics file: {filename}")
                except OSError as e:
                    print(f"Error deleting file {file_path}: {e}")

        # Now, deduplicate the remaining files
        hashes = {}
        for filename in os.listdir(year_dir):
            file_path = os.path.join(year_dir, filename)
            if os.path.isfile(file_path):
                file_hash = get_file_hash(file_path)
                if file_hash in hashes:
                    #print(f"  Found duplicate: {filename} is a duplicate of {hashes[file_hash]}")
                    try:
                        os.remove(file_path)
                        print(f"  Deleted duplicate file: {filename}")
                    except OSError as e:
                        print(f"Error deleting file {file_path}: {e}")
                else:
                    hashes[file_hash] = filename
    print("\nProcessing complete.")

if __name__ == '__main__':
    # Set the base directory to the current directory.
    # Change this if your year folders are in a different location.
    base_directory = 'attachments_by_year'
    clean_and_deduplicate(base_directory)
