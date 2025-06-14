# Gmail Exporter

Quick utilities to process a mbox to attachments and text files by year.

The mbox data is intended to be completely (somewhat destructively) exported into the `yearly_text_archives` and `attachments_by_year` folders.



## IMPORTANT WARNING(S)

* This low importance project is mostly vibe coded over a 1 hour period with a 1 year old child.
* The code is NOT reviewed. This is just one useful view of the data.
* Keep a backup of your mbox if you intend any destructive operations relying on this view of your data.


## Steps

Uses Python 3.12.9

1. `python process_to_text.py --ingest`
2. Add an index to your database before using `--write`: `CREATE INDEX idx_thread_id ON emails (thread_id);`
3. `python process_to_text.py --write`
4. `python audit_random_sample.py`
5. `audit_random_sample.py` - needs a param, there will be failures, the point is to know what they are.
6. deduplicate - i audited this one quickly

## Notes

- You can kill the extract script and rerun it later. 
- You can't kill the write script, it will overwrite the txt files.
- I only ran the attachment export once.
- i didn't even audit the audit script

## Final State

- Database contains just text, it's an intermediate store for batching
- `yearly_text_archives` contains all text of emails in the mbox (not html)
- `attachments_by_year` contains all attachments, tons of stuff you don't want, like ics calendar invites and email signature images.
    - optionally deduplicate it
