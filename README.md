# Gmail Exporter

WARNING: 

> This is vibe coded. Keep a backup of your mbox if you intend any destructive operations.
> The code is NOT reviewed. This is just one useful view of the data.
> The data is intended to be completely exported.
> THIS CODE IS WITHOUT ANY GUARANTEES.

Quick utilities to process a mbox to attachments and text files by year.


## Information

Add an index to your database before using `--write`: `CREATE INDEX idx_thread_id ON emails (thread_id);`

