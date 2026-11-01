# Design and trade-off note

- **Quarantine, don't drop**: Every bad row goes to `rejects` with file, line number, reason and the raw row, so the data owner can fix and resend it.
- **Hash of the file as `batch_id`**: Reruns are detected from the content, not the file name.
- **One transaction per file**: Nothing is half-loaded, and a retry is just running the same command. 
- **Upsert guarded by `event_ts`**: Late data cannot overwrite a newer version of the same transaction.
- **Simple statistics instead of PSI/KS tests**: Mean shift in standard deviations and change in percentage share are easy to explain in the alert text. 
- **One baseline batch**: Easy to understand and audit. 
- **Validation with pandas DataFrames**: Null checks, type checks and duplicate checks are one-line column operations (`isna`, `to_numeric`, `isin`, `duplicated`), which is shorter than looping over rows. Trade-off: the whole file is held in memory, so very large files would need chunked reading.
- **Not done on purpose**: Scheduler, distributed processing, streaming. The task asks for a tested vertical slice.
