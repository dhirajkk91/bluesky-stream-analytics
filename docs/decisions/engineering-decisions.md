# Decisions

## D-001: Store raw events exactly as received

The collector doesn't clean, reshape or filter anything. It reads `time_us` to know which file the event goes in, and writes the original text.

**Why:** if code further down the pipeline has a bug, I can rebuild everything from the raw files. If the collector transformed events and got it wrong, that data would be gone.

**Cost:** raw JSON is bigger than a cleaned-up format. Compression helps (D-002).

## D-002: Hourly JSON Lines files, compressed with zstd

- **JSON Lines.** Easy to append to, easy to read back line by line, and Athena can query it directly later.
- **Hourly files in `date=…/hour=…` folders.** That's small enough that a crash or a bad file only affects an hour, and the folder layout lets Athena skip hours a query doesn't need.
- **zstd.** It's built into Python 3.14, so no extra dependency, and Athena can read zstd-compressed text.
- **Uncompressed while the hour is in progress, compressed once it's done.** If the active file were compressed, a crash could leave a broken compressed stream that's hard to read back. With plain text, a crash can only damage the last line, and recovery just cuts that line off.

**Alternative I didn't take:** Parquet straight away. It's much better for querying, but you can't append to it one event at a time, and it would mean buffering in memory. That belongs in a later step that converts raw files into proper tables.

## D-003: Save the cursor only after the data is on disk

Every 5 seconds the current file is `fsync`'d, and only then is the cursor saved.

**Why:** writing to a file doesn't mean it's on disk yet. It can sit in memory for a while. If the cursor were saved first and the machine crashed, the cursor would point past events that were never stored, and they'd never be fetched again. The order rules that out.

**Why not every event:** `fsync` is slow. Every 5 seconds is a fair balance. The worst case after a hard crash is replaying about 5 seconds of events.

The cursor file itself is written to a temp file and renamed into place, so it's never half-written. A cursor file that's there but unreadable stops the collector with an error, instead of quietly starting from live and leaving a gap.

## D-004: Rewind 5 seconds on every resume and accept duplicates

Whenever the collector connects with a resume point, it asks for events starting 5 seconds earlier.

**Why:** the switch from replaying old events to live ones isn't perfectly clean on Jetstream's side, and my cursor can be a few seconds behind what was actually written. Overlapping a little means no gaps.

**Cost:** duplicates on every reconnect or restart. They're cheap to remove later, because events carry enough identifying fields to spot repeats. A gap can't be fixed later.

## D-005: Connect to one fixed Jetstream server

The collector always uses `jetstream2.us-east.bsky.network`.

**Why:** each Jetstream server stamps `time_us` itself, so the same event has slightly different timestamps on different servers. A cursor saved from one server doesn't resume cleanly on another.

**Cost:** if that server is down, the collector waits and retries instead of failing over. Switching servers would need a bigger rewind and more duplicates. I'll handle that if it actually happens.

## D-006: Resume from the newer of the cursor and the recovered data

**Found by testing:** I killed the collector 3 seconds after it started, before its first cursor save. On restart there was no cursor, so it started from live, and everything after the last event in the file was lost.

**Fix:** crash recovery now reports the newest `time_us` it found in the files it rescued. The collector resumes from whichever is newer: that, or the saved cursor. It's safe because recovery syncs those files to disk before they're used.

## D-007: Reconnect with exponential backoff and jitter

After a dropped connection, the wait before retrying doubles each time (1s, 2s, 4s, up to 60s), and the actual wait is random between half and all of that.

**Why:** retrying instantly in a loop hammers a server that's already having trouble. The randomness stops many clients that dropped at the same moment from all reconnecting at the same moment too.

## D-008: Collector server on Oracle Cloud, data on AWS

The always-on server runs on Oracle Cloud's free ARM tier. Storage and SQL are on AWS.

**Why Oracle for the server:** it gives 2 CPUs and 12 GB of memory free with no end date. The cheapest sensible AWS server (`t4g.small`, 2 GB) is on a free trial only until the end of 2026, then about $18 a month with its disk and public IP, and it's too small for the services later steps add. An 8 GB AWS server is around $49 a month.

**Why AWS for the data:** S3, Glue and Athena are what data engineering roles ask for, Athena queries files directly for a few cents, and Oracle's free object storage (20 GB) would fill in about three days.

**Cost:** two cloud accounts to manage, and the server could be hard to get if Oracle runs out of free ARM capacity. Fallback: run the collector on a `t4g.small` until that's solved.

## D-009: Upload in a separate command, checksummed, never overwriting

**Why separate:** the collector's only job is not to miss events. If S3 is down or the credentials are wrong, uploads fail and retry on the next run, and collection isn't affected.

**Why the checksum:** S3 verifies the SHA-256 and rejects a corrupted upload, so a local file is never deleted on the strength of a broken copy.

**Why `If-None-Match`:** a run that uploads a file but dies before deleting it would otherwise upload it again on the next run. The conditional write makes S3 refuse instead, and the uploader treats that as done. Uploading the same files twice is always safe.

## D-010: The server's AWS user can only upload

The IAM user on the server has one permission: `s3:PutObject` under `raw/`. It can't list, read or delete anything, so leaked keys can't expose or destroy data. Uploading with a checksum and `If-None-Match` needs nothing more.

Its access key is created by hand, not in Terraform, so the secret never ends up in the Terraform state file.

## D-011: Terraform state stays local for now

The state file lives on my laptop and is ignored by git. With one person and a handful of resources, a remote backend adds setup without solving a real problem yet. It moves to S3 if the infrastructure grows or CI starts applying changes.

## D-012: systemd, not Docker, on the server

The collector and uploader run as plain systemd units from a git checkout.

**Why:** there's one Python app with one dependency file. systemd already handles what's needed: start on boot, restart on crash, a clean `SIGTERM` on stop, a schedule for the uploader, and logs. Docker would add an image build and a registry without solving anything yet.

**Revisit when:** several services have to run side by side, like a streaming broker or monitoring.

## D-013: The uploader's key lives in a root-only file read by systemd

`/etc/bluesky/upload.env` is owned by root with mode `600`. systemd reads it as root and passes the values to the uploader as environment variables, so the `bluesky` user can't read the file itself, and the key is never in the repo or in shell history.

## D-014: The raw table keeps each event as one JSON string

`raw_events` has a single `line` column, and a view extracts the fields used for exploring.

**Why:** Bluesky records are deeply nested and change as Bluesky and other apps add fields. A fixed schema at this layer would fail or silently drop fields when that happens. Keeping the line as-is matches D-001: the raw layer is exactly the files.

**Cost:** every query parses JSON, which is slower than reading typed columns. Fine for exploring; metrics will be built on typed tables.

## D-015: Partition projection instead of a crawler

The table describes the `date=…/hour=…` layout and Athena computes the partitions at query time.

**Why:** new hours are queryable the moment they're uploaded, with nothing to schedule, run or forget. A crawler would cost money and lag behind; registering partitions by hand would be another job to keep alive.

**Cost:** the layout is fixed in the table definition. Changing the folder pattern means changing the table too.

## D-016: Every query runs in a workgroup with a scan limit

The workgroup stops any query that would read more than 10 GB (about $0.05) and its settings override the client's.

**Why:** Athena charges by bytes read, and one query without a date filter over months of data could scan hundreds of gigabytes. The limit makes that a failed query instead of a bill.

## D-017: Compress finished hours on a background thread

**Found in production:** the collector's log showed the previous hour's file finishing 10 seconds into the next hour, and that minute's status line covering 64 seconds. Compressing about 1 GB ran on the event loop, so for about 10 seconds every hour nothing was read from Jetstream. No events were lost (they waited in the connection), but the pause grows with volume, and if it ever outlasted the connection's keep-alive it would cause the disconnects it's meant to avoid.

**Fix:** the file is still closed and synced on the main loop, then a single background thread compresses it. Python's zstd releases the interpreter lock while it works, so the loop keeps running. On a 400 MB test file, the longest pause in the event loop went from 2,629 ms to 1.1 ms.

**Safety:** the cursor only counts events that were synced before the file was handed off, shutdown waits for compression to finish, and a failed compression stops the collector so systemd restarts it and recovery redoes the work.

## D-018: Event-sourced, not medallion

The architecture is an event log with derived read models: raw event log → event store → projections → marts. It isn't bronze, silver and gold.

**Why:** Jetstream is a stream of changes to records, not snapshots of data. The question that matters most is "what happened" versus "what is true now", and event sourcing makes that the main boundary:

- The event store is append-only, so it's a full history that can be replayed.
- Projections fold events in order (create, update, delete) into current state, and can be dropped and rebuilt from the store at any time.
- Deletes have one obvious home. The log records that a delete happened, and every projection removes the deleted record.

Reads and writes are split in the CQRS sense: the store is shaped for appending, and each projection or mart is shaped for its queries. The marts use Kimball dimensional modeling (facts and dimensions, with SCD type 2 for account history).

**Why not medallion:** it organizes data by how clean it is, which suits batch and snapshot sources better than a stream of changes.

**What it isn't:** processing is hourly micro-batches over a replayable file log, not a stream processor, so it isn't Kappa or Lambda.

## D-019: Spark in local mode on the server

The job that loads the event store is PySpark running on the Oracle server in local mode (one machine, every core), started by a systemd timer.

**Honestly:** this data doesn't need Spark. An hour is about 1.5 million events, roughly 1 GB of JSON, and DuckDB or Polars on one machine would handle it. I picked Spark because the compute is free on the server, it writes Iceberg tables with `MERGE` well, and the same job would run on a cluster (EMR, Glue) unchanged if the volume grew 100 times.

**Alternatives:**

- Athena SQL alone, writing Iceberg with `MERGE`. Cheapest and simplest, about $2 a month, but no Spark anywhere in the project.
- AWS Glue jobs or EMR Serverless. Managed Spark, but around $20 a month for an hourly job, twice the budget.

**Limits:** the job shares 2 cores and 12 GB with the collector. It runs at a lower CPU priority with a 6 GB memory cap, so the collector always wins.

**Measured afterwards:** the same transform on the same real hour, with both engines held to the server's 2 cores ([details](../validation/testing.md#spark-against-duckdb)):

| | DuckDB | Spark |
|---|---|---|
| Time | 11 s | 82 s |
| CPU time | 20 s | 135 s |
| Peak memory | 1.6 GB | 2.6 GB |

DuckDB is about 7 times faster on this data, needs less memory, and produced exactly the same events. At this volume, Spark's ability to spread work across a cluster buys nothing.

## D-020: Iceberg tables in the Glue catalog

The event store, and later the projections and marts, are Iceberg tables stored under `warehouse/` in the same bucket, registered in Glue (`bluesky_store` for the event store), and queried with Athena.

**Why Iceberg:** honoring deletes means removing single rows from the projections' files in S3. Plain Parquet would need hand-written code to find and rewrite the right files. Iceberg does row-level `MERGE` and `DELETE`, every write is all-or-nothing, and old versions stay readable until they're expired. Athena reads and writes it natively. Delta Lake and Hudi would also work, but Iceberg has the best support on AWS.

**Format v2, set explicitly,** because that's what Athena supports today. The job creates its own tables; Terraform only makes the database and the job's permissions.

## D-021: Hourly, insert-only merge limited to the batch's time range

Every hour the job loads whatever the uploader has handed over and runs `MERGE … WHEN NOT MATCHED THEN INSERT`, joining on the event key and on the batch's `event_time` range.

- **Insert-only** because an event never changes once it's sent. A repeat is the same event delivered again, so there's nothing to update.
- **Limited to the batch's range** so the merge only reads the table files that could hold a match: the hour being loaded, plus the previous one after a restart's rewind. A test deletes an earlier hour's files and checks the merge still succeeds, so it can't be quietly scanning the whole table.
- **Hourly rather than daily:** both cost the same, since the compute is free and the reads are the same in total. Hourly keeps data about 1.5 hours fresh instead of up to 2 days, and keeps a failed run's impact to one hour. Daily metrics in later layers will run daily.

Loading the same files twice adds nothing, so a crash at any point is fixed by running again.

## D-022: Process the local copy, not the one in S3

After uploading, the uploader moves the file to `uploaded/` instead of deleting it. The job reads from there and deletes the file after its commit.

**Why:** reading every hour back out of S3 to the server would be about 137 GB a month of data transfer, around $3 a month past AWS's free 100 GB. The local copy is the same bytes, and S3 still has the original for rebuilds.

**Why only `uploaded/`, not `events/`:** a file there is already in S3, and the uploader won't touch it again, so the job never races the uploader and never puts anything into the event store that isn't safely stored first.

**Cost:** if the job is down, files pile up locally (about 5 GB a day, with about 90 GB free). Monitoring will watch that.

## D-023: Raw files kept for 14 days

An S3 lifecycle rule deletes `raw/` files after 14 days.

**Why:** raw files hold full post text, including posts that were later deleted, so expiring them is part of honoring deletes, not just saving money. Fourteen days leaves two weeks to spot a bug and rebuild the event store from raw. Without the rule, raw storage grows about 137 GB a month.

**Cost:** the event store can't be rebuilt from scratch for days older than that, and a field not extracted today can't be added for those days later.

## D-024: No record content in the event store

The events table keeps what metrics need (who, when, what kind, aimed at whom, reply links, languages) and none of the record content: no post text, no profile text, no images or links.

**Why:** content that's deleted on Bluesky would otherwise live on in an append-only log kept for months, where it can't be removed without breaking the log's rule. Nothing planned needs it. Trending topics can come from hashtags, which can be added as their own column when that step comes.
