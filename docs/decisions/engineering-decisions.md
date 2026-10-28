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

**Replaced by [D-025](#d-025-duckdb-for-the-hourly-job).** Kept here because the measurement below is why.

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

## D-021: Hourly, insert-only appends limited to the batch's time range

Every hour the job loads whatever the uploader has handed over, drops the events the table already has with an anti-join on the event key, and appends the rest in one commit. The anti-join only reads table rows inside the batch's `event_time` range.

- **Insert-only** because an event never changes once it's sent. A repeat is the same event delivered again, so there's nothing to update.
- **Limited to the batch's range** so only the table files that could hold a match are read: the hour being loaded, plus the previous one after a restart's rewind. That keeps each run the same size however big the table gets.
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

## D-025: DuckDB for the hourly job

The hourly job that loads the event store runs on DuckDB instead of Spark. It writes the same Iceberg table, through Glue's Iceberg REST endpoint.

**Why:** measured on the same real hour, DuckDB did the same work in 11 seconds against Spark's 82, with 1.6 GB of memory against 2.6 GB, and produced exactly the same events ([D-019](#d-019-spark-in-local-mode-on-the-server), [details](../validation/testing.md#spark-against-duckdb)). Spark spreads work across machines; at about 1.7 million events an hour on one 2-core server, there's nothing to spread. DuckDB also removes Java, the Iceberg jars and the JVM's memory overhead from the server.

**In production:** the hourly runs take 10 to 16 seconds for about 1 to 1.6 million events, against 91 to 106 seconds on Spark for the same size of hour.

**Checked before switching:** DuckDB created a partitioned table in Glue, appended to it, ran an insert-only `MERGE` twice (the second added nothing), and ran a `MERGE` that updated one row and deleted another. Athena then read the result correctly, including DuckDB's delete files.

**What it gives up:**

- Glue support in DuckDB's Iceberg extension is marked experimental. The job fails loudly if a write fails, and nothing local is deleted until a commit succeeds, so a failure costs a retry, not data.
- DuckDB writes deletes as small delete files (merge-on-read) rather than rewriting data files. That matters for the projections, which will need regular compaction.
- DuckDB doesn't run Iceberg table maintenance (compacting files, expiring old snapshots). Athena's `OPTIMIZE` and `VACUUM` can.

**Spark isn't gone.** `benchmarks/spark_transform.py` keeps the Spark version, and the benchmark checks the two produce identical rows. If a job ever outgrows one machine (recomputing months of history, for example), that's where Spark comes back, on a cluster started only for that job.

## D-026: A leaner event store

Three changes to the event store's columns, measured from Iceberg's own per-column sizes on the first 26.3 million events:

| Change | Share of the table it took |
|---|---|
| Drop `cid`, the record's 59-character content hash | 42% |
| Drop `time_us`, the same value as `event_time` | 6% |
| Split `subject_uri` into `subject_collection` and `subject_rkey` | Most of 16%, since `subject_did` already held the account |

**Why these:** no metric or projection uses `cid`; projections order a record's changes by `rev`. `event_time` already stores microseconds. And the URI's three parts rebuild it exactly.

**Cost:** changing the columns means rebuilding the table from the raw files, which S3 keeps for 14 days. A use for `cid` later would need it added back and those 14 days reloaded.

**Result:** after the rebuild, the table holds 47,956,749 events in 1.50 GB, **31.3 bytes an event against 77**, 59% smaller. At about 38 million events a day that's roughly 1.2 GB a day instead of 2.8. What's left is mostly random-looking identifiers (`subject_did`, `rkey`, `rev`, `subject_rkey`) at 4 to 6 bytes each, close to what they can compress to.

## D-027: Sync to disk on its own thread

**Found in production:** Jetstream dropped the collector's connection about 14 times an hour during long, heavy jobs on the server, and about once every 9 hours otherwise ([details](../validation/testing.md#connection-drops-and-heavy-jobs)). The collector synced its file to disk every 5 seconds on its main loop. When another job keeps the disk busy, one sync can take seconds, and while it waits nothing reads from the connection.

**Fix:** the main loop now only hands the buffered events to the operating system, which is quick, and a single background thread runs the `fsync` and then saves the cursor. Events keep being read and written while it runs. The cursor still only moves once the `fsync` has returned, so it never points past data that isn't on disk, and shutdown waits for a sync in progress before saving the final cursor.

**Measured from now on:** each status line reports the longest sync and the longest main-loop pause of the last minute, and a sync over 1 second or a pause of 2 seconds or more is logged as a warning. That turns the explanation into something the log can confirm or rule out.

**Test:** a sync slowed to 0.3 seconds still lets events be written meanwhile, with the main loop never pausing more than 0.2 seconds. With the sync moved back onto the main loop, the same test fails.

**Limits:** the main loop still writes to the file itself. If the operating system ever holds up those writes, which it can do when too much data is waiting to reach the disk, the loop could still stall. The pause measurement would show that.

**Result:** it did. During a deliberate heavy job, syncs took up to 40 seconds and the connection stayed up, but the main loop was still blocked for 2 to 5 seconds at a time by the writes themselves. That led to [D-028](#d-028-read-and-write-on-separate-threads).

## D-028: Read and write on separate threads

**Found:** with only the sync moved off the main loop ([D-027](#d-027-sync-to-disk-on-its-own-thread)), a deliberate heavy job on the server still blocked the main loop 13 times in 11 minutes, for 2 to 5 seconds each ([details](../validation/testing.md#the-heavy-job-test)). The rest of the disk work was still on it: every write, and the hourly rollover's sync. When a big download or a DuckDB spill fills the disk's queue, Linux makes every program that writes wait, including the collector.

**Fix:** split the collector in two, joined by a queue.

- **The asyncio loop only reads.** It takes each message off the connection, reads `time_us`, and puts both on the queue.
- **One writer thread does all the disk work.** It owns the hourly files: writing, rolling over, syncing every 5 seconds and saving the cursor. Only it touches the writer, so no locking is needed.

**What stays the same:** the cursor only moves after an `fsync`, so a crash still replays from the last sync and never skips anything. Shutdown writes everything still queued before saving the final cursor. A failed write or compression stops the collector with its cursor saved, as before.

**The queue's limit:** 200,000 events, about 7 minutes at the usual rate and about 150 MB. A 40-second stall needs about 20,000. If the disk is stuck long enough to fill it, the loop stops reading until there's room rather than growing without limit. That's safe because of the cursor: at worst Jetstream drops the connection, and the reconnect replays from the newest event received.

**Resuming:** a reconnect resumes from the newest event received, not the newest written, because queued events will still be written.

**Simpler too:** D-027's duplicated file handle, which let the main loop keep writing during a sync, isn't needed any more, because the thread that syncs is the only one that writes.

**Test:** slowed-down syncs and slowed-down writes both leave the main loop pausing under 0.2 seconds, with the queue absorbing the delay and every event written once. With writing put back on the main loop, the slow-write test fails. The kill test, run again on the new code, still loses nothing.

**Result:** the same heavy job again, on the server. Syncs still took up to 45 seconds, but the main loop never paused more than 0.05 seconds (against 5.2 before), the collector stayed 0.1 seconds behind live, and the queue peaked at 20,753 events during the longest sync. No drops ([details](../validation/testing.md#the-heavy-job-test)).

## D-029: Projections are appended to, not updated in place

**The obvious design:** keep one row per record and `MERGE` each hour's changes into it, updating edits and removing deletes.

**What the probe found** ([details](../validation/testing.md#duckdb-and-the-projections)): `MERGE` is DuckDB's only atomic way to change rows (a delete then an insert in one transaction made two commits), and to find the rows it changes it reads every file whose keys fall between the batch's smallest and largest key. Every hour has a few deletes of old records, unlikes of things liked weeks ago, which stretch that range across nearly the whole table. So each hourly `MERGE` would read most of the projection from S3, and the server is outside AWS: past the free 100 GB a month, every GB read costs about $0.09. By the second month that's tens of GB an hour.

**Decision:** the hourly run only appends: new versions of records to `records`, and a marker per delete to `record_deletes`. Which version is current is decided by a view ([D-031](#d-031-the-highest-rev-wins-and-deletes-leave-a-marker)). Nothing already written is read, so the hourly cost doesn't grow with the tables.

**Cost of it:** queries do the work of picking the latest version and skipping deleted records, and deleted rows stay in the files until something rewrites them. That's a daily compaction in Athena, which runs inside AWS and so reads for free apart from Athena's per-byte price. It's the next step, and it also sets how long a deleted record can physically remain.

**Alternatives:** Iceberg equality deletes, which remove rows by key without reading them first, are exactly this idea built into the format, but DuckDB only writes positional deletes. Running the hourly fold in Athena would avoid the transfer cost but not the growing scan.

## D-030: New store commits are found by their Iceberg sequence number

`project` has to read only what the event store gained since its last run.

**Decision:** Iceberg gives every commit a sequence number and records, for every data file, the one it was added in. DuckDB's `iceberg_metadata` lists them. `project` folds the files above the sequence number its last run logged, one commit at a time, and reads them straight from S3. The store is append-only, so its files are its contents; a delete file would break that, so it stops the run.

**Rejected:** a `loaded_at` column on the store. The probe showed adding the column works, but the files written before it have no statistics for it, so a filter on it can never skip them: every run would open every old file. A window on `event_time` would miss backfilled hours, whose events are old but new to the store. Folding the batch `process` just loaded, from memory, would need no reading at all, but the two jobs commit separately, so a crash between them would lose that batch for the projections for good.

## D-031: The highest rev wins, and deletes leave a marker

**Rule:** a record's current version is its highest `rev` among creates and edits, and it's deleted if a delete has a higher `rev`. `rev` is the account's repo revision, which only goes up, so it gives the true order of a record's changes however late or repeated the events arrive. The highest of a set doesn't depend on order, so folding any events any number of times in any order gives the same views. Reruns and backfills need nothing special.

**Why markers:** if a delete removed the row, a create arriving after it, from a backfill or a replay, would bring the record back. A marker outranks it. It also covers deletes of records created before collection began: they're markers with nothing to hide, and the check query counts them.

**Privacy:** a marker keeps only the key, `rev` and time. What a deleted like pointed at isn't copied into it. The record's earlier row is hidden by the view at once and physically removed by compaction.

## D-032: Accounts are a view over the event store

An account's current handle and status come from identity and account events, about 0.1% of the store. A view takes the latest of each by sequence number straight from the store, so there's no table to keep in sync. If it gets slow as the store grows, it becomes a projection like the others. Account history (SCD type 2) will come from the same events in the marts.

## D-033: Compaction is tiered by record type and age, to fit the budget

**Problem:** the projections only append ([D-029](#d-029-projections-are-appended-to-not-updated-in-place)), so deleted records stay in the files, hidden by the views, until something rewrites them. Run by hand on the real tables, a compaction of the whole `records` table read about 9 GB for a 2.6 GB table, about 5 cents. The table grows by about 1 GB a day, so doing that daily would pass $25 a month within three months, against a $10 budget for the whole project.

**What decides the reach:** how old a record is when it's deleted, measured over 2.5 million deletes: 36.6% under a day, 15.7% one to seven days, 10.6% one week to a month, 37.1% older.

**Decision:** `records` is partitioned by the day each record was created, so a run can rewrite only recent days. Compaction runs nightly in Athena at three reaches: the last 3 days on ordinary nights, everything for posts and reposts plus the last 8 days for the rest on Sundays, and everything on the 1st of the month. That gives a stated policy: a deleted record is hidden within 2 hours; physically removed within 2 days if it was deleted within a day of being created; and within 8 days at most for posts and reposts, 32 days at most for likes, follows and blocks. Estimated at about $2 a month at six months of data, logged per step in `compaction_runs` so the estimate gets checked.

**Why posts get the shorter deadline:** they carry more (reply links, languages, who replied to whom), and they're about a tenth of the rows, so a weekly full pass over them is cheap. Likes, follows and blocks are nine tenths of the table and get theirs monthly.

**Why Athena:** it runs inside AWS, so reading the tables costs only its price per byte scanned. The same work from the server would also pay for every byte leaving AWS.

**Rejected:** honoring deletes only within a window after creation, say an hour for likes and a week for posts, and ignoring later ones. It would be the cheapest option, but Bluesky's developer guidelines require a way to delete content a user asks to delete, with no time limit; the project's ground rule says the same; and over a third of deletes are of records more than a month old, so counts like current follows would drift from the truth. Every delete is honored; only how soon the bytes leave S3 varies.

## D-034: Deleted records are redacted in the event store

**Problem:** the event store keeps the event that created every record, including what a like pointed at and which post a reply answered. Removing a deleted like from the projections doesn't remove that.

**Decision:** compaction also blanks the subject, reply links and languages in the store events that created or edited a deleted record, and then rewrites the files so the original values are gone. The events themselves stay: that an account liked something at a given time still counts toward activity metrics. The store is append-only apart from this.

**Making it cheap:** redaction is sourced from `records` before the deleted rows are removed, because those rows hold each version's exact event time. That tells Athena which hour partitions of the store to read and rewrite, instead of scanning the whole store.

**Rejected:** leaving the store as it was, as interaction metadata rather than content. It holds no text, but "if someone deletes something, I delete it too" has no exception for metadata.

**Knock-on:** redaction and file rewrites are commits to the store that add files without adding events. The projections job now reads the store's Iceberg metadata through Glue and folds only files from append commits ([D-030](#d-030-new-store-commits-are-found-by-their-iceberg-sequence-number)).

## D-035: One writer at a time

Compaction (through Athena) and the hourly jobs (through DuckDB) commit to the same Iceberg tables. When two writers commit to a table at once, one of them fails. Both are started from the server, so each wraps its command in `flock` on the same lock file, and whichever comes second waits. Compaction is also timed for 03:45 UTC, well away from the hourly :20, so the wait is rare.

## D-036: dbt Core on Athena for the marts

**Decision:** the marts are dbt models, built in Athena with dbt Core 1.12 and the `dbt-athena` adapter, as Iceberg tables in their own Glue database.

**Why dbt:** the marts are many SQL models depending on each other, with tests. dbt gives the dependency order, incremental builds, tests and documentation, and it's the standard tool for exactly this. Plain SQL files run by a small Python job, like `compact`, would mean rebuilding much of that by hand.

**Why Athena:** for the same reason as compaction ([D-033](#d-033-compaction-is-tiered-by-record-type-and-age-to-fit-the-budget)): it runs inside AWS, so reading the event store costs only Athena's price per byte, with no data leaving AWS.

**Why not dbt v2:** it's now dbt's default, but it doesn't support Athena. dbt Core v1 is still fully supported.

**Testing on two engines:** the same models run on DuckDB in CI, against a hand-made sample with known answers. The handful of functions that differ between Athena's SQL and DuckDB's are macros that pick the right one per engine.

**Incremental builds:** each night clears and rebuilds the last two days, since an hour can reach the event store late. Clearing and appending, rather than a `MERGE`, means a rebuilt day can lose rows as well as change them.

## D-037: Active means wrote something, and signups are inferred

**Active accounts:** an account is active on a day if it wrote to any Bluesky record type. The firehose only carries writes, so accounts that only read can't be counted. Every chart of active accounts says "active writers".

**New accounts:** the feed has no signup event. An account counts as new on the day it creates its profile record, if nothing was seen from it more than an hour before. Profile creation is the clearest sign of a new account, and the one-hour rule stops accounts that existed before collection, and only set a profile now, from counting as new.

**Checked on real data** ([details](../validation/testing.md#new-accounts-checked)): of the 18,521 accounts that created a profile on 26 October, only 29 had written anything more than an hour before, and 18,422 (99.5%) also had an identity event, a handle being set, within the hour. Profile creation does mark new accounts, about 18,500 to 19,000 a day.

**Rejected:** "first seen" as the signup day. Every existing account is first seen sometime after collection starts, so for weeks "new accounts" would mostly be old ones.

## D-038: Accounts are hashed, and handles kept, in private marts

The marts key accounts by a SHA-256 hash of their DID, so raw account IDs don't spread into every table. DIDs are public, so anyone with a list of them could match the hashes: it's pseudonymization, not anonymization, and isn't presented as more.

`dim_account` keeps handles. They're public on Bluesky, and they're what its history tracks (SCD type 2). The marts are never published as tables; only aggregates leave, with small groups suppressed. A deleted account's handles are removed at compaction ([D-039](#d-039-handles-are-looked-up-not-read-from-the-feed)).

## D-039: Handles are looked up, not read from the feed

**Found:** the first build of `dim_account` had handles for 3 of 1.8 million accounts. Jetstream's identity events had the handle in 28 of 74,610 cases. In the protocol the field is optional: an identity event says an account's identity changed, and anyone who needs the new handle is expected to resolve it. I'd built and tested the model against sample events that had handles, without checking what the real feed sends.

**Decision:** a new hourly job, `resolve-handles`, reads the identity events in each new store commit and asks Bluesky's public API (`app.bsky.actor.getProfiles`) for those accounts' handles, 25 a call, at most two calls a second, backing off on 429s and server errors. Answers go into `account_handles`, and `dim_account` takes its handle history from there ([details](../architectures/handles.md)).

**Why the API rather than resolving DID documents:** the AppView returns the handle only after checking it both ways (the DID claims the handle and the handle points back at the DID), and returns `handle.invalid` otherwise. Doing that check ourselves would mean a DNS or HTTPS request per handle on top of fetching the DID document. One call covers 25 accounts.

**What's not kept:** the API also returns display names and bios. They're profile content, and the project keeps no record content ([D-026](#d-026-a-leaner-event-store)), so they're dropped.

**Deletes:** handles are public, but a deleted account's handles are removed by compaction, so they don't outlive the account here.

**Rejected:** the first plan, account history on status alone. Status changes are real (deactivations, deletions, suspensions: about 70,000 in the first days), but handle changes are the more useful history, and an enrichment step with an outside API, rate limits and retries is worth having in the pipeline anyway.

