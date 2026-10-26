# Testing

## Automated tests

`uv run pytest` runs everything below. None of it needs a network connection or an AWS account.

| Area | What's checked |
|---|---|
| Cursor | Save and load, missing file means start live, a corrupt file raises instead of starting live, no temp file left behind |
| Writer | Rollover at the hour boundary, an earlier-hour event stays in the current file, compressed output matches the input, unique file names per run |
| Crash recovery | A half-written last line is cut off and every complete line kept, a crash after compression, leftover temp and empty files |
| Collector, end to end | Against a fake Jetstream on localhost that honours `?cursor=`: collects everything, survives a dropped connection, restarts from the saved cursor, resumes from recovered data newer than the cursor |
| Uploader | Against a fake S3: exact bytes stored, an existing object is never overwritten, the local copy moves to `uploaded/` only after S3 has it, one failure doesn't stop the rest |
| Slow disk | Syncs slowed to 0.3 seconds, or every 200th write slowed to 0.3 seconds: the main loop never pauses more than 0.2 seconds, events wait in the queue meanwhile, and every event is written exactly once with the cursor at the last one. With writing put back on the main loop, the slow-write check fails. Also: a slow sync is logged, a full queue holds reading back without losing anything, a stop writes everything still queued before saving the cursor, and a failed write stops the collector with the cursor of the last sync |
| Background compression | A rollover returns without waiting for compression, close waits for it, and a failed compression is raised and leaves the `.open` file for recovery. End to end, a failed compression stops the collector with its cursor saved |
| Uploader request shape | Against botocore's own S3 model, so a wrong parameter name or a mishandled `412` fails the test |
| Event parsing | Real-shaped likes, reposts, follows, posts, deletes, identity and account events become the right columns, with the parsing SQL the job runs. A like's target splits into account, record type and key, post text never comes through, unreadable lines are dropped, `event_time` is UTC to the microsecond, and only true repeats are removed |
| Event store load, end to end | The job's own SQL against a table in a local DuckDB database: loads files and deletes them, loading the same events again adds nothing, a restart's rewind into the next hour's file is matched against the earlier hour, batches load oldest first. The Glue and Iceberg side is covered by the check below and by the real loads |
| Projections | Creates become current records with the creation time decoded from the key. A delete hides its record and keeps only the key, `rev` and time. A delete folded before its create still wins, folding the same commit three times changes nothing, an edit replaces the post in either order, a key reused after a delete is current again, and a delete of a record never seen is only a marker. Other record types and account events aren't projected. Only commits after the last run are folded, and each run is logged. The accounts view picks the latest handle and status |
| Backfill download | Only finished files under the requested day or hour, across pages of results, and nothing already waiting locally is downloaded again |

## Kill test

The real `collect` command ran against a fake server sending 20,000 events, and was stopped three times:

1. killed with `SIGKILL` before its first cursor save,
2. killed with `SIGKILL` again after cursor saves,
3. stopped with `SIGTERM`.

| | |
|---|---|
| Events sent | 20,000 |
| Unique events saved | 20,000 |
| Missing | 0 |
| Duplicate lines from the rewinds | 6,862 |

The test was run again when reading and writing moved to separate threads ([D-028](../decisions/engineering-decisions.md#d-028-read-and-write-on-separate-threads)), with the same result: all 20,000 events saved, none missing.

The first run of this test found a gap: with no cursor saved yet, the restart began from live and lost everything after the last event in the file. Recovery now reports the newest event it rescued and the collector resumes from there ([D-006](../decisions/engineering-decisions.md#d-006-resume-from-the-newer-of-the-cursor-and-the-recovered-data)).

## First run on the real feed

About 7 minutes from a laptop, starting at the live edge, with no reconnects:

| | |
|---|---|
| Events | 188,318, about 476 per second |
| Unreadable lines | 0 |
| Size | 108.6 MB raw, 26.9 MB with zstd (about 4x smaller) |
| Mix | likes 65%, reposts 11.5%, posts 10.9%, follows 9.4% |

## On the server

Checked on the Oracle server after setup:

| Check | Result |
|---|---|
| Collector rate | About 440 to 460 events per second, 0.1 s behind live |
| Memory | About 90 MB |
| Reboot | systemd stopped the collector cleanly (no recovery needed), started it again on boot, and it resumed 5 seconds before its last saved event. The first minute after boot ran at about 650 events per second while it caught up on the minute the server was down, then settled back to normal |
| Upload | The timer's first run uploaded the hour file finished at shutdown (11.1 MB) with the upload-only IAM user. Running it again found nothing to do |

## First checks in SQL

The saved Athena queries in `sql/checks/`, run over the first 4 hours 45 minutes on the server (18:14 to 23:00 UTC):

| dt | hr | Rows stored | Distinct events | Duplicates |
|---|---|---|---|---|
| 2026-10-24 | 18 | 1,176,934 | 1,174,635 | 2,299 |
| 2026-10-24 | 19 | 1,564,621 | 1,564,621 | 0 |
| 2026-10-24 | 20 | 1,536,315 | 1,532,044 | 4,271 |
| 2026-10-24 | 21 | 1,533,174 | 1,533,174 | 0 |
| 2026-10-24 | 22 | 1,497,721 | 1,497,721 | 0 |

- **Missing minutes:** none, including across the reboot.
- **Hour 18:** the 2,299 duplicates are the reboot's 5-second rewind, at about 460 events per second.
- **Hour 20:** the collector's log shows the Jetstream connection dropped twice ("no close frame received or sent"), 50 seconds apart. Each time it reconnected within a second with the 5-second rewind, which accounts for the 4,271 duplicates, and no minute is missing.
- **Hours with no restart or reconnect:** zero duplicates.

Event mix over the same window:

| Record type | Operation | Share |
|---|---|---|
| `app.bsky.feed.like` | create | 66.27% |
| `app.bsky.feed.repost` | create | 11.12% |
| `app.bsky.feed.post` | create | 9.44% |
| `app.bsky.graph.follow` | create | 7.01% |
| `app.bsky.feed.like` | delete | 1.06% |
| `app.bsky.graph.follow` | delete | 1.05% |
| `app.bsky.feed.post` | delete | 0.47% |

Account and identity events were under 0.1% each. Record types from other apps on the network (for example `place.stream.livestream`) show up at well under 0.1% each.

## Compression stall

Compressing a finished hour originally ran on the collector's event loop. Measured on a 400 MB file of realistic events (97 MB compressed):

| | Compression time | Longest event-loop pause |
|---|---|---|
| On the event loop | 2.6 s | 2,629 ms |
| On a background thread | 2.6 s | 1.1 ms |

On the server the hourly file is about 1 GB, which showed up in the log as a 10-second pause at the top of the hour ([D-017](../decisions/engineering-decisions.md#d-017-compress-finished-hours-on-a-background-thread)).

The collector's log at the top of the hour on the server, before and after the change:

| | Before (00:00 UTC) | After (01:00 UTC) |
|---|---|---|
| Next hour's file started | 00:00:10 | 01:00:00 |
| Previous hour's file finished | before the next one started | 01:00:09, after the next one started |

Compressing the hour still takes about 9 seconds, but it now happens next to the stream instead of in front of it. The status line covering the change read 25,937 events in 61 s, 0.1 s behind live, in line with the minutes around it.

## The event store on real data

The first run of the event store job, then on Spark, loaded everything collected so far: the raw files for 21 hours, downloaded back from S3 with `fetch-raw`.

| | |
|---|---|
| Files | 23, in 8 commits of up to 3 files |
| Lines read | 26,294,783 |
| Unreadable | 0 |
| Repeats removed | 13,102 |
| Events stored | 26,281,681 |
| Time | 24.5 minutes on the server's 2 ARM cores, about 18,000 events a second |

**Cross-checks:**

- **Against the SQL checks.** For the hours the Athena duplicates check had already covered, the job's numbers match it exactly: 2,741,555 lines and 2,299 repeats for hours 18 and 19, and 4,567,210 lines and 4,271 repeats for hours 20 to 22.
- **Against Athena.** `SELECT count(*) FROM bluesky_store.events` returns 26,281,681, the same number the job reported adding.

Every repeat traces back to something in the collector's log:

| Repeats | Cause |
|---|---|
| 2,299 | The reboot test |
| 4,271 | Two dropped connections, 50 seconds apart |
| 4,204 | The restart that deployed the compression change. Its two files for hour 00 overlap by the rewind plus up to 5 seconds since the last cursor save |
| 2,328 | Jetstream closing the connection at 14:56:57 |

After the backfill, the timer took over. Each run picks up the hour the uploader handed over 10 minutes earlier:

| | |
|---|---|
| Events per hour | 1.63 to 1.89 million |
| Time per run, on Spark | 91 to 106 seconds, startup included |
| Files left waiting in `uploaded/` | none |

The first hourly runs had repeats too, and again each one lines up with the collector's log:

| Repeats | Cause |
|---|---|
| 9,492 | Four dropped connections in hours 15 and 16, about 2,370 each |
| 2,253 | One dropped connection in hour 17 |

Five of the six drops that day, all but the one at 14:56, happened while the Spark job was running: four during the 25-minute backfill and one during the heaviest hourly run. More on that below.

## Spark against DuckDB

`benchmarks/compare_engines.py` runs the event store's transform with both engines on the same raw files: read the zstd JSON lines, parse the same fields, drop repeats on the same event key, and write Parquet with zstd. Neither writes to Iceberg, so this compares the engines, not the catalog. Each engine runs in its own process, held to 2 threads, and the script checks that both outputs hold the same events (row count, sum of timestamps and number of accounts).

On the server, with one real hour (1,691,007 events, 239 MB compressed), two runs each:

| | DuckDB | Spark |
|---|---|---|
| Time | 11.2 s, 10.5 s | 82.7 s, 81.7 s |
| CPU time | 20 s | 134 to 136 s |
| Peak memory | 1.5 to 1.6 GB | 2.5 to 2.6 GB |
| Output | 130 MB | 151 MB |
| Events | 1,691,007 | 1,691,007 |

Both outputs matched exactly, and the event count is the same one the hourly job added for that hour. This is why the job moved to DuckDB ([D-025](../decisions/engineering-decisions.md#d-025-duckdb-for-the-hourly-job)). The benchmark now compares the two outputs row for row, and they stay identical with the leaner columns of [D-026](../decisions/engineering-decisions.md#d-026-a-leaner-event-store).

## DuckDB writing to Glue

Before the job moved to DuckDB, a throwaway script checked every operation it and the projections need, against a test table in the Glue catalog:

| Step | Result |
|---|---|
| Create a table partitioned by hour | OK. Glue's endpoint needs the table's location given explicitly |
| Append 2 rows | 2 added |
| Insert-only `MERGE` of 1 existing and 1 new row | 1 added |
| The same `MERGE` again | 0 added |
| `MERGE` that updates one row and deletes another | 2 changed |
| Read back in DuckDB | the expected 2 rows |
| Read back in Athena | the same 2 rows, so Athena applies DuckDB's delete files |

## DuckDB and the projections

A second throwaway script checked what the projections would need, against two test tables in Glue. For each query it counted the data files DuckDB actually downloaded, from its HTTP log:

| Step | Result |
|---|---|
| Add a column to an existing table | Works, and the older rows read as empty |
| Filter on the new column | 4 of 5 files read: files written before the column existed have no statistics for it, so they can't be skipped |
| List the table's files with the commit that added each | `iceberg_metadata` gives every file's sequence number |
| Filter on record type and month, in a table partitioned by both | 1 of 6 files read |
| `MERGE` that only applies a newer `rev`, including turning a row into a deleted marker | Correct, in 1 commit |
| How `MERGE` finds the rows it changes | Reads every file whose keys fall in the batch's key range: 4 of 6 |
| Delete then insert in one transaction | 2 commits, so not atomic |

The last three are why the projections append instead of updating rows ([D-029](../decisions/engineering-decisions.md#d-029-projections-are-appended-to-not-updated-in-place)), and the first three why they find new commits by sequence number ([D-030](../decisions/engineering-decisions.md#d-030-new-store-commits-are-found-by-their-iceberg-sequence-number)).

## The event store on DuckDB

The table was rebuilt with the leaner columns by the DuckDB job, from the 35 raw files S3 had at the time:

| | |
|---|---|
| Files | 35, in 12 commits of up to 3 files |
| Unreadable lines | 0 |
| Repeats removed | 27,360 |
| Events stored | 46,578,576 |
| Time | 25 minutes |

The first seven batches covered the same files as the Spark backfill and matched it exactly, line for line and repeat for repeat. The 25 minutes was no faster than Spark's, because the largest 3-hour batches (over 5 million lines) hit DuckDB's 3 GB memory limit and spilled to disk. Batches of 3 to 4 million lines took 33 to 74 seconds; those over 4.5 million took 144 to 343.

Once the timer took over, one file an hour:

| | |
|---|---|
| Events per hour | 0.92 to 1.65 million |
| Time per run | 10 to 16 seconds |
| Unreadable lines | 0 |

Every repeat in these loads again lines up with a dropped connection in the collector's log:

| Repeats | Cause |
|---|---|
| 4,630 | Drops at 14:56 and 15:53 |
| 9,443 | Three drops between 16:02 and 16:08, and one at 17:20 |
| 2,513 | A drop at 01:34 |
| 1,707 | A drop at 03:58 |
| 11,445 | Seven drops between 04:01 and 04:19 |

The saved `store-vs-raw` query checks the whole path end to end, independently of the job: Athena counts the distinct events in the raw JSON files hour by hour, with the same event key, and compares them with the rows in the event store. Run for the first full day the store held, 25 October:

| | |
|---|---|
| Hours | 24 |
| Distinct raw events | 32,640,195 |
| Rows in the event store | 32,640,195 |
| Hours with an event missing from the store | 0 |
| Hours with an extra row in the store | 0 |

Every hour matched exactly, from 0.90 million events in the quietest hour (08:00) to 1.75 million in the busiest (21:00). Hour 20:00 is the 1,691,007 events used in the engine comparison above.

## Connection drops and heavy jobs

The collector's log shows a pattern in when Jetstream connections drop. Most drops are abrupt: the connection ends without a close frame.

| When | Time | Drops |
|---|---|---|
| Spark backfill | 25 minutes | 4 |
| Heaviest Spark hourly run | 4 minutes | 1 |
| DuckDB backfill, including downloading the raw files | 27 minutes | 8 |
| Everything else, from the first event to the afternoon of the third day | about 43 hours | 5 |

That's about 14 drops an hour during a long, heavy job against about one every 9 hours the rest of the time, including during the normal 10 to 16 second hourly runs. One explanation fits: the collector syncs its file to disk every 5 seconds on its main loop, and while the disk is busy with a job's reads and spills, that sync can stall. While it's stalled, nothing reads from the connection, and Jetstream may drop a client that falls behind. No events were lost either way: every drop was followed by a reconnect with the 5-second rewind. To check the explanation, the sync moved to its own thread and the collector started logging slow syncs and any pause of its main loop ([D-027](../decisions/engineering-decisions.md#d-027-sync-to-disk-on-its-own-thread)). Then a heavy job was run on purpose.

## The heavy-job test

The same load as a backfill, on purpose: `fetch-raw` downloaded all of 25 October from S3, about 5.5 GB, and `process` loaded it in batches of 3 files into a throwaway local DuckDB file, so the real event store wasn't touched. The download took under 2 minutes and the load 9.

The test was run twice, once with the sync on its own thread ([D-027](../decisions/engineering-decisions.md#d-027-sync-to-disk-on-its-own-thread)) and again after reading and writing were split ([D-028](../decisions/engineering-decisions.md#d-028-read-and-write-on-separate-threads)). Both times the load added 32,640,195 events, exactly the number of distinct events Athena's `store-vs-raw` check counted for that day, so the job and the independent check agree to the event.

The collector during each run:

| | Normal | Sync on its own thread | Reading and writing split |
|---|---|---|---|
| Longest sync | 0.01 to 0.04 s | 40 s, and 27 syncs over 1 s | 45 s, and 24 syncs over 1 s |
| Longest main-loop pause | 0.00 s | 5.2 s, and 13 pauses of 2 s or more | **0.05 s** |
| Longest the queue got | | | 20,753 events |
| Behind live | 0.1 s | | 0.1 s throughout |
| Events a second | about 480 | 470 to 525 | 460 to 550 |
| Connection drops | | 0 | 0 |

- **The explanation holds.** A sync that normally takes a few hundredths of a second took up to 40 seconds while the disk was busy. Before D-027 each of those ran on the main loop, so nothing read from the connection for that long.
- **The sync thread helped.** The collector kept reading at its normal rate throughout and the connection stayed up.
- **It wasn't the whole story.** The main loop still stopped for up to 5 seconds at a time, because the writes themselves were still on it, and when the disk is overloaded Linux makes writes wait too. That's why reading and writing were then split onto separate threads ([D-028](../decisions/engineering-decisions.md#d-028-read-and-write-on-separate-threads)).
- **With them split, the disk no longer reaches the connection.** The disk was just as slow, with one sync taking 45 seconds, but the longest the main loop paused in any minute was 0.05 seconds, and the newest event received was never more than 0.1 seconds behind live. The delay showed up where it should: the queue grew to 20,753 events during the 45-second sync, about what 45 seconds of events adds up to, and emptied again within a minute. Back to normal after the job, the queue stays at around 100 to 300 events.
