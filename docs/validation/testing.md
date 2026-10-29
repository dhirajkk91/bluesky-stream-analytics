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
| Projections | Creates become current records with the creation time decoded from the key. A delete hides its record and keeps only the key, `rev` and time. A delete folded before its create still wins, folding the same commit three times changes nothing, an edit replaces the post in either order, a key reused after a delete is current again, and a delete of a record never seen is only a marker. Other record types and account events aren't projected. Only commits after the last run are folded, commits that only rewrote rows are skipped, and each run is logged with its oldest delete. The accounts view picks the latest handle and status |
| Handle lookups | Against a fake API: 25 accounts a call and each account once, accounts the API doesn't return get no handle, a 429 waits as long as asked and a 503 doubles, five failures stop the run, a 400 is retried account by account, and calls are spaced to two a second. Against local store files: each new commit is looked up once, with the newest identity event per account, and runs are logged |
| Marts | `dbt build` on DuckDB over a few hand-made days of four accounts: daily, weekly and monthly active, new accounts, volumes, engagement received without self-engagement, top 1% share and Gini, retention only for days that have happened, a new account version for each handle and status change, with handles taken from the lookups, not the identity events. For posts: a placeholder for a parent never seen, with its author and creation time; engagement per post and by the post's age in hours; replies to unseen posts; posts by language; and a placeholder giving way to the real post when its create arrives late. An incremental build redoes the recent days with late events and adds the new day, without duplicates. Every dbt test in the project runs too |
| Compaction | Run as SQL against local tables: removes rows with a newer delete or version within reach, and nothing current; redacts the store events of deleted records only, keeping every event; leaves records out of the tier's reach alone until a wider tier; running again changes nothing; the handles of deleted accounts are forgotten. Which tier runs on which date and how far each reaches. A failed step stops the run and is logged, and every step is logged in one statement |
| Monitoring | The ping URL for each check, a missing key skips pinging, a failure ping carries the end of the log, and a ping that can't get through is retried and never raises. The health check passes a healthy server and catches a stalled collector, a missing or broken cursor, files waiting too long to upload or be processed, and a full disk. Every scheduled job's unit pings its check and alerts on failure |
| Dashboard export | Run on the sample marts: the first, partial day is marked, and the post age curve and languages cover the whole days. Signup days under 100 accounts are left out, and languages under 100 posts a day fold into "other". How long deletes stayed visible, and Athena's results read back with their types across pages |
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

## The projections on real data

The first run of `project` folded everything the event store held, one store commit at a time:

| | |
|---|---|
| Store commits | 28, in 49 files |
| Events folded | 66,291,600 commits of the five projected record types |
| Appended | 63,930,039 record versions and 2,361,561 delete markers |
| Time | about 10 minutes |

The next hourly run folded its one new commit in 14 seconds, right after the event store load.

The saved `projections-vs-store` query then recounted, straight from the event store and with separate SQL, how many records should exist now, and compared that with the `current_records` view:

| Record type | Current records | Difference | Deletes of records never seen | No creation time |
|---|---|---|---|---|
| Likes | 45,746,017 | 0 | 408,684 | 84 |
| Reposts | 7,378,265 | 0 | 173,360 | 339 |
| Posts | 6,812,274 | 0 | 241,571 | 15,020 |
| Follows | 4,138,162 | 0 | 707,802 | 47 |
| Blocks | 638,973 | 0 | 7,291 | 0 |

- **Every type matches**, 64.7 million records in all.
- **Most deletes are of records created before collection started**: about 1.54 million, around 65% of all deletes so far. Follows show it most, 708 thousand unfollows against 4.1 million current follows, since follows are long-lived. Those deletes are the reason for markers ([D-031](../decisions/engineering-decisions.md#d-031-the-highest-rev-wins-and-deletes-leave-a-marker)): without one, a replayed create could bring the record back.
- **Records with no creation time are almost all valid TIDs with impossible times.** 15,485 of the 15,490 have TID-shaped keys, but decode to the first seconds of 1970 or to centuries from now (2174 to 2540 in the examples checked). So some apps create keys from a counter or random bits rather than the clock. The other 5 aren't TIDs at all. My first guess, posts imported from other sites with their original dates, was wrong: those would decode to real past dates.

## Compaction measured by hand

Before automating compaction, a throwaway script ran each step once in Athena against the real tables and recorded its time and bytes scanned: remove every row with a newer delete or version, `OPTIMIZE` the files, `VACUUM` old snapshots. It ran twice, about an hour and a half apart.

**First run.** `records` went from 65,585,286 rows to 64,713,691: 871,595 superseded rows removed, leaving exactly the number of current records the check had counted.

**Second run**, with two hours of new data since:

| | Before | After |
|---|---|---|
| `records` | 132 files, 68,065,291 rows | 78 files, 68,019,989 rows |
| `record_deletes` | 155 files | 5 files |
| `events` | 52 files, about 44 MB each | unchanged |
| Rows hidden by a newer delete | 45,132 | 0 |
| Older versions | 186 | 0 |

`records` ended with exactly one row per current record, and `projections-vs-store` still matched for every record type.

| Step | Time | Scanned |
|---|---|---|
| Remove rows hidden by a newer delete | 11 s | 2,662 MB |
| Remove older versions | 11 s | 3,653 MB |
| `OPTIMIZE` records | 135 s | 2,622 MB |
| `OPTIMIZE` record_deletes | 11 s | 62 MB |
| `VACUUM`, each table | 6 to 11 s | 0 |

The removals read the whole table even with only two hours of changes, about 9 GB for a 2.6 GB table. That's what made compaction tiered ([D-033](../decisions/engineering-decisions.md#d-033-compaction-is-tiered-by-record-type-and-age-to-fit-the-budget)).

**A false alarm worth keeping.** Looking only at the second run, the numbers didn't add up: the store held the creates of 912,696 records that were later deleted, but `records` had held only 45,132 of them. It looked as if DuckDB had lost rows on the way in. Iceberg's own history of the table (`records$snapshots`) settled it in one query: every append had added exactly the rows its log line reported, and the missing rows had been removed by the first run, which I'd lost track of. The lesson: check a table's commit history before suspecting the writer.

**How old records are when they're deleted.** Every delete marker carries the record's creation time decoded from its key, so this covers all 2,512,516 deletes so far, including those of records created before collection began:

| Age at delete | Deletes | Share |
|---|---|---|
| under a day | 919,187 | 36.6% |
| 1 to 7 days | 395,239 | 15.7% |
| 7 to 31 days | 265,647 | 10.6% |
| older | 932,381 | 37.1% |
| unknown (no creation time) | 62 | 0.0% |

**Redaction, counted but not run:** the event store held 912,770 creates and edits of records that were later deleted, the rows redaction would blank.

## Compaction in production

After the projections were rebuilt with day partitions, the first compaction ran the monthly tier by hand, so it reached everything, including the 912,770 store events of deleted records waiting to be redacted. Then the checks, all in Athena:

| Check | Result |
|---|---|
| `projections-vs-store` | 0 difference for every record type |
| Store events of deleted records with a subject, reply link or languages left | 0 |
| Rows in `records` hidden by a newer delete | 0 |
| `store-vs-raw` for 26 October, after redaction | every hour matches the raw files: no event lost or added |

The nightly timers took over from there. The first daily run and the vacuum two hours later ran on their own, and the hourly runs after compaction folded only their own hour: the store commits compaction made were skipped. Cost and time per run are in [compaction](../architectures/compaction.md#cost).

**Time to hide a deleted record**, from `projection_runs` over the first 13 hourly runs: 85 minutes on average, 141 at worst. The worst was an hour that missed its run because Jetstream stalled ([incident](incidents.md#jetstream-went-quiet-for-15-minutes-27-october)).

## New accounts, checked

The feed has no signup event, so the marts treat an account's first profile record as its signup ([D-037](../decisions/engineering-decisions.md#d-037-active-means-wrote-something-and-signups-are-inferred)). Before relying on that, two queries on the event store:

| Day | Active writers | Profiles created | Identity events | Account events |
|---|---|---|---|---|
| 25 October | 1,253,076 | 19,200 | 26,989 | 29,271 |
| 26 October | 1,322,431 | 18,521 | 25,041 | 32,908 |

Of the 18,521 accounts that created a profile on 26 October:

| | Accounts | Share |
|---|---|---|
| Had written anything more than an hour before | 29 | 0.2% |
| Had a handle set (an identity event) within the hour | 18,422 | 99.5% |

A new account gets its handle and its profile at almost the same moment, and an existing account would have shown up writing earlier. So profile creation with nothing earlier is a sound signup signal, at about 18,500 to 19,000 new accounts a day.

## The marts and the handle lookups in production

The first marts build in Athena passed all of its tests, and its daily active writers matched the direct query above exactly: 1,253,076 on 25 October and 1,322,431 on the 26th. Its new accounts, 19,196 and 18,511, are the profiles created those days less the handful of accounts seen writing earlier. More in [marts](../architectures/marts.md#first-build).

That build also showed a gap the sample data had hidden: `dim_account` had handles for 3 of 1.8 million accounts. Jetstream's identity events carry the handle in 28 of 74,610 cases, so handles are now looked up ([D-039](../decisions/engineering-decisions.md#d-039-handles-are-looked-up-not-read-from-the-feed)). The first lookup run found 72,319 of 83,466 accounts, and after a rebuild `dim_account` had handles for 71,673 accounts. More in [handle lookups](../architectures/handles.md#in-production).

## Monitoring in production

- **Healthy:** the first `check-health` run reported "newest event 1s old, 0 files to upload, 0 files to process, disk 7% used", and its ping reached healthchecks.io.
- **A failure:** starting `bluesky-alert@bluesky-upload.service` by hand sent a fail ping with the last 40 lines of the upload log. The check went red, the email arrived with the log in it, and the next real upload turned it green with a recovery email.
- **Every job:** each check turned green on its job's first run, except `bluesky-marts`, whose timer turned out never to have been enabled ([details](../architectures/monitoring.md#in-production)).

## The dashboard in production

The first publish failed twice. The first time, the workflow couldn't find its two settings, so it never asked AWS for anything. The second time AWS refused the token: the trust policy expected the repository as `dhirajkk91/bluesky-stream-analytics`, but GitHub's tokens name it with numeric IDs too, `dhirajkk91@72024303/bluesky-stream-analytics@1426294014`. CloudTrail logs every refused attempt with the token's subject, which is how the real value turned up. The policy now matches it exactly. Once live, the page said deletes stayed visible for 538 minutes on average, with a longest of 3,320. The average had included the 34 runs that rebuilt the projections back to back, each folding deletes from up to two days earlier. Leaving out runs that start within 30 minutes of the one before brought it back to what the scheduled runs show: 80 minutes, and 141 when an hour's file missed its run.
