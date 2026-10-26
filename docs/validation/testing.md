# Testing

## Automated tests

`uv run pytest` runs everything below. None of it needs a network connection or an AWS account. The Spark tests need Java 17 or 21, and the Iceberg ones need the runtime jar in `.jars/` (`deploy/fetch-jars.sh` downloads it); without the jar they're skipped.

| Area | What's checked |
|---|---|
| Cursor | Save and load, missing file means start live, a corrupt file raises instead of starting live, no temp file left behind |
| Writer | Rollover at the hour boundary, an earlier-hour event stays in the current file, compressed output matches the input, unique file names per run |
| Crash recovery | A half-written last line is cut off and every complete line kept, a crash after compression, leftover temp and empty files |
| Collector, end to end | Against a fake Jetstream on localhost that honours `?cursor=`: collects everything, survives a dropped connection, restarts from the saved cursor, resumes from recovered data newer than the cursor |
| Uploader | Against a fake S3: exact bytes stored, an existing object is never overwritten, the local copy moves to `uploaded/` only after S3 has it, one failure doesn't stop the rest |
| Background compression | A rollover returns without waiting for compression, close waits for it, and a failed compression is raised and leaves the `.open` file for recovery. End to end, a failed compression stops the collector with its cursor saved |
| Uploader request shape | Against botocore's own S3 model, so a wrong parameter name or a mishandled `412` fails the test |
| Event parsing | Real-shaped likes, reposts, follows, posts, deletes, identity and account events become the right typed columns. Post text never comes through, unreadable lines are dropped, `event_time` is UTC, and only true repeats are removed |
| Event store load, end to end | Against a local Iceberg catalog: loads files and deletes them, loading the same events again adds nothing, a restart's rewind into the next hour's file is matched against the earlier hour, each batch is its own commit, rows are partitioned by event hour |
| Merge stays within its hours | An earlier hour's data files are deleted from disk before the next load. The merge still succeeds, so it isn't reading them. The same setup with overlapping hours fails, which shows the check works |
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

The first run of the event store job loaded everything collected so far: the raw files for 21 hours, downloaded back from S3 with `fetch-raw`.

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
| Time per run | 91 to 106 seconds, Spark startup included |
| Files left waiting in `uploaded/` | none |

The first hourly runs had repeats too, and again each one lines up with the collector's log:

| Repeats | Cause |
|---|---|
| 9,492 | Four dropped connections in hours 15 and 16, about 2,370 each |
| 2,253 | One dropped connection in hour 17 |

**Open question:** these drops were abrupt (the connection ended without a close frame). Five of the six on this day happened while the Spark job was running: four during the 25-minute backfill and one during the heaviest hourly run. The eight lighter hourly runs after that had none, and the two drops on the first day happened with no Spark job at all. That's not enough to call it a cause. Reconnects per hour, and whether the job was running at the time, will be tracked once monitoring exists.

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

Both outputs matched exactly, and the event count is the same one the hourly job added for that hour. What this means for the choice of engine is in [D-019](../decisions/engineering-decisions.md#d-019-spark-in-local-mode-on-the-server).
