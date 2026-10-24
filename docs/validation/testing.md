# Testing

## Automated tests

`uv run pytest` runs everything below. None of it needs a network connection or an AWS account.

| Area | What's checked |
|---|---|
| Cursor | Save and load, missing file means start live, a corrupt file raises instead of starting live, no temp file left behind |
| Writer | Rollover at the hour boundary, an earlier-hour event stays in the current file, compressed output matches the input, unique file names per run |
| Crash recovery | A half-written last line is cut off and every complete line kept, a crash after compression, leftover temp and empty files |
| Collector, end to end | Against a fake Jetstream on localhost that honours `?cursor=`: collects everything, survives a dropped connection, restarts from the saved cursor, resumes from recovered data newer than the cursor |
| Uploader | Against a fake S3: exact bytes stored, an existing object is never overwritten, a failed upload keeps the local copy, one failure doesn't stop the rest |
| Uploader request shape | Against botocore's own S3 model, so a wrong parameter name or a mishandled `412` fails the test |

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
