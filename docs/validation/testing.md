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
