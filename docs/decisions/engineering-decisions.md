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
