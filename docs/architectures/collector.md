# Collector

## Purpose

`collect` keeps a connection open to Jetstream and saves every event to hourly files on disk. It can be killed at any moment, by a crash, a reboot or a deploy, and the next start picks up where it left off without a gap.

It runs as two parts that only meet at a queue: an asyncio loop that reads from the connection, and a writer thread that does all the disk work. A busy disk slows the writer down but never the reading ([D-028](../decisions/engineering-decisions.md#d-028-read-and-write-on-separate-threads)).

## Goals and non-goals

Goals:

- Store every event exactly as received, so later layers can always be rebuilt from the raw files.
- Never lose an event that was written to disk, whatever point the process dies at.
- Resume after any stop without a gap, accepting a few seconds of duplicates to get there.

Non-goals:

- Removing duplicates, parsing records or handling deletes. Later layers do that from the raw files.
- Failing over to another Jetstream server. Cursors don't line up across servers.

## Design

```mermaid
flowchart TD
    A([start]) --> B["Recover files left by a crash"]
    B --> C["Resume point = newer of<br/>saved cursor and newest recovered event"]
    C --> D{"Resume point?"}
    D -- none --> E["Connect at the live edge"]
    D -- yes --> F["Connect with ?cursor=<br/>resume point − 5 s"]
    E --> G
    F --> G["For each message:<br/>read time_us, put it on the queue"]
    G -->|"connection drops"| H["Wait 1, 2, 4 … 60 s with jitter"]
    H --> F
    G -->|"SIGTERM / Ctrl+C"| I["Writer finishes the queue and the file,<br/>saves the final cursor"]

    G -.-> Q[("Queue<br/>up to 200,000 events")]
    Q -.-> W["Writer thread:<br/>append to the current hour's file,<br/>every 5 s fsync it, then save the cursor"]
```

1. **Recover.** Files left behind by a run that didn't shut down cleanly are finished first (see [A file's life](#a-files-life)).
2. **Pick a resume point.** The newer of the saved cursor and the newest event found in recovered files. The second matters because events written after the last cursor save are still in the file.
3. **Connect.** With a resume point, connect with `?cursor=` set 5 seconds before it. Without one, start from live.
4. **Queue.** Each message is parsed just enough to read `time_us`, and the raw text goes on a queue. That's all the main loop does with it.
5. **Write.** The writer thread takes events off the queue and appends them to the current hour's file, rolling over to a new file when a later hour starts.
6. **Checkpoint.** Every 5 seconds the writer thread forces the file onto disk and only then saves the cursor, so the cursor can never point past data that isn't on disk. If the disk is slow, events simply wait in the queue meanwhile.
7. **Full queue.** The queue holds up to 200,000 events, about 7 minutes. If it ever fills, the main loop stops reading until there's room. Nothing is lost: Jetstream holds the events, or drops the connection and replays them on reconnect.
8. **Status.** Every 60 seconds it logs events received, the rate, how far behind live the newest received event is, the longest the queue got, how far behind live the newest event on disk is, and the longest sync and longest main-loop pause in that minute. A sync over 1 second, the main loop being blocked for 2 seconds or more, and a full queue are logged as warnings straight away.
9. **Reconnect.** If the connection drops, it waits (doubling up to 60 seconds, randomized within each step) and resumes from the newest event received, minus the rewind. Events still in the queue will be written anyway, so they don't need to be fetched again. The wait resets after a successful connection.
10. **Shut down.** `SIGTERM` or Ctrl+C cancels the connection. The writer thread writes everything still queued, finishes the current file and saves the final cursor.

## What is stored

```text
<data dir>/
├── events/
│   └── date=YYYY-MM-DD/
│       ├── hour=13/
│       │   └── 1761224400123456_3f9a1c2e.jsonl.zst    finished, never modified again
│       └── hour=14/
│           └── 1761228000004211_3f9a1c2e.jsonl.open   hour in progress
└── state/
    └── cursor.json                                     {"time_us": 1761228012345678}
```

- **Format.** JSON Lines: one raw event per line, exactly as Jetstream sent it.
- **Folders.** `date=…/hour=…` is the Hive partition layout, so Athena can read only the hours a query needs.
- **File names.** The `time_us` of the file's first event plus a random id per collector run, so two runs never write to the same file.
- **Which folder an event lands in.** It follows the event's `time_us`, but a file only moves forward in time. A replayed event from the previous hour goes into the current file instead of reopening a finished one. Readers should use each event's `time_us`, not the folder.

## A file's life

```mermaid
stateDiagram-v2
    direction LR
    [*] --> Open: first event of a new hour
    Open --> Open: append events
    Open --> Compressing: next hour starts / shutdown<br/>(closed and synced, then compressed<br/>on a background thread)
    Compressing --> Finished: synced, renamed to .jsonl.zst
    Finished --> [*]: uploaded and deleted
    Open --> Recovering: process killed
    Recovering --> Compressing: half-written last line cut off
```

1. During its hour, events are appended to `<name>.jsonl.open`, uncompressed.
2. When the next hour starts, or on shutdown, the writer thread flushes, syncs and closes the file, so its events are on disk before anything else happens. A separate compression thread then compresses it to `<name>.jsonl.zst.tmp`, syncs it, renames it to `<name>.jsonl.zst` and deletes the `.open` file. Writing carries on meanwhile; on shutdown it waits for the compression to finish.
3. If the process dies before that, the next start-up recovers it:
   - a `.zst.tmp` is an unfinished compression, so it's deleted;
   - an `.open` whose `.zst` already exists means the crash came after compression, so the `.open` is deleted;
   - any other `.open` gets its half-written last line cut off, then goes through step 2.

Appending uncompressed until the hour is over means a crash can only ever damage the last line of a file.

## Failure handling

| What happens | Result |
|---|---|
| Process killed mid-write | Recovery keeps every complete line, drops the partial one, and resumes from the newest saved event minus 5 seconds |
| Killed before the first cursor save | No cursor yet, but recovery finds the newest event in the file and resumes from there |
| Connection drops | Reconnects with backoff, resuming from the newest received event. How often drops happen, and a pattern in when, is in [testing](../validation/testing.md#connection-drops-and-heavy-jobs) |
| Cursor file unreadable | Stops with an error instead of starting from live, which would leave a gap |
| Unreadable message (no `time_us`) | Skipped and counted in the status log |
| Disk slow, for example while another job is busy with it | Only the writer thread waits. Events keep being read and pile up in the queue, then get written once the disk catches up. Slow syncs are logged, and the status line shows how long the queue got |
| Disk stuck for minutes | The queue fills and the main loop stops reading until there's room. Jetstream may drop the connection; the reconnect replays from the newest received event |
| Writing fails (for example, disk full) | The writer thread stops, the collector stops with the error and saves the cursor of the last sync. systemd restarts it |
| Compression fails (for example, disk full) | The collector stops with the error and saves its cursor. systemd restarts it, and recovery compresses the leftover `.open` file |
