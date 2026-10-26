# bluesky-stream-analytics

## What I'm building

Bluesky has a public live feed called Jetstream. It streams pretty much everything public happening on the network as it happens: posts, likes, reposts, follows, profile changes, deletes. No login needed to read it.

The goal is to collect that feed nonstop and turn it into the kind of numbers a product team would actually look at, then put them on a simple public dashboard anyone can open and poke around in.

Questions I want to be able to answer:

- How many people are active on Bluesky each day, week and month?
- Of the accounts that showed up this week, how many are still around a day later? A week? A month?
- Are a small number of accounts getting most of the likes and reposts, or is it spread out?
- What's trending right now, and how fast do things rise and fade?
- What does a new account do in its first few days that makes it more likely to stick around?

## Why this

I wanted to work with a live stream that never stops, not a dataset that just sits there. And this feed is messy in interesting ways:

- the same event can show up twice
- things can arrive out of order, so a reply might show up before the post it's replying to
- there's no easy way to tell if you missed something
- people delete stuff, and I need to respect that

Getting correct numbers out of all that is the part I'm most curious about.

## Where it's at

So far I've got a collector that connects to the feed, saves every event to hourly compressed files, and remembers where it left off. I can kill it in the middle of a run, restart it, and nothing goes missing. I tested that by killing it on purpose: 20,000 events sent, all 20,000 saved. That test also caught a bug where a crash in the first few seconds left a gap, which is fixed now.

First look at the real feed: around 475 events a second, about two thirds of them likes, and the files shrink about 4x when compressed.

It now runs around the clock on a free Oracle Cloud ARM server, as a systemd service that comes back on its own after a crash or a reboot. I rebooted the server on purpose to check: it picked up 5 seconds before where it stopped and caught up on the missed minute within the next one. Every 10 minutes, finished hourly files get uploaded to S3 by an AWS user that's only allowed to upload, and the AWS side (bucket, permissions, budget alert) is set up with Terraform. Tests and Terraform checks run on every pull request.

The data in S3 can now be queried with SQL in Athena, straight from the files, with partitions picked up automatically and a cap on what any query can cost. The first checks found no missing minutes, and the only duplicates lined up exactly with a reboot and two dropped connections, which is what the collector is designed to do.

The design is event-sourced rather than the usual bronze/silver/gold: the feed is a log of things that happened, so the log is the source of truth and everything else (current state, metrics) is derived from it and can be rebuilt by replaying it. [Here's why](docs/architectures/end-to-end-pipeline.md#architecture).

The event store is done: an hourly job on the same server appends each finished hour to an Iceberg table in S3, with every event exactly once and no post text. Raw files now expire after 14 days, partly to save money, but mostly because they hold text people may have deleted.

I built the job in Spark first, then ran the same work through DuckDB on a real hour: identical output, 11 seconds instead of 82. So it runs on DuckDB now, and each hour loads in 10 to 16 seconds. [The decision log](docs/decisions/engineering-decisions.md#d-025-duckdb-for-the-hourly-job) has the numbers. Measuring the table's columns also showed one unused hash was 42% of its size, so that's gone too, and the table went from 77 bytes an event to 31.

## The plan

1. ~~Connect to the feed and watch events come in~~ 
2. ~~Save every event without losing anything if it crashes~~ 
3. ~~Run it 24/7 in the cloud, with files going to S3~~
4. ~~Make the saved data queryable with SQL~~
5. Turn the log into proper tables: an event store with each event once (built), then projections of current state with deletes applied (next)
6. Build the product metrics on top, as a star schema
7. Add monitoring so I know when something breaks
8. Put up the public dashboard

This will probably change as I learn more.

## Ground rules

- Only public data, and I never interact with anyone on the network.
- Nothing about individual people gets published, only totals.
- If someone deletes something, I delete it too.
- Costs stay tiny: free tiers and a small budget.

## Running it

You'll need [uv](https://docs.astral.sh/uv/).

```bash
uv run watch                      # live summary of the stream every 10 seconds, nothing saved
uv run collect                    # save every event under ./data
uv run upload --bucket <bucket>   # copy finished hourly files to S3
uv run pytest                     # run the tests
```

```bash
uv run process --catalog local --database events.duckdb      # append data/uploaded/ to a local DuckDB table
uv run process --warehouse s3://<bucket>/warehouse           # the same, into the Iceberg table in Glue
uv run fetch-raw --bucket <bucket> --date 2026-10-24         # download a day of raw files to load again
uv run project --warehouse s3://<bucket>/warehouse           # fold new event-store commits into the projections
uv run --group bench python benchmarks/compare_engines.py <raw files>   # Spark against DuckDB, needs Java 17 or 21
```

The AWS setup lives in `infra/aws/` as Terraform. The SQL in `sql/` gets saved into Athena by Terraform: open the Athena query editor, pick the `bluesky-stream-analytics` workgroup, run `create-events-view` once, then any of the checks.

## More detail

**Architecture**
- [End-to-end pipeline](docs/architectures/end-to-end-pipeline.md): how the pieces fit together
- [Collector](docs/architectures/collector.md): saving every event and surviving crashes
- [Uploader](docs/architectures/uploader.md): getting files safely into S3
- [Event store](docs/architectures/event-store.md): the hourly job and the table with every event once
- [Query layer](docs/architectures/query-layer.md): SQL with Athena
- [Infrastructure](docs/architectures/infrastructure.md): the Oracle server and the AWS side

**Decisions**
- [Engineering decisions](docs/decisions/engineering-decisions.md): why I built it this way

**Validation**
- [Testing](docs/validation/testing.md): tests, the kill test, real runs on the server, Spark against DuckDB, and a pattern in connection drops
- [CI](docs/validation/ci.md)
