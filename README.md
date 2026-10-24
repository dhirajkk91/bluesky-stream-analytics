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

Finished files now get uploaded to S3, and the AWS side (bucket, permissions, budget alert) is set up with Terraform. Tests and Terraform checks run on every pull request.

What's not done yet: it still runs on my laptop. Next up is moving it to an always-on server so it can collect around the clock.

## The plan

1. ~~Connect to the feed and watch events come in~~ 
2. ~~Save every event without losing anything if it crashes~~ 
3. Run it 24/7 in the cloud, with files going to S3 (S3 part done, server next)
4. Make the saved data queryable with SQL
5. Clean it up into proper tables (dedupe, handle deletes, late events)
6. Build the product metrics on top
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

The AWS setup lives in `infra/aws/` as Terraform.

## More detail

**Architecture**
- [End-to-end pipeline](docs/architectures/end-to-end-pipeline.md): how the pieces fit together
- [Collector](docs/architectures/collector.md): saving every event and surviving crashes
- [Uploader](docs/architectures/uploader.md): getting files safely into S3
- [Infrastructure](docs/architectures/infrastructure.md): the Oracle server and the AWS side

**Decisions**
- [Engineering decisions](docs/decisions/engineering-decisions.md): why I built it this way

**Validation**
- [Testing](docs/validation/testing.md): tests, the kill test, and the first real run
- [CI](docs/validation/ci.md)
