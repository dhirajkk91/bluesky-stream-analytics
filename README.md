# bluesky-stream-analytics

getting started

## The idea

Bluesky has a public live feed called Jetstream. It streams pretty much everything public happening on the network as it happens: posts, likes, reposts, follows, profile changes, deletes. No login needed to read it.

I want to collect that feed all the time and turn it into the kind of numbers a product team would actually look at. Eventually I'd like a simple dashboard anyone can open and poke around in.

## Questions I want to be able to answer

- How many people are active on Bluesky each day, week and month?
- Of the accounts that showed up this week, how many are still around a day later? A week? A month?
- Are a small number of accounts getting most of the likes and reposts, or is it spread out?
- What's trending right now, and how fast do things rise and fade?
- What does a new account do in its first few days that makes it more likely to stick around?

## Why this

I want to learn how to work with a live data stream that never stops, not just a dataset that sits there. And this feed is messy in interesting ways:

- the same event can show up twice
- things can arrive out of order, so a reply might show up before the post it's replying to
- there's no easy way to tell if you missed something
- people delete stuff, and I need to respect that

Figuring out how to still get correct numbers out of all that is the part I'm most curious about.

## How I'm planning to build it

Roughly:

1. Connect to the feed and just watch events come in on my laptop
2. Save them to files without losing anything if it crashes
3. Move it to a cloud server so it runs 24/7
4. Make the saved data queryable with SQL
5. Clean it up into proper tables (dedupe, handle deletes, late events)
6. Build the product metrics on top
7. Add monitoring so I know when something breaks
8. Put up the public dashboard

This list will probably change as I go.

## Ground rules

- Only public data, and I never interact with anyone on the network.
- Nothing about individual people gets published, only totals.
- If someone deletes something, I delete it too.
- I keep costs tiny. This runs on free tiers and a small budget.

## Status

Idea stage. Nothing built yet.