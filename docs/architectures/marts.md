# Marts

## Purpose

The marts are where the product metrics come from: how many accounts are active, how many are new and whether they stay, what people do, and how concentrated attention is. They're a Kimball star schema built with dbt from the [event store](event-store.md), so each metric is defined once, in SQL, with tests.

This is the first phase: everything at the level of accounts. Posts, threads and replies whose parent we never saw come next.

## Goals and non-goals

Goals:

- Every metric defined once, in one place, with its caveats written next to it.
- Rebuilt daily for the day before, at the cost of a cent or so.
- Tested twice: on the real data in Athena, and on a hand-made sample in CI with numbers worked out by hand.

Non-goals:

- Publishing anything. The marts are private; only aggregates leave, through the dashboard, with small groups suppressed.
- Real time. Daily is enough for these metrics, and keeps the cost flat.

## Design

```mermaid
flowchart LR
    EV[("bluesky_store.events")]
    AH[("bluesky_store.account_handles")]
    subgraph dbt["dbt, run in Athena at 04:30 UTC"]
        SC["stg_commits<br/>writes, classified"]
        SA["stg_account_changes<br/>status events and<br/>handle lookups"]
        FA["fct_account_activity_daily<br/>account × day"]
        FE["fct_engagement_received_daily<br/>account × day"]
        IF["int_account_firsts<br/>first seen, signup"]
        DA["dim_account<br/>SCD type 2"]
        DD["dim_date"]
        MD["mart_daily_metrics<br/>day"]
        MR["mart_retention<br/>signup day × day N"]
    end
    EV --> SC
    EV --> SA
    AH --> SA
    SC --> FA
    SC --> FE
    FA --> IF
    SA --> DA
    IF --> DA
    FA --> MD
    FE --> MD
    IF --> MD
    DD --> MD
    IF --> MR
    FA --> MR
```

| Model | Kind | One row per | Built |
|---|---|---|---|
| `dim_date` | dimension | day of collection | in full |
| `dim_account` | dimension, SCD type 2 | version of an account: a new one whenever its handle or status changes | in full |
| `fct_account_activity_daily` | fact | account and day it wrote anything: posts, replies, likes, reposts, follows, blocks, edits, deletes | the last 2 days again, each night |
| `fct_engagement_received_daily` | fact | account and day it received likes, reposts, replies or follows from another account | the last 2 days again |
| `mart_daily_metrics` | metrics | day | the last 2 days again |
| `mart_retention` | metrics | signup day and day 1, 7 or 30 | in full |

The facts and daily metrics are incremental: each night clears and rebuilds the last 2 days, since an hour can reach the event store late. The facts are a few rows per account per day and the dimension a few per account, so rebuilding those in full stays cheap.

## The metrics, defined

| Metric | Definition |
|---|---|
| Daily, weekly, monthly active | Accounts that wrote anything to a Bluesky record type (post, like, follow, profile edit and so on) on that day, in the 7 days, or in the 30 days ending that day |
| New accounts | Accounts that created their profile that day with nothing seen from them more than an hour before. See below |
| Posts, replies, likes, reposts, follows, blocks, deletes | Writes of each kind that day. A reply is a post with a parent |
| Engagement received | Likes, reposts and replies from other accounts. Liking your own post doesn't count |
| Top 1% share | The share of the day's engagement received by the top 1% of the accounts that received any, rounded up to at least one account |
| Gini | How unequal engagement received was that day: 0 if every account that received any got the same, close to 1 if one got nearly all |
| Day N retention | Of the accounts that signed up on a day, the share that wrote anything exactly N days later |

**Caveats that stay next to these numbers:**

- **Active means wrote something.** The firehose carries writes only, so an account that only reads is invisible. These are active writers, not users.
- **The first days undercount the windows.** Weekly figures cover a full week from 30 October and monthly ones a full month from 22 November; `weekly_complete` and `monthly_complete` say which days are which.
- **New accounts are inferred.** The feed has no "signup" event. An account's first profile record is the clearest sign; requiring that nothing was seen from the account more than an hour earlier rules out accounts that existed before collection and only now set a profile. Checked on a real day, 99.5% of profile creators also had a handle set within the hour, as new accounts do ([details](../validation/testing.md#new-accounts-checked)).
- **Engagement on deleted records stops counting** once compaction redacts them, but only for the days rebuilt after that. Counts for older days stay as they were built.
- **Follows only from 24 October on.** There's no follower count, because follows made before collection began are invisible.
- **Handles come from lookups.** Jetstream's identity events almost never carry the handle, so `resolve-handles` asks Bluesky's API for it whenever an account's identity changes ([handle lookups](handles.md)). Accounts whose identity hasn't changed since collection began have no handle in their history, only status.
- **Display names aren't tracked.** The event store keeps no record content, and the handle lookups deliberately don't keep it either, so account history follows handles and status only.

## Privacy

- Accounts are keyed by a SHA-256 hash of their DID. DIDs are public, so this is pseudonymization, not anonymization: it keeps raw account IDs out of the marts, nothing more ([D-038](../decisions/engineering-decisions.md#d-038-accounts-are-hashed-and-handles-kept-in-private-marts)).
- `dim_account` keeps handles, which are public on Bluesky and are what its history tracks. The marts are never published as tables.
- The facts are counts. A deleted like doesn't make the fact that an account liked something on a day wrong.

## Running it

`bluesky-marts.timer` starts `dbt build` at 04:30 UTC, after compaction and before the vacuum. It builds every model and runs every test; a failing test fails the run. The first run finds no tables and builds everything from 24 October.

The SQL runs in Athena through the `dbt-athena` adapter, in the `bluesky_marts` Glue database, as Iceberg tables. dbt v2 doesn't support Athena, so this is dbt Core 1.12 ([D-036](../decisions/engineering-decisions.md#d-036-dbt-core-on-athena-for-the-marts)).

## Testing

- **On the real data**, every night: unique keys, no nulls where there shouldn't be, every activity date in `dim_date`, one current version per account, daily ≤ weekly ≤ monthly active, shares and rates between 0 and 1. Account statuses outside the known list only warn.
- **On a sample**, in CI: `tests/test_marts.py` writes a few days of hand-made events for four accounts into DuckDB, builds the same models with `dbt-duckdb`, and checks every metric against values worked out by hand, then checks that an incremental build redoes recent days without duplicating them. The few spots where Athena's SQL and DuckDB's differ are macros in `transform/macros/portable.sql`.

## Cost

A nightly build reads the last two days of the event store's commit columns and the marts themselves, around 1 to 2 GB, so about a cent. To be measured on the first runs.
