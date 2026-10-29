# Marts

## Purpose

The marts are where the product metrics come from: how many accounts are active, how many are new and whether they stay, what people do, and how concentrated attention is. They're a Kimball star schema built with dbt from the [event store](event-store.md), so each metric is defined once, in SQL, with tests.

They come in two parts: accounts (activity, engagement received, history, retention) and posts (engagement per post, how fast it arrives, languages, and replies to posts never seen).

Each metric has its caveats written next to it, below. The marts are rebuilt every night for about a cent, and tested twice: on the real data in Athena, and on a hand-made sample in CI with the numbers worked out by hand. Nothing is published from here; only totals leave, through the [dashboard](dashboard.md), with small groups folded away.

## Design

```mermaid
flowchart LR
    EV[("bluesky_store.events")]
    AH[("bluesky_store.account_handles")]
    CR[("bluesky_store.current_records<br/>projections")]
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
        DP["dim_post<br/>with placeholders"]
        FP["fct_post_engagement_daily<br/>post × day"]
        MA["mart_engagement_by_post_age<br/>day × hours old"]
        MP["mart_post_metrics_daily<br/>day"]
        ML["mart_posts_by_language_daily<br/>day × language"]
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
    CR --> DP
    SC --> FP
    SC --> MA
    SC --> ML
    DP --> MP
    FP --> MP
    MA --> MP
```

| Model | Kind | One row per | Built |
|---|---|---|---|
| `dim_date` | dimension | day of collection | in full |
| `dim_account` | dimension, SCD type 2 | version of an account: a new one whenever its handle or status changes | in full |
| `fct_account_activity_daily` | fact | account and day it wrote anything: posts, replies, likes, reposts, follows, blocks, edits, deletes | the last 2 days again, each night |
| `fct_engagement_received_daily` | fact | account and day it received likes, reposts, replies or follows from another account | the last 2 days again |
| `mart_daily_metrics` | metrics | day | the last 2 days again |
| `mart_retention` | metrics | signup day and day 1, 7 or 30 | in full |
| `dim_post` | dimension | post that exists now, plus a placeholder for every post a reply points at that hasn't been seen | in full, from the projections |
| `fct_post_engagement_daily` | fact | post and day it received likes, reposts or replies from other accounts | the last 2 days again |
| `mart_engagement_by_post_age` | metrics | day and the age of the post, in hours, when the engagement came | the last 2 days again |
| `mart_post_metrics_daily` | metrics | day | the last 2 days again |
| `mart_posts_by_language_daily` | metrics | day and first tagged language | the last 2 days again |

The facts and daily metrics are incremental: each night clears and rebuilds the last 2 days, since an hour can reach the event store late. The account dimension is a few rows per account, so rebuilding it in full stays cheap.

## Posts never seen

Some replies answer posts that were made before collection began, or whose create hasn't arrived yet: Jetstream can deliver a reply before its parent. A star schema needs every reply's parent to have a row to join to, so `dim_post` adds a placeholder for each one (what Kimball calls an inferred member). A placeholder still has the author, from the parent's URI, and the creation time, decoded from its key. Once the real post's create arrives, the next build has the real row instead, and the placeholder is gone; a test checks every reply's parent and root have a row.

A deleted parent gets a placeholder too, since it's no longer in `current_records`, so "unseen" means not seen or since deleted.

`dim_post` is built in full each night from the [projections](projections.md)' `current_records`, not from the event store. That way a deleted post drops out of it at the next build, along with its reply links, with no deletion logic of its own ([D-040](../decisions/engineering-decisions.md#d-040-posts-come-from-the-projections-with-placeholders-for-unseen-parents)).

## The metrics, defined

| Metric | Definition |
|---|---|
| Daily, weekly, monthly active | Accounts that wrote anything to a Bluesky record type (post, like, follow, profile edit and so on) on that day, in the 7 days, or in the 30 days ending that day |
| New accounts | Accounts that created their profile that day with nothing seen from them more than an hour before. See below |
| Posts, replies, likes, reposts, follows, blocks, deletes | Writes of each kind that day. A reply is a post with a parent, and counts as a reply, not a post |
| Engagement received | Likes, reposts and replies from other accounts. Liking your own post doesn't count |
| Top 1% share | The share of the day's engagement received by the top 1% of the accounts that received any, rounded up to at least one account |
| Gini | How unequal engagement received was that day: 0 if every account that received any got the same, close to 1 if one got nearly all |
| Day N retention | Of the accounts that signed up on a day, the share that wrote anything exactly N days later |
| Posts engaged | Posts that received any like, repost or reply from another account that day |
| Replies to unseen posts | Replies made that day whose parent has only a placeholder in `dim_post` |
| Engagement by post age | Likes, reposts and replies received, by whole hours between the post's creation and the engagement. 48 means two days or more; -1 means the post's key doesn't give a believable creation time |
| Share to posts under a day | The share of the day's engagement that went to posts less than 24 hours old |
| Posts by language | Posts and replies made that day, by the first language the author tagged; `none` when untagged |

**Caveats that stay next to these numbers:**

- **Active means wrote something.** The firehose carries writes only, so an account that only reads is invisible. These are active writers, not users.
- **The first days undercount the windows.** Weekly figures cover a full week from 30 October and monthly ones a full month from 22 November; `weekly_complete` and `monthly_complete` say which days are which.
- **New accounts are inferred.** The feed has no "signup" event. An account's first profile record is the clearest sign; requiring that nothing was seen from the account more than an hour earlier rules out accounts that existed before collection and only now set a profile. Checked on a real day, 99.5% of profile creators also had a handle set within the hour, as new accounts do ([details](../validation/testing.md#new-accounts-checked)).
- **Engagement on deleted records stops counting** once compaction redacts them, but only for the days rebuilt after that. Counts for older days stay as they were built.
- **Follows only from 24 October on.** There's no follower count, because follows made before collection began are invisible.
- **Handles come from lookups.** Jetstream's identity events almost never carry the handle, so `resolve-handles` asks Bluesky's API for it whenever an account's identity changes ([handle lookups](handles.md)). Accounts whose identity hasn't changed since collection began have no handle in their history, only status.
- **A post's age comes from its key**, which is the creation time the author's app chose. A few apps make keys from counters or random bits; those land in the -1 bucket.
- **"Unseen" is as of the night the day was built.** A placeholder replaced later doesn't change older days.
- **Quote posts don't count as engagement.** A quote is an embed in the post's content, which the event store doesn't keep.
- **Display names aren't tracked.** The event store keeps no record content, and the handle lookups deliberately don't keep it either, so account history follows handles and status only.

## Privacy

- Accounts are keyed by a SHA-256 hash of their DID. DIDs are public, so this is pseudonymization, not anonymization: it keeps raw account IDs out of the marts, nothing more ([D-038](../decisions/engineering-decisions.md#d-038-accounts-are-hashed-and-handles-kept-in-private-marts)).
- `dim_account` keeps handles, which are public on Bluesky and are what its history tracks. The marts are never published as tables.
- The facts are counts. A deleted like doesn't make the fact that an account liked something on a day wrong.

## Running it

`bluesky-marts.timer` starts `dbt build` at 04:30 UTC, after compaction and before the vacuum. It builds every model and runs every test; a failing test fails the run. Then `export-dashboard` writes the totals the [dashboard](dashboard.md) shows. The first run finds no tables and builds everything from 24 October.

The SQL runs in Athena through the `dbt-athena` adapter, in the `bluesky_marts` Glue database, as Iceberg tables. dbt v2 doesn't support Athena, so this is dbt Core 1.12 ([D-036](../decisions/engineering-decisions.md#d-036-dbt-core-on-athena-for-the-marts)).

## Testing

- **On the real data**, every night: unique keys, no nulls where there shouldn't be, every activity date in `dim_date`, every reply's parent and root in `dim_post`, one current version per account, daily ≤ weekly ≤ monthly active, shares and rates between 0 and 1, post ages within the buckets. Account statuses outside the known list only warn.
- **On a sample**, in CI: `tests/test_marts.py` writes a few days of hand-made events for four accounts into DuckDB, folds them into the projections, builds the same models with `dbt-duckdb`, and checks every metric against values worked out by hand. It also checks that an incremental build redoes recent days without duplicating them, and that a placeholder gives way to the real post when its create arrives late. The few spots where Athena's SQL and DuckDB's differ are macros in `transform/macros/portable.sql`.

## First build

The first `dbt build` in Athena ran every model and all 20 tests in 63 seconds, with no failures. Its numbers match the independent queries run before the marts existed:

| | 25 October | 26 October | Checked against |
|---|---|---|---|
| Daily active writers | 1,253,076 | 1,322,431 | the same, from a direct query on the event store |
| New accounts | 19,196 | 18,511 | 19,200 and 18,521 profiles created, less the few accounts seen earlier |
| Likes | 21.5 million | 23.5 million | |
| Top 1% share of engagement received | 50% | 52% | |
| Gini of engagement received | 0.88 | 0.88 | |
| Day-1 retention of that day's new accounts | 21.5% | | |

Two first findings: half of all likes, reposts and replies go to the top 1% of the accounts that receive any, and about one new account in five writes anything the next day.

### The post models

Their first build ran with the rest, all 46 models and tests passing in 3.5 minutes. `mart_engagement_by_post_age` took 2 of those, reading every day since 24 October; nightly it only redoes two.

| | 24 October | 25 October | 26 October | 27 October |
|---|---|---|---|---|
| Posts | 421,403 | 1,831,456 | 1,956,242 | 1,982,009 |
| Replies | 406,378 | 1,488,104 | 1,552,754 | 1,583,602 |
| Replies to unseen posts | 30.0% | 10.3% | 4.9% | 3.2% |
| Posts that got any engagement | 1.2 million | 3.5 million | 3.5 million | 3.5 million |
| Engagement on posts | 7.0 million | 25.8 million | 28.2 million | 27.4 million |
| Share to posts under a day old | 84% | 84% | 86% | 85% |

24 October is the part of the day after collection began. `dim_post` had 12.66 million live posts, 46% of them replies, and 330,158 placeholders.

**How fast things fade.** Engagement on 27 October by the age of the post it went to:

| Post age | Share of the day's engagement | Cumulative |
|---|---|---|
| under 1 hour | 33.6% | 33.6% |
| 1 to 2 hours | 10.3% | 43.9% |
| 2 to 3 hours | 6.5% | 50.4% |
| 3 to 12 hours | 23.3% | 73.7% |
| 12 to 24 hours | 10.9% | 84.6% |
| 1 to 2 days | 5.6% | 90.3% |
| 2 days or more | 9.7% | 100% |

A third of all likes, reposts and replies go to posts less than an hour old, and half to posts under three hours old. After that the hourly count roughly halves every 11 hours through the first day, then falls faster, halving in 6 hours from hour 24 to 30. Whether that bend is a feed dropping posts after a day, or just one day's rhythm, needs more days to tell. 0.09% of engagement went to posts whose key gave no believable creation time.

**Replies to unseen posts fall as history builds up.** On the first day, 30% of replies answered a post from before collection began. By the fourth, 3.2% did, which is roughly the share of replies that answer posts more than a few days old, or ones deleted since.

**Languages**, by first tag, on 27 October: English 56%, untagged 15%, Japanese 10%, then Portuguese, Spanish and German at about 3% each, French 1.9%, Nepali 1.5%, Korean 1.2% and Dutch 0.8%.

## Cost

A nightly build reads the last two days of the event store's commit columns and the marts themselves, around 1 to 2 GB, so about a cent. To be measured on the first runs.
