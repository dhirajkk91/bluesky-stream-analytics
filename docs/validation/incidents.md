# Incidents

Things that went wrong in production, what they cost, and what changed because of them.

## Jetstream went quiet for 15 minutes (27 October)

**What happened.** At 07:58:39 UTC Jetstream dropped the connection, as it does a few times a day. The collector reconnected a second later with its cursor, as designed. Jetstream sent about 4,000 events, then nothing at all for 15 minutes. The connection stayed open the whole time: no error, no close. At 08:14 the events came back in a burst, up to 2,677 a second against the usual 240, and by 08:17 the stream was back to normal.

```
07:58:39  Connection error: no close frame received or sent
07:58:40  Connected to wss://jetstream2.us-east.bsky.network/subscribe?cursor=…
07:59:13  4,226 events in 60s (70.4/s), 47.6s behind live
08:00:13  0 events in 60s, 107.6s behind live
   …       0 events every minute, 60 seconds further behind each time
08:14:13  0 events in 60s, 947.6s behind live
08:15:13  98,616 events in 60s (1643.6/s), 0.1s behind live
08:16:13  160,646 events in 60s (2677.4/s), 0.1s behind live
08:17:13  14,738 events in 60s (245.6/s), 0.1s behind live
```

**Were events lost?** It doesn't look like it. Counted by minute in the event store:

| Minutes (UTC) | Events | Normal for that many minutes |
|---|---|---|
| 07:58 | 6,181 | about 14,000 |
| 07:59 to 08:13 | none | about 210,000 |
| 08:14 and 08:15 | 255,938 | about 28,000 |
| 07:58 to 08:15 in all | 262,119 | about 254,000 |

The burst holds the missing minutes' worth of events. They're stamped 08:14 and 08:15 because `time_us` is the time Jetstream processed an event, not the time it happened, and Jetstream itself was catching up. So the stall was on Jetstream's side, and every one of its clients would have seen the same. With no sequence numbers in Jetstream, "about the right number" is as far as the check can go.

**What it cost.**

- About 230,000 events have an `event_time` up to 16 minutes late. Hourly and daily metrics barely notice; per-minute charts of that morning show a hole and a spike.
- The collector closes an hour's file when the first event of the next hour arrives. With nothing arriving, hour 07's file only closed in the burst, was uploaded at 08:20:03, and missed the hourly job that had started 2 seconds earlier. It was loaded an hour late, at 09:20.
- Deletes in hour 07 stayed visible for up to 141 minutes, over the 2 hours the deletion policy states. Across the 13 hourly runs since the projections were rebuilt, the average was 85 minutes.

**What changed.**

- [Monitoring](../architectures/monitoring.md) was added: an email when the newest event is more than 5 minutes old, so a stall like this one is noticed while it happens.
- `event_time` is documented as Jetstream's processing time, which can lag the real time during an outage like this one.
- The deletion policy states the measured time to hide a deleted record, and when it can be longer.
- Nothing in the collector. Reconnecting after a silence would only have reached the same stalled server, and switching servers isn't possible without a gap, because cursors don't line up between them. Spotting a silence quickly is left to monitoring, above.
