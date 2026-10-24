-- Share of events by kind, record type and operation.
SELECT
    kind,
    collection,
    operation,
    count(*) AS events,
    round(100.0 * count(*) / sum(count(*)) OVER (), 2) AS pct
FROM events
GROUP BY kind, collection, operation
ORDER BY events DESC
LIMIT 25
