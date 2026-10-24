-- One row per raw event, with the fields used for exploring the stream. event_time is UTC.
-- Duplicates are kept; they are removed in the clean tables.
CREATE OR REPLACE VIEW events AS
SELECT
    dt,
    hr,
    json_extract_scalar(j, '$.did') AS did,
    CAST(json_extract_scalar(j, '$.time_us') AS bigint) AS time_us,
    CAST(from_unixtime(CAST(json_extract_scalar(j, '$.time_us') AS bigint) / 1e6) AT TIME ZONE 'UTC' AS timestamp) AS event_time,
    json_extract_scalar(j, '$.kind') AS kind,
    json_extract_scalar(j, '$.commit.collection') AS collection,
    json_extract_scalar(j, '$.commit.operation') AS operation,
    json_extract_scalar(j, '$.commit.rkey') AS rkey,
    json_extract_scalar(j, '$.commit.rev') AS rev,
    coalesce(json_extract_scalar(j, '$.identity.seq'), json_extract_scalar(j, '$.account.seq')) AS seq,
    line AS raw
FROM (
    SELECT dt, hr, line, json_parse(line) AS j
    FROM raw_events
)
