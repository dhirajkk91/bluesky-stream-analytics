-- Events per stored hour, with the time span each file covers.
SELECT
    dt,
    hr,
    count(*) AS events,
    min(event_time) AS first_event,
    max(event_time) AS last_event
FROM events
GROUP BY dt, hr
ORDER BY dt, hr
