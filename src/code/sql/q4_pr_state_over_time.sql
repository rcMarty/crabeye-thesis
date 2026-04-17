WITH
all_transitions AS (
    SELECT issue, created_at AS timestamp, 'created' AS event_type
    FROM issues
    WHERE repository = 'rust-lang/rust' AND is_pr = true   -- parametr: $1
    UNION ALL
    SELECT issue, timestamp, event AS event_type
    FROM issue_event_history
    WHERE repository = 'rust-lang/rust' AND is_pr = true   -- parametr: $1
      AND event IN ('closed', 'merged', 'reopened')
),
ordered_transitions AS (
    SELECT issue, timestamp, event_type,
           LEAD(timestamp) OVER (PARTITION BY issue ORDER BY timestamp) AS next_ts
    FROM all_transitions
),
open_periods AS (
    SELECT issue,
           tsrange(timestamp, COALESCE(next_ts, 'infinity'::timestamp), '[)') AS period
    FROM ordered_transitions
    WHERE event_type IN ('created', 'reopened')
),
label_transitions AS (
    SELECT issue, timestamp, action,
           LEAD(timestamp) OVER (PARTITION BY issue ORDER BY timestamp) AS next_ts
    FROM issue_labels_history
    WHERE repository = 'rust-lang/rust' AND is_pr = true   -- parametr: $1
      AND label = 'S-waiting-on-review'                    -- parametr: $4
),
label_active_periods AS (
    -- Intervaly, kdy je label aktivní: [ADDED, next label event)
    SELECT issue,
           tsrange(timestamp, COALESCE(next_ts, 'infinity'::timestamp), '[)') AS period
    FROM label_transitions
    WHERE action = 'ADDED'
),
in_state_periods AS (
    SELECT lap.issue,
           lap.period * op.period AS valid_period
    FROM label_active_periods lap
    JOIN open_periods op
      ON lap.issue   = op.issue
     AND lap.period && op.period
    WHERE NOT isempty(lap.period * op.period)
),
period_deltas AS (
    -- Každý interval -> (+1 při začátku, -1 při konci)
    SELECT lower(valid_period)::date AS event_date,  1 AS delta FROM in_state_periods
    UNION ALL
    SELECT upper(valid_period)::date AS event_date, -1 AS delta
    FROM in_state_periods
    WHERE NOT upper_inf(valid_period)
),
daily_deltas AS (
    SELECT event_date, SUM(delta) AS daily_change
    FROM period_deltas
    GROUP BY event_date
),
base AS (
    -- Kumulativní součet před začátkem sledovaného okna
    SELECT COALESCE(SUM(daily_change), 0) AS cnt
    FROM daily_deltas
    WHERE event_date < '2025-12-01'::date    -- parametr: $2
),
date_series AS (
    SELECT d::date AS day
    FROM generate_series(
        '2025-12-01'::timestamp,             -- parametr: $2
        '2026-01-01'::timestamp,             -- parametr: $3
        '1 day'::interval
    ) d
)
SELECT ds.day AS date,
       ((SELECT cnt FROM base)
         + COALESCE(SUM(dd.daily_change) OVER (ORDER BY ds.day), 0))::bigint AS count
FROM date_series ds
LEFT JOIN daily_deltas dd ON dd.event_date = ds.day
ORDER BY ds.day;
