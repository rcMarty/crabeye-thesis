WITH latest_labels AS (
    -- Pro každý PR zachová nejnovější label-event pro cílový S-* label k T
    SELECT DISTINCT ON (issue) issue, action
    FROM issue_labels_history
    WHERE repository  = 'rust-lang/rust'
      AND is_pr       = true
      AND label       = 'S-waiting-on-review'   -- parametr: $3
      AND timestamp  <= '2026-01-01'             -- parametr: $2
    ORDER BY issue, timestamp DESC
),
latest_state AS (
    -- Pro každý PR zachová nejnovější stavovou změnu (closed/merged/reopened) k T
    SELECT DISTINCT ON (issue) issue, event
    FROM issue_event_history
    WHERE repository  = 'rust-lang/rust'         -- parametr: $1
      AND is_pr       = true
      AND event      IN ('closed', 'merged', 'reopened')
      AND timestamp  <= '2026-01-01'             -- parametr: $2
    ORDER BY issue, timestamp DESC
)
SELECT COUNT(*)
FROM latest_labels ll
LEFT JOIN latest_state ls ON ls.issue = ll.issue
WHERE ll.action = 'ADDED'
  -- PR musí být k T otevřen (žádná zavírací událost, nebo poslední byla 'reopened')
  AND (ls.issue IS NULL OR ls.event = 'reopened');
