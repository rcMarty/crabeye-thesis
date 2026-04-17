SELECT COUNT(DISTINCT issue)
FROM issue_event_history
WHERE repository  = 'rust-lang/rust'    -- parametr: $1
  AND is_pr       = true
  AND event       = 'merged'
  AND timestamp  <= '2026-01-01';       -- parametr: $2
