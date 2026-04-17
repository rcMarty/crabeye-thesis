SELECT fa.file_path, count(*) AS editions
FROM file_activity fa
JOIN contributors_teams ct
  ON ct.contributor_id = fa.contributor_id
 AND ct.team           = 'compiler'               -- parametr: $4
WHERE fa.repository  = 'rust-lang/rust'          -- parametr: $1
  AND fa.timestamp  BETWEEN '2025-06-01'          -- parametr: $2
                        AND '2026-01-01'          -- parametr: $3
GROUP BY fa.file_path
ORDER BY editions DESC;
