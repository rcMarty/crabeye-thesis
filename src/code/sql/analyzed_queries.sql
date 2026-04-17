-- =============================================================================
-- Analyzované SQL dotazy – aplikace ranal
-- Databáze: PostgreSQL 14, repozitář: rust-lang/rust
-- Počty řádků: issues ~200 000, issue_event_history ~564 000,
--              issue_labels_history ~355 000, file_activity ~525 000
-- =============================================================================


-- ---------------------------------------------------------------------------
-- Q1: Počet PR ve stavu „S-waiting-on-review" k danému datu
--     (funkce: get_pr_count_in_state – větev WaitingForReview/Bors/Author)
--
--     Princip: DISTINCT ON (issue) zachová pro každý PR pouze nejnovější
--     label-event a nejnovější stavovou změnu k časovému řezu T.
--     LEFT JOIN zajistí zahrnutí PR bez stavové změny (nikdy nezavřené).
-- ---------------------------------------------------------------------------

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

-- bez indexů
Aggregate  (cost=54969.47..54969.48 rows=1 width=8) (actual time=224.771..224.772 rows=1 loops=1)
"  Buffers: shared read=9859, temp read=995 written=1000"
  ->  Merge Right Join  (cost=51815.93..54969.46 rows=3 width=0) (actual time=182.061..224.642 rows=2857 loops=1)
        Merge Cond: (issue_event_history.issue = ll.issue)
        Filter: ((issue_event_history.issue IS NULL) OR (issue_event_history.event = 'reopened'::text))
        Rows Removed by Filter: 2142
"        Buffers: shared read=9859, temp read=995 written=1000"
        ->  Unique  (cost=29140.47..29762.87 rows=98502 width=23) (actual time=100.334..118.678 rows=77980 loops=1)
"              Buffers: shared read=5476, temp read=516 written=519"
              ->  Sort  (cost=29140.47..29451.67 rows=124480 width=23) (actual time=100.332..109.557 rows=122968 loops=1)
"                    Sort Key: issue_event_history.issue, issue_event_history.""timestamp"" DESC"
                    Sort Method: external merge  Disk: 4128kB
"                    Buffers: shared read=5476, temp read=516 written=519"
                    ->  Seq Scan on issue_event_history  (cost=0.00..16051.00 rows=124480 width=23) (actual time=11.171..72.431 rows=123000 loops=1)
"                          Filter: (is_pr AND (""timestamp"" <= '2026-01-01 00:00:00'::timestamp without time zone) AND (repository = 'rust-lang/rust'::text) AND (event = ANY ('{closed,merged,reopened}'::text[])))"
                          Rows Removed by Filter: 441000
                          Buffers: shared read=5476
        ->  Materialize  (cost=22675.46..23971.02 rows=287 width=8) (actual time=81.719..101.572 rows=4999 loops=1)
"              Buffers: shared read=4383, temp read=479 written=481"
              ->  Subquery Scan on ll  (cost=22675.46..23970.30 rows=287 width=8) (actual time=81.717..101.064 rows=4999 loops=1)
                    Filter: (ll.action = 'ADDED'::text)
                    Rows Removed by Filter: 45001
"                    Buffers: shared read=4383, temp read=479 written=481"
                    ->  Unique  (cost=22675.46..23252.87 rows=57394 width=22) (actual time=81.708..98.456 rows=50000 loops=1)
"                          Buffers: shared read=4383, temp read=479 written=481"
                          ->  Sort  (cost=22675.46..22964.17 rows=115482 width=22) (actual time=81.706..91.247 rows=114999 loops=1)
"                                Sort Key: issue_labels_history.issue, issue_labels_history.""timestamp"" DESC"
                                Sort Method: external merge  Disk: 3832kB
"                                Buffers: shared read=4383, temp read=479 written=481"
                                ->  Seq Scan on issue_labels_history  (cost=0.00..10595.48 rows=115482 width=22) (actual time=0.013..45.778 rows=114999 loops=1)
"                                      Filter: (is_pr AND (""timestamp"" <= '2026-01-01 00:00:00'::timestamp without time zone) AND (repository = 'rust-lang/rust'::text) AND (label = 'S-waiting-on-review'::text))"
                                      Rows Removed by Filter: 240000
                                      Buffers: shared read=4383
Planning:
  Buffers: shared hit=13 dirtied=1
Planning Time: 0.416 ms
Execution Time: 226.566 ms


-- indexy
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('Aggregate  (cost=16047.76..16047.77 rows=1 width=8) (actual time=84.096..84.098 rows=1 loops=1)');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('  Buffers: shared hit=171 read=1912');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('  ->  Merge Right Join  (cost=0.84..16047.75 rows=3 width=0) (actual time=0.173..83.881 rows=2857 loops=1)');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('        Merge Cond: (issue_event_history.issue = ll.issue)');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('        Filter: ((issue_event_history.issue IS NULL) OR (issue_event_history.event = ''reopened''::text))');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('        Rows Removed by Filter: 2142');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('        Buffers: shared hit=171 read=1912');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('        ->  Result  (cost=0.42..6338.42 rows=98502 width=23) (actual time=0.074..37.006 rows=77980 loops=1)');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('              Buffers: shared hit=73 read=809');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('              ->  Unique  (cost=0.42..6338.42 rows=98502 width=23) (actual time=0.071..30.278 rows=77980 loops=1)');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                    Buffers: shared hit=73 read=809');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                    ->  Index Only Scan using idx_pr_event_hist_state_changes on issue_event_history  (cost=0.42..6027.22 rows=124480 width=23) (actual time=0.070..18.099 rows=122968 loops=1)');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                          Index Cond: ((repository = ''rust-lang/rust''::text) AND ("timestamp" <= ''2026-01-01 00:00:00''::timestamp without time zone))');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                          Heap Fetches: 0');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                          Buffers: shared hit=73 read=809');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('        ->  Materialize  (cost=0.42..8473.75 rows=287 width=8) (actual time=0.088..42.098 rows=4999 loops=1)');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('              Buffers: shared hit=98 read=1103');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('              ->  Subquery Scan on ll  (cost=0.42..8473.03 rows=287 width=8) (actual time=0.077..41.258 rows=4999 loops=1)');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                    Filter: (ll.action = ''ADDED''::text)');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                    Rows Removed by Filter: 45001');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                    Buffers: shared hit=98 read=1103');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                    ->  Result  (cost=0.42..7755.61 rows=57394 width=22) (actual time=0.069..38.559 rows=50000 loops=1)');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                          Buffers: shared hit=98 read=1103');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                          ->  Unique  (cost=0.42..7755.61 rows=57394 width=22) (actual time=0.067..34.059 rows=50000 loops=1)');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                                Buffers: shared hit=98 read=1103');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                                ->  Index Only Scan using idx_pr_labels_repo_label_issue_ts on issue_labels_history  (cost=0.42..7466.90 rows=115482 width=22) (actual time=0.067..23.605 rows=114999 loops=1)');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                                      Index Cond: ((repository = ''rust-lang/rust''::text) AND (label = ''S-waiting-on-review''::text) AND ("timestamp" <= ''2026-01-01 00:00:00''::timestamp without time zone))');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                                      Heap Fetches: 0');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                                      Buffers: shared hit=98 read=1103');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('Planning:');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('  Buffers: shared hit=487 read=12');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('Planning Time: 4.383 ms');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('Execution Time: 84.240 ms');



-- ---------------------------------------------------------------------------
-- Q2: Kumulativní počet merged PR k danému datu
--     (funkce: get_pr_count_in_state – větev Merged)
--
--     Princip: stav „merged" je trvalý – stačí COUNT(DISTINCT issue)
--     nad jedinou tabulkou s indexovatelným filtrem.
-- ---------------------------------------------------------------------------

SELECT COUNT(DISTINCT issue)
FROM issue_event_history
WHERE repository  = 'rust-lang/rust'    -- parametr: $1
  AND is_pr       = true
  AND event       = 'merged'
  AND timestamp  <= '2026-01-01';       -- parametr: $2

-- bez indexů
Aggregate  (cost=15248.56..15248.57 rows=1 width=8) (actual time=31.584..31.678 rows=1 loops=1)
  Buffers: shared hit=32 read=5444
  ->  Gather  (cost=1000.00..15134.90 rows=45464 width=8) (actual time=6.060..24.844 rows=45000 loops=1)
        Workers Planned: 2
        Workers Launched: 2
        Buffers: shared hit=32 read=5444
        ->  Parallel Seq Scan on issue_event_history  (cost=0.00..9588.50 rows=18943 width=8) (actual time=3.032..21.065 rows=15000 loops=3)
"              Filter: (is_pr AND (""timestamp"" <= '2026-01-01 00:00:00'::timestamp without time zone) AND (repository = 'rust-lang/rust'::text) AND (event = 'merged'::text))"
              Rows Removed by Filter: 173000
              Buffers: shared hit=32 read=5444
Planning Time: 0.064 ms
Execution Time: 31.725 ms


--indexy
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('Aggregate  (cost=2122.71..2122.72 rows=1 width=8) (actual time=9.693..9.694 rows=1 loops=1)');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('  Buffers: shared hit=1 read=274');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('  ->  Index Only Scan using idx_pr_event_hist_merged on issue_event_history  (cost=0.41..2009.05 rows=45464 width=8) (actual time=0.082..5.982 rows=45000 loops=1)');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('        Index Cond: ((repository = ''rust-lang/rust''::text) AND ("timestamp" <= ''2026-01-01 00:00:00''::timestamp without time zone))');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('        Heap Fetches: 0');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('        Buffers: shared hit=1 read=274');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('Planning:');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('  Buffers: shared hit=3');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('Planning Time: 0.129 ms');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('Execution Time: 9.726 ms');

-- ---------------------------------------------------------------------------
-- Q3: Soubory modifikované členy daného týmu v časovém okně
--     (funkce: get_files_modified_by_team)
--
--     Princip: Nested Loop Join přes members týmu (contributors_teams)
--     a pokrývající index na file_activity (repository, contributor_id, timestamp).
-- ---------------------------------------------------------------------------

SELECT fa.file_path, count(*) AS editions
FROM file_activity fa
JOIN contributors_teams ct
  ON ct.contributor_id = fa.contributor_id
 AND ct.team           = 'team-21'               -- parametr: $4
WHERE fa.repository  = 'rust-lang/rust'          -- parametr: $1
  AND fa.timestamp  BETWEEN '2025-06-01'          -- parametr: $2
                        AND '2026-01-01'          -- parametr: $3
GROUP BY fa.file_path
ORDER BY editions DESC;

-- bez indexů
Sort  (cost=12553.71..12553.72 rows=1 width=40) (actual time=16.521..18.592 rows=0 loops=1)
  Sort Key: (count(*)) DESC
  Sort Method: quicksort  Memory: 25kB
  Buffers: shared read=7721
  ->  GroupAggregate  (cost=12553.68..12553.70 rows=1 width=40) (actual time=16.512..18.583 rows=0 loops=1)
        Group Key: fa.file_path
        Buffers: shared read=7721
        ->  Sort  (cost=12553.68..12553.69 rows=1 width=32) (actual time=16.511..18.582 rows=0 loops=1)
              Sort Key: fa.file_path
              Sort Method: quicksort  Memory: 25kB
              Buffers: shared read=7721
              ->  Nested Loop  (cost=1000.28..12553.67 rows=1 width=32) (actual time=16.496..18.566 rows=0 loops=1)
                    Buffers: shared read=7721
                    ->  Gather  (cost=1000.00..12549.23 rows=1 width=40) (actual time=16.495..18.564 rows=0 loops=1)
                          Workers Planned: 2
                          Workers Launched: 2
                          Buffers: shared read=7721
                          ->  Parallel Seq Scan on file_activity fa  (cost=0.00..11549.12 rows=1 width=40) (actual time=13.778..13.779 rows=0 loops=3)
"                                Filter: ((""timestamp"" >= '2025-06-01 00:00:00'::timestamp without time zone) AND (""timestamp"" <= '2026-01-01 00:00:00'::timestamp without time zone) AND (repository = 'rust-lang/rust'::text))"
                                Rows Removed by Filter: 175000
                                Buffers: shared read=7721
                    ->  Index Only Scan using contributors_teams_pkey on contributors_teams ct  (cost=0.28..4.30 rows=1 width=8) (never executed)
                          Index Cond: ((team = 'team-21'::text) AND (contributor_id = fa.contributor_id))
                          Heap Fetches: 0
Planning:
  Buffers: shared hit=19
Planning Time: 0.351 ms
Execution Time: 18.659 ms


--indexy
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('Sort  (cost=246.10..246.11 rows=1 width=40) (actual time=0.712..0.714 rows=0 loops=1)');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('  Sort Key: (count(*)) DESC');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('  Sort Method: quicksort  Memory: 25kB');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('  Buffers: shared hit=81 read=87');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('  ->  GroupAggregate  (cost=246.07..246.09 rows=1 width=40) (actual time=0.704..0.705 rows=0 loops=1)');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('        Group Key: fa.file_path');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('        Buffers: shared hit=81 read=87');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('        ->  Sort  (cost=246.07..246.08 rows=1 width=32) (actual time=0.703..0.704 rows=0 loops=1)');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('              Sort Key: fa.file_path');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('              Sort Method: quicksort  Memory: 25kB');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('              Buffers: shared hit=81 read=87');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('              ->  Nested Loop  (cost=0.71..246.06 rows=1 width=32) (actual time=0.672..0.673 rows=0 loops=1)');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                    Buffers: shared hit=78 read=87');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                    ->  Index Only Scan using contributors_teams_pkey on contributors_teams ct  (cost=0.28..5.23 rows=54 width=8) (actual time=0.037..0.044 rows=54 loops=1)');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                          Index Cond: (team = ''team-21''::text)');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                          Heap Fetches: 0');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                          Buffers: shared read=3');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                    ->  Index Only Scan using idx_file_activity_repo_contrib_ts on file_activity fa  (cost=0.42..4.45 rows=1 width=40) (actual time=0.011..0.011 rows=0 loops=54)');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                          Index Cond: ((repository = ''rust-lang/rust''::text) AND (contributor_id = ct.contributor_id) AND ("timestamp" >= ''2025-06-01 00:00:00''::timestamp without time zone) AND ("timestamp" <= ''2026-01-01 00:00:00''::timestamp without time zone))');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                          Heap Fetches: 0');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                          Buffers: shared hit=78 read=84');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('Planning:');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('  Buffers: shared hit=189 read=9');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('Planning Time: 1.456 ms');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('Execution Time: 0.783 ms');


-- ---------------------------------------------------------------------------
-- Q4: Denní vývoj počtu PR ve stavu „S-waiting-on-review" v časovém rozsahu
--     (funkce: get_pr_count_in_state_over_time – větev WaitingForReview/Bors/Author)
--
--     Princip (delta-event přístup):
--       1. all_transitions  – UNION ALL: vznik PR + stavové změny
--       2. ordered_transitions + LEAD() – neprotínající se intervaly otevřenosti
--       3. open_periods     – tsrange [created|reopened, next_event)
--       4. label_transitions + LEAD() – intervaly aktivity labelu
--       5. label_active_periods – tsrange [ADDED, next_label_event)
--       6. in_state_periods – průnik open × label pomocí operátoru &&  a *
--       7. period_deltas    – každý interval → (+1 start, −1 konec)
--       8. daily_deltas     – SUM(delta) seskupeno po dnech
--       9. base             – kumulativní součet před začátkem rozsahu
--      10. date_series + LEFT JOIN + běžící SUM → výsledná časová řada
--
--     Složitost: O(events + days) místo naivního O(days × periods).
-- ---------------------------------------------------------------------------

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
-- Intervaly, kdy je label aktivní: [ADDED, next label event)
label_active_periods AS (
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
-- Každý interval → (+1 při začátku, −1 při konci)
-- Díky LEAD() jsou intervaly neprotínající se per issue →
-- běžící SUM ≡ COUNT(DISTINCT issue) v libovolném čase.
period_deltas AS (
    SELECT lower(valid_period)::date AS event_date,  1 AS delta
    FROM in_state_periods
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
-- Kumulativní součet před začátkem sledovaného okna
base AS (
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

--bez indexů
WindowAgg  (cost=91102.20..91127.20 rows=1000 width=12) (actual time=650.329..650.352 rows=32 loops=1)
"  Buffers: shared hit=2719 read=9699, temp read=1923 written=1932"
  CTE in_state_periods
    ->  Merge Join  (cost=77897.55..91027.14 rows=29 width=40) (actual time=247.479..579.868 rows=68713 loops=1)
          Merge Cond: (label_transitions.issue = ordered_transitions.issue)
"          Join Filter: ((tsrange(label_transitions.""timestamp"", COALESCE(label_transitions.next_ts, 'infinity'::timestamp without time zone), '[)'::text) && tsrange(ordered_transitions.""timestamp"", COALESCE(ordered_transitions.next_ts, 'infinity'::timestamp without time zone), '[)'::text)) AND (NOT isempty((tsrange(label_transitions.""timestamp"", COALESCE(label_transitions.next_ts, 'infinity'::timestamp without time zone), '[)'::text) * tsrange(ordered_transitions.""timestamp"", COALESCE(ordered_transitions.next_ts, 'infinity'::timestamp without time zone), '[)'::text)))))"
          Rows Removed by Join Filter: 1714
"          Buffers: shared hit=2719 read=9699, temp read=1587 written=1596"
          ->  Subquery Scan on label_transitions  (cost=21788.97..25542.49 rows=68037 width=24) (actual time=78.839..146.360 rows=68570 loops=1)
                Filter: (label_transitions.action = 'ADDED'::text)
                Rows Removed by Filter: 46429
"                Buffers: shared hit=32 read=4351, temp read=462 written=464"
                ->  WindowAgg  (cost=21788.97..24098.83 rows=115493 width=30) (actual time=78.837..136.727 rows=114999 loops=1)
"                      Buffers: shared hit=32 read=4351, temp read=462 written=464"
                      ->  Sort  (cost=21788.97..22077.70 rows=115493 width=22) (actual time=78.824..91.057 rows=114999 loops=1)
"                            Sort Key: issue_labels_history.issue, issue_labels_history.""timestamp"""
                            Sort Method: external merge  Disk: 3696kB
"                            Buffers: shared hit=32 read=4351, temp read=462 written=464"
                            ->  Seq Scan on issue_labels_history  (cost=0.00..9707.99 rows=115493 width=22) (actual time=0.027..44.984 rows=114999 loops=1)
                                  Filter: (is_pr AND (repository = 'rust-lang/rust'::text) AND (label = 'S-waiting-on-review'::text))
                                  Rows Removed by Filter: 240000
                                  Buffers: shared hit=32 read=4351
          ->  Materialize  (cost=56108.58..65034.06 rows=2744 width=24) (actual time=168.628..362.274 rows=172425 loops=1)
"                Buffers: shared hit=2687 read=5348, temp read=1125 written=1132"
                ->  Subquery Scan on ordered_transitions  (cost=56108.58..65027.20 rows=2744 width=24) (actual time=168.625..330.538 rows=152998 loops=1)
"                      Filter: (ordered_transitions.event_type = ANY ('{created,reopened}'::text[]))"
                      Rows Removed by Filter: 120000
"                      Buffers: shared hit=2687 read=5348, temp read=1125 written=1132"
                      ->  WindowAgg  (cost=56108.58..61596.96 rows=274419 width=45) (actual time=168.621..305.726 rows=272998 loops=1)
"                            Buffers: shared hit=2687 read=5348, temp read=1125 written=1132"
                            ->  Sort  (cost=56108.58..56794.63 rows=274419 width=37) (actual time=168.611..198.680 rows=272999 loops=1)
"                                  Sort Key: issues.issue, issues.created_at"
                                  Sort Method: external merge  Disk: 9000kB
"                                  Buffers: shared hit=2687 read=5348, temp read=1125 written=1132"
                                  ->  Append  (cost=0.00..23816.29 rows=274419 width=37) (actual time=2.104..97.129 rows=273000 loops=1)
                                        Buffers: shared hit=2687 read=5348
                                        ->  Seq Scan on issues  (cost=0.00..5059.00 rows=149927 width=48) (actual time=2.103..19.937 rows=150000 loops=1)
                                              Filter: (is_pr AND (repository = 'rust-lang/rust'::text))
                                              Rows Removed by Filter: 50000
                                              Buffers: shared hit=2559
                                        ->  Seq Scan on issue_event_history  (cost=0.00..14641.00 rows=124492 width=23) (actual time=7.965..63.360 rows=123000 loops=1)
"                                              Filter: (is_pr AND (repository = 'rust-lang/rust'::text) AND (event = ANY ('{closed,merged,reopened}'::text[])))"
                                              Rows Removed by Filter: 441000
                                              Buffers: shared hit=128 read=5348
  CTE daily_deltas
    ->  HashAggregate  (cost=2.14..2.62 rows=48 width=12) (actual time=649.253..649.416 rows=1857 loops=1)
          Group Key: ((lower(in_state_periods.valid_period))::date)
          Batches: 1  Memory Usage: 393kB
"          Buffers: shared hit=2719 read=9699, temp read=1923 written=1932"
          ->  Append  (cost=0.00..1.90 rows=48 width=8) (actual time=247.483..626.233 rows=137426 loops=1)
"                Buffers: shared hit=2719 read=9699, temp read=1923 written=1932"
                ->  CTE Scan on in_state_periods  (cost=0.00..0.72 rows=29 width=8) (actual time=247.482..604.886 rows=68713 loops=1)
"                      Buffers: shared hit=2719 read=9699, temp read=1587 written=1931"
                ->  CTE Scan on in_state_periods in_state_periods_1  (cost=0.00..0.75 rows=19 width=8) (actual time=0.024..13.418 rows=68713 loops=1)
                      Filter: (NOT upper_inf(valid_period))
                      Buffers: temp read=336 written=1
  InitPlan 3 (returns $2)
    ->  Aggregate  (cost=1.12..1.13 rows=1 width=32) (actual time=0.213..0.213 rows=1 loops=1)
          ->  CTE Scan on daily_deltas  (cost=0.00..1.08 rows=16 width=8) (actual time=0.001..0.124 rows=1856 loops=1)
                Filter: (event_date < '2025-12-01'::date)
                Rows Removed by Filter: 1
  ->  Sort  (cost=71.29..73.79 rows=1000 width=12) (actual time=650.106..650.108 rows=32 loops=1)
        Sort Key: ((d.d)::date)
        Sort Method: quicksort  Memory: 26kB
"        Buffers: shared hit=2719 read=9699, temp read=1923 written=1932"
        ->  Hash Left Join  (cost=1.56..21.46 rows=1000 width=12) (actual time=650.082..650.090 rows=32 loops=1)
              Hash Cond: ((d.d)::date = dd.event_date)
"              Buffers: shared hit=2719 read=9699, temp read=1923 written=1932"
              ->  Function Scan on generate_series d  (cost=0.00..10.00 rows=1000 width=8) (actual time=0.017..0.019 rows=32 loops=1)
              ->  Hash  (cost=0.96..0.96 rows=48 width=12) (actual time=650.047..650.047 rows=1857 loops=1)
                    Buckets: 2048 (originally 1024)  Batches: 1 (originally 1)  Memory Usage: 96kB
"                    Buffers: shared hit=2719 read=9699, temp read=1923 written=1932"
                    ->  CTE Scan on daily_deltas dd  (cost=0.00..0.96 rows=48 width=12) (actual time=649.255..649.754 rows=1857 loops=1)
"                          Buffers: shared hit=2719 read=9699, temp read=1923 written=1932"
Planning:
  Buffers: shared hit=6
Planning Time: 0.809 ms
Execution Time: 653.919 ms


--s indexy
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('WindowAgg  (cost=71398.06..71423.06 rows=1000 width=12) (actual time=516.721..516.743 rows=32 loops=1)');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('  Buffers: shared hit=4648, temp read=1461 written=1468');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('  CTE in_state_periods');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('    ->  Merge Join  (cost=47187.99..71323.01 rows=29 width=40) (actual time=118.732..451.768 rows=68713 loops=1)');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('          Merge Cond: (label_transitions.issue = ordered_transitions.issue)');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('          Join Filter: ((tsrange(label_transitions."timestamp", COALESCE(label_transitions.next_ts, ''infinity''::timestamp without time zone), ''[)''::text) && tsrange(ordered_transitions."timestamp", COALESCE(ordered_transitions.next_ts, ''infinity''::timestamp without time zone), ''[)''::text)) AND (NOT isempty((tsrange(label_transitions."timestamp", COALESCE(label_transitions.next_ts, ''infinity''::timestamp without time zone), ''[)''::text) * tsrange(ordered_transitions."timestamp", COALESCE(ordered_transitions.next_ts, ''infinity''::timestamp without time zone), ''[)''::text)))))');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('          Rows Removed by Join Filter: 1714');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('          Buffers: shared hit=4645, temp read=1125 written=1132');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('          ->  Subquery Scan on label_transitions  (cost=0.57..14759.52 rows=68037 width=24) (actual time=0.089..84.470 rows=68570 loops=1)');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                Filter: (label_transitions.action = ''ADDED''::text)');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                Rows Removed by Filter: 46429');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                Buffers: shared hit=1204');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                ->  WindowAgg  (cost=0.57..13315.86 rows=115493 width=30) (actual time=0.088..75.818 rows=114999 loops=1)');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                      Buffers: shared hit=1204');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                      ->  Incremental Sort  (cost=0.57..11294.73 rows=115493 width=22) (actual time=0.077..33.547 rows=114999 loops=1)');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                            Sort Key: issue_labels_history.issue, issue_labels_history."timestamp"');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                            Presorted Key: issue_labels_history.issue');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                            Full-sort Groups: 3572  Sort Method: quicksort  Average Memory: 27kB  Peak Memory: 27kB');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                            Buffers: shared hit=1204');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                            ->  Index Only Scan using idx_pr_labels_repo_label_issue_ts on issue_labels_history  (cost=0.42..7178.28 rows=115493 width=22) (actual time=0.031..16.227 rows=114999 loops=1)');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                                  Index Cond: ((repository = ''rust-lang/rust''::text) AND (label = ''S-waiting-on-review''::text))');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                                  Heap Fetches: 0');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                                  Buffers: shared hit=1201');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('          ->  Materialize  (cost=47187.42..56112.90 rows=2744 width=24) (actual time=118.633..300.325 rows=172425 loops=1)');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                Buffers: shared hit=3441, temp read=1125 written=1132');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                ->  Subquery Scan on ordered_transitions  (cost=47187.42..56106.04 rows=2744 width=24) (actual time=118.625..270.821 rows=152998 loops=1)');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                      Filter: (ordered_transitions.event_type = ANY (''{created,reopened}''::text[]))');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                      Rows Removed by Filter: 120000');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                      Buffers: shared hit=3441, temp read=1125 written=1132');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                      ->  WindowAgg  (cost=47187.42..52675.80 rows=274419 width=45) (actual time=118.622..247.445 rows=272998 loops=1)');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                            Buffers: shared hit=3441, temp read=1125 written=1132');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                            ->  Sort  (cost=47187.42..47873.47 rows=274419 width=37) (actual time=118.614..147.617 rows=272999 loops=1)');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                                  Sort Key: issues.issue, issues.created_at');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                                  Sort Method: external merge  Disk: 9000kB');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                                  Buffers: shared hit=3441, temp read=1125 written=1132');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                                  ->  Append  (cost=0.00..14895.12 rows=274419 width=37) (actual time=2.830..47.461 rows=273000 loops=1)');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                                        Buffers: shared hit=3441');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                                        ->  Seq Scan on issues  (cost=0.00..5059.00 rows=149927 width=48) (actual time=2.829..21.697 rows=150000 loops=1)');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                                              Filter: (is_pr AND (repository = ''rust-lang/rust''::text))');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                                              Rows Removed by Filter: 50000');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                                              Buffers: shared hit=2559');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                                        ->  Index Only Scan using idx_pr_event_hist_state_changes on issue_event_history  (cost=0.42..5719.84 rows=124492 width=23) (actual time=0.035..12.330 rows=123000 loops=1)');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                                              Index Cond: (repository = ''rust-lang/rust''::text)');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                                              Heap Fetches: 0');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                                              Buffers: shared hit=882');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('  CTE daily_deltas');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('    ->  HashAggregate  (cost=2.14..2.62 rows=48 width=12) (actual time=515.737..515.881 rows=1857 loops=1)');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('          Group Key: ((lower(in_state_periods.valid_period))::date)');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('          Batches: 1  Memory Usage: 393kB');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('          Buffers: shared hit=4645, temp read=1461 written=1468');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('          ->  Append  (cost=0.00..1.90 rows=48 width=8) (actual time=118.735..495.436 rows=137426 loops=1)');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                Buffers: shared hit=4645, temp read=1461 written=1468');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                ->  CTE Scan on in_state_periods  (cost=0.00..0.72 rows=29 width=8) (actual time=118.735..475.504 rows=68713 loops=1)');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                      Buffers: shared hit=4645, temp read=1125 written=1467');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                ->  CTE Scan on in_state_periods in_state_periods_1  (cost=0.00..0.75 rows=19 width=8) (actual time=0.021..12.388 rows=68713 loops=1)');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                      Filter: (NOT upper_inf(valid_period))');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                      Buffers: temp read=336 written=1');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('  InitPlan 3 (returns $2)');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('    ->  Aggregate  (cost=1.12..1.13 rows=1 width=32) (actual time=0.202..0.203 rows=1 loops=1)');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('          ->  CTE Scan on daily_deltas  (cost=0.00..1.08 rows=16 width=8) (actual time=0.001..0.115 rows=1856 loops=1)');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                Filter: (event_date < ''2025-12-01''::date)');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                Rows Removed by Filter: 1');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('  ->  Sort  (cost=71.29..73.79 rows=1000 width=12) (actual time=516.510..516.511 rows=32 loops=1)');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('        Sort Key: ((d.d)::date)');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('        Sort Method: quicksort  Memory: 26kB');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('        Buffers: shared hit=4648, temp read=1461 written=1468');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('        ->  Hash Left Join  (cost=1.56..21.46 rows=1000 width=12) (actual time=516.454..516.463 rows=32 loops=1)');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('              Hash Cond: ((d.d)::date = dd.event_date)');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('              Buffers: shared hit=4645, temp read=1461 written=1468');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('              ->  Function Scan on generate_series d  (cost=0.00..10.00 rows=1000 width=8) (actual time=0.015..0.018 rows=32 loops=1)');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('              ->  Hash  (cost=0.96..0.96 rows=48 width=12) (actual time=516.415..516.415 rows=1857 loops=1)');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                    Buckets: 2048 (originally 1024)  Batches: 1 (originally 1)  Memory Usage: 96kB');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                    Buffers: shared hit=4645, temp read=1461 written=1468');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                    ->  CTE Scan on daily_deltas dd  (cost=0.00..0.96 rows=48 width=12) (actual time=515.739..516.182 rows=1857 loops=1)');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('                          Buffers: shared hit=4645, temp read=1461 written=1468');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('Planning:');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('  Buffers: shared hit=247 read=7');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('Planning Time: 2.361 ms');
INSERT INTO "MY_TABLE"("QUERY PLAN") VALUES ('Execution Time: 519.828 ms');
