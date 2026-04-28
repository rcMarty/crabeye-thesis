# Crabeye — Implementation Reference

> **Purpose of this document:** Factual description of the Crabeye backend implementation for use as
> context by AI assistants working on the diploma thesis text. Everything stated here is derived
> directly from the source code.

---

## 1. Project Overview

**Crabeye** (name derived from "Repository Analyzer") is a backend service written in **Rust (edition
2021)** that collects, stores and analyses data from GitHub repositories — primarily the [
`rust-lang/rust`](https://github.com/rust-lang/rust) compiler repository. It is the practical part
of a diploma thesis.

The application has three operational modes, selected via CLI subcommands (powered by `clap`):

| Subcommand                                              | Description                                                                                                                                                                                                                                                                                                                                |
|---------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `sync-all --sync <N\|YYYY-MM-DD> [--full-history true]` | **Data ingestion.** Fetches PRs, issues, contributors and file activity from GitHub and stores them in PostgreSQL. `--sync 5` fetches last 5 pages (500 items), `--sync 2025-01-01` fetches everything updated since that date. `--full-history` additionally downloads the full timeline (label changes, state events) for each issue/PR. |
| `backfill`                                              | **Retroactive history fill.** Finds issues/PRs in the database that have no event history rows and fetches their timeline from GitHub retroactively.                                                                                                                                                                                       |
| `serve`                                                 | **REST API server** on `0.0.0.0:7878` with auto-generated OpenAPI documentation. Concurrently runs a **background sync loop** (`StateMonitor`) that every 60 seconds fetches new data from GitHub using `Since(last_known_event_timestamp)` mode with full history.                                                                        |

---

## 2. Directory Structure

```
backend/
├── Cargo.toml                 # Dependencies and project metadata
├── build.rs                   # Re-run triggers for migrations
├── docker-compose.yml         # PostgreSQL 14 (Alpine) container
├── migrations/
│   ├── 20251117165302_init.sql           # Table definitions
│   ├── 20251117165303_add_indexes.sql    # Indexes + unique constraints
│   ├── 20251117165304_constraints.sql    # Foreign keys + check constraints
│   ├── 20251117165305_optimize_indexes.sql # Covering/partial indexes for analytics
│   └── 20251117165306_add_created_at.sql # Split timestamp → created_at + edited_at on issues table
├── src/
│   ├── main.rs                # Entry point: config → DB → CLI dispatch / API server
│   ├── commands.rs            # CLI definition (clap): SyncAll, Backfill, Serve
│   ├── config.rs              # Environment-based configuration (.env / env vars)
│   ├── misc.rs                # Progress bar helpers (indicatif)
│   ├── api/
│   │   ├── mod.rs             # Shared request/response types, Pagination, GroupingLevel, FileNode
│   │   ├── app_state.rs       # AppState struct (holds Database clone)
│   │   ├── docs.rs            # OpenAPI doc routes: Scalar, Redoc, Swagger UIs
│   │   ├── review.rs          # PR-related endpoints (7 routes)
│   │   ├── issues.rs          # Issue-related endpoints (1 route)
│   │   └── teams.rs           # Team-related endpoints (1 route)
│   ├── db/
│   │   ├── mod.rs             # Database struct (PgPool wrapper)
│   │   ├── inserts.rs         # Write operations: upsert PRs/issues/contributors/teams/file_activity
│   │   ├── queries.rs         # Read-only analytical queries (the core of the analysis)
│   │   ├── misc.rs            # Helper queries: get_all_teams, get_user_id_by_name, backfill helpers
│   │   └── model/
│   │       ├── mod.rs         # IssueLike trait, BackfillRecord
│   │       ├── pr_event.rs    # PrEvent, PullRequestStatus, PullRequestStatusRequest, FileActivity
│   │       ├── issue.rs       # Issue, IssueStatus, IssueEvent, IssueLabel, LabelEventAction
│   │       ├── team_member.rs # TeamMember, Team, Contributor
│   │       ├── paginated_response.rs # PaginatedResponse<T>
│   │       └── responses.rs   # TopFilesResponse
│   ├── git/
│   │   ├── mod.rs             # SyncHandler: orchestrates the full sync pipeline
│   │   ├── git.rs             # Repo: local git operations via git2 (clone, fetch, diff)
│   │   └── github/
│   │       ├── mod.rs         # GitHubApi struct, SyncMode enum, constructor, low-level request helpers
│   │       ├── rate_limit.rs  # Rate limit detection + retry with exponential backoff (backoff crate)
│   │       ├── fetch.rs       # Public API: get_issues, get_pull_requests, get_authorized_users, process_backfill
│   │       └── parse.rs       # Timeline fetching, PR/issue parsing, label/event extraction
│   └── monitoring/
│       ├── mod.rs
│       └── state_tracker.rs   # StateMonitor: periodic background sync loop (60s interval)
└── test_repos/
    └── rust/                  # Cloned rust-lang/rust repository (used for file diffs)
```

---

## 3. Technology Stack

| Crate                    | Version                             | Role                                                                                |
|--------------------------|-------------------------------------|-------------------------------------------------------------------------------------|
| **axum**                 | 0.8.8                               | HTTP framework for the REST API                                                     |
| **aide**                 | 0.15.1                              | Automatic OpenAPI schema generation from axum routes (Scalar, Redoc, Swagger UIs)   |
| **sqlx**                 | 0.8.6                               | Async PostgreSQL driver with **compile-time checked queries**, migration runner     |
| **octocrab**             | fork: `github.com/rcMarty/octocrab` | GitHub REST API client (PRs, issues, timeline events)                               |
| **git2**                 | 0.20.4                              | libgit2 bindings — clone/fetch repository, diff merge commits to get modified files |
| **rust_team_data**       | from `github.com/rust-lang/team`    | Official Rust-lang team membership data (teams, members, roles)                     |
| **tokio**                | 1.48.0                              | Async runtime (multi-threaded)                                                      |
| **clap**                 | 4.5.52                              | CLI argument parsing with derive macros                                             |
| **chrono**               | 0.4.42                              | Date/time handling (NaiveDate, DateTime\<Utc\>)                                     |
| **serde** + **schemars** | 1.0 / 0.9                           | Serialization and JSON Schema generation (used by aide for OpenAPI)                 |
| **tower-http**           | 0.6.8                               | CORS middleware layer                                                               |
| **backoff**              | 0.4                                 | Exponential backoff retry for GitHub API rate limit protection (tokio feature)      |
| **indicatif**            | 0.18.3                              | Terminal progress bars for long sync operations                                     |
| **secrecy**              | 0.10.3                              | Safe handling of GitHub tokens                                                      |
| **reqwest**              | 0.13.2                              | HTTP client (used to fetch `rust_team_data` JSON)                                   |
| **indexmap**             | 2.13.0                              | Ordered map for deterministic JSON output                                           |

**Database:** PostgreSQL 14 (Alpine), containerized via Docker Compose, port 5431→5432.

---

## 4. Database Schema

Issues and Pull Requests share a single `issues` table, differentiated by the `is_pr` boolean
column. This unified design simplifies foreign keys and allows shared history tables.

### 4.1 Tables

```
┌─────────────────────┐       ┌──────────────────────────┐
│       teams          │       │     contributors          │
│──────────────────────│       │───────────────────────────│
│ team (PK, TEXT)      │◄──┐   │ github_id (PK, BIGINT)   │
│ subteam_of (FK→teams)│   │   │ github_name (TEXT)        │
│ kind (TEXT)          │   │   │ name (TEXT, nullable)     │
└─────────────────────┘   │   └───────────────────────────┘
                          │              │
                  ┌───────┴──────────────┴───────┐
                  │    contributors_teams (M:N)   │
                  │───────────────────────────────│
                  │ team (PK, FK→teams)           │
                  │ contributor_id (PK, FK→contri)│
                  └───────────────────────────────┘

┌────────────────────────────────────┐
│             issues                  │
│─────────────────────────────────────│
│ repository (PK, TEXT)              │
│ issue (PK, BIGINT)                 │
│ is_pr (BOOLEAN, NOT NULL)          │
│ contributor_id (FK→contributors)   │
│ current_state (TEXT)               │
│ created_at (TIMESTAMP, NOT NULL)   │
│ edited_at (TIMESTAMP, NOT NULL)    │
│ merge_sha (TEXT, nullable)         │
└────────────────────────────────────┘
         │ (repository, issue)
         │
    ┌────┴────────────────────────┬──────────────────────────────┐
    ▼                             ▼                              ▼
┌───────────────────────┐  ┌────────────────────────┐  ┌─────────────────────┐
│ issue_event_history    │  │ issue_labels_history    │  │   file_activity      │
│────────────────────────│  │─────────────────────────│  │──────────────────────│
│ id (SERIAL PK)        │  │ id (SERIAL PK)          │  │ id (SERIAL PK)      │
│ repository (FK)       │  │ repository (FK)         │  │ repository (FK)     │
│ issue (FK)            │  │ issue (FK)              │  │ issue (FK)          │
│ is_pr (BOOLEAN)       │  │ is_pr (BOOLEAN)         │  │ file_path (TEXT)    │
│ event (TEXT)          │  │ label (TEXT)             │  │ contributor_id (BIG)│
│ timestamp (TIMESTAMP) │  │ timestamp (TIMESTAMP)   │  │ activity_type (TEXT)│
│                       │  │ action (TEXT: ADDED/     │  │ timestamp (TIMESTAMP│
│ UNIQUE(repo,issue,    │  │         REMOVED)         │  │                     │
│   timestamp,event)    │  │ UNIQUE(repo,issue,       │  │ UNIQUE(repo,issue,  │
└───────────────────────┘  │   timestamp,label)       │  │   timestamp,file)   │
                           └─────────────────────────┘  └─────────────────────┘
```

### 4.2 Tracked Events

Events stored in `issue_event_history.event`:

- `merged` — PR was merged
- `closed` — issue/PR was closed
- `reopened` — issue/PR was reopened
- `committed` — a commit was pushed (PR only)
- `commented` — a comment was posted
- `reviewed` — a review was submitted (PR only)

Labels stored in `issue_labels_history`:

- Any GitHub label with its ADDED/REMOVED action and timestamp
- Particularly important: `S-waiting-on-review`, `S-waiting-on-bors`, `S-waiting-on-author` (Rust
  project workflow labels)

### 4.3 Indexes

The schema uses **28+ indexes** across 4 migration files, including:

- **Partial indexes** with `WHERE is_pr = true/false` to separate PR and issue workloads
- **Covering indexes** with `INCLUDE` columns to enable index-only scans (e.g.,
  `idx_pr_event_hist_state_changes` includes `event` column)
- **Composite indexes** aligned with `DISTINCT ON` and `ORDER BY` patterns used in analytical
  queries
- **Optimized indexes** for the specific analytical query patterns:
    - `idx_pr_event_hist_state_changes` — state-change events (closed/merged/reopened) for DISTINCT
      ON + LEAD window queries
    - `idx_pr_event_hist_merged` — merged events only for cumulative counts
    - `idx_pr_labels_repo_label_issue_ts` — label history for S-* label queries
    - `idx_file_activity_repo_contrib_ts` — contributor file activity with INCLUDE(file_path, issue)

---

## 5. Data Ingestion Pipeline

The ingestion is orchestrated by `SyncHandler` (in `src/git/mod.rs`) and proceeds in this order:

### 5.1 Team & Contributor Sync

1. Fetches `teams.json` from the official [rust-lang/team](https://github.com/rust-lang/team)
   repository data API
2. Parses all teams (name, subteam_of, kind: team/working_group/project_group/marker_team) and their
   members
3. Bulk upserts contributors using `UNNEST` arrays
4. Bulk upserts teams
5. **Replaces** the entire `contributors_teams` join table (DELETE + INSERT) to reflect current
   membership

### 5.2 Pull Request Sync

1. Fetches PRs from GitHub API via `octocrab`, paginated (100 per page)
2. Two sync modes:
    - `Last(N)` — fetches the last N pages sorted by creation date descending
    - `Since(date)` — fetches pages sorted by update date descending until all PRs have
      `updated_at < date`
3. Each PR is parsed into a `PrEvent` with status determined from:
    - `IssueState::Open` + current labels → checks for `S-waiting-on-*` labels
    - `IssueState::Closed` + `merged_at` present → `Merged` (with `merge_commit_sha`)
    - `IssueState::Closed` + no `merged_at` → `Closed`
4. **With `--full-history`:** For each PR, fetches the full timeline via `list_timeline_events`
   API (paginated, 100/page):
    - `Labeled`/`Unlabeled` events → `IssueLabel` (label name, timestamp, ADDED/REMOVED)
    - `Merged`/`Closed`/`Reopened`/`Committed`/`Commented`/`Reviewed` events → `IssueEvent`
    - Timeline fetching is optimized: stops early if events overlap with already-stored data (checks
      `get_last_update`)
5. Upserts into `issues` table with **optimistic concurrency**:
   `ON CONFLICT DO UPDATE ... WHERE issues.edited_at < EXCLUDED.edited_at`. The `created_at` column
   is set on first insert and never updated.
6. If history is present, bulk inserts into `issue_event_history` and `issue_labels_history` using
   `UNNEST` with `ON CONFLICT DO NOTHING`
7. For merged PRs: uses `git2` to diff the merge commit against its first parent → collects modified
   file paths → bulk inserts into `file_activity`

### 5.3 Issue Sync

Same as PR sync but:

- Uses the issues API endpoint instead of pulls
- Skips entries that have `pull_request` field set (those are PRs, not issues)
- No file activity extraction (issues don't have merge commits)
- Sets `is_pr = false` in all database rows

### 5.4 Backfill

1. Queries `issues` table for records that have **no rows** in `issue_event_history`
2. Processes them in chunks of 100
3. For each record, fetches the full timeline from GitHub and parses labels + events
4. Bulk inserts the history via `insert_history`

### 5.5 Background Sync (Serve mode)

`StateMonitor` runs a loop with a 60-second `tokio::time::interval`:

1. Fetches the latest event timestamp from `issue_event_history` for the repository
2. Calls `sync_with_full_info(SyncMode::Since(last_timestamp))`
3. This updates the local git repository (`git fetch`), then syncs teams, PRs and issues that have
   been updated since the last known event

### 5.6 GitHub API Rate Limit Protection

All GitHub REST API calls (PR/issue listing, timeline event fetching) are wrapped with a
`retry_on_rate_limit` helper (in `src/git/github/rate_limit.rs`) that provides edge-case protection
against hitting GitHub's 5,000 req/hour limit:

- **Detection:** Inspects `octocrab::Error` debug output for rate-limit indicators: `"rate limit"`,
  `"too many requests"`, `"secondary rate"`, `"abuse detection"`, HTTP `429`
- **Retry strategy:** Uses the `backoff` crate with exponential backoff:
    - Initial interval: **30 seconds**
    - Maximum interval: **5 minutes**
    - Maximum total elapsed time: **10 minutes** (then gives up and propagates the error)
- **Non-rate-limit errors** (4xx, 5xx, network) are immediately returned without retry
- In normal periodic sync mode (~60 req/hour), the rate limiter is never triggered; it serves purely
  as a safety net for bulk operations (initial sync, backfill)

---

## 6. REST API Endpoints

All endpoints are served on `0.0.0.0:7878`. OpenAPI documentation is auto-generated via `aide`.

### 6.1 PR Endpoints (`/api/pr`)

| Method | Path                      | Description                                                                | Key Parameters                                                                 |
|--------|---------------------------|----------------------------------------------------------------------------|--------------------------------------------------------------------------------|
| GET    | `/reviewers`              | Contributors who touched files matching a path prefix within a time window | `repository`, `file` (LIKE prefix), `last_n_days`, `anchor_date`, `pagination` |
| GET    | `/top-n-files`            | N most recent file touches by a user                                       | `repository`, `name` (github_name, ILIKE search), `top_n`, `last_n_days`       |
| GET    | `/prs-in-state`           | Count of PRs in a specific state at a given date                           | `repository`, `timestamp`, `state` (enum: 6 variants)                          |
| GET    | `/prs-in-state-over-time` | Time-series of daily PR counts in a state                                  | `repository`, `state`, `anchor_date`, `last_n_days` (1–90)                     |
| GET    | `/pr-history/{issue}`     | Full PR state reconstruction at a date (labels + events)                   | `repository`, `timestamp`                                                      |
| GET    | `/waiting-for-review`     | Paginated PRs currently in any S-waiting-* state                           | `repository`, `pagination`                                                     |
| GET    | `/files-modified-by-team` | Files modified by team members, with optional folder grouping              | `repository`, `team_name`, `anchor_date`, `last_n_days`, `group_level`         |

### 6.2 Issue Endpoints (`/api/issue`)

| Method | Path                    | Description                          | Key Parameters            |
|--------|-------------------------|--------------------------------------|---------------------------|
| GET    | `/issue-events/{issue}` | Events of an issue on a specific day | `repository`, `timestamp` |

### 6.3 Team Endpoints (`/api/teams`)

| Method | Path | Description               |
|--------|------|---------------------------|
| GET    | `/`  | List all known team names |

### 6.4 Documentation & Health

| Method | Path                     | Description                  |
|--------|--------------------------|------------------------------|
| GET    | `/docs`                  | Scalar API documentation UI  |
| GET    | `/docs/redoc`            | Redoc API documentation UI   |
| GET    | `/docs/swagger`          | Swagger API documentation UI |
| GET    | `/docs/private/api.json` | Raw OpenAPI JSON schema      |
| GET    | `/health`                | Health check (returns "OK")  |

### 6.5 Response Types

- **Paginated responses** use `PaginatedResponse<T>` with `items`, `total_count`, `page`, `per_page`
- **Error responses** use `ApiError { message: String }` with appropriate HTTP status codes (404,
    500)
- **File grouping** (`/files-modified-by-team`) supports three modes via `group_level` parameter:
    - `"none"` (default) → flat `IndexMap<String, i64>` sorted by count descending
    - A number (e.g., `2`) → tree structure grouped by folder depth, returned as
      `FileNode { name, modifications, children }`
    - `"all"` → full folder hierarchy tree

---

## 7. Domain Model

### 7.1 Core Types

**`PrEvent`** — Represents a Pull Request:

```
repository: String          # e.g. "rust-lang/rust"
pr_number: i64              # GitHub PR number
author_id: i64              # GitHub user ID of the author
created_at: DateTime<Utc>   # When the PR was created on GitHub (immutable)
state: PullRequestStatus    # Current status (enum, see below); the `time` field inside each variant represents `edited_at`
events_history: Option<Vec<IssueEvent>>   # Timeline events (if fetched)
labels_history: Option<Vec<IssueLabel>>   # Label changes (if fetched)
```

**`PullRequestStatus`** — Enum with 6 variants, each carrying an `edited_at` timestamp (`time`
field):

- `WaitingForReview { time }` — has `S-waiting-on-review` label
- `WaitingForBors { time }` — has `S-waiting-on-bors` label
- `WaitingForAuthor { time }` — has `S-waiting-on-author` label
- `Open { time }` — open without a specific waiting label
- `Closed { time }` — closed without merge
- `Merged { merge_sha, time }` — merged with the merge commit SHA

**`PullRequestStatusRequest`** — Enum for API filter parameters (same 6 variants, no attached data).
Maps to label strings via `Display`: `WaitingForReview` → `"S-waiting-on-review"`, etc.

**`Issue`** — Represents a GitHub Issue (not PR):

```
repository: String
issue_number: i64
author_id: i64
created_at: DateTime<Utc>   # When the issue was created on GitHub (immutable)
status: IssueStatus         # Open { time } | Closed { time } — the `time` field represents `edited_at`
events_history: Option<Vec<IssueEvent>>
labels_history: Option<Vec<IssueLabel>>
```

**`IssueEvent`** — A point-in-time event: `{ event: String, timestamp: NaiveDateTime }`

**`IssueLabel`** — A label change:
`{ label: String, timestamp: NaiveDateTime, action: LabelEventAction }` where `LabelEventAction` is
`ADDED | REMOVED`.

**`Contributor`** — `{ github_id: u64, github_name: String, name: Option<String> }`

**`TeamMember`** — Extends Contributor with `teams: Vec<Team>` where
`Team { team, subteam_of, kind }`.

**`FileActivity`** — `{ repository, pr, file_path, user_id, timestamp }` — a record of a file being
modified in a merged PR.

### 7.2 The `IssueLike` Trait

A trait that unifies `PrEvent`, `Issue`, and `BackfillRecord` for generic history insertion:

```rust
trait IssueLike {
    fn events_history(&self) -> Option<&Vec<IssueEvent>>;
    fn labels_history(&self) -> Option<&Vec<IssueLabel>>;
    fn repository(&self) -> &String;
    fn issue_number(&self) -> i64;
    fn author_id(&self) -> i64;
    fn is_pr(&self) -> bool;
}
```

This allows the `insert_issues_history` function to accept any slice of `IssueLike` items and
bulk-insert their event + label history rows in a single transaction.

### 7.3 The Rust Project Workflow Labels

The `rust-lang/rust` repository uses specific `S-*` labels to track PR workflow state:

- **`S-waiting-on-review`** — PR is waiting for a reviewer to look at it
- **`S-waiting-on-author`** — PR needs changes from the author
- **`S-waiting-on-bors`** — PR has been approved and is in the merge queue (bors is the Rust
  project's merge bot)

These labels are the primary mechanism for tracking PR workflow state in the analytical queries. The
tool tracks the full history of label additions and removals to reconstruct the state at any point
in time.

---

## 8. Analytical Query Design

The analytical queries in `src/db/queries.rs` are the core of the analysis tool. They use several
advanced PostgreSQL patterns:

### 8.1 Point-in-Time State Reconstruction

**Pattern: `DISTINCT ON` for latest event per entity**

PostgreSQL-specific `DISTINCT ON (issue)` with `ORDER BY issue, timestamp DESC` retrieves only the
most recent event per PR in a single scan. Used in:

- `get_pr_count_in_state` — finding the latest state-change event and latest label event per PR up
  to a given timestamp
- `get_prs_waiting_for_review` — finding the latest label event per (issue, label) pair
- `get_pr_history_from` — finding the latest S-* label per PR

Example (from `get_pr_count_in_state`, label-based states):

```sql
WITH latest_labels AS (SELECT DISTINCT ON (issue) issue, action
                       FROM issue_labels_history
                       WHERE repository = $1
                         AND is_pr = true
                         AND label = $3
                         AND timestamp <= $2
                       ORDER BY issue, timestamp DESC),
     latest_state AS (SELECT DISTINCT ON (issue) issue, event
                      FROM issue_event_history
                      WHERE repository = $1
                        AND is_pr = true
                        AND event IN ('closed', 'merged', 'reopened')
                        AND timestamp <= $2
                      ORDER BY issue, timestamp DESC)
SELECT COUNT(*)
FROM latest_labels ll
         LEFT JOIN latest_state ls ON ls.issue = ll.issue
WHERE ll.action = 'ADDED'
  AND (ls.issue IS NULL OR ls.event = 'reopened')
```

### 8.2 Time-Series via Interval Construction

**Pattern: `LEAD()` window function to convert point events into time intervals**

Instead of re-scanning the event tables for every day in a range (O(days × events)), the queries
convert point-in-time events into half-open intervals `[start, end)` using the `LEAD()` window
function, then count how many intervals overlap each day. This reduces complexity to O(events +
days × active_periods).

The general pattern:

1. **Build intervals** — `LEAD(timestamp) OVER (PARTITION BY issue ORDER BY timestamp)` gives the
   next event's timestamp, creating `[current_ts, next_ts)` intervals
2. **Filter relevant intervals** — e.g., keep only intervals starting with 'created'/'reopened' for
   open periods
3. **Intersect interval sets** — for composite states (e.g., "PR is open AND has label"), use
   `GREATEST`/`LEAST` to compute the intersection
4. **Count per day** — join with `generate_series` date range and count distinct issues whose
   interval covers each day

Example (from `get_pr_count_in_state_over_time`, label-based waiting states):

```sql
-- open_periods: [created|reopened, next close/merge)
-- label_active_periods: [ADDED, next label event)
-- Intersection: PR is in target state when BOTH open AND label active
in_state_periods AS (
    SELECT lap.issue,
           GREATEST(lap.start_ts, op.start_ts) AS start_ts,
           LEAST(lap.end_ts, op.end_ts)         AS end_ts
    FROM label_active_periods lap
    JOIN open_periods op
      ON lap.issue = op.issue
     AND lap.start_ts < op.end_ts
     AND lap.end_ts   > op.start_ts
)
```

### 8.3 Cumulative Counts

**Pattern: Running SUM with window function for monotonic states**

For merged PRs (once merged, always merged), the query:

1. Finds each PR's first merge date
2. Counts merges before the date range (base count)
3. Uses `SUM(daily_new_merges) OVER (ORDER BY day)` as a running total added to the base

### 8.4 Bulk Insert Optimization

**Pattern: PostgreSQL `UNNEST` for batch inserts**

All bulk inserts use `UNNEST($1::TYPE[], $2::TYPE[], ...)` instead of individual INSERT statements.
Combined with `ON CONFLICT DO NOTHING` (for history) or
`ON CONFLICT DO UPDATE ... WHERE edited_at < EXCLUDED.edited_at` (for main records, optimistic
upsert — only update if incoming data is newer). The `created_at` column is set on first insert and
preserved on subsequent upserts.

### 8.5 Day Alignment

All timestamp-based queries align to UTC day boundaries (`00:00:00`). Date ranges use
`NaiveDate.and_hms_opt(0, 0, 0)` in Rust and
`generate_series($start::timestamp, $end::timestamp, '1 day'::interval)` in SQL. Open-ended
intervals use sentinel `'9999-12-31'::timestamp`.

---

## 9. Configuration

Configuration is loaded from environment variables (with `.env` file support via `dotenvy`):

| Variable       | Required | Default          | Description                                                                        |
|----------------|----------|------------------|------------------------------------------------------------------------------------|
| `GITHUB_TOKEN` | Yes      | —                | GitHub personal access token for API access                                        |
| `DATABASE_URL` | No       | `sqlite:data.db` | PostgreSQL connection string (despite the SQLite default, the app uses PostgreSQL) |
| `REPO_OWNER`   | Yes      | —                | GitHub repository owner (e.g., `rust-lang`)                                        |
| `REPO_NAME`    | Yes      | —                | GitHub repository name (e.g., `rust`)                                              |
| `LOG_LEVEL`    | No       | `info`           | Logging level (trace, debug, info, warn, error)                                    |

---

## 10. Known Limitations & TODOs

1. **No automated tests** — the project has no test suite
2. **Single-repository focus** — configured via environment variables for one repository at a time
3. **Forked octocrab dependency** — uses a custom fork at `github.com/rcMarty/octocrab` (may contain
   patches for timeline event parsing)
4. **No webhook support** — data ingestion is polling-based only (periodic sync every 60s in serve
   mode)
5. **Full join table replacement** — `contributors_teams` is completely DELETEd and re-INSERTed on
   every team sync, which is acceptable for the current scale but not ideal
6. **No incremental file activity** — file diffs are only computed for merged PRs (requires the
   merge commit in the local git clone)
7. **Local git clone required** — the application clones the entire target repository to
   `./test_repos/{repo_name}` for file diff operations
8. **DATABASE_URL default is misleading** — defaults to SQLite path but the application exclusively
   uses PostgreSQL (all SQL is PostgreSQL-specific)
9. **Single GitHub token** — uses one PAT (5,000 req/hour); no token rotation or GitHub App
   authentication for higher limits

