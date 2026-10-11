## Bronze Layer Data Model

### bronze_commits
| Column | Type | Notes |
|---|---|---|
| sha | STRING | **Primary key** |
| commit | STRUCT<author, committer, message> | Nested, as received from source |
| html_url | STRING | |
| author | STRUCT<login, type, site_admin> | GitHub account object (hashed login) |
| committer | STRUCT<login, type, site_admin> | GitHub account object (hashed login) |
| _repo | STRING | Repository identifier |
| load_timestamp | TIMESTAMP | When this record was ingested into Bronze |
| load_type | STRING | "full" or "incremental" |

### bronze_issues
| Column | Type | Notes |
|---|---|---|
| id | LONG | GitHub issue ID |
| number | LONG | Part of composite key with _repo |
| title | STRING | |
| user | STRUCT<login, type, site_admin> | Hashed reporter identity |
| state | STRING | open / closed |
| created_at, updated_at, closed_at | STRING | Raw ISO8601 strings (cast happens in Silver) |
| comments | LONG | |
| labels | ARRAY<STRUCT<id, name, color>> | |
| _repo | STRING | |
| load_timestamp | TIMESTAMP | When this record was ingested into Bronze |

## Silver Layer Data Model

### silver_commits
| Column | Type | Notes |
|---|---|---|
| commit_sha | STRING | **Primary key** (merge key) |
| repo_name | STRING | |
| author_name_hash | STRING | |
| author_email_hash | STRING | |
| authored_at | TIMESTAMP | Cast from string |
| committer_name_hash | STRING | |
| committed_at | TIMESTAMP | Cast from string |
| message | STRING | |
| author_login_hash | STRING | |
| load_type | STRING | |
| load_timestamp | TIMESTAMP | When this record was processed into Silver |

### silver_issues
| Column | Type | Notes |
|---|---|---|
| repo_name | STRING | **Primary key (composite with issue_number)** |
| issue_number | INT | **Primary key (composite with repo_name)** |
| title | STRING | |
| reporter_login_hash | STRING | |
| state | STRING | |
| created_at, updated_at, closed_at | TIMESTAMP | Cast from string |
| comment_count | INT | |
| load_type | STRING | |
| load_timestamp | TIMESTAMP | |

## pipeline_execution_logs (audit table)
| Column | Type |
|---|---|
| run_id | STRING |
| layer | STRING |
| parameter | STRING |
| start_time | TIMESTAMP |
| end_time | TIMESTAMP |
| status | STRING |
| rows_affected | LONG |
| error_message | STRING |

## Execution Guide

### Standard incremental load
Set widgets in `bronze_ingest`:
- `input_path` → path to the new incremental JSON file
- `load_type` → `"incremental"`

Run the notebook, then run `silver_transform` to merge the new data in.

### Backfill (any historical file)
Set `input_path` to any older/historical file's path instead of the current one — nothing is hardcoded to "today." Bronze is append-only, so re-running is safe. Silver uses MERGE INTO, so re-running it is always idempotent — verified by zero duplicate `commit_sha` / `(repo_name, issue_number)` pairs after multiple runs on the same data.