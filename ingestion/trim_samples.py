"""
Trims down full_load_sample.json / incremental_load_sample.json so they
stay a reasonable "sample" size instead of tens of MB.

Run this from the ingestion/ folder (same place as fetch_github_data.py):
    python trim_samples.py
"""

import os
import json

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
SAMPLES_DIR = os.path.join(PROJECT_ROOT, "data", "samples")

# Keep at most this many commits/issues PER REPO in each file
MAX_PER_REPO = 60

# Fields to strip from each commit/issue record - these carry large,
# rarely-needed nested data (avatar URLs, parent lists, big HTML bodies)
COMMIT_FIELDS_TO_KEEP = ["sha", "commit", "author", "committer", "html_url", "_repo"]
ISSUE_FIELDS_TO_KEEP = [
    "id", "number", "title", "user", "state", "created_at", "updated_at",
    "closed_at", "comments", "html_url", "labels", "_repo"
]


def trim_record(record, fields_to_keep):
    return {k: v for k, v in record.items() if k in fields_to_keep}


def trim_file(path):
    if not os.path.exists(path):
        print(f"Skipping {path} (not found)")
        return

    with open(path, "r") as f:
        data = json.load(f)

    original_commit_count = len(data.get("commits", []))
    original_issue_count = len(data.get("issues", []))

    # Group by repo so we cap PER repo, not just take the first N overall
    def cap_per_repo(records, fields_to_keep):
        by_repo = {}
        for r in records:
            repo = r.get("_repo", "unknown")
            by_repo.setdefault(repo, [])
            if len(by_repo[repo]) < MAX_PER_REPO:
                by_repo[repo].append(trim_record(r, fields_to_keep))
        result = []
        for repo_records in by_repo.values():
            result.extend(repo_records)
        return result

    data["commits"] = cap_per_repo(data.get("commits", []), COMMIT_FIELDS_TO_KEEP)
    data["issues"] = cap_per_repo(data.get("issues", []), ISSUE_FIELDS_TO_KEEP)

    with open(path, "w") as f:
        json.dump(data, f, indent=2)

    new_size_mb = os.path.getsize(path) / (1024 * 1024)
    print(f"{os.path.basename(path)}:")
    print(f"  commits: {original_commit_count} -> {len(data['commits'])}")
    print(f"  issues:  {original_issue_count} -> {len(data['issues'])}")
    print(f"  new file size: {new_size_mb:.2f} MB")


if __name__ == "__main__":
    trim_file(os.path.join(SAMPLES_DIR, "full_load_sample.json"))
    trim_file(os.path.join(SAMPLES_DIR, "incremental_load_sample.json"))
    print("\nDone trimming.")