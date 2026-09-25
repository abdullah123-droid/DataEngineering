"""
GitHub Data Ingestion Script
Pulls commits and issues for a list of repos: both a FULL load (large history)
and an INCREMENTAL load (only recent changes).

Designed for slow/unstable internet: saves progress after EVERY repo, and
can be safely re-run - it will skip repos it already finished and only
fetch the ones that are missing.
"""

import os
import json
import time
import requests
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
SAMPLES_DIR = os.path.join(PROJECT_ROOT, "data", "samples")
os.makedirs(SAMPLES_DIR, exist_ok=True)

FULL_LOAD_PATH = os.path.join(SAMPLES_DIR, "full_load_sample.json")
INCREMENTAL_LOAD_PATH = os.path.join(SAMPLES_DIR, "incremental_load_sample.json")
CHECKPOINT_PATH = os.path.join(SAMPLES_DIR, "_checkpoint.json")

load_dotenv(os.path.join(PROJECT_ROOT, ".env"))
TOKEN = os.getenv("GITHUB_TOKEN")

HEADERS = {
    "Authorization": f"token {TOKEN}",
    "Accept": "application/vnd.github+json"
}

REPOS = [
    "facebook/react",
    "vuejs/vue",
    "tensorflow/tensorflow",
    "apache/spark",
    "pallets/flask",
    "tiangolo/fastapi",
    "psf/requests",
    "django/django",
    "pandas-dev/pandas",
    "scikit-learn/scikit-learn",
    "nodejs/node",
    "microsoft/vscode",
    "kubernetes/kubernetes",
    "docker/compose",
    "expressjs/express",
    "axios/axios",
    "twbs/bootstrap",
    "sveltejs/svelte",
]


def load_json(path, default):
    if os.path.exists(path):
        try:
            with open(path, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return default
    return default


def save_json(path, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


def fetch_paginated(url, params, max_pages):
    """Fetch pages of results, retrying each page up to 5 times on failure."""
    all_results = []
    for page in range(1, max_pages + 1):
        params["page"] = page
        params["per_page"] = 100

        max_retries = 5
        response = None
        for attempt in range(1, max_retries + 1):
            try:
                response = requests.get(url, headers=HEADERS, params=params, timeout=20)
                break
            except requests.exceptions.RequestException as e:
                print(f"    attempt {attempt}/{max_retries} failed: {type(e).__name__}")
                if attempt < max_retries:
                    wait = 5 * attempt
                    print(f"    waiting {wait}s before retry...")
                    time.sleep(wait)

        if response is None:
            print(f"    giving up on page {page} after {max_retries} attempts")
            return all_results, False  # False = incomplete

        if response.status_code != 200:
            print(f"    HTTP {response.status_code}: {response.text[:150]}")
            return all_results, False

        data = response.json()
        if not data:
            break

        all_results.extend(data)
        print(f"    page {page}: got {len(data)} records")
        time.sleep(0.5)

    return all_results, True  # True = completed fully


def fetch_repo_data(repo, since_param=None, max_pages=3):
    """Fetch commits + issues for one repo. Returns (data_dict, success_bool)."""
    commit_params = {}
    issue_params = {"state": "all"}
    if since_param:
        commit_params["since"] = since_param
        issue_params["since"] = since_param

    print(f"  commits for {repo}")
    commits, commits_ok = fetch_paginated(
        f"https://api.github.com/repos/{repo}/commits", commit_params, max_pages
    )
    for c in commits:
        c["_repo"] = repo

    print(f"  issues for {repo}")
    issues, issues_ok = fetch_paginated(
        f"https://api.github.com/repos/{repo}/issues", issue_params, max_pages
    )
    for i in issues:
        i["_repo"] = repo

    return {"commits": commits, "issues": issues}, (commits_ok and issues_ok)


def run_load(load_name, output_path, since_param, max_pages, checkpoint_key):
    print(f"\n=== {load_name} ===")

    existing = load_json(output_path, {"commits": [], "issues": []})
    checkpoint = load_json(CHECKPOINT_PATH, {})
    done_repos = set(checkpoint.get(checkpoint_key, []))

    for repo in REPOS:
        if repo in done_repos:
            print(f"Skipping {repo} (already done)")
            continue

        print(f"{repo}:")
        data, success = fetch_repo_data(repo, since_param=since_param, max_pages=max_pages)
        existing["commits"].extend(data["commits"])
        existing["issues"].extend(data["issues"])

        # Save progress immediately after every repo, success or not
        save_json(output_path, existing)

        if success:
            done_repos.add(repo)
            checkpoint[checkpoint_key] = sorted(done_repos)
            save_json(CHECKPOINT_PATH, checkpoint)
            print(f"  {repo} done and saved.")
        else:
            print(f"  {repo} INCOMPLETE - will retry on next run. Progress saved so far.")

    total_repos = len(REPOS)
    done_count = len(done_repos)
    print(f"\n{load_name} finished: {done_count}/{total_repos} repos fully complete.")
    print(f"Totals so far: {len(existing['commits'])} commits, {len(existing['issues'])} issues")
    if done_count < total_repos:
        print(f"NOT ALL REPOS DONE. Just re-run this script - it will pick up where it left off.")


if __name__ == "__main__":
    if not TOKEN:
        print("ERROR: No token found. Check your .env file at the project root.")
        exit(1)

    run_load(
        "FULL load", FULL_LOAD_PATH,
        since_param=None, max_pages=3, checkpoint_key="full_done"
    )

    since_date = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ")
    run_load(
        "INCREMENTAL load", INCREMENTAL_LOAD_PATH,
        since_param=since_date, max_pages=2, checkpoint_key="incremental_done"
    )

    print("\nAll done (or see messages above if some repos still need a re-run).")