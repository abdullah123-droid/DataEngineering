"""
Hashes author/committer emails and names in the sample JSON files so the
committed samples don't expose raw PII, per TA feedback.

Run from the ingestion/ folder:
    python anonymize_samples.py
"""

import os
import json
import hashlib

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
SAMPLES_DIR = os.path.join(PROJECT_ROOT, "data", "samples")

SALT = "de-project-2026-salt"  # keep this consistent across the whole project


def hash_value(value):
    if not value:
        return value
    return hashlib.sha256((SALT + value.lower().strip()).encode()).hexdigest()[:16]


def anonymize_commit(c):
    for role in ("author", "committer"):
        block = c.get("commit", {}).get(role)
        if block:
            if "email" in block:
                block["email"] = hash_value(block["email"])
            if "name" in block:
                block["name"] = hash_value(block["name"])
    return c


def anonymize_issue(i):
    user = i.get("user")
    if user:
        if "login" in user:
            user["login"] = hash_value(user["login"])
        if "id" in user:
            user["id"] = None
        # drop profile/avatar links entirely
        user.pop("avatar_url", None)
        user.pop("html_url", None)
    return i


def process_file(path):
    if not os.path.exists(path):
        print(f"Skipping {path} (not found)")
        return
    with open(path, "r") as f:
        data = json.load(f)

    data["commits"] = [anonymize_commit(c) for c in data.get("commits", [])]
    data["issues"] = [anonymize_issue(i) for i in data.get("issues", [])]

    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"Anonymized {os.path.basename(path)}: "
          f"{len(data['commits'])} commits, {len(data['issues'])} issues")


if __name__ == "__main__":
    process_file(os.path.join(SAMPLES_DIR, "full_load_sample.json"))
    process_file(os.path.join(SAMPLES_DIR, "incremental_load_sample.json"))
    print("Done.")