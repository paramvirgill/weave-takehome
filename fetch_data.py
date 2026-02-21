#!/usr/bin/env python3
"""
Fetch GitHub data for PostHog/posthog, score engineers by impact,
and save the top 5 to data.json.
"""

import os
import json
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")
if not GITHUB_TOKEN:
    raise SystemExit("Error: set GITHUB_TOKEN environment variable.")

REPO = "PostHog/posthog"
API = "https://api.github.com"
HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json",
}
SINCE = (datetime.now(timezone.utc) - timedelta(days=90)).isoformat()

# ── Helpers ───────────────────────────────────────────────────────────────────


def is_bot(username: str) -> bool:
    return "[bot]" in (username or "")


def paginate(url: str, params: Optional[dict] = None) -> List[dict]:
    """GET every page and return combined results."""
    params = dict(params or {})
    params.setdefault("per_page", 100)
    results = []  # type: List[dict]
    page = 1
    while True:
        params["page"] = page
        resp = requests.get(url, headers=HEADERS, params=params)
        if resp.status_code == 403 and "rate limit" in resp.text.lower():
            reset = int(resp.headers.get("X-RateLimit-Reset", time.time() + 60))
            wait = max(reset - int(time.time()), 1) + 5
            print(f"  ⏳ Rate-limited — sleeping {wait}s …")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        data = resp.json()
        if not data:
            break
        results.extend(data)
        page += 1
    return results


# ── Step 1: Fetch merged PRs ─────────────────────────────────────────────────


def search_issues(query: str) -> List[dict]:
    """Paginate the Search API (returns {items:[…]}, capped at 1000)."""
    results = []  # type: List[dict]
    page = 1
    while True:
        resp = requests.get(
            f"{API}/search/issues",
            headers=HEADERS,
            params={"q": query, "sort": "updated", "per_page": 100, "page": page},
        )
        if resp.status_code == 403 and "rate limit" in resp.text.lower():
            reset = int(resp.headers.get("X-RateLimit-Reset", time.time() + 60))
            wait = max(reset - int(time.time()), 1) + 5
            print(f"  ⏳ Rate-limited — sleeping {wait}s …")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        data = resp.json()
        items = data.get("items", [])
        if not items:
            break
        results.extend(items)
        if len(results) >= data.get("total_count", 0):
            break
        page += 1
    return results


def fetch_merged_prs() -> List[dict]:
    """Return all merged PRs in the last 90 days, chunked by week."""
    print("📦 Fetching merged PRs …")

    start = datetime.now(timezone.utc) - timedelta(days=90)
    end = datetime.now(timezone.utc)
    raw = []  # type: List[dict]
    seen = set()  # type: set

    chunk_start = start
    while chunk_start < end:
        chunk_end = min(chunk_start + timedelta(days=7), end)
        date_range = f"{chunk_start.strftime('%Y-%m-%d')}..{chunk_end.strftime('%Y-%m-%d')}"
        query = f"repo:{REPO} is:pr is:merged merged:{date_range}"
        print(f"   Searching PRs merged {date_range} …")
        items = search_issues(query)
        for item in items:
            if item["number"] not in seen:
                seen.add(item["number"])
                raw.append(item)
        chunk_start = chunk_end

    print(f"   Found {len(raw)} merged PRs total")

    prs = []  # type: List[dict]
    for i, item in enumerate(raw, 1):
        login = (item.get("user") or {}).get("login", "")
        if is_bot(login):
            continue
        number = item["number"]
        if i % 20 == 0 or i == len(raw):
            print(f"   Fetching PR details … {i}/{len(raw)}")
        # need additions/deletions from the PR endpoint
        detail = requests.get(
            f"{API}/repos/{REPO}/pulls/{number}", headers=HEADERS
        )
        if detail.status_code == 403 and "rate limit" in detail.text.lower():
            reset = int(detail.headers.get("X-RateLimit-Reset", time.time() + 60))
            wait = max(reset - int(time.time()), 1) + 5
            print(f"  ⏳ Rate-limited — sleeping {wait}s …")
            time.sleep(wait)
            detail = requests.get(
                f"{API}/repos/{REPO}/pulls/{number}", headers=HEADERS
            )
        detail.raise_for_status()
        d = detail.json()
        prs.append(
            {
                "number": number,
                "author": login,
                "avatar_url": (item.get("user") or {}).get("avatar_url", ""),
                "title": item.get("title", ""),
                "body_length": len(item.get("body") or ""),
                "additions": d.get("additions", 0),
                "deletions": d.get("deletions", 0),
                "created_at": item.get("created_at"),
                "merged_at": d.get("merged_at"),
                "is_revert": "revert" in (item.get("title") or "").lower(),
            }
        )
    print(f"   ✅ {len(prs)} PRs (bots excluded)")
    return prs


# ── Step 2: Fetch reviews + review comments ──────────────────────────────────


def fetch_reviews_and_comments(pr_numbers: List[int]) -> Tuple[List[dict], List[dict]]:
    """Return (reviews, review_comments) for given PR numbers."""
    print("🔍 Fetching reviews and review comments …")
    reviews = []  # type: List[dict]
    comments = []  # type: List[dict]
    total = len(pr_numbers)
    for i, number in enumerate(pr_numbers, 1):
        if i % 25 == 0 or i == total:
            print(f"   PR reviews … {i}/{total}")

        # Reviews
        try:
            rv = paginate(f"{API}/repos/{REPO}/pulls/{number}/reviews")
        except requests.HTTPError:
            rv = []
        for r in rv:
            login = (r.get("user") or {}).get("login", "")
            if is_bot(login):
                continue
            reviews.append(
                {
                    "pr_number": number,
                    "reviewer": login,
                    "submitted_at": r.get("submitted_at"),
                }
            )

        # Review comments
        try:
            rc = paginate(f"{API}/repos/{REPO}/pulls/{number}/comments")
        except requests.HTTPError:
            rc = []
        for c in rc:
            login = (c.get("user") or {}).get("login", "")
            if is_bot(login):
                continue
            comments.append(
                {
                    "pr_number": number,
                    "reviewer": login,
                    "body_length": len(c.get("body") or ""),
                }
            )

    print(f"   ✅ {len(reviews)} reviews, {len(comments)} review comments")
    return reviews, comments


# ── Step 3: Aggregate per engineer ────────────────────────────────────────────


def aggregate(prs, reviews, comments) -> Dict[str, dict]:
    """Build per-engineer stats."""
    print("📊 Aggregating per-engineer stats …")
    engineers = {}  # type: Dict[str, dict]

    def ensure(login, avatar=""):
        if login not in engineers:
            engineers[login] = {
                "username": login,
                "avatar_url": avatar,
                "prs_merged": 0,
                "pr_sizes": [],
                "revert_count": 0,
                "pr_desc_lengths": [],
                "reviews_given": 0,
                "review_comment_lengths": [],
                "review_turnarounds": [],
            }

    # PRs
    pr_created_map = {}  # type: Dict[int, str]
    for pr in prs:
        author = pr["author"]
        ensure(author, pr.get("avatar_url", ""))
        e = engineers[author]
        e["prs_merged"] += 1
        e["pr_sizes"].append(pr["additions"] + pr["deletions"])
        e["pr_desc_lengths"].append(pr["body_length"])
        if pr["is_revert"]:
            e["revert_count"] += 1
        pr_created_map[pr["number"]] = pr["created_at"]

    # Reviews
    for rv in reviews:
        reviewer = rv["reviewer"]
        ensure(reviewer)
        e = engineers[reviewer]
        e["reviews_given"] += 1
        # turnaround = time from PR open → review submitted
        pr_created = pr_created_map.get(rv["pr_number"])
        submitted = rv.get("submitted_at")
        if pr_created and submitted:
            try:
                t0 = datetime.fromisoformat(pr_created.replace("Z", "+00:00"))
                t1 = datetime.fromisoformat(submitted.replace("Z", "+00:00"))
                hrs = max((t1 - t0).total_seconds() / 3600, 0)
                e["review_turnarounds"].append(hrs)
            except Exception:
                pass

    # Review comments
    for c in comments:
        reviewer = c["reviewer"]
        ensure(reviewer)
        engineers[reviewer]["review_comment_lengths"].append(c["body_length"])

    # Flatten
    for e in engineers.values():
        sizes = e.pop("pr_sizes")
        e["avg_pr_size"] = round(sum(sizes) / len(sizes), 1) if sizes else 0
        descs = e.pop("pr_desc_lengths")
        e["avg_pr_description_length"] = (
            round(sum(descs) / len(descs), 1) if descs else 0
        )
        e["revert_rate"] = (
            round(e.pop("revert_count") / e["prs_merged"], 3)
            if e["prs_merged"]
            else 0
        )
        lengths = e.pop("review_comment_lengths")
        e["avg_review_comment_length"] = (
            round(sum(lengths) / len(lengths), 1) if lengths else 0
        )
        turnarounds = e.pop("review_turnarounds")
        e["avg_review_turnaround_hrs"] = (
            round(sum(turnarounds) / len(turnarounds), 1) if turnarounds else 0
        )

    print(f"   ✅ {len(engineers)} engineers aggregated")
    return engineers


# ── Step 4: Score ─────────────────────────────────────────────────────────────


def min_max(values: List[float]) -> List[float]:
    """Normalize list of values to 0-1 using min-max."""
    lo, hi = min(values), max(values)
    if hi == lo:
        return [1.0] * len(values)
    return [(v - lo) / (hi - lo) for v in values]


def pr_size_score(avg_size: float) -> float:
    """1.0 if in [200, 800], scale down outside."""
    if 200 <= avg_size <= 800:
        return 1.0
    if avg_size < 200:
        return max(avg_size / 200, 0)
    # avg_size > 800
    return max(1 - (avg_size - 800) / 2000, 0)


def turnaround_score(avg_hrs: float) -> float:
    """1.0 if under 24 hrs, scale down after."""
    if avg_hrs <= 24:
        return 1.0
    return max(1 - (avg_hrs - 24) / 168, 0)  # decays over ~1 week


def score_engineers(engineers: Dict[str, dict]) -> List[dict]:
    """Compute impact scores and return sorted list."""
    print("🏆 Scoring engineers …")

    people = list(engineers.values())
    if not people:
        return []

    # Raw sub-scores (before normalisation)
    raw_prs = [e["prs_merged"] for e in people]
    raw_pr_size = [pr_size_score(e["avg_pr_size"]) for e in people]
    raw_no_revert = [1 - e["revert_rate"] for e in people]
    raw_reviews = [e["reviews_given"] for e in people]
    raw_comment_len = [e["avg_review_comment_length"] for e in people]
    raw_turnaround = [turnaround_score(e["avg_review_turnaround_hrs"]) for e in people]

    # Min-max normalise
    n_prs = min_max(raw_prs)
    n_pr_size = min_max(raw_pr_size)
    n_no_revert = min_max(raw_no_revert)
    n_reviews = min_max(raw_reviews)
    n_comment_len = min_max(raw_comment_len)
    n_turnaround = min_max(raw_turnaround)

    for i, e in enumerate(people):
        quality = n_prs[i] * 0.4 + n_pr_size[i] * 0.3 + n_no_revert[i] * 0.3
        review = n_reviews[i] * 0.4 + n_comment_len[i] * 0.3 + n_turnaround[i] * 0.3
        impact = quality * 0.5 + review * 0.5
        e["quality_score"] = round(quality, 4)
        e["review_score"] = round(review, 4)
        e["impact_score"] = round(impact, 4)

    people.sort(key=lambda e: e["impact_score"], reverse=True)

    # Assign ranks and generate summaries (top 5)
    top5 = people[:5]
    for rank, e in enumerate(top5, 1):
        e["rank"] = rank
        e["summary"] = (
            f"{e['username']} merged {e['prs_merged']} PRs over 90 days "
            f"averaging {e['avg_pr_size']:.0f} lines, and gave "
            f"{e['reviews_given']} reviews with an average comment length of "
            f"{e['avg_review_comment_length']:.0f} chars — ranking them "
            f"#{rank} for overall impact."
        )

    print(f"   ✅ Top 5 scored")
    return top5


# ── Step 5: Save ──────────────────────────────────────────────────────────────


def save(top5: List[dict]) -> None:
    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "engineers": [
            {
                "rank": e["rank"],
                "username": e["username"],
                "avatar_url": e["avatar_url"],
                "impact_score": e["impact_score"],
                "quality_score": e["quality_score"],
                "review_score": e["review_score"],
                "prs_merged": e["prs_merged"],
                "avg_pr_size": e["avg_pr_size"],
                "revert_rate": e["revert_rate"],
                "reviews_given": e["reviews_given"],
                "avg_review_comment_length": e["avg_review_comment_length"],
                "avg_review_turnaround_hrs": e["avg_review_turnaround_hrs"],
                "summary": e["summary"],
            }
            for e in top5
        ],
    }
    with open("data.json", "w") as f:
        json.dump(output, f, indent=2)
    print("💾 Saved data.json")


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"🚀 Analysing {REPO} — last 90 days (since {SINCE[:10]})\n")

    prs = fetch_merged_prs()
    pr_numbers = [pr["number"] for pr in prs]
    reviews, comments = fetch_reviews_and_comments(pr_numbers)
    engineers = aggregate(prs, reviews, comments)
    top5 = score_engineers(engineers)
    save(top5)

    print("\n🎉 Done! Top 5 engineers:")
    for e in top5:
        print(f"   #{e['rank']} {e['username']} — impact {e['impact_score']:.2f}")

