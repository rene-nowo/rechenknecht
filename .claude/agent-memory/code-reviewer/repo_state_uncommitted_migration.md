---
name: repo-state-uncommitted-migration
description: rechenknecht's nested git repo carries a large pre-existing uncommitted diff unrelated to any single brief — use mtimes, not git diff/status, to isolate one task's changes
metadata:
  type: project
---

As of 2026-08-11, `apps/rechenknecht` (its own nested git repo, never committed to during agent
sessions per repo convention) has a large standing `git status` diff — dozens of deleted/modified
files (old `Rechenknecht.py`, filing HTML files, `pyproject.toml` new deps, `footlocker_expected.csv`,
etc.) that predate any specific brief; these come from an in-progress migration off the old
monolithic script toward `ingest.py` / `src/db.py` / `pages/` / `dashboard.py`. Confirmed via mtime:
files from that migration cluster around `14:12` on 2026-08-11, hours before a same-day brief's
session window (`21:38`–`21:52`).

**Why this matters:** `git diff`/`git status` alone cannot tell you what one specific brief/task
touched — everything looks dirty regardless. A reviewer who diffs against HEAD will see hundreds
of unrelated lines and either waste time auditing them or (worse) miss that they're pre-existing.

**How to apply:** to scope a review to one task, use `stat -c '%y' <file>` and cluster by the
report's stated session window, not `git diff --stat`. A file whose mtime sits hours/days outside
the reported session is not this task's change even if `git status` marks it modified.
