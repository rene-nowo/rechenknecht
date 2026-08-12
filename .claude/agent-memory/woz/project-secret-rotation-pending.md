---
name: project-secret-rotation-pending
description: A live sec-api.io key sat in rechenknecht's src/Sec_api.py and is still in git history at commit 3a211f2 — file deleted 2026-08-10, rotation still owed
metadata:
  type: project
---

`src/Sec_api.py` held a hardcoded sec-api.io API key (64 hex chars, `sha256[0:12] = 12e029ffbab4`). I deleted the file on 2026-08-10 as part of the `rechenknecht-fix-review-findings` brief. **The key was verified LIVE at that point** (one `POST https://api.sec-api.io` → HTTP 200 with a result body) and it is committed in git history at `3a211f2` ("Ch: change classes to src dir").

**Why:** deleting the working-tree file does not remove a secret from history — anyone who can clone the repo can still read it. This was flagged to René on 2026-08-10; as of that date I have no confirmation the key was rotated at sec-api.io.

**How to apply:** if any future rechenknecht work touches secrets, git history, or a repo-publication/open-sourcing question, check first whether this key was rotated. Do **not** reintroduce a `sec_api` dependency — the package was never installed and the file was dead (zero importers). If asked to scrub history, that is a `git filter-repo` job on a repo with its own nested `.git`, and it needs René's explicit go.

Related: [[project-golden-test-is-contested]]
