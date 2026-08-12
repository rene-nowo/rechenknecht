---
name: tooling-ts-only-gates
description: contra-gate.sh / exists.sh only search .ts/.tsx/.js/.jsx/.mts/.cts — verified inapplicable to Python apps like rechenknecht
metadata:
  type: feedback
---

`.claude/scripts/contra-gate.sh` and `.claude/scripts/exists.sh` are hardcoded to TypeScript/
JavaScript extensions (`exists.sh`: `EXT_RE='\.(ts|tsx|js|jsx|mts|cts)$'`; `contra-gate.sh`'s
concept sweep: `--include='*.ts' --include='*.tsx'` only). Verified 2026-08-11 by reading both
scripts directly.

**Why this matters:** for a Python app (rechenknecht), running these gates produces a technically
non-empty but *meaningless* result (e.g. 21 "defs" that are all in an unrelated TS app like
`apps/reflex`) — 0 real hits because the glob can never match `.py` files. An implementer who
runs the gate, notices the verdict doesn't apply, and falls back to a manual `grep --include=*.py`
is doing the RIGHT thing, not skipping the Question Cascade step — do not flag "didn't get a
useful contra-gate verdict" as a finding when the app is Python. Confirmed a real report
(`rechenknecht-screener-signal`) did exactly this and the reasoning held up under my own
re-verification of the scripts.

**How to apply:** when reviewing a Python (or other non-TS) app's report that mentions running
these scripts, check whether the report itself flagged the extension mismatch. If yes, treat the
manual grep the implementer ran as the real Job-3 evidence, not the gate's verdict line.
