---
name: verify-streamlit-pages-live
description: how to independently re-render a rechenknecht Streamlit page in-process to verify UI claims (column order, captions, live distributions) without a browser
metadata:
  type: project
---

`streamlit.testing.v1.AppTest` runs a page's full body (including DB queries) in-process, which is
how the implementer's own reports get numbers like signal distributions — and how I re-verified
them independently. Recipe (must run from the app's venv with `RK_DB_URL`/`OP_SERVICE_ACCOUNT_TOKEN`
exported, see [[project-rechenknecht-supabase-pooler]]):

```python
from streamlit.testing.v1 import AppTest
at = AppTest.from_file("/abs/path/to/apps/rechenknecht/pages/Screener.py", default_timeout=120)
at.run()
at.exception            # should be empty ElementList()
at.dataframe[0].value   # the rendered pandas DataFrame, incl. computed columns
at.sidebar.toggle[i].set_value(True).run()   # flip a sidebar toggle and re-render
at.caption               # list of caption elements, .value is the text
```

Path must be absolute (or run with `os.chdir` into the app dir first) — `AppTest.from_file`
resolves relative paths against the *calling script's* location, not the cwd.

**Why this matters:** this is the only way to check UI-level claims (column position, caption/
tooltip text, live per-ticker signal values) since such logic lives in a page file that "runs its
whole body on import and therefore cannot be unit-tested" (the app's own stated reason for
splitting `src/screener.py` out) — the pytest suite never exercises `pages/Screener.py` itself, so
UI-level regressions (wrong column order, caption silently dropped) would NOT be caught by "119
passed". Confirmed live-checked figures (mli/tsla/msft/baba/xom/nvmi/vips/wb signals and raw
price/avg_eps/bvps) against a report's hand-verification table and they matched exactly.
