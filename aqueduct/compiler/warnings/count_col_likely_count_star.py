"""`COUNT(col)` where `COUNT(*)` is the likely intent.

`COUNT(col)` silently skips rows where `col` is NULL. Users almost always
want either total row count (`COUNT(*)`) or non-null cardinality (and if so,
should be writing `COUNT(DISTINCT col)`). Flag the bare `COUNT(col)` form.
False-positive on intentional null-aware counts is acceptable — easy to
silence by switching to `COUNT(col + 0)` or adding a comment.
"""

from __future__ import annotations

import re
from typing import Any

RULE_ID = "count_col_likely_count_star"

# COUNT(<simple identifier>) — exclude `COUNT(*)` and `COUNT(DISTINCT ...)`.
_COUNT_COL = re.compile(
    r"\bCOUNT\s*\(\s*(?!\*|DISTINCT\b)([A-Za-z_][\w.]*)\s*\)",
    re.IGNORECASE,
)


def check(manifest: Any) -> list[str]:
    out: list[str] = []
    for m in manifest.modules:
        if m.type != "Channel":
            continue
        cfg = m.config or {}
        if cfg.get("op") != "sql":
            continue
        sql = cfg.get("query", "")
        matches = _COUNT_COL.findall(sql)
        for col in matches:
            out.append(
                f"Channel '{m.id}' query uses COUNT({col}) — NULL values in "
                f"'{col}' are silently excluded. If you want total row count, "
                "use COUNT(*); if you want non-null cardinality, use "
                "COUNT(DISTINCT col); to keep null-skipping behaviour "
                "explicit, qualify the expression."
            )
    return out
