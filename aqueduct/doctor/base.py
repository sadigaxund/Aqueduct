"""Shared primitives for the doctor checks.

Kept in a tiny leaf module so both the package ``__init__`` (spark / network
cluster + ``run_doctor``) and ``checks_io`` (leaf connectivity checks) can
import ``CheckResult`` / ``_ms`` without a circular import.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Literal


@dataclass
class CheckResult:
    name: str
    status: Literal["ok", "fail", "warn", "skip"]
    detail: str
    elapsed_ms: int = 0
    # Data-driven render policy (renderer stays generic — no per-name branches).
    group: str = "general"        # "spark" | "stores" | "agent" | "io" | …  (visual grouping deferred)
    quiet_when_ok: bool = False   # low-signal env detail (cloudpickle): hide row when green, show on warn/fail


def _ms(t: float) -> int:
    return int((time.monotonic() - t) * 1000)
