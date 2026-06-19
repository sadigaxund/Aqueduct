"""Schema-drift classification — pure, engine-agnostic (no pyspark).

Diffs a baseline source schema against a freshly-read live schema and classifies
each change as **breaking** or **benign**:

* **breaking** — a column was *dropped* or *changed type*. A downstream Channel
  that names the column (or relies on its type) will fail. These trigger a
  predicted heal.
* **benign** — a column was *added*. A `SELECT named_cols` pipeline does not
  break on a superset source, so additions do not trigger a heal by default
  (they are still recorded for audit).

A rename surfaces as a drop + an add — the drop is breaking and fires the heal,
while the added column name is offered to the agent as a rename candidate.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

ChangeKind = Literal["dropped", "type_changed", "added"]


@dataclass(frozen=True)
class SchemaChange:
    column: str
    kind: ChangeKind
    baseline_type: str | None = None   # None for 'added'
    live_type: str | None = None       # None for 'dropped'

    @property
    def breaking(self) -> bool:
        return self.kind in ("dropped", "type_changed")

    def describe(self) -> str:
        if self.kind == "dropped":
            return f"column {self.column!r} dropped (was {self.baseline_type})"
        if self.kind == "type_changed":
            return f"column {self.column!r} type changed {self.baseline_type} → {self.live_type}"
        return f"column {self.column!r} added ({self.live_type})"


@dataclass(frozen=True)
class DriftResult:
    changes: tuple[SchemaChange, ...] = ()

    @property
    def breaking(self) -> tuple[SchemaChange, ...]:
        return tuple(c for c in self.changes if c.breaking)

    @property
    def benign(self) -> tuple[SchemaChange, ...]:
        return tuple(c for c in self.changes if not c.breaking)

    @property
    def has_drift(self) -> bool:
        return bool(self.changes)

    @property
    def has_breaking(self) -> bool:
        return bool(self.breaking)

    @property
    def status(self) -> str:
        if not self.changes:
            return "no_drift"
        return "drift_breaking" if self.has_breaking else "drift_benign"

    @property
    def dropped_columns(self) -> tuple[str, ...]:
        return tuple(c.column for c in self.changes if c.kind == "dropped")

    @property
    def added_columns(self) -> tuple[str, ...]:
        return tuple(c.column for c in self.changes if c.kind == "added")


def diff_schemas(baseline: dict[str, str], live: dict[str, str]) -> DriftResult:
    """Classify the difference between a baseline and a live source schema.

    Schemas are ``{column_name: spark_simple_type}`` maps. Comparison is
    case-sensitive on names and exact on type strings (Spark's ``simpleString``).
    """
    changes: list[SchemaChange] = []
    for col, btype in baseline.items():
        if col not in live:
            changes.append(SchemaChange(col, "dropped", baseline_type=btype))
        elif live[col] != btype:
            changes.append(SchemaChange(col, "type_changed", baseline_type=btype, live_type=live[col]))
    for col, ltype in live.items():
        if col not in baseline:
            changes.append(SchemaChange(col, "added", live_type=ltype))
    return DriftResult(changes=tuple(changes))
