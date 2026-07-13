#!/usr/bin/env python3
"""Engine capability declaration dev tool (Phase 78).

The capability framework holds two INDEPENDENT sources in agreement:
  - the leaf walker (CODE) — `aqueduct/executor/capability_leaves.py` (Blueprint
    grammar) + `config_leaves.py` (aqueduct.yml engine config)
  - each engine's declaration (DATA) — `aqueduct/executor/<engine>/capabilities.yml`

Because they are independent, they can disagree — which is what makes the
closure test able to fail at all. This tool is how you reconcile them.

Workflow when you add a schema/config key:
    1. the build breaks (closure test + engine registration both fail)
    2. `python scripts/capabilities.py sync`   -> appends the new leaf to every
       engine's YAML as `undeclared`
    3. a human replaces each `undeclared` with a real verdict
    4. the build passes

Implementing a NEW engine starts here too:

    python scripts/capabilities.py scaffold --engine duckdb

writes a COMPLETE `capabilities.yml` with every leaf set to `undeclared`, and the
build refuses to register the engine until each row is a real decision. This is
deliberately NOT a static template (it would go stale the moment the grammar
changed) and NOT "copy Spark's table" (that hands you ~261 `supported` rows — a
new engine silently claiming to support everything, which is the exact blindness
this framework exists to prevent).

Usage:
  python scripts/capabilities.py check   # exit 1 if any engine YAML is out of sync
  python scripts/capabilities.py sync    # append missing leaves as `undeclared`
  python scripts/capabilities.py scaffold --engine <name>
                                         # full all-`undeclared` table for a NEW engine
  python scripts/capabilities.py docs    # render the engine matrix into
                                         # docs/compatibility.md (between markers)
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO))

from aqueduct.executor.capabilities import Support  # noqa: E402
from aqueduct.executor.capability_leaves import all_leaves  # noqa: E402
from aqueduct.executor.config_leaves import all_config_leaves  # noqa: E402

_DOCS = _REPO / "docs" / "compatibility.md"
_START = "<!-- ENGINE_MATRIX_START -->"
_END = "<!-- ENGINE_MATRIX_END -->"

UNDECLARED = Support.UNDECLARED.value


def governed_leaves() -> frozenset[str]:
    """Blueprint grammar leaves ∪ engine-config leaves — the checklist."""
    return all_leaves() | all_config_leaves()


def engine_declarations() -> list[Path]:
    """Every `capabilities.yml` shipped under `aqueduct/executor/`."""
    return sorted((_REPO / "aqueduct" / "executor").glob("*/capabilities.yml"))


def _load_rows(path: Path) -> dict:
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return raw.get("leaves") or {}


def _verdict_of(row) -> str:
    return row if isinstance(row, str) else (row or {}).get("support", "")


def cmd_check(_args) -> int:
    leaves = governed_leaves()
    bad = 0
    for path in engine_declarations():
        rows = _load_rows(path)
        missing = sorted(leaves - set(rows))
        orphaned = sorted(set(rows) - leaves)
        undeclared = sorted(k for k, v in rows.items() if _verdict_of(v) == UNDECLARED)
        rel = path.relative_to(_REPO)
        if missing or orphaned or undeclared:
            bad = 1
            print(f"✗ {rel}")
            for leaf in missing:
                print(f"    MISSING     {leaf}  (no verdict declared)")
            for leaf in undeclared:
                print(f"    UNDECLARED  {leaf}  (needs a real verdict)")
            for leaf in orphaned:
                print(f"    ORPHANED    {leaf}  (not a real leaf — renamed/removed?)")
        else:
            print(f"✓ {rel} — {len(rows)} leaves, all declared")
    if bad:
        print("\nRun `python scripts/capabilities.py sync`, then replace each "
              "`undeclared` with a real verdict.")
    return bad


def cmd_sync(_args) -> int:
    """Append every missing leaf to each engine YAML as `undeclared`.

    Deliberately NEVER writes a real verdict and never removes a row — a human
    decides what an engine does with a new leaf. Orphaned rows are reported,
    not deleted, so a rename is reviewed rather than silently dropped.
    """
    leaves = governed_leaves()
    for path in engine_declarations():
        rows = _load_rows(path)
        missing = sorted(leaves - set(rows))
        orphaned = sorted(set(rows) - leaves)
        rel = path.relative_to(_REPO)
        if not missing:
            print(f"✓ {rel} — already complete ({len(rows)} leaves)")
        else:
            text = path.read_text(encoding="utf-8").rstrip("\n")
            block = "\n".join(f"  {leaf}: {UNDECLARED}" for leaf in missing)
            path.write_text(f"{text}\n{block}\n", encoding="utf-8")
            print(f"+ {rel} — appended {len(missing)} leaf/leaves as `{UNDECLARED}`:")
            for leaf in missing:
                print(f"    {leaf}")
        if orphaned:
            print(f"! {rel} — {len(orphaned)} orphaned row(s) NOT removed "
                  "(review + delete by hand if the leaf really is gone):")
            for leaf in orphaned:
                print(f"    {leaf}")
    print("\nNow replace each `undeclared` with a real verdict "
          "(supported | unsupported | ignored_with_warning).")
    return 0


_SCAFFOLD_HEADER = """\
# {engine} engine capability declaration — SCAFFOLD. Every row below is `undeclared`.
#
# READ THIS BEFORE EDITING:
#
#   * Every row must be replaced with a REAL verdict:
#         supported | unsupported | ignored_with_warning
#     A row may also carry `requires:` (dependency version constraints, checked
#     by `aqueduct doctor`) and `hint:` (actionable text shown when the leaf is
#     refused). See aqueduct/executor/spark/capabilities.yml for the shape.
#
#   * The build FAILS while any `undeclared` row remains. Engine registration
#     raises EnginePluginError and the closure test stays red. That is
#     deliberate: `undeclared` means "nobody has decided yet", which is NOT the
#     same as `unsupported` ("we decided this engine cannot do it").
#
#   * Spark's capabilities.yml is a REFERENCE TO READ, NOT A FILE TO COPY.
#     Copying it hands you ~{n} `supported` rows, i.e. a silent claim that this
#     engine supports the entire grammar and every aqueduct.yml key. That
#     blind-inheritance is exactly what this framework exists to prevent —
#     decide each leaf on its own merits.
#
#   * This file was generated from the live grammar + config walkers, so it
#     cannot go stale relative to a static template. Re-run
#     `python scripts/capabilities.py sync` whenever a new leaf lands.
#
# Register the engine by adding it to pyproject.toml's
# [project.entry-points."aqueduct.engines"] table.

engine: {engine}

leaves:
"""


def cmd_scaffold(args) -> int:
    """Write a COMPLETE capabilities.yml for a brand-new engine, all `undeclared`.

    This is the answer to "how do I implement a new engine". Not a static
    template (it would go stale the moment the grammar changed) and emphatically
    not "copy Spark's table" (that would inherit ~261 `supported` rows — a new
    engine silently claiming to support everything, the precise blindness the
    capability framework exists to prevent). Generated from the walker, so it
    cannot drift and cannot smuggle in a default verdict.
    """
    engine = args.engine
    out = Path(args.out) if args.out else (
        _REPO / "aqueduct" / "executor" / engine / "capabilities.yml"
    )
    if out.exists() and not args.force:
        print(f"✗ {out.relative_to(_REPO) if out.is_relative_to(_REPO) else out} already "
              "exists — refusing to overwrite. Use --force, or `sync` to top it up.")
        return 1

    leaves = sorted(governed_leaves())
    body = "".join(f"  {leaf}: {UNDECLARED}\n" for leaf in leaves)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(
        _SCAFFOLD_HEADER.format(engine=engine, n=len(leaves)) + body, encoding="utf-8"
    )

    rel = out.relative_to(_REPO) if out.is_relative_to(_REPO) else out
    print(f"✓ wrote {rel}")
    print(f"  {len(leaves)} leaves, ALL `{UNDECLARED}` "
          f"({len(all_leaves())} grammar + {len(all_config_leaves())} config)")
    print()
    print(f"  The build will REFUSE to register engine {engine!r} until every row is a")
    print("  real verdict (supported | unsupported | ignored_with_warning).")
    print("  Spark's capabilities.yml is a reference to read, not a file to copy.")
    return 0


def _matrix_markdown() -> str:
    """Render the engine capability matrix FROM THE DECLARATIONS.

    The YAML is the source of truth, so this is generated, not hand-maintained.
    A 261-row table per engine would be noise, so the matrix reports the summary
    (verdict counts per engine) plus every row that is NOT a plain unconditional
    `supported` — the version-gated, ignored, and unsupported leaves are exactly
    the ones a user needs to know about.
    """
    decls = engine_declarations()
    engines = [p.parent.name for p in decls]
    rows_by_engine = {p.parent.name: _load_rows(p) for p in decls}

    out: list[str] = []
    out.append("")
    out.append("<!-- Generated by `python scripts/capabilities.py docs` — do not edit by hand. -->")
    out.append("")
    out.append("### Declared capability totals")
    out.append("")
    out.append("| Engine | Leaves declared | Supported | Version-gated | Ignored with warning | Unsupported |")
    out.append("|---|---|---|---|---|---|")
    for eng in engines:
        rows = rows_by_engine[eng]
        supported = [k for k, v in rows.items() if _verdict_of(v) == "supported"]
        gated = [k for k, v in rows.items() if isinstance(v, dict) and v.get("requires")]
        ignored = [k for k, v in rows.items() if _verdict_of(v) == "ignored_with_warning"]
        unsupported = [k for k, v in rows.items() if _verdict_of(v) == "unsupported"]
        out.append(
            f"| `{eng}` | {len(rows)} | {len(supported)} | {len(gated)} | "
            f"{len(ignored)} | {len(unsupported)} |"
        )
    out.append("")
    out.append("### Conditional and refused capabilities")
    out.append("")
    out.append("Every leaf that is not unconditionally supported. A version-gated leaf runs "
               "only above the stated dependency version (`aqueduct doctor` checks this against "
               "what is actually installed); an ignored leaf is accepted but has no effect; an "
               "unsupported leaf fails compilation.")
    out.append("")
    out.append("| Engine | Capability leaf | Verdict | Requires | Notes |")
    out.append("|---|---|---|---|---|")
    any_row = False
    for eng in engines:
        for leaf, row in sorted(rows_by_engine[eng].items()):
            verdict = _verdict_of(row)
            requires = row.get("requires") if isinstance(row, dict) else None
            hint = row.get("hint", "") if isinstance(row, dict) else ""
            if verdict == "supported" and not requires:
                continue
            any_row = True
            req = ", ".join(f"`{d}{s}`" for d, s in (requires or {}).items()) or "—"
            out.append(f"| `{eng}` | `{leaf}` | {verdict} | {req} | {hint or '—'} |")
    if not any_row:
        out.append("| — | — | — | — | Every declared leaf is unconditionally supported. |")
    out.append("")
    return "\n".join(out)


def cmd_docs(_args) -> int:
    text = _DOCS.read_text(encoding="utf-8")
    if _START not in text or _END not in text:
        print(f"✗ {_DOCS.relative_to(_REPO)} is missing the "
              f"{_START} / {_END} markers.")
        return 1
    head, rest = text.split(_START, 1)
    _, tail = rest.split(_END, 1)
    new = f"{head}{_START}{_matrix_markdown()}{_END}{tail}"
    if new == text:
        print(f"✓ {_DOCS.relative_to(_REPO)} — engine matrix already up to date")
        return 0
    _DOCS.write_text(new, encoding="utf-8")
    print(f"✓ {_DOCS.relative_to(_REPO)} — engine matrix regenerated")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("check", help="exit 1 if any engine YAML is missing/undeclared leaves")
    sub.add_parser("sync", help="append missing leaves to each engine YAML as `undeclared`")
    sub.add_parser("docs", help="regenerate the engine matrix in docs/compatibility.md")

    sc = sub.add_parser(
        "scaffold",
        help="write a COMPLETE capabilities.yml for a NEW engine, every leaf `undeclared`",
    )
    sc.add_argument("--engine", required=True, help="engine name, e.g. duckdb")
    sc.add_argument("--out", default=None,
                    help="output path (default: aqueduct/executor/<engine>/capabilities.yml)")
    sc.add_argument("--force", action="store_true", help="overwrite an existing file")

    args = ap.parse_args()
    return {
        "check": cmd_check,
        "sync": cmd_sync,
        "docs": cmd_docs,
        "scaffold": cmd_scaffold,
    }[args.cmd](args)


if __name__ == "__main__":
    raise SystemExit(main())
