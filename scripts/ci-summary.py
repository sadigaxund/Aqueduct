#!/usr/bin/env python3
"""Read pytest JUnit XML and write a GitHub Step Summary markdown report.

Usage:
  python scripts/ci-summary.py <junitxml> [--name "Job Name"]

Writes to GITHUB_STEP_SUMMARY when in CI, otherwise stdout.
"""

from __future__ import annotations

import sys
import os
import xml.etree.ElementTree as ET
from pathlib import Path


def _duration_sec(txt: str) -> float:
    try:
        return float(txt)
    except (ValueError, TypeError):
        return 0.0


def build_report(xml_path: str, job_name: str = "") -> str:
    tree = ET.parse(xml_path)
    root = tree.getroot()
    ts = root.find("testsuite")
    if ts is None:
        ts = root

    total = int(ts.get("tests", 0))
    errors = int(ts.get("errors", 0))
    failures = int(ts.get("failures", 0))
    skipped = int(ts.get("skipped", 0))
    passed = total - errors - failures - skipped
    time_s = _duration_sec(ts.get("time", "0"))

    lines = []
    heading = job_name or Path(xml_path).stem.replace("_", " ").title()
    lines.append(f"## Test Results: {heading}")
    lines.append("")

    # Overview badge row
    emoji = ":white_check_mark:" if failures == 0 and errors == 0 else ":x:"
    lines.append(f"**{emoji} {passed} passed**, {failures} failed, {errors} errors, {skipped} skipped — {time_s:.1f}s")
    lines.append("")

    if failures > 0 or errors > 0:
        lines.append("### Failures")
        lines.append("")
        lines.append("| Test | Type | Message |")
        lines.append("|------|------|---------|")
        for case in ts.findall("testcase"):
            for child in case:
                if child.tag in ("failure", "error"):
                    msg = (child.get("message", "") or "").strip()
                    short_msg = msg.split("\n")[0][:120] if msg else "(no message)"
                    lines.append(
                        f"| `{case.get('name', '?')}` "
                        f"| {child.tag} "
                        f"| {short_msg} |"
                    )
        lines.append("")

    if skipped > 0:
        skipped_cases = []
        for case in ts.findall("testcase"):
            for child in case:
                if child.tag == "skipped":
                    skipped_cases.append(case.get("name", "?"))
        if skipped_cases:
            lines.append(f"**Skipped ({skipped}):** `{'`, `'.join(skipped_cases)}`")
            lines.append("")

    # Per-test timing breakdown (only when there are failures)
    if failures > 0 or errors > 0:
        lines.append("<details><summary>Per-test breakdown</summary>")
        lines.append("")
        lines.append("| Test | Status | Time |")
        lines.append("|------|--------|------|")
        for case in ts.findall("testcase"):
            name = case.get("name", "?")
            t = _duration_sec(case.get("time", "0"))
            has_err = any(child.tag in ("failure", "error") for child in case)
            icon = ":x:" if has_err else ":white_check_mark:"
            lines.append(f"| `{name}` | {icon} | {t:.2f}s |")
        lines.append("")
        lines.append("</details>")
        lines.append("")

    return "\n".join(lines)


def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/ci-summary.py <junitxml> [--name 'Job Name']", file=sys.stderr)
        sys.exit(1)

    xml_path = sys.argv[1]
    job_name = ""
    if "--name" in sys.argv:
        idx = sys.argv.index("--name")
        if idx + 1 < len(sys.argv):
            job_name = sys.argv[idx + 1]

    report = build_report(xml_path, job_name)

    dest = os.environ.get("GITHUB_STEP_SUMMARY")
    if dest:
        with open(dest, "a") as f:
            f.write(report + "\n")
    else:
        print(report)


if __name__ == "__main__":
    main()
