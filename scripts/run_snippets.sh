#!/usr/bin/env bash
# Run all self-contained gallery snippets and print a pytest-style summary.
#
# Four result states (only FAIL fails the run — see the exit-code note at
# the bottom of this file):
#
#   PASS         The snippet ran (and, if present, inspect_results.py ran)
#                cleanly.
#   FAIL         Something broke that shouldn't have — a pip install, a
#                populate script, the aqueduct run itself, or inspect.
#   SKIP         We CHOSE not to run it — see SKIP_REASONS below. A
#                deliberate human decision (today: needs a live external
#                service the CI runner doesn't have).
#   UNSUPPORTED  The TARGET ENGINE cannot run it — the blueprint uses a
#                capability leaf that engine's capabilities.yml declares
#                `unsupported` (aqueduct/executor/capabilities.py). Detected
#                from the CompileError the compiler's capability gate raises
#                (aqueduct/compiler/capability_check.py) — never a
#                hand-maintained "doesn't work on engine X" list, so a
#                verdict flipping in either direction shows up here without
#                editing this script. See `unsupported_leaves` below.
#
# SKIP and UNSUPPORTED are never conflated: an UNSUPPORTED count dropping
# means a verdict flipped to supported; one rising after a change is a
# regression. Folding them into one bucket would make both invisible.
#
# Usage:
#   ./scripts/run_snippets.sh [-v] [-vv] [-f PATTERN] [-e ENGINE] [SNIPPETS_DIR]
#
#   -v         Show populate/generate, aqueduct-run, and inspect output inline
#   -vv        Show everything inline including pip-install
#   -f PATTERN Only run snippets whose name contains PATTERN (e.g. -f 28)
#   -e ENGINE  Target execution engine (e.g. spark, duckdb). Forwarded to
#              every `aqueduct run` as `--set deployment.engine=ENGINE`. This
#              is not a new user-facing concept — it's this script passing
#              through the `-s/--set` flag `aqueduct run` already has.
#              Default: unset — each snippet runs on whatever its own
#              aqueduct.yml declares (today: spark for every snippet), i.e.
#              a bare `bash scripts/run_snippets.sh` behaves exactly as
#              before this flag existed.
#
# SNIPPETS_DIR defaults to gallery/snippets/ relative to the repo root.
# The script resolves the repo root from its own location, so it works from any cwd.

set -euo pipefail

# ---------------------------------------------------------------------------
# Arg parse
# ---------------------------------------------------------------------------
VERBOSE=0
SNIPPETS_DIR=""
FILTER=""
ENGINE=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        -v) VERBOSE=1; shift ;;
        -vv) VERBOSE=2; shift ;;
        -vvv) VERBOSE=3; shift ;;
        -f|--filter) FILTER="$2"; shift 2 ;;
        -e|--engine) ENGINE="$2"; shift 2 ;;
        --) shift; SNIPPETS_DIR="${1:-}"; break ;;
        -*)
            echo "Unknown flag: $1"
            echo "Usage: $0 [-v] [-vv] [-f PATTERN] [-e ENGINE] [SNIPPETS_DIR]"
            exit 1
            ;;
        *) SNIPPETS_DIR="$1"; shift ;;
    esac
done

# Forwarded to every `aqueduct run` invocation. Empty when $ENGINE is unset,
# which word-splits away to nothing below (same pattern as $aq_config_flag).
ENGINE_SET_FLAG=""
if [[ -n "$ENGINE" ]]; then
    ENGINE_SET_FLAG="--set deployment.engine=$ENGINE"
fi

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
SNIPPETS_DIR="${SNIPPETS_DIR:-$REPO_ROOT/gallery/snippets}"

# Workspace lives under the repo by default (not /tmp) to avoid filling the
# /tmp quota. pyspark alone is ~500 MB; 20 runs × fresh venv would exhaust
# most /tmp quotas. Override with AQ_SNIPPET_WORKDIR env var.
WORK_BASE="${AQ_SNIPPET_WORKDIR:-$REPO_ROOT/.snippet_runs}"

# Snippets skipped because they need a live external service the CI runner
# doesn't have — a deliberate human decision (SKIP), never a stand-in for
# "the target engine can't run this" (that's UNSUPPORTED, detected from the
# capability gate, never hand-listed here — see unsupported_leaves below).
#
# name -> reason, so the summary prints WHY, not just that it happened.
#
# This map used to also carry 12_assert_null_sampling and
# 44_dag_cycle_detection under a comment claiming both needed a live
# service, or weren't compatible with the one-blueprint-per-snippet
# assumption below — neither reason was true for either entry:
#   - 12_assert_null_sampling's flakiness was an UNSEEDED data generator
#     (populate_data.py used `random` with no seed), not an external
#     dependency — fixed by seeding it (see that file).
#   - 44_dag_cycle_detection's dag/blueprint.yml is SUPPOSED to fail (it
#     demonstrates cycle detection); skipping it tested nothing. It's now
#     run as an expected-failure assertion — see run_dag_cycle_snippet
#     below, which also handles its second blueprint
#     (dag/blueprint_fixed.yml).
# Nothing in the gallery today needs a live external service, so this map
# is empty. It stays a name->reason structure (not deleted outright) so the
# next genuine live-service skip has somewhere to land with a real reason
# instead of reintroducing a bare list.
declare -A SKIP_REASONS=()

# Colours
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
RESET='\033[0m'

# Whether we're running in GitHub Actions
IN_CI="${GITHUB_ACTIONS:-false}"

# ---------------------------------------------------------------------------
# GitHub Actions helpers
# ---------------------------------------------------------------------------
gh_group() {
    if [[ "$IN_CI" == "true" ]]; then echo "::group::$1"; fi
}

gh_group_end() {
    if [[ "$IN_CI" == "true" ]]; then echo "::endgroup::"; fi
}

gh_annotate_error() {
    local snippet="$1" step="$2"
    if [[ "$IN_CI" == "true" ]]; then
        local file="gallery/snippets/${snippet}/blueprint.yml"
        echo "::error file=$file,title=${snippet} / ${step}::snippet failed"
    fi
}

# One row of the GitHub Actions step summary table. `detail` carries the
# skip reason or the UNSUPPORTED leaf id(s) — empty for PASS/FAIL.
gh_summary_row() {
    local snippet="$1" status="$2" duration="$3" detail="${4:-}"
    if [[ "$IN_CI" == "true" && -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
        local engine_label="${ENGINE:-default}"
        local marker
        case "$status" in
            PASS) marker=":white_check_mark: PASS" ;;
            FAIL) marker=":x: FAIL" ;;
            SKIP) marker=":heavy_minus_sign: SKIP" ;;
            UNSUPPORTED) marker=":no_entry_sign: UNSUPPORTED" ;;
            *) marker="$status" ;;
        esac
        local dur="${duration:--}"
        local det="${detail:--}"
        echo "| $snippet | $engine_label | $marker | $dur | $det |" >> "$GITHUB_STEP_SUMMARY"
    fi
}

# ---------------------------------------------------------------------------
# Setup: isolated tmp workspace
# ---------------------------------------------------------------------------
RUN_ID="$(cat /proc/sys/kernel/random/uuid 2>/dev/null || python3 -c 'import uuid; print(uuid.uuid4())')"
WORK_DIR="$WORK_BASE/${RUN_ID:0:8}"
mkdir -p "$WORK_DIR"

echo -e "${CYAN}${BOLD}Aqueduct snippet runner${RESET}"
echo -e "  Workspace : $WORK_DIR"
echo -e "  Snippets  : $SNIPPETS_DIR"
echo -e "  Engine    : ${ENGINE:-(default from each aqueduct.yml)}"
echo -e "  Verbose   : $VERBOSE  (0=quiet 1=run-output 2=all-output)"
echo ""

# Copy snippets into workspace
cp -r "$SNIPPETS_DIR"/. "$WORK_DIR/snippets/"

# ---------------------------------------------------------------------------
# Python 3.12 venv — shared and reused across runs to avoid re-downloading
# pyspark (~500 MB) every time. Lives at $WORK_BASE/venv, not inside the
# per-run dir, so it survives between invocations.
# ---------------------------------------------------------------------------
VENV="$WORK_BASE/venv"
PIP_TMPDIR="$WORK_BASE/.pip_tmp"
mkdir -p "$PIP_TMPDIR"
export TMPDIR="$PIP_TMPDIR"

_pip_ok() { "$VENV/bin/python" -c "import pandas, pyarrow, rich, pyspark, duckdb" 2>/dev/null; }

if [[ ! -d "$VENV" ]] || ! _pip_ok; then
    [[ -d "$VENV" ]] && echo -e "${YELLOW}Venv version mismatch — recreating...${RESET}"
    [[ ! -d "$VENV" ]] && echo -e "${CYAN}Creating Python 3.12 venv (first run only)...${RESET}"
    python3.12 -m venv "$VENV"
    # shellcheck source=/dev/null
    source "$VENV/bin/activate"
    echo -e "${CYAN}Installing dependencies...${RESET}"
    pip install --disable-pip-version-check -e "$REPO_ROOT[spark,llm]"
    pip install --disable-pip-version-check pandas pyarrow rich
else
    echo -e "${CYAN}Reusing existing venv at $VENV${RESET}"
    # shellcheck source=/dev/null
    source "$VENV/bin/activate"
    pip install --quiet --disable-pip-version-check -e "$REPO_ROOT[spark,llm]"
fi
echo ""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Prints the skip reason on stdout and returns 0 if $1 is skipped, else
# returns 1 with no output. Substring match against SKIP_REASONS' keys,
# same matching style as the old SKIP_PATTERNS list.
skip_reason_for() {
    local name="$1" pat
    for pat in "${!SKIP_REASONS[@]}"; do
        if [[ "$name" == *"$pat"* ]]; then
            echo "${SKIP_REASONS[$pat]}"
            return 0
        fi
    done
    return 1
}

# Detects an UNSUPPORTED classification from a failed aqueduct-run log: the
# compiler's capability gate (aqueduct/compiler/capability_check.py) raises
# a CompileError reading "Engine capability gate failed: ..." when the
# target engine declares a leaf the blueprint uses `unsupported`. Any other
# failure (a genuine blueprint bug, a missing file, ...) does not match this
# and stays a FAIL — this is deliberately a narrow, specific string, not a
# generic "the run failed" catch-all.
is_unsupported_failure() {
    local log="$1"
    [[ -f "$log" ]] && grep -q "Engine capability gate failed" "$log"
}

# Extracts the capability leaf id(s) that triggered an UNSUPPORTED
# classification (e.g. "egress.mode.merge" or, if a blueprint hits more
# than one, a comma-joined list) — so the summary names WHAT the engine
# refused, not just that it did.
unsupported_leaves() {
    local log="$1"
    grep -oE "uses '[^']+'" "$log" 2>/dev/null \
        | sed -E "s/uses '([^']+)'/\1/" \
        | sort -u \
        | paste -sd, -
}

FAILURES=()
UNSUPPORTED_ENTRIES=()   # "snippet :: leaf(s)" — the DuckDB-coverage answer

run_step() {
    local mode="capture" label
    if [[ "$1" == "--display" ]]; then mode="display"; shift; fi
    label="$1"; shift

    local log="$WORK_DIR/logs/${SNIPPET_NAME}_${label}.log"
    mkdir -p "$(dirname "$log")"

    if [[ "$mode" == "display" && "$VERBOSE" -ge 1 ]]; then
        if "$@" 2>&1 | tee "$log"; then
            echo -e "    ${GREEN}✓${RESET} $label"
            return 0
        else
            echo -e "    ${RED}✗${RESET} $label  (see $log)"
            FAILURES+=("$SNIPPET_NAME / $label")
            return 1
        fi
    else
        if "$@" >"$log" 2>&1; then
            echo -e "    ${GREEN}✓${RESET} $label"
            return 0
        else
            echo -e "    ${RED}✗${RESET} $label  (see $log)"
            FAILURES+=("$SNIPPET_NAME / $label")
            return 1
        fi
    fi
}

# Same run/log/tee mechanics as run_step, but does NOT auto-record a
# failure into FAILURES and does not print a ✓/✗ line itself — the caller
# needs to inspect the log FIRST (is_unsupported_failure) to decide whether
# this is a FAIL or an UNSUPPORTED before deciding what to print/record.
# Used only for the aqueduct-run step, where that distinction matters.
run_capture() {
    local mode="capture" label
    if [[ "$1" == "--display" ]]; then mode="display"; shift; fi
    label="$1"; shift

    local log="$WORK_DIR/logs/${SNIPPET_NAME}_${label}.log"
    mkdir -p "$(dirname "$log")"

    if [[ "$mode" == "display" && "$VERBOSE" -ge 1 ]]; then
        "$@" 2>&1 | tee "$log"
    else
        "$@" >"$log" 2>&1
    fi
}

# ---------------------------------------------------------------------------
# 44_dag_cycle_detection special case
# ---------------------------------------------------------------------------
# This snippet has no root blueprint.yml by design — it's a multi-blueprint
# manual walkthrough. dag/blueprint.yml is a DELIBERATE compile-time cycle
# (demonstrates Kahn's-algorithm cycle detection); dag/blueprint_fixed.yml is
# the same DAG with the cycle edge removed. Both are exercised here directly
# instead of via the generic single-blueprint flow below.
#
# hook/*.yml in this same snippet directory demonstrate a *runtime*
# hook-chain cycle guard (a separate, standalone demo per the README) and
# are intentionally not part of this harness.
#
# Sets SNIPPET_STATUS (PASS/FAIL/UNSUPPORTED) and, for UNSUPPORTED,
# UNSUPPORTED_LEAF — same contract the generic flow uses, so the shared
# duration/summary code after the pushd/popd block needs no special case.
run_dag_cycle_snippet() {
    local aq_config_flag=""
    [[ -f aqueduct.yml ]] && aq_config_flag="--config aqueduct.yml"

    # Step 0 — populate / generate scripts. Same convention every other
    # snippet uses (Step 2 of the generic flow below) — 44 was previously the
    # one snippet with no populate*.py, which is why data/nodes.csv only ever
    # existed as an untracked local file (gallery/**/data/ is git-ignored by
    # design, .gitignore:160) and silently vanished. See populate_data.py and
    # tests/test_gallery.py's regression guard for this snippet.
    for populate_script in $(find . -maxdepth 1 -name 'populate*.py' | sort); do
        run_step $_disp "populate-$(basename "$populate_script" .py)" python3 "$populate_script" || {
            SNIPPET_STATUS="FAIL"
            return
        }
    done

    local rc=0
    # shellcheck disable=SC2086
    run_capture $_disp "cycle-detected" aqueduct run dag/blueprint.yml $aq_config_flag $ENGINE_SET_FLAG || rc=$?
    local broken_log="$WORK_DIR/logs/${SNIPPET_NAME}_cycle-detected.log"

    if [[ $rc -eq 0 ]]; then
        echo -e "    ${RED}✗${RESET} cycle-detected  (expected dag/blueprint.yml to fail with a cycle error, but it succeeded — see $broken_log)"
        FAILURES+=("$SNIPPET_NAME / cycle-detected")
        SNIPPET_STATUS="FAIL"
        return
    fi
    if ! grep -q "Cycle detected in module graph" "$broken_log" 2>/dev/null; then
        echo -e "    ${RED}✗${RESET} cycle-detected  (dag/blueprint.yml failed, but not with the expected cycle error — see $broken_log)"
        FAILURES+=("$SNIPPET_NAME / cycle-detected")
        SNIPPET_STATUS="FAIL"
        return
    fi
    echo -e "    ${GREEN}✓${RESET} cycle-detected  (dag/blueprint.yml correctly fails, naming the cycle)"

    rc=0
    # shellcheck disable=SC2086
    run_capture $_disp "fixed-run" aqueduct run dag/blueprint_fixed.yml $aq_config_flag $ENGINE_SET_FLAG || rc=$?
    local fixed_log="$WORK_DIR/logs/${SNIPPET_NAME}_fixed-run.log"

    if [[ $rc -eq 0 ]]; then
        echo -e "    ${GREEN}✓${RESET} fixed-run"
        SNIPPET_STATUS="PASS"
    elif is_unsupported_failure "$fixed_log"; then
        UNSUPPORTED_LEAF="$(unsupported_leaves "$fixed_log")"
        echo -e "    ${YELLOW}⊘${RESET} fixed-run  (engine does not support: $UNSUPPORTED_LEAF)"
        SNIPPET_STATUS="UNSUPPORTED"
    else
        echo -e "    ${RED}✗${RESET} fixed-run  (see $fixed_log)"
        FAILURES+=("$SNIPPET_NAME / fixed-run")
        SNIPPET_STATUS="FAIL"
    fi
}

PASS_COUNT=0; FAIL_COUNT=0; SKIP_COUNT=0; UNSUPPORTED_COUNT=0

# Initialise GitHub step summary
if [[ "$IN_CI" == "true" && -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
    {
        echo "## Aqueduct Snippet Runner — engine: ${ENGINE:-default}"
        echo ""
        echo "| Snippet | Engine | Result | Duration | Detail |"
        echo "|---------|--------|--------|----------|--------|"
    } > "$GITHUB_STEP_SUMMARY"
fi

# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
for snippet_path in "$WORK_DIR/snippets"/*/; do
    SNIPPET_NAME="$(basename "$snippet_path")"

    if [[ -n "$FILTER" && "$SNIPPET_NAME" != *"$FILTER"* ]]; then
        continue
    fi

    if reason="$(skip_reason_for "$SNIPPET_NAME")"; then
        echo -e "${YELLOW}SKIP${RESET}  $SNIPPET_NAME  ($reason)"
        gh_summary_row "$SNIPPET_NAME" "SKIP" "" "$reason"
        SKIP_COUNT=$(( SKIP_COUNT + 1 ))
        continue
    fi

    echo -e "${BOLD}RUN${RESET}   $SNIPPET_NAME"
    gh_group "$SNIPPET_NAME"

    pushd "$snippet_path" > /dev/null
    SNIPPET_STATUS="PASS"
    UNSUPPORTED_LEAF=""

    # Timing
    SNIPPET_START=$(date +%s%N)

    _disp=""; [[ "$VERBOSE" -ge 1 ]] && _disp="--display"
    _disp_pip=""; [[ "$VERBOSE" -ge 2 ]] && _disp_pip="--display"

    if [[ "$SNIPPET_NAME" == "44_dag_cycle_detection" ]]; then
        run_dag_cycle_snippet
    else
        # Step 1 — requirements.txt
        if [[ -f requirements.txt ]]; then
            run_step $_disp_pip "pip-install" pip install --quiet --disable-pip-version-check -r requirements.txt || SNIPPET_STATUS="FAIL"
        fi

        # Step 2 — populate / generate scripts
        if [[ "$SNIPPET_STATUS" == "PASS" ]]; then
            for populate_script in $(find . -maxdepth 1 -name 'populate*.py' | sort); do
                run_step $_disp "populate-$(basename "$populate_script" .py)" python3 "$populate_script" || SNIPPET_STATUS="FAIL"
            done
        fi

        # Step 3 — aqueduct run. Classifies FAIL vs UNSUPPORTED (see
        # is_unsupported_failure) rather than treating every non-zero exit
        # as a FAIL.
        if [[ "$SNIPPET_STATUS" == "PASS" ]]; then
            aq_config_flag=""
            if [[ -f aqueduct.yml ]]; then aq_config_flag="--config aqueduct.yml"; fi
            rc=0
            # shellcheck disable=SC2086
            run_capture $_disp "aqueduct-run" aqueduct run blueprint.yml $aq_config_flag $ENGINE_SET_FLAG || rc=$?
            log="$WORK_DIR/logs/${SNIPPET_NAME}_aqueduct-run.log"
            if [[ $rc -eq 0 ]]; then
                echo -e "    ${GREEN}✓${RESET} aqueduct-run"
            elif is_unsupported_failure "$log"; then
                UNSUPPORTED_LEAF="$(unsupported_leaves "$log")"
                echo -e "    ${YELLOW}⊘${RESET} aqueduct-run  (engine does not support: $UNSUPPORTED_LEAF)"
                SNIPPET_STATUS="UNSUPPORTED"
            else
                echo -e "    ${RED}✗${RESET} aqueduct-run  (see $log)"
                FAILURES+=("$SNIPPET_NAME / aqueduct-run")
                SNIPPET_STATUS="FAIL"
            fi
        fi

        # Step 4 — inspect_results.py
        if [[ "$SNIPPET_STATUS" == "PASS" && -f inspect_results.py ]]; then
            run_step $_disp "inspect" python3 inspect_results.py || SNIPPET_STATUS="FAIL"
        fi
    fi

    popd > /dev/null

    SNIPPET_END=$(date +%s%N)
    SNIPPET_DURATION_MS=$(( (SNIPPET_END - SNIPPET_START) / 1000000 ))
    SNIPPET_DURATION="$(( SNIPPET_DURATION_MS / 1000 )).$(( SNIPPET_DURATION_MS % 1000 / 100 ))s"

    case "$SNIPPET_STATUS" in
        PASS)
            echo -e "  ${GREEN}→ PASS${RESET}  $SNIPPET_NAME  (${SNIPPET_DURATION})"
            gh_summary_row "$SNIPPET_NAME" "PASS" "$SNIPPET_DURATION"
            PASS_COUNT=$(( PASS_COUNT + 1 ))
            ;;
        UNSUPPORTED)
            echo -e "  ${YELLOW}→ UNSUPPORTED${RESET}  $SNIPPET_NAME  (${SNIPPET_DURATION})  leaf: $UNSUPPORTED_LEAF"
            gh_summary_row "$SNIPPET_NAME" "UNSUPPORTED" "$SNIPPET_DURATION" "leaf: $UNSUPPORTED_LEAF"
            UNSUPPORTED_ENTRIES+=("$SNIPPET_NAME :: $UNSUPPORTED_LEAF")
            UNSUPPORTED_COUNT=$(( UNSUPPORTED_COUNT + 1 ))
            ;;
        *)
            echo -e "  ${RED}→ FAIL${RESET}  $SNIPPET_NAME  (${SNIPPET_DURATION})"
            gh_summary_row "$SNIPPET_NAME" "FAIL" "$SNIPPET_DURATION"
            gh_annotate_error "$SNIPPET_NAME" "aqueduct-run"
            FAIL_COUNT=$(( FAIL_COUNT + 1 ))
            ;;
    esac

    gh_group_end
    echo ""

done

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo -e "Done — ${GREEN}${PASS_COUNT} passed${RESET}  ${RED}${FAIL_COUNT} failed${RESET}  ${YELLOW}${SKIP_COUNT} skipped${RESET}  ${CYAN}${UNSUPPORTED_COUNT} unsupported${RESET}"

# Extract the root-cause error line(s) from a log — shows the actual exception
# message, not the Java stack trace bottom.
#
# Checked in priority order:
#   1. Aqueduct's own `✗ <module> — <msg>` / `✗ blueprint failed` lines (the
#      CLI's `cli/style.py::error()` output, plain-text since a log file is
#      never a TTY) — this is the ROOT CAUSE for an `aqueduct-run` step
#      failure, even when the underlying py4j/Spark exception it wraps is
#      truncated to something generic like "source not found or unreadable"
#      (a Delta jar/pyspark version mismatch showed up exactly this way —
#      see gallery/snippets/03_ingress_delta_incremental's fix history).
#   2. Py4J/pyspark exception markers, for a failure that reaches this log
#      un-wrapped by Aqueduct (e.g. a populate*.py script's own traceback —
#      the "populate-<script>" label already tells you the failure is
#      upstream of aqueduct-run, not the downstream symptom).
#   3. Fallback: last non-empty lines.
extract_error_summary() {
    local log="$1" max_lines="${2:-5}"
    if [[ ! -f "$log" ]]; then return; fi
    local found
    found=$(grep -E '(^✗ |Traceback \(most recent call last\)|pyspark\.sql\.utils|py4j\.protocol|Py4JJavaError|Caused by:|Exception:|^Error: |^ERROR |SparkException|AnalysisException|StreamingQueryException)' "$log" | head -5 || true)
    if [[ -n "$found" ]]; then
        echo "$found"
    else
        # Fallback: last non-empty lines
        grep -v '^$' "$log" | tail -"$max_lines"
    fi
}

if (( ${#FAILURES[@]} > 0 )); then
    echo ""
    echo -e "${RED}${BOLD}=== FAILURES ===${RESET}"
    for entry in "${FAILURES[@]}"; do
        snippet="${entry%% /*}"
        step="${entry#* / }"
        log="$WORK_DIR/logs/${snippet}_${step}.log"
        echo ""
        echo -e "  ${RED}FAILED${RESET} $snippet / $step"
        if [[ -f "$log" ]]; then
            extract_error_summary "$log" 5 | sed 's/^/    /'
        fi
    done
    echo ""
fi

# Every UNSUPPORTED snippet + the leaf(s) that caused it — the real,
# measured answer to "what does this engine not cover yet", never a
# hand-curated list (see is_unsupported_failure / unsupported_leaves above).
if (( ${#UNSUPPORTED_ENTRIES[@]} > 0 )); then
    echo ""
    echo -e "${YELLOW}${BOLD}=== UNSUPPORTED (engine: ${ENGINE:-default}) ===${RESET}"
    for entry in "${UNSUPPORTED_ENTRIES[@]}"; do
        echo "  $entry"
    done
    echo ""
fi

# Append failure details to GitHub step summary
if [[ "$IN_CI" == "true" && -n "${GITHUB_STEP_SUMMARY:-}" && ${#FAILURES[@]} -gt 0 ]]; then
    {
        echo ""
        echo "### Failures"
        echo ""
        for entry in "${FAILURES[@]}"; do
            snippet="${entry%% /*}"
            step="${entry#* / }"
            log="$WORK_DIR/logs/${snippet}_${step}.log"
            echo "<details><summary><code>$snippet / $step</code></summary>"
            echo ""
            echo '```'
            if [[ -f "$log" ]]; then
                extract_error_summary "$log" 20
            fi
            echo '```'
            echo "</details>"
            echo ""
        done
    } >> "$GITHUB_STEP_SUMMARY"
fi

# Only FAIL fails the run. SKIP is a deliberate human decision and
# UNSUPPORTED is an engine-capability fact, not a defect — both are
# informational and must never turn a matrix lane red on their own. This
# matters most on `snippets-lts` (blocking): an unsupported leaf must never
# be indistinguishable from a broken snippet there.
if (( FAIL_COUNT > 0 )); then
    exit 1
fi
