#!/usr/bin/env bash
# Run all self-contained gallery snippets in an isolated tmp dir + Python 3.12 venv.
# Skipped: 44_dag_cycle_detection (no root blueprint.yml by design — it's a
# multi-file manual walkthrough: dag/blueprint.yml is an intentional compile
# error, dag/blueprint_fixed.yml and hook/*.yml are separate standalone demos —
# incompatible with this script's one-blueprint-per-snippet assumption).
#
# Usage:
#   ./scripts/run_snippets.sh [-v] [-vv] [SNIPPETS_DIR]
#
#   -v    Show populate/generate, aqueduct-run, and inspect output inline
#   -vv   Show everything inline including pip-install
#
# SNIPPETS_DIR defaults to gallery/snippets/ relative to the repo root.
# The script resolves the repo root from its own location, so it works from any cwd.

set -euo pipefail

# ---------------------------------------------------------------------------
# Arg parse
# ---------------------------------------------------------------------------
VERBOSE=0
SNIPPETS_DIR=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        -v) VERBOSE=1; shift ;;
        -vv) VERBOSE=2; shift ;;
        -vvv) VERBOSE=3; shift ;;
        --) shift; SNIPPETS_DIR="${1:-}"; break ;;
        -*)
            echo "Unknown flag: $1"
            echo "Usage: $0 [-v] [-vv] [SNIPPETS_DIR]"
            exit 1
            ;;
        *) SNIPPETS_DIR="$1"; shift ;;
    esac
done

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

# Snippets that require live external services, or aren't compatible with the
# one-blueprint-per-snippet assumption below — skip entirely
SKIP_PATTERNS=(
    "12_assert_null_sampling",
    "44_dag_cycle_detection"
)

# Colours
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
RESET='\033[0m'

# ---------------------------------------------------------------------------
# Setup: isolated tmp workspace
# ---------------------------------------------------------------------------
RUN_ID="$(cat /proc/sys/kernel/random/uuid 2>/dev/null || python3 -c 'import uuid; print(uuid.uuid4())')"
WORK_DIR="$WORK_BASE/${RUN_ID:0:8}"
mkdir -p "$WORK_DIR"

echo -e "${CYAN}${BOLD}Aqueduct snippet runner${RESET}"
echo -e "  Workspace : $WORK_DIR"
echo -e "  Snippets  : $SNIPPETS_DIR"
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
# pip writes build wheels to $TMPDIR during install. On systems with a small
# tmpfs quota this exhausts /tmp before the install completes. Redirect pip's
# temp dir to a subdir of the workspace (on the real disk) for the duration
# of this script.
PIP_TMPDIR="$WORK_BASE/.pip_tmp"
mkdir -p "$PIP_TMPDIR"
export TMPDIR="$PIP_TMPDIR"

_pip_ok() { "$VENV/bin/python" -c "import pandas, pyarrow, rich, pyspark" 2>/dev/null; }

if [[ ! -d "$VENV" ]] || ! _pip_ok; then
    [[ -d "$VENV" ]] && echo -e "${YELLOW}Venv incomplete — reinstalling...${RESET}"
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
    # Re-install aqueduct so source changes are picked up
    pip install --quiet --disable-pip-version-check -e "$REPO_ROOT[spark,llm]"
fi
echo ""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
is_skipped() {
    local name="$1"
    for pat in "${SKIP_PATTERNS[@]}"; do
        if [[ "$name" == *"$pat"* ]]; then
            return 0
        fi
    done
    return 1
}

run_step() {
    # run_step [--capture|--display] LABEL CMD [ARGS...]
    # --capture (default): redirect to log, show only status
    # --display:            show output inline + log
    local mode="capture"
    local label
    if [[ "$1" == "--display" ]]; then
        mode="display"
        shift
    fi
    label="$1"; shift

    local log="$WORK_DIR/logs/${SNIPPET_NAME}_${label}.log"
    mkdir -p "$(dirname "$log")"

    if [[ "$mode" == "display" && "$VERBOSE" -ge 1 ]]; then
        # Display inline + log simultaneously
        if "$@" 2>&1 | tee "$log"; then
            echo -e "    ${GREEN}✓${RESET} $label"
            return 0
        else
            echo -e "    ${RED}✗${RESET} $label  (see $log)"
            return 1
        fi
    else
        # Quiet: log only
        if "$@" >"$log" 2>&1; then
            echo -e "    ${GREEN}✓${RESET} $label"
            return 0
        else
            echo -e "    ${RED}✗${RESET} $label  (see $log)"
            return 1
        fi
    fi
}

PASS_COUNT=0; FAIL_COUNT=0; SKIP_COUNT=0

# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
for snippet_path in "$WORK_DIR/snippets"/*/; do
    SNIPPET_NAME="$(basename "$snippet_path")"

    if is_skipped "$SNIPPET_NAME"; then
        echo -e "${YELLOW}SKIP${RESET}  $SNIPPET_NAME"
        SKIP_COUNT=$(( SKIP_COUNT + 1 ))
        continue
    fi

    echo -e "${BOLD}RUN${RESET}   $SNIPPET_NAME"
    pushd "$snippet_path" > /dev/null

    SNIPPET_OK=true

    # Populate, run, and inspect display inline at -v; pip noise only at -vv
    _disp=""
    [[ "$VERBOSE" -ge 1 ]] && _disp="--display"
    _disp_pip=""
    [[ "$VERBOSE" -ge 2 ]] && _disp_pip="--display"

    # Step 1 — requirements.txt
    if [[ -f requirements.txt ]]; then
        run_step $_disp_pip "pip-install" pip install --quiet --disable-pip-version-check -r requirements.txt || SNIPPET_OK=false
    fi

    # Step 2 — populate / generate scripts (run ALL found)
    if $SNIPPET_OK; then
        for populate_script in $(find . -maxdepth 1 -name 'populate*.py' | sort); do
            run_step $_disp "populate-$(basename "$populate_script" .py)" python3 "$populate_script" || SNIPPET_OK=false
        done
    fi

    # Step 3 — aqueduct run
    if $SNIPPET_OK; then
        aq_config_flag=""
        if [[ -f aqueduct.yml ]]; then aq_config_flag="--config aqueduct.yml"; fi
        # shellcheck disable=SC2086
        run_step $_disp "aqueduct-run" aqueduct run blueprint.yml $aq_config_flag || SNIPPET_OK=false
    fi

    # Step 4 — inspect_results.py
    if $SNIPPET_OK && [[ -f inspect_results.py ]]; then
        run_step $_disp "inspect" python3 inspect_results.py || SNIPPET_OK=false
    fi

    popd > /dev/null

    if $SNIPPET_OK; then
        echo -e "  ${GREEN}→ PASS${RESET}  $SNIPPET_NAME"
        PASS_COUNT=$(( PASS_COUNT + 1 ))
    else
        echo -e "  ${RED}→ FAIL${RESET}  $SNIPPET_NAME  (logs: $WORK_DIR/logs/)"
        FAIL_COUNT=$(( FAIL_COUNT + 1 ))
    fi
    echo ""

done

echo -e "Done — ${GREEN}${PASS_COUNT} passed${RESET}  ${RED}${FAIL_COUNT} failed${RESET}  ${YELLOW}${SKIP_COUNT} skipped${RESET}"

if (( FAIL_COUNT > 0 )); then
    exit 1
fi
