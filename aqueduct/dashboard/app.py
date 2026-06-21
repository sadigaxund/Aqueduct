"""Streamlit observability dashboard — the script run by `aqueduct dashboard`.

Runs ONLY under `streamlit run` (never imported by the base package): it imports
`streamlit` + `plotly` + `pandas` at module top, which live in the optional
`dashboard` extra. The `aqueduct dashboard` CLI checks the extra is installed
before launching, and passes the config path / store dir through the environment
(`AQ_DASH_CONFIG`, `AQ_DASH_STORE_DIR`).

Read-only. No `st.cache_data` on run data — every script run (incl. browser
refresh / F5) re-reads with short-lived connections so the viewer never holds a
DuckDB handle that would block a running pipeline's writer. (Doctor — config
health, not run data — is memoised per browser session; F5 re-runs it.)
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import streamlit as st

from aqueduct.config import load_config
from aqueduct.stores import queries as q

# status → cell colour (real colored cells, no emoji)
_COLOR = {
    "success": "#1a7f37", "ok": "#1a7f37",
    "error": "#cf222e", "fail": "#cf222e",
    "warn": "#9a6700", "skip": "#57606a", "skipped": "#57606a",
}


def _style(df: pd.DataFrame, status_cols=("status",)):
    cols = [c for c in status_cols if c in df.columns]

    def paint(v):
        c = _COLOR.get(str(v).lower())
        return f"background-color:{c};color:white;font-weight:600" if c else ""

    sty = df.style
    if cols:
        sty = sty.map(paint, subset=list(cols))
    return sty


def _table(rows: list[dict], status_cols=("status",)):
    """Static, full-length table with colored status cells (no chrome / no nudge)."""
    if not rows:
        st.caption("— nothing here —")
        return
    st.table(_style(pd.DataFrame(rows), status_cols))


# ── Tabs ─────────────────────────────────────────────────────────────────────

def _fleet_tab(cfg, store_dir):
    summ = q.fleet_summary(cfg, store_dir=store_dir)
    if not summ:
        st.info("No observability stores found yet. Run a blueprint first.")
        return
    total_runs = sum(s.runs for s in summ)
    total_succ = sum(s.successes for s in summ)
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Blueprints", len(summ))
    c2.metric("Runs", total_runs)
    c3.metric("Success rate", f"{(total_succ / total_runs * 100) if total_runs else 0:.0f}%")
    c4.metric("Heal attempts", sum(s.heal_attempts for s in summ))

    _table([
        {"blueprint": s.blueprint_id, "runs": s.runs, "success %": round(s.success_rate * 100, 1),
         "errors": s.errors, "heal attempts": s.heal_attempts, "last run": s.last_run or ""}
        for s in summ
    ], status_cols=())

    rot = q.runs_over_time(cfg, store_dir=store_dir)
    if rot:
        days = sorted({d.day for d in rot})
        statuses = sorted({d.status for d in rot})
        grid = {s: {d: 0 for d in days} for s in statuses}
        for d in rot:
            grid[d.status][d.day] = d.count
        st.caption("Runs over time")
        st.line_chart(pd.DataFrame({s: [grid[s][d] for d in days] for s in statuses}, index=days))

    dist = q.failure_categories(cfg, store_dir=store_dir)
    if dist:
        st.caption("Failure categories")
        st.bar_chart(pd.Series(dist, name="count"))


def _collect_runs(handles):
    """All runs across every store → (list[RunRow], parallel list of handles)."""
    runs, owners = [], []
    for h in handles:
        try:
            for r in q.list_runs(h.store, limit=500):
                runs.append(r)
                owners.append(h)
        except Exception:
            continue
    order = sorted(range(len(runs)), key=lambda i: runs[i].started_at or "", reverse=True)
    return [runs[i] for i in order], [owners[i] for i in order]


def _runs_tab(handles):
    if not handles:
        st.info("No observability stores found yet.")
        return
    runs, owners = _collect_runs(handles)
    if not runs:
        st.info("No runs yet.")
        return

    # Inline filters (no sidebar). Empty blueprint filter = all.
    fc1, fc2 = st.columns([3, 2])
    bps = sorted({r.blueprint_id for r in runs})
    pick_bp = fc1.multiselect("Filter blueprint", bps, placeholder="all blueprints")
    pick_status = fc2.selectbox("Filter status", ["all", "success", "error", "skipped"])
    keep = [
        i for i, r in enumerate(runs)
        if (not pick_bp or r.blueprint_id in pick_bp)
        and (pick_status == "all" or r.status == pick_status)
    ]
    fruns = [runs[i] for i in keep]
    fowners = [owners[i] for i in keep]
    if not fruns:
        st.caption("— no runs match the filter —")
        return

    df = pd.DataFrame([
        {"status": r.status, "run_id": r.run_id, "blueprint": r.blueprint_id, "started": r.started_at or ""}
        for r in fruns
    ])
    st.caption("Click a row to inspect · click a header to sort")
    event = st.dataframe(
        _style(df), width="stretch", hide_index=True,
        height=min(38 + 36 * len(fruns), 560),
        on_select="rerun", selection_mode="single-row", key="runs_table",
    )
    sel = event.selection.rows if event and event.selection else []
    run, owner = (fruns[sel[0]], fowners[sel[0]]) if sel else (fruns[0], fowners[0])

    st.divider()
    st.markdown(f"### Run `{run.run_id}`  ·  {run.blueprint_id}")
    det = q.run_detail(owner.store, run.run_id)
    if det is None:
        return
    prof = {p.module_id: p for p in det.profile}
    _table([
        {"status": m.status, "module": m.module_id,
         "duration_ms": getattr(prof.get(m.module_id), "duration_ms", None),
         "rows_out": getattr(prof.get(m.module_id), "records_written", None),
         "bytes_out": getattr(prof.get(m.module_id), "bytes_written", None),
         "error": (m.error or "")[:200]}
        for m in det.modules
    ])


def _lineage_tab(handles):
    if not handles:
        st.info("No observability stores found yet.")
        return
    # Lineage is inherently per-blueprint → inline picker of stores that HAVE it.
    options = []
    for h in handles:
        try:
            if q.lineage(h.store, limit=1):
                options.append(h)
        except Exception:
            continue
    if not options:
        st.info("No column-lineage captured yet (needs a SQL Channel).")
        return
    labels = [h.label for h in options]
    pick = st.selectbox("Blueprint", labels)
    handle = options[labels.index(pick)]
    rows = q.lineage(handle.store)

    import plotly.graph_objects as go

    labels_n: list[str] = []
    colors: list[str] = []
    src_idx: dict[str, int] = {}
    dst_idx: dict[str, int] = {}

    def node(name, is_src):
        cache = src_idx if is_src else dst_idx
        if name not in cache:
            cache[name] = len(labels_n)
            labels_n.append(name)
            colors.append("#4C78A8" if is_src else "#54A24B")
        return cache[name]

    s, t = [], []
    for r in rows:
        src = f"{r.source_table}.{r.source_column}" if r.source_table else r.source_column
        s.append(node(src, True))
        t.append(node(r.output_column, False))
    fig = go.Figure(go.Sankey(
        arrangement="snap",
        node=dict(label=labels_n, color=colors, pad=20, thickness=16,
                  line=dict(width=0), hovertemplate="%{label}<extra></extra>"),
        link=dict(source=s, target=t, value=[1] * len(s), color="rgba(130,130,130,0.3)",
                  hovertemplate="%{source.label} → %{target.label}<extra></extra>"),
    ))
    fig.update_layout(height=min(max(260, 28 * max(len(src_idx), len(dst_idx))), 800),
                      margin=dict(l=8, r=8, t=8, b=8), font=dict(size=13))
    st.plotly_chart(fig, width="stretch")
    _table([{"channel": r.channel_id, "output": r.output_column,
             "source_table": r.source_table, "source_column": r.source_column} for r in rows],
           status_cols=())


def _doctor_tab(config_path):
    # Config health, not run data → memoise per session; F5 re-runs.
    if "doctor_results" not in st.session_state:
        try:
            from aqueduct.doctor import run_doctor
            st.session_state["doctor_results"] = run_doctor(
                config_path=Path(config_path) if config_path else None, skip_spark=True)
            st.session_state["doctor_error"] = None
        except Exception as exc:  # noqa: BLE001
            st.session_state["doctor_results"] = []
            st.session_state["doctor_error"] = str(exc)
    if st.session_state.get("doctor_error"):
        st.error(f"doctor failed: {st.session_state['doctor_error']}")
        return
    st.caption("Health checks (Spark probe skipped). Press F5 to re-run.")
    _table([{"status": r.status, "check": r.name, "detail": r.detail or ""}
            for r in st.session_state["doctor_results"]])


def _config_tab(cfg):
    rows = []
    dep = getattr(cfg, "deployment", None)
    if dep is not None:
        for f in ("engine", "target", "master_url", "env"):
            if hasattr(dep, f):
                rows.append({"key": f"deployment.{f}", "value": str(getattr(dep, f))})
    stores = getattr(cfg, "stores", None)
    if stores is not None:
        for sname in ("observability", "depot", "blob", "benchmark"):
            s = getattr(stores, sname, None)
            b = getattr(s, "backend", None) if s is not None else None
            if b is not None:
                rows.append({"key": f"stores.{sname}.backend", "value": str(b)})
    agent = getattr(cfg, "agent", None)
    if agent is not None:
        for f in ("provider", "model"):
            if getattr(agent, f, None):
                rows.append({"key": f"agent.{f}", "value": str(getattr(agent, f))})
    _table(rows, status_cols=())
    st.caption("Read-only. Secret values are never read or displayed.")


def main() -> None:
    st.set_page_config(page_title="Aqueduct Dashboard", layout="wide")
    config_path = os.environ.get("AQ_DASH_CONFIG") or None
    store_dir = os.environ.get("AQ_DASH_STORE_DIR") or None
    try:
        cfg = load_config(Path(config_path) if config_path else None)
    except Exception as exc:  # noqa: BLE001
        st.error(f"config error: {exc}")
        return

    backend = getattr(cfg.stores.observability, "backend", "duckdb")
    st.title("Aqueduct")
    st.caption(f"observability backend: **{backend}** · read-only viewer · F5 to refresh")
    handles = q.discover_stores(cfg, store_dir=store_dir)

    fleet, runs, lineage, doctor, config = st.tabs(
        ["Fleet", "Runs", "Lineage", "Doctor", "Config"])
    with fleet:
        _fleet_tab(cfg, store_dir)
    with runs:
        _runs_tab(handles)
    with lineage:
        _lineage_tab(handles)
    with doctor:
        _doctor_tab(config_path)
    with config:
        _config_tab(cfg)


# streamlit runs the target script with __name__ == "__main__".
if __name__ == "__main__":
    main()
