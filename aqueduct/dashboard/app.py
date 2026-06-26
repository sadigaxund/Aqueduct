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

import json
import os

# Force pyspark initialisation early so narwhals/plotly don't trigger a
# circular import when type-checking DataFrames.
try:
    import pyspark.sql  # noqa: F401
except Exception:
    pass  # pyspark early-import is best-effort; circular-import workaround must not break a headless viewer

from datetime import datetime
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

_md = "\u2014"  # em-dash, pre-computed for Python 3.11 f-string compat

from aqueduct.redaction import redact  # noqa: E402  (intentional mid-file import)
from aqueduct.stores import queries as q  # noqa: E402  (intentional mid-file import)

# status → cell colour (real colored cells, no emoji)
_COLOR = {
    "success": "#1a7f37", "ok": "#1a7f37",
    "error": "#cf222e", "fail": "#cf222e",
    "warn": "#9a6700", "skip": "#57606a", "skipped": "#57606a",
}

# Columns that hold numbers — right-aligned, narrow.
_NUMERIC = frozenset({
    "runs", "success %", "errors", "heal attempts",
    "duration_ms", "rows_out", "bytes_out", "count",
    "records_read", "bytes_read", "duration",
})

# Per-column min-width hints so narrow values don't hog space.
_COL_WIDTH: dict[str, str] = {
    "status": "80px",
    "runs": "60px",
    "success %": "80px",
    "errors": "60px",
    "heal attempts": "100px",
    "duration_ms": "90px",
    "rows_out": "80px",
    "bytes_out": "80px",
    "records_read": "90px",
    "bytes_read": "80px",
    "duration": "80px",
    "run_id": "220px",
    "blueprint": "180px",
    "channel": "150px",
    "module": "150px",
    "output": "150px",
    "source_table": "140px",
    "source_column": "140px",
    "check": "200px",
    "key": "200px",
    "fingerprint": "100px",
    "first seen": "180px",
    "last seen": "180px",
}


def _style(df: pd.DataFrame, status_cols=("status",),
           numeric_cols: frozenset[str] | None = None,
           col_width: dict[str, str] | None = None,
           formats: dict[str, str] | None = None):
    """Apply status coloring, numeric right-alignment, formatting, and column widths."""
    if numeric_cols is None:
        numeric_cols = _NUMERIC
    if col_width is None:
        col_width = _COL_WIDTH

    def paint(v):
        c = _COLOR.get(str(v).lower())
        return f"background-color:{c};color:white;font-weight:600" if c else ""

    sty = df.style

    # status colours
    scols = [c for c in status_cols if c in df.columns]
    if scols:
        sty = sty.map(paint, subset=list(scols))

    # right-align known numeric columns
    ncols = [c for c in df.columns if c in numeric_cols]
    if ncols:
        sty = sty.set_properties(subset=ncols, **{"text-align": "right"})

    # column-level CSS — min-width for known widths, prevent date wrapping
    styles = []
    for c in df.columns:
        w = col_width.get(c)
        if w:
            styles.append({"selector": f"th.col_heading.col{c} , td.col{c}",
                           "props": [("min-width", w), ("white-space", "nowrap")]})
        else:
            styles.append({"selector": f"td.col{c}",
                           "props": [("white-space", "nowrap")]})
    if styles:
        sty = sty.set_table_styles(styles, overwrite=False)

    # formatted display (%, commas, etc.) — na_rep prevents None values
    # from crashing pandas Styler.format with "{:,}" et al.
    if formats:
        fmtd = {c: f for c, f in formats.items() if c in df.columns}
        if fmtd:
            sty = sty.format(fmtd, na_rep="")

    return sty


def _table(rows: list[dict], status_cols=("status",),
           numeric_cols: frozenset[str] | None = None,
           col_width: dict[str, str] | None = None,
           formats: dict[str, str] | None = None,
           max_static: int = 20):
    """Colored-status table. Static + full-length when small; past `max_static`
    rows it switches to a height-capped scrollable dataframe so a busy fleet
    (many blueprints / modules / lineage columns) can't blow the page open."""
    if not rows:
        st.caption("— nothing here —")
        return
    styled = _style(pd.DataFrame(rows), status_cols, numeric_cols, col_width, formats)
    if len(rows) > max_static:
        st.dataframe(styled, width="stretch", hide_index=True, height=560)
    else:
        st.table(styled)


def _count_yaxis(fig, max_val: float = 0) -> None:
    """Integer y-ticks that stay readable at ANY scale.

    `dtick=1` is right for small counts (no fractional 0.5 ticks) but
    catastrophic for large ones — a gridline every unit means thousands of ticks
    (e.g. a distinct-count in the millions would hang the browser). Force unit
    ticks only when the max is small; otherwise let Plotly auto-space and format
    ticks as integers. Pass max_val=0 (default) when the range is unknown/large.
    """
    if 0 < max_val <= 12:
        fig.update_yaxes(dtick=1, tickformat="d")
    else:
        fig.update_yaxes(tickformat="d")


# ── Tabs ─────────────────────────────────────────────────────────────────────

def _blueprint_handle_map(handles):
    """Map blueprint_id → StoreHandle across all stores.

    Postgres has one handle for all blueprints; DuckDB has one per file.
    This normalises both so the UI can select by blueprint_id.
    """
    bp_to_handle = {}
    for h in handles:
        try:
            for r in q.list_runs(h.store, limit=500):
                bp_to_handle.setdefault(r.blueprint_id, h)
        except Exception:
            continue
    return bp_to_handle

def _st_blob(handle, path_str: str, blueprint_id: str = "") -> None:
    """Read a blob via the configured object store, redact, and display."""
    from aqueduct.stores.object_store import make_blob_store
    root = handle.duckdb_path.parent if handle.duckdb_path else None
    if root is None and handle.blob_root and blueprint_id:
        root = handle.blob_root / blueprint_id
    if root is None:
        st.caption("Blob viewing not supported for this backend.")
        return
    blob = make_blob_store(handle.blob_backend, handle.blob_location, root)
    try:
        raw = blob.materialize(path_str)
    except Exception as exc:
        st.caption(f"Could not load blob: {exc}")
        return
    if not raw or raw == path_str:
        return
    try:
        obj = json.loads(raw)
        st.json(redact(obj))
    except json.JSONDecodeError:
        st.code(redact(raw), language="text")


def _fleet_tab(cfg, store_dir):
    summ = q.fleet_summary(cfg, store_dir=store_dir)
    if not summ:
        st.info("No observability stores found yet. Run a blueprint first.")
        return
    total_runs = sum(s.runs for s in summ)
    total_succ = sum(s.successes for s in summ)

    hc = q.heal_coverage(cfg, store_dir=store_dir)
    zero_token = hc.get("cached", 0) + hc.get("replayed", 0)
    total_resolved = zero_token + hc.get("llm", 0)
    cov_pct = f"{zero_token / total_resolved * 100:.0f}%" if total_resolved else "\u2014"

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Blueprints", len(summ))
    c2.metric("Runs", total_runs)
    c3.metric("Success Rate", f"{(total_succ / total_runs * 100) if total_runs else 0:.0f}%")
    c4.metric("Heal Attempts", sum(s.heal_attempts for s in summ))
    c5.metric("Zero-Token Coverage", cov_pct)

    rot = q.runs_over_time(cfg, store_dir=store_dir)
    dist = q.failure_categories(cfg, store_dir=store_dir)
    gates = q.gate_rejection_rates(cfg, store_dir=store_dir)
    has_rot = bool(rot)
    has_dist = bool(dist)
    has_gates = bool(gates)

    ncharts = sum([has_rot, has_dist, has_gates])
    if ncharts == 3:
        cols = st.columns(3)
        lc, mc, rc = cols[0], cols[1], cols[2]
    elif ncharts == 2:
        lc, rc = st.columns(2)
        mc = None
    elif has_rot:
        lc = st.container()
        mc = rc = None
    else:
        lc = mc = None
        rc = st.container() if (has_dist or has_gates) else None

    if has_rot:
        days = sorted({d.day for d in rot})
        statuses = sorted({d.status for d in rot})
        grid = {s: {d: 0 for d in days} for s in statuses}
        for d in rot:
            grid[d.status][d.day] = d.count
        with lc:
            st.caption("Runs Over Time")
            fig = px.line(
                pd.DataFrame({s: [grid[s][d] for d in days] for s in statuses}, index=days),
                x=days, y=statuses,
                labels={"value": "runs", "index": "", "variable": "status"},
            )
            fig.update_xaxes(tickangle=45, title=None)
            _count_yaxis(fig, max((grid[s][d] for s in statuses for d in days), default=0))
            fig.update_layout(legend=dict(orientation="h", y=1.02, x=0), height=400, margin=dict(l=8, r=8, t=8, b=8))
            st.plotly_chart(fig, width="stretch")

    if has_dist and rc is not None:
        with rc:
            st.caption("Failure Categories")
            fig = px.bar(
                x=list(dist.keys()), y=list(dist.values()),
                labels={"x": "", "y": "count"},
            )
            _count_yaxis(fig, max(dist.values(), default=0))
            fig.update_xaxes(tickangle=45, title=None)
            fig.update_layout(height=400, margin=dict(l=8, r=8, t=8, b=8))
            st.plotly_chart(fig, width="stretch")

    if has_gates:
        target = mc if mc is not None else (rc if has_dist else lc) if has_dist else lc
        with target:
            st.caption("Gate Rejections")
            fig = px.bar(
                x=list(gates.keys()), y=list(gates.values()),
                labels={"x": "", "y": "count"},
            )
            _count_yaxis(fig, max(gates.values(), default=0))
            fig.update_xaxes(tickangle=45, title=None)
            fig.update_layout(height=400, margin=dict(l=8, r=8, t=8, b=8))
            st.plotly_chart(fig, width="stretch")

    _table([
        {"blueprint": s.blueprint_id, "runs": s.runs, "success %": round(s.success_rate * 100, 1),
         "errors": s.errors, "heal attempts": s.heal_attempts, "last run": s.last_run or ""}
        for s in summ
    ], status_cols=(),
       formats={"success %": "{:.1f}%", "runs": "{:,}", "errors": "{:,}", "heal attempts": "{:,}"})


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


def _duration(started: str | None, finished: str | None) -> str:
    """Human-readable duration (e.g. '35s', '2m 15s') or '—'."""
    if not started or not finished:
        return "\u2014"
    try:
        s = datetime.fromisoformat(started)
        f = datetime.fromisoformat(finished)
        secs = int((f - s).total_seconds())
        if secs < 60:
            return f"{secs}s"
        return f"{secs // 60}m {secs % 60}s"
    except Exception:
        return "\u2014"


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
        {"status": r.status, "run_id": r.run_id, "blueprint": r.blueprint_id,
         "duration": _duration(r.started_at, r.finished_at), "started": r.started_at or ""}
        for r in fruns
    ])
    st.caption("Click a row to inspect detail")
    nrows = len(df)
    table_h = min(36 + 35 * nrows, 560) if nrows > 0 else None
    event = st.dataframe(
        _style(df), width="stretch", hide_index=True,
        height=table_h,
        on_select="rerun", selection_mode="single-row", key="runs_table",
    )
    sel_rows = event.selection.rows if event and event.selection else []
    if not sel_rows:
        return
    run = fruns[sel_rows[0]]
    owner = fowners[sel_rows[0]]

    st.divider()
    st.markdown(f"### `{run.run_id}`  ·  {run.blueprint_id}")
    det = q.run_detail(owner.store, run.run_id)
    if det is None:
        return
    prof = {p.module_id: p for p in det.profile}
    def _v(p, attr):
        v = getattr(p, attr, None) if p else None
        return 0 if v is None else v

    mt1, mt2, mt3 = st.tabs(["Modules", "Quality", "Trends"])

    with mt1:
        _table([
            {"status": m.status, "module": m.module_id,
             "records_read": _v(prof.get(m.module_id), "records_read"),
             "rows_out": _v(prof.get(m.module_id), "records_written"),
             "bytes_read": _v(prof.get(m.module_id), "bytes_read"),
             "bytes_out": _v(prof.get(m.module_id), "bytes_written"),
             "duration_ms": _v(prof.get(m.module_id), "duration_ms"),
             "error": (m.error or "")[:200]}
            for m in det.modules
        ], formats={"duration_ms": "{:,}", "rows_out": "{:,}", "bytes_out": "{:,}",
                    "records_read": "{:,}", "bytes_read": "{:,}"})

        if run.status == "error" or any(m.status == "error" for m in det.modules):
            fc = q.failure_context(owner.store, run.run_id)
            if fc is not None:
                head = f"**module** `{fc.failed_module}`"
                if fc.error_class:
                    head += f"  ·  **{fc.error_class}**"
                if fc.object_name:
                    head += f"  ·  object `{fc.object_name}`"
                st.markdown(head)
                if fc.suggested_columns:
                    st.markdown("**suggested columns:** "
                                + ", ".join(f"`{c}`" for c in fc.suggested_columns))
                st.code(fc.error_message or "(no message)", language="text")
                if fc.stack_trace:
                    with st.expander("Stack trace"):
                        _st_blob(owner, fc.stack_trace, run.blueprint_id)
                if fc.provenance_json:
                    with st.expander("Provenance"):
                        _st_blob(owner, fc.provenance_json, run.blueprint_id)
                if fc.manifest_json:
                    with st.expander("Manifest"):
                        _st_blob(owner, fc.manifest_json, run.blueprint_id)
            else:
                for m in det.modules:
                    if m.error:
                        st.markdown(f"**`{m.module_id}`**")
                        st.code(m.error, language="text")

    with mt3:
        mod_ids = sorted({p.module_id for p in det.profile})
        tc1, tc2 = st.columns([2, 3])
        trend_mod = tc1.selectbox("Module", mod_ids, key="trend_mod")
        metric_key = tc2.selectbox("Metric", list(q.METRIC_LABELS),
                                   format_func=lambda k: q.METRIC_LABELS[k], key="trend_metric")
        trend = q.module_trends(owner.store, run.blueprint_id, trend_mod)
        if len(trend) >= 2:
            trend_df = pd.DataFrame([
                {"run": t.run_id[:8], "started": t.started_at[:19] if t.started_at else "?",
                 metric_key: getattr(t, metric_key) or 0}
                for t in reversed(trend)
            ])
            fig = px.line(trend_df, x="started", y=metric_key,
                          title=f"{trend_mod} — {q.METRIC_LABELS[metric_key]} across recent runs",
                          markers=True)
            fig.update_layout(xaxis_title=None, height=280, margin=dict(l=8, r=8, t=32, b=8))
            st.plotly_chart(fig, width="stretch")
        elif trend:
            st.caption(f"Only one data point for {trend_mod} — need ≥2 runs with profile data.")
        else:
            st.caption(f"No trend data for {trend_mod}.")

    with mt2:
        avail_sigs = [s for s in q.PROBE_METRIC_LABELS
                      if q.probe_signals(owner.store, run.blueprint_id, s, limit=1)]
        if not avail_sigs:
            st.caption("No probe signals for this blueprint.")
        else:
            sig_type = st.selectbox("Signal type", avail_sigs,
                                    format_func=lambda k: q.PROBE_METRIC_LABELS[k],
                                    key="cq_sig")
            sigs = q.probe_signals(owner.store, run.blueprint_id, sig_type)
            if sig_type == "null_rates":
                cols = sorted({c for s in sigs for c in (s.payload.get("null_rates") or {})})
                col = st.selectbox("Column", cols, key="cq_col")
                vals = [(s.started_at[:19], s.payload.get("null_rates", {}).get(col))
                        for s in reversed(sigs)]
                df = pd.DataFrame(vals, columns=["run", f"null_rate_{col}"])
                st.dataframe(df, width="stretch", hide_index=True)
                if len(vals) >= 2:
                    fig = px.line(df, x="run", y=f"null_rate_{col}", markers=True,
                                  title=f"Null rate — {col}")
                    fig.update_layout(height=240, margin=dict(l=8, r=8, t=32, b=8))
                    st.plotly_chart(fig, width="stretch")
            elif sig_type == "value_distribution":
                cols = sorted({c for s in sigs for c in (s.payload.get("stats") or {})})
                col = st.selectbox("Column", cols, key="cq_vd_col")
                stat_key = st.selectbox("Stat", ["min", "max", "mean", "stddev"],
                                        key="cq_vd_stat")
                vals = []
                for s in reversed(sigs):
                    st_ = (s.payload.get("stats") or {}).get(col, {})
                    vals.append((s.started_at[:19], st_.get(stat_key)))
                df = pd.DataFrame(vals, columns=["run", stat_key])
                st.dataframe(df, width="stretch", hide_index=True)
                if len(vals) >= 2:
                    fig = px.line(df, x="run", y=stat_key, markers=True,
                                  title=f"{stat_key} — {col}")
                    fig.update_layout(height=240, margin=dict(l=8, r=8, t=32, b=8))
                    st.plotly_chart(fig, width="stretch")
            elif sig_type == "distinct_count":
                cols = sorted({c for s in sigs for c in (s.payload.get("distinct_counts") or {})})
                rows_l = []
                for s in reversed(sigs):
                    dc = s.payload.get("distinct_counts") or {}
                    rows_l.append({"run": s.started_at[:19], **dc})
                df = pd.DataFrame(rows_l)
                st.dataframe(_style(df), width="stretch", hide_index=True)
                if len(rows_l) >= 2:
                    fig = px.line(df, x="run", y=cols, markers=True,
                                  title="Distinct count per column")
                    _count_yaxis(fig, 0)
                    fig.update_layout(height=240, margin=dict(l=8, r=8, t=32, b=8))
                    st.plotly_chart(fig, width="stretch")
            elif sig_type == "schema_snapshot":
                prev: dict[str, str] = {}
                for s in sigs:
                    fields = {f["name"]: f["type"] for f in (s.payload.get("fields") or [])}
                    changed = {c: (prev[c], fields[c]) for c in fields  # noqa: F841  # TODO: surface schema-snapshot diff (incomplete feature)
                               if c in prev and prev[c] != fields[c]}
                    prev = fields
                rows_l = []
                for s in reversed(sigs):
                    fields = s.payload.get("fields") or []
                    rows_l.append({"run": s.started_at[:19],
                                   **{f["name"]: f["type"] for f in fields}})
                df = pd.DataFrame(rows_l)
                st.dataframe(df, width="stretch", hide_index=True)


def _lineage_tab(handles):
    if not handles:
        st.info("No observability stores found yet.")
        return
    bp_to_handle = _blueprint_handle_map(handles)
    # Discover which blueprint_ids have lineage or fingerprint data.
    scored = []
    for bp, h in bp_to_handle.items():
        has_col = q.lineage(h.store, limit=1) != []
        has_fp = q.channel_fingerprints(h.store, bp) != []
        if has_col or has_fp:
            scored.append((bp, h, has_col, has_fp))
    if not scored:
        st.info("No column-lineage or SQL fingerprint data yet (needs a SQL Channel).")
        return
    pick = st.selectbox("Blueprint", [s[0] for s in scored])
    handle, has_col, has_fp = next(
        (h, c, f) for bp, h, c, f in scored if bp == pick
    )

    import plotly.graph_objects as go

    if has_col:
        # Only the latest run's lineage — each run appends its own set.
        latest = q.list_runs(handle.store, blueprint_id=pick, limit=1)
        lp_run = latest[0].run_id if latest else None
        rows = q.lineage(handle.store, run_id=lp_run) if lp_run else []

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

    if has_fp:
        fps = q.channel_fingerprints(handle.store, pick)
        st.divider()
        st.markdown("**SQL Changelog**")
        import difflib as _dl
        from collections import defaultdict as _dd
        by_ch: dict[str, list] = _dd(list)
        for f in fps:
            by_ch[f.channel_id].append(f)
        for ch in sorted(by_ch):
            versions = sorted(by_ch[ch], key=lambda x: x.first_seen)
            # Build diff pairs so each version shows the diff from its predecessor
            pairs: list[tuple] = []
            for i, v in enumerate(versions):
                diff_text = ""
                if i > 0:
                    prev = versions[i - 1]
                    diff_text = "\n".join(_dl.unified_diff(
                        prev.canonical_sql.splitlines(),
                        v.canonical_sql.splitlines(),
                        fromfile=f"v{i}",
                        tofile=f"v{i + 1}",
                        lineterm="",
                    ))
                pairs.append((v, diff_text, i + 1))
            label = f"{ch}  —  {len(versions)} version{'s' if len(versions) > 1 else ''}"
            with st.expander(label):
                for v, diff_text, num in reversed(pairs):
                    st.caption(f"v{num}")
                    st.code(v.fingerprint, language="text")
                    st.code(v.canonical_sql, language="sql")
                    if diff_text:
                        st.caption(f"diff v{num - 1} → v{num}")
                        st.code(diff_text, language="diff")

    # ── Drift timeline ──────────────────────────────────────────────────
    drift = q.drift_events(handle.store, pick)
    if drift:
        st.divider()
        st.markdown("**Schema Drift**")
        with st.expander(f"Timeline ({len(drift)} check{'s' if len(drift) > 1 else ''})"):
            fig = go.Figure()
            st_colors = {
                "baseline_set": "#4C78A8",
                "no_drift": "#54A24B",
                "drift_benign": "#F58518",
                "drift_breaking": "#E45756",
            }
            xs = [ev["checked_at"] for ev in drift]
            # Dashed connector line
            fig.add_trace(go.Scatter(
                x=xs, y=[0] * len(xs),
                mode="lines", line=dict(dash="dash", color="#aaa", width=1.5),
                hoverinfo="skip", showlegend=False,
            ))
            # Markers with concise labels + rich hover
            marker_colors, marker_sizes, text_labels, hover_texts = [], [], [], []
            for ev in drift:
                cls = st_colors.get(ev["status"], "#999")
                marker_colors.append(cls)
                marker_sizes.append(16)
                if ev["status"] == "baseline_set":
                    text_labels.append(f"{len(ev['live_schema'])} cols")
                elif ev["status"] == "no_drift":
                    text_labels.append("✓")
                elif ev["status"] == "drift_benign":
                    names = [c["column"] for c in ev.get("benign_changes", [])]
                    text_labels.append(f"+{', '.join(names)}" if names else "benign")
                elif ev["status"] == "drift_breaking":
                    names = [c["column"] for c in ev.get("breaking_changes", [])]
                    text_labels.append(f"-{', '.join(names)}" if names else "breaking")
                else:
                    text_labels.append(ev["status"])

                h = f"<b>{ev['status']}</b><br>{ev['checked_at'][:19]}<br>"
                h += "<b>Schema:</b> " + ", ".join(
                    f"{k}: {v}" for k, v in ev.get("live_schema", {}).items()
                ) + "<br>"
                for c in ev.get("breaking_changes", []):
                    h += f"<span style='color:#E45756'>BREAKING:</span> {c['column']} dropped (was {c.get('baseline_type', '?')})<br>"
                for c in ev.get("benign_changes", []):
                    h += f"<span style='color:#F58518'>benign:</span> {c['column']} added ({c.get('live_type', '')})<br>"
                if ev.get("patch_id"):
                    h += f"<b>patch:</b> {ev['patch_id']}<br>"
                hover_texts.append(h)

            fig.add_trace(go.Scatter(
                x=xs, y=[0] * len(xs),
                mode="markers+text",
                marker=dict(size=marker_sizes, color=marker_colors,
                            line=dict(width=1.5, color="white")),
                text=text_labels, textposition="top center",
                hovertext=hover_texts, hoverinfo="text",
                showlegend=False,
            ))
            fig.update_layout(
                height=200, showlegend=False,
                margin=dict(l=8, r=8, t=8, b=8),
                yaxis=dict(visible=False, range=[-1.5, 1.5]),
                xaxis=dict(title=""),
            )
            st.plotly_chart(fig, width="stretch")

            # Detail table
            _table([
                {
                    "time": ev["checked_at"][:19].replace("T", " "),
                    "status": ev["status"],
                    "live_schema": ", ".join(f"{k}: {v}" for k, v in ev.get("live_schema", {}).items()),
                    "breaking": "; ".join(
                        f"{c['column']} ({c['kind']}{', was ' + c['baseline_type'] if c.get('baseline_type') else ''})"
                        for c in ev.get("breaking_changes", [])
                    ) or "\u2014",
                    "benign": "; ".join(
                        f"{c['column']} ({c['kind']}: {c.get('live_type', '')})"
                        for c in ev.get("benign_changes", [])
                    ) or "\u2014",
                    "patch": ev.get("patch_id") or "\u2014",
                }
                for ev in drift
            ], status_cols=("status",))


def _fmt_dur(ms: int) -> str:
    if ms < 1000:
        return f"{ms}ms"
    if ms < 60_000:
        return f"{ms / 1000:.1f}s"
    return f"{ms // 60_000}m {ms % 60_000 // 1000}s"


def _performance_tab(handles, cfg, store_dir):
    if not handles:
        st.info("No observability stores found yet.")
        return

    bp_to_handle = _blueprint_handle_map(handles)
    bps = sorted(bp_to_handle.keys())
    if not bps:
        st.info("No runs found.")
        return
    c_bp, c_mod = st.columns([1, 1])
    pick = c_bp.selectbox("Blueprint", bps, key="perf_bp")
    handle = bp_to_handle[pick]

    runs = q.list_runs(handle.store, limit=20, blueprint_id=pick)
    if not runs:
        st.info("No runs for this blueprint yet.")
        return

    rows = []
    for r in runs:
        det = q.run_detail(handle.store, r.run_id)
        if det is None or not det.profile:
            continue
        for p in det.profile:
            rows.append({
                "run": r.run_id[:8], "started": (r.started_at or "")[:19],
                "module": p.module_id,
                "duration_ms": p.duration_ms or 0,
                "records_read": p.records_read or 0,
                "records_written": p.records_written or 0,
                "bytes_read": p.bytes_read or 0,
                "bytes_written": p.bytes_written or 0,
            })

    if not rows:
        st.info("No module profile data yet.")
        return

    df = pd.DataFrame(rows)
    mods = sorted(df["module"].unique())
    sel = c_mod.selectbox("Module", mods, key="perf_mod")

    mdf = df[df["module"] == sel].sort_values("started").reset_index(drop=True)

    # ── 1. Cost vs Data ────────────────────────────────────────────────────
    st.markdown("**Cost vs Data**")
    if len(mdf) >= 2:
        first = mdf.iloc[0]
        last = mdf.iloc[-1]
        dur_growth = last["duration_ms"] / first["duration_ms"] if first["duration_ms"] else 1
        row_growth = last["records_read"] / first["records_read"] if first["records_read"] else 1
        if dur_growth >= 3 and row_growth < 1.5:
            st.warning(
                f"**{sel}**: duration ↑{dur_growth:.1f}× but rows ↑{row_growth:.1f}× "
                "— likely skew or a non-scaling query; inspect shuffles below.")

        fig = px.scatter(mdf, x="records_read", y="duration_ms",
                         hover_data={"run": True, "started": True},
                         title=f"{sel} — duration vs records read",
                         labels={"records_read": "records read",
                                 "duration_ms": "duration (ms)"},
                         height=300)
        fig.update_layout(margin=dict(l=8, r=8, t=32, b=8))
        st.plotly_chart(fig, width="stretch")
    else:
        st.caption("Need ≥2 runs with profile data for this module.")

    # ── 2. Throughput Trend ────────────────────────────────────────────────
    st.markdown("**Throughput Trend**")
    if len(mdf) >= 2:
        mdf["throughput"] = mdf["records_written"] / (mdf["duration_ms"] / 1000)
        mdf["throughput"] = mdf["throughput"].replace(
            [float("inf"), -float("inf")], 0).fillna(0)
        fig = px.line(mdf, x="run", y="throughput", markers=True,
                      title=f"{sel} — rows/sec over runs",
                      hover_data={"started": True, "throughput": ":.1f"})
        fig.update_layout(height=300, margin=dict(l=8, r=8, t=32, b=8))
        st.plotly_chart(fig, width="stretch")
    else:
        st.caption("Need ≥2 runs to show throughput trend.")

    # ── 3. Plan Complexity ─────────────────────────────────────────────────
    st.markdown("**Plan Complexity**")
    plans = q.plan_metrics(handle.store, pick, limit=200)
    plan_rows = [p for p in plans if p.module_id == sel]
    if plan_rows:
        pdf = pd.DataFrame([{
            "run": p.run_id[:8], "started": p.started_at[:19],
            "shuffles": p.exchange_count, "python UDFs": p.python_udf_count,
            "broadcasts": p.broadcast_count,
        } for p in reversed(plan_rows)])
        if len(plan_rows) >= 2:
            st.dataframe(pdf, width="stretch", hide_index=True)
            fig = px.line(pdf, x="run",
                          y=["shuffles", "python UDFs", "broadcasts"],
                          markers=True,
                          title=f"{sel} — plan metrics over runs",
                          hover_data={"started": True})
            _count_yaxis(fig, pdf[["shuffles", "python UDFs", "broadcasts"]].to_numpy().max())
            fig.update_layout(height=300, margin=dict(l=8, r=8, t=32, b=8))
            st.plotly_chart(fig, width="stretch")
        else:
            st.dataframe(pdf, width="stretch", hide_index=True)
            st.caption("Only one data point for plan complexity.")
    else:
        st.caption(
            "No plan-complexity data for this module "
            "(requires a real pipeline run — sandbox runs don't produce "
            "explain snapshots).")

    # ── 4. Actionable Module Table ─────────────────────────────────────────
    st.markdown("**Module Profile Summary**")
    summ = df.groupby("module").agg(
        runs=("run", "nunique"),
        avg_duration_ms=("duration_ms", "mean"),
        max_duration_ms=("duration_ms", "max"),          # slowest run — tail latency avg hides
        total_records_read=("records_read", "sum"),
        total_records_written=("records_written", "sum"),
    ).reset_index()
    summ = summ.sort_values("max_duration_ms", ascending=False)  # worst-case first
    if plans:
        plan_summ = pd.DataFrame([{
            "module": p.module_id, "shuffles": p.exchange_count,
            "python UDFs": p.python_udf_count, "broadcasts": p.broadcast_count,
        } for p in plans]).groupby("module").last().reset_index()
        summ = summ.merge(plan_summ, on="module", how="left").fillna(0)
        for c in ("shuffles", "python UDFs", "broadcasts"):
            summ[c] = summ[c].astype(int)
    _table(summ.to_dict("records"), status_cols=(),
           numeric_cols=_NUMERIC | {"avg_duration_ms", "max_duration_ms",
                                     "total_records_read",
                                     "total_records_written", "shuffles",
                                     "python UDFs", "broadcasts"},
           formats={"avg_duration_ms": "{:,.0f}", "max_duration_ms": "{:,.0f}",
                    "total_records_read": "{:,}",
                    "total_records_written": "{:,}"})

    # ── 5. Table Maintenance (OPTIMIZE / VACUUM cost) ──────────────────────
    # A write-side storage-hygiene cost, not a data-quality signal — lives here
    # with the other timing metrics. Empty unless an Egress runs `maintenance:`
    # on a lakehouse table (Delta/Iceberg/Hudi).
    st.markdown("**Table Maintenance**")
    maint = q.maintenance_metrics(cfg, store_dir=store_dir)
    if maint:
        _table([
            {"run": (m["run_id"] or "")[:8], "module": m["module_id"],
             "optimize_ms": m["optimize_ms"], "vacuum_ms": m["vacuum_ms"],
             "when": (m["captured_at"] or "")[:19]}
            for m in maint
        ], status_cols=(),
           numeric_cols=_NUMERIC | {"optimize_ms", "vacuum_ms"},
           formats={"optimize_ms": "{:,}", "vacuum_ms": "{:,}"})
    else:
        st.caption("No OPTIMIZE / VACUUM ops recorded — set `maintenance:` on a "
                   "Delta/Iceberg/Hudi Egress to compact & clean tables post-write.")


def _quality_tab(cfg, store_dir):
    st.markdown("**Assert Failures**")
    af = q.assert_failures(cfg, store_dir=store_dir)
    if af:
        af_df = pd.DataFrame([
            {"run": a.run_id[:8], "blueprint": a.blueprint_id,
             "rule": a.error_type, "error": a.error_message[:120]}
            for a in af
        ])
        _table(af_df.to_dict("records"), status_cols=())

        if len(af) >= 2:
            af_trend = pd.DataFrame([
                {"run": a.run_id[:8], "blueprint": a.blueprint_id,
                 "started": a.started_at[:19]}
                for a in reversed(af)
            ])
            af_agg = af_trend.groupby(["run", "blueprint"]).size().reset_index(name="failures")
            fig = px.line(af_agg, x="run", y="failures", color="blueprint",
                          markers=True, title="Assert Failures Per Run",
                          hover_data={"run": True, "blueprint": True, "failures": True})
            fig.update_traces(hovertemplate=
                "<b>%{x}</b><br>blueprint: %{customdata[0]}<br>failures: %{y}<extra></extra>")
            _count_yaxis(fig, af_agg["failures"].max())
            fig.update_layout(height=280, margin=dict(l=8, r=8, t=32, b=8))
            st.plotly_chart(fig, width="stretch")
    else:
        st.caption("No assert failures recorded yet.")

    st.markdown("**Spillway Volume**")
    qv = q.quarantine_volumes(cfg, store_dir=store_dir)
    qv_filt = [v for v in qv if any(k in v.module_id.lower()
                                     for k in ("quarantine", "spill", "reject", "bad", "error"))]
    if qv_filt:
        qv_rows = sorted(qv_filt, key=lambda v: v.started_at)
        qv_df = pd.DataFrame([
            {"run": v.run_id[:8], "blueprint": v.blueprint_id,
             "module": v.module_id, "spillway rows": v.records_written,
             "started": v.started_at[:19]}
            for v in qv_rows
        ])
        if len(qv_rows) >= 2:
            max_r = max(v.records_written for v in qv_rows)
            min_r = min(v.records_written for v in qv_rows)
            ratio = max_r / min_r if min_r else 1
            show_log = ratio > 100
            if show_log:
                log_on = st.checkbox("Log scale", key="spill_log")
            fig = px.bar(qv_df, x="run", y="spillway rows", color="blueprint",
                         title="Spillway Rows Per Run",
                         labels={"spillway rows": "rows"})
            fig.update_traces(hovertemplate=
                "<b>%{x}</b><br>%{y} rows<extra></extra>")
            if show_log and log_on:
                fig.update_yaxes(type="log")
            else:
                _count_yaxis(fig, max_r)
            fig.update_layout(height=280, margin=dict(l=8, r=8, t=32, b=8))
            st.plotly_chart(fig, width="stretch")
        else:
            st.metric("Spillway Rows", qv_rows[0].records_written)
        with st.expander("Detail"):
            st.dataframe(_style(qv_df), width="stretch", hide_index=True)
    else:
        st.caption("No spillway rows recorded yet.")

    st.markdown("**Probe Signals**")
    bp_to_handle = _blueprint_handle_map(
        q.discover_stores(cfg, store_dir=store_dir))
    bps = sorted(bp_to_handle.keys())
    if bps:
        c_bp, c_sig = st.columns([1, 1])
        pick = c_bp.selectbox("Blueprint", bps, key="quality_probe_bp")
        handle = bp_to_handle.get(pick)

        if not handle:
            st.caption("Store not found.")
            return

        available = q.probe_signal_types(handle.store, pick)
        type_options = [t for t in q.PROBE_METRIC_LABELS if t in available]
        if not type_options:
            st.caption("No probe signals for this blueprint.")
            return
        sig_type = c_sig.selectbox(
            "Signal type", type_options,
            format_func=lambda k: q.PROBE_METRIC_LABELS[k],
            key="quality_probe_sig")

        sigs = q.probe_signals(handle.store, pick, sig_type, limit=30)
        if not sigs:
            st.caption(f"No {q.PROBE_METRIC_LABELS[sig_type].lower()} signals for this blueprint.")
            return

        if sig_type == "null_rates":
            cols = sorted({c for s in sigs
                           for c in (s.payload.get("null_rates") or {})})
            if not cols:
                st.caption("No columns in null-rate payload.")
                return
            col = st.selectbox("Column", cols, key="quality_null_col")
            vals = [
                (s.run_id[:8], s.started_at[:19],
                 s.payload.get("null_rates", {}).get(col))
                for s in reversed(sigs)
            ]
            ndf = pd.DataFrame(vals, columns=["run", "started", f"null_rate_{col}"])
            st.dataframe(ndf, width="stretch", hide_index=True)
            if len(vals) >= 2:
                fig = px.line(ndf, x="run", y=f"null_rate_{col}", markers=True,
                              title=f"Null Rate — {col}",
                              hover_name="run",
                              hover_data={"started": True, f"null_rate_{col}": ":.4%"})
                fig.update_layout(height=260, margin=dict(l=8, r=8, t=32, b=8))
                st.plotly_chart(fig, width="stretch")

        elif sig_type == "value_distribution":
            cols = sorted({c for s in sigs
                           for c in (s.payload.get("stats") or {})})
            if not cols:
                st.caption("No columns in value-distribution payload.")
                return
            c_col, c_stat = st.columns([1, 1])
            col = c_col.selectbox("Column", cols, key="quality_vd_col")
            stat_key = c_stat.selectbox("Statistic", ["min", "max", "mean", "stddev"],
                                        key="quality_vd_stat")
            vals = []
            for s in reversed(sigs):
                st_ = (s.payload.get("stats") or {}).get(col, {})
                vals.append((s.run_id[:8], s.started_at[:19],
                             st_.get(stat_key)))
            df = pd.DataFrame(vals, columns=["run", "started", stat_key])
            st.dataframe(df, width="stretch", hide_index=True)
            if len(vals) >= 2:
                fig = px.line(df, x="run", y=stat_key, markers=True,
                              title=f"{stat_key} — {col}",
                              hover_name="run",
                              hover_data={"started": True, stat_key: ":.2f"})
                fig.update_layout(height=260, margin=dict(l=8, r=8, t=32, b=8))
                st.plotly_chart(fig, width="stretch")

        elif sig_type == "distinct_count":
            cols = sorted({c for s in sigs
                           for c in (s.payload.get("distinct_counts") or {})})
            if not cols:
                st.caption("No columns in distinct-count payload.")
                return
            rows_l = []
            for s in reversed(sigs):
                dc = s.payload.get("distinct_counts") or {}
                rows_l.append({"run": s.run_id[:8], "started": s.started_at[:19],
                               **dc})
            df = pd.DataFrame(rows_l)
            st.dataframe(_style(df), width="stretch", hide_index=True)
            if len(rows_l) >= 2:
                # Columns can differ by orders of magnitude (a flag with 2
                # distinct values vs an id with millions) — offer a log axis so
                # the small series don't flatten to the baseline.
                _nums = [v for r in rows_l for c in cols
                         if isinstance((v := r.get(c)), (int, float)) and v > 0]
                log_on = False
                if _nums and max(_nums) / min(_nums) > 100:
                    log_on = st.checkbox("Log scale", key="quality_dc_log")
                fig = px.line(df, x="run", y=cols, markers=True,
                              title="Distinct count per column",
                              hover_name="run",
                              hover_data={"started": True})
                if log_on:
                    fig.update_yaxes(type="log")
                else:
                    _count_yaxis(fig, 0)
                fig.update_layout(height=260, margin=dict(l=8, r=8, t=32, b=8))
                st.plotly_chart(fig, width="stretch")

        elif sig_type == "schema_snapshot":
            prev: dict[str, str] = {}
            for s in sigs:
                fields = {f["name"]: f["type"]
                          for f in (s.payload.get("fields") or [])}
                changed = {c: (prev[c], fields[c]) for c in fields  # noqa: F841  # TODO: surface schema-snapshot diff (incomplete feature)
                           if c in prev and prev[c] != fields[c]}
                prev = fields
            rows_l = []
            for s in reversed(sigs):
                fields = s.payload.get("fields") or []
                rows_l.append({"run": s.run_id[:8], "started": s.started_at[:19],
                               **{f["name"]: f["type"] for f in fields}})
            df = pd.DataFrame(rows_l)
            st.dataframe(df, width="stretch", hide_index=True)
    else:
        st.caption("No blueprints found.")


def _heal_tab(cfg, store_dir):
    hc = q.heal_coverage(cfg, store_dir=store_dir)
    detail = q.heal_attempt_details(cfg, store_dir=store_dir)
    if not detail and not hc:
        st.info("No healing data yet. Run a blueprint that fails with `agent:` configured.")
        return

    subtabs = st.tabs(["Overview", "Patches"])
    with subtabs[0]:
        _heal_overview(cfg, store_dir, hc, detail)
    with subtabs[1]:
        _patches_subtab(cfg, store_dir)


def _heal_overview(cfg, store_dir, hc, detail):
    gates = q.gate_rejection_rates(cfg, store_dir=store_dir)

    # ── KPI row — heal pipeline → patch outcomes ────────────────────────
    total = sum(hc.values())
    zero_token = hc.get("cached", 0) + hc.get("replayed", 0)
    pl = q.patch_lifecycle_counts(cfg, store_dir=store_dir)
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Heal Events", total)
    c2.metric("Zero-Token %", f"{zero_token / total * 100:.0f}%" if total else "\u2014")
    c3.metric("Cache Hits", hc.get("cached", 0))
    c4.metric("Pending Patches", pl.get("pending", 0))
    c5.metric("Applied Patches", pl.get("applied", 0))
    c6.metric("Rejected Patches", pl.get("rejected", 0))

    # ── Charts row ───────────────────────────────────────────────────────
    left, right = st.columns(2)
    with left:
        st.caption("Resolution Distribution")
        if hc:
            fig = px.pie(
                values=list(hc.values()), names=list(hc.keys()),
                hole=0.4,
            )
            fig.update_layout(height=300, margin=dict(l=8, r=8, t=8, b=8))
            st.plotly_chart(fig, width="stretch")

    with right:
        st.caption("Gate Rejections")
        if gates:
            fig = px.bar(
                x=list(gates.keys()), y=list(gates.values()),
                labels={"x": "", "y": "count"},
            )
            _count_yaxis(fig, max(gates.values(), default=0))
            fig.update_xaxes(tickangle=45, title=None)
            fig.update_layout(height=300, margin=dict(l=8, r=8, t=8, b=8))
            st.plotly_chart(fig, width="stretch")

    # ── Success by failure category ──────────────────────────────────────
    categories: dict[str, dict[str, int]] = {}
    for d in detail:
        cat = d["failure_category"] or "unknown"
        c = categories.setdefault(cat, {"total": 0, "success": 0})
        c["total"] += 1
        if d["run_success_after_patch"]:
            c["success"] += 1
    if categories:
        st.caption("Heal Success Rate By Failure Category")
        cat_df = pd.DataFrame([
            {"category": cat, "success rate": v["success"] / v["total"] * 100,
             "count": v["total"]}
            for cat, v in categories.items()
        ])
        fig = px.bar(cat_df, x="category", y="success rate", text="count",
                     labels={"category": "", "success rate": "%"})
        fig.update_xaxes(tickangle=45, title=None)
        fig.update_layout(height=300, margin=dict(l=8, r=8, t=8, b=8))
        st.plotly_chart(fig, width="stretch")

    # ── Attempt-waterfall table ──────────────────────────────────────────
    if detail:
        st.caption(f"Recent heal attempts ({len(detail)} rows)")
        _table([
            {"run": d["run_id"][:12], "attempt": d["attempt_num"],
             "latency": f"{d['latency_ms'] // 1000}s" if d.get("latency_ms") else "\u2014",
             "gate": d["gate_that_rejected"] or "\u2014",
             "stop": d["stop_reason"] or "\u2014", "tokens": (d["tokens_in"] or 0) + (d["tokens_out"] or 0),
             "category": d.get("failure_category") or "\u2014",
             "resolution": d.get("resolution") or "\u2014",
             "patched": "\u2713" if d.get("patch_applied") else "\u2014",
             "success": "\u2713" if d.get("run_success_after_patch") else "\u2014"}
            for d in detail[:50]
        ], status_cols=("resolution",),
           formats={"tokens": "{:,}"})


def _render_op(op: dict) -> str:
    """Render a patch operation dict as a readable one-liner."""
    t = op.get("op") or op.get("type") or "unknown"
    mid = op.get("module_id") or op.get("target") or ""
    if t == "set_module_config_key":
        return f"set config  {mid}.{op.get('key', '')} = \"{op.get('value', '')}\""
    if t == "replace_module_config":
        return f"replace config  {mid}"
    if t == "replace_module_label":
        return f"rename  {mid}  \u2192 \"{op.get('label', '')}\""
    if t == "insert_module":
        return f"insert {op.get('type', 'module')} \"{op.get('label', '')}\""
    if t == "remove_module":
        return f"remove module  {mid}"
    if t == "replace_context_value":
        return f"set context  {op.get('key', '')} = \"{op.get('value', '')}\""
    if t == "add_probe":
        return f"attach probe  {mid}  on {op.get('attach_to', '')}"
    if t == "replace_edge":
        return f"rewire  {op.get('from', '')} \u2192 {op.get('to', '')}"
    if t == "set_module_on_failure":
        return f"set on_failure  {mid}"
    if t == "replace_retry_policy":
        return "replace retry policy"
    if t == "add_arcade_ref":
        return f"add arcade ref  {op.get('ref', '')}"
    if t == "defer_to_human":
        return "\u26a0 cannot auto-fix \u2014 needs human intervention"
    if t == "replace_macro":
        return f"replace macro  {op.get('name', '')}"
    if t == "set_spark_config":
        return f"set spark config  {op.get('key', '')} = \"{op.get('value', '')}\""
    return f"{t}  {mid}  ({json.dumps({k: v for k, v in op.items() if k not in ('op', 'type', 'module_id', 'target')})})"


def _find_blueprint_file(path_str: str | None, bp_id: str) -> Path | None:
    """Locate the blueprint YAML on disk (explicit path, then blueprints/ globs)."""
    if path_str and Path(path_str).is_file():
        return Path(path_str)
    for base in (Path.cwd() / "blueprints", Path("blueprints")):
        if not base.is_dir():
            continue
        exact = base / f"{bp_id}.yml"
        if exact.is_file():
            return exact
        matches = sorted(base.glob(f"*{bp_id}.yml"))
        if matches:
            return matches[0]
    return None


def _render_patch_diff(picked, patch_data) -> None:
    """Before/after YAML diff via the REAL engine path — identical machinery to
    `aqueduct patch preview` / `patch apply` (PatchSpec.model_validate →
    apply_patch_to_dict → render_unified_diff). No bespoke op-apply: the diff is
    exactly what approving the patch produces.
    """
    if not patch_data or not (patch_data.get("operations") or []):
        return
    is_pending = picked.status == "pending"
    label = ("Before / After Diff  ·  simulated preview" if is_pending
             else "Before / After Diff  ·  what these operations would change")
    with st.expander(label, expanded=is_pending):
        bp_path = _find_blueprint_file(patch_data.get("blueprint_path"), picked.blueprint_id)
        if bp_path is None:
            st.caption("Blueprint file not found on disk — cannot render diff.")
            return
        if not is_pending:
            st.caption(
                "Already applied — the change is in the blueprint on disk. Showing what "
                "these operations would change against the *current* file (empty = already "
                "present). Use `git diff` / `aqueduct rollback` for the historical view.")
        try:
            from aqueduct.patch.apply import _yaml_load, apply_patch_to_dict
            from aqueduct.patch.grammar import PatchSpec
            from aqueduct.patch.preview import render_unified_diff

            spec = PatchSpec.model_validate(patch_data)
            before = _yaml_load(bp_path)            # apply_patch_to_dict copies; before unmutated
            after = apply_patch_to_dict(before, spec)
            diff_text = render_unified_diff(before, after)
            if diff_text.strip():
                st.code(diff_text, language="diff")
            else:
                st.caption("No changes — operations already reflected in the blueprint.")
        except Exception as exc:  # noqa: BLE001 — degrade to the operations list below
            st.caption(f"Diff unavailable ({exc}). See Operations below.")


def _patches_subtab(cfg, store_dir):
    patches = q.patch_list(cfg, store_dir=store_dir)
    if not patches:
        st.info("No patches have been generated yet. Run a blueprint with `agent:` configured and a deliberate failure.")
        return

    sel = st.dataframe(
        pd.DataFrame([
            {
                "status": p.status,
                "patch_id": p.patch_id,
                "blueprint": p.blueprint_id,
                "error": p.error_class or "\u2014",
                "module": p.where_field or "\u2014",
                "source": p.source or "\u2014",
                "ops": ", ".join(p.ops),
                "created": p.created_at[:19] if p.created_at else "\u2014",
            }
            for p in patches
        ]),
        width="stretch",
        column_config={
            "status": st.column_config.TextColumn("Status", width="small"),
            "patch_id": st.column_config.TextColumn("Patch ID", width="medium"),
            "blueprint": st.column_config.TextColumn("Blueprint", width="medium"),
            "error": st.column_config.TextColumn("Error", width="medium"),
            "module": st.column_config.TextColumn("Module", width="small"),
            "source": st.column_config.TextColumn("Source", width="small"),
            "ops": st.column_config.TextColumn("Operations", width="medium"),
        },
        on_select="rerun",
        selection_mode="single-row",
        key="patch_selector",
    )

    if not sel["selection"]["rows"]:
        st.caption("Select a patch to see its detail.")
        return

    idx = sel["selection"]["rows"][0]
    picked = patches[idx]
    patch_data = q.load_patch_file(picked.patch_id)

    # ── Header ───────────────────────────────────────────────────────────
    st.divider()
    _badge = {"pending": "#9a6700", "applied": "#1a7f37",
              "rejected": "#cf222e"}.get(picked.status, "#57606a")
    st.markdown(
        f"#### `{picked.patch_id}` "
        f"<span style='background:{_badge};color:white;padding:1px 8px;border-radius:4px;"
        f"font-size:0.55em;vertical-align:middle;text-transform:uppercase;"
        f"letter-spacing:0.5px'>{picked.status}</span>",
        unsafe_allow_html=True,
    )
    # Identity attributes are text, not numbers \u2014 markdown, not metric tiles.
    st.markdown(
        f"**Blueprint** `{picked.blueprint_id}`  \u00b7  "
        f"**Module** `{picked.where_field or _md}`  \u00b7  "
        f"**Error** `{picked.error_class or _md}`  \u00b7  "
        f"**Source** {picked.source or _md}"
    )
    conf = patch_data.get("confidence") if patch_data else None
    _meta = (f"Created {picked.created_at[:19] if picked.created_at else _md} "
             f"\u00b7 prompt v{picked.prompt_version or _md}")
    if conf is not None:
        _meta += f" \u00b7 confidence {conf:.0%}"
    st.caption(_meta)

    # ── Rationale & Root Cause ───────────────────────────────────────────
    with st.expander("Rationale & Root Cause", expanded=True):
        if picked.rationale:
            st.markdown(picked.rationale)
        else:
            st.caption("No rationale recorded.")
        if patch_data and patch_data.get("root_cause"):
            st.markdown("---")
            st.markdown("**Root cause**")
            st.markdown(patch_data["root_cause"])

    # ── Generation Metrics ───────────────────────────────────────────────
    ho_rows = q.heal_attempt_details(cfg, store_dir=store_dir, limit=500)
    gen = [d for d in ho_rows if d.get("patch_id") == picked.patch_id]
    # Prefer llm resolution (has real model/metrics) over replayed
    gen.sort(key=lambda d: 0 if d.get("resolution") == "llm" else 1)
    if gen:
        g = gen[0]
        metrics = []
        if g.get("model"):
            metrics.append(("Model", g["model"]))
        if (g.get("tokens_in") or 0) + (g.get("tokens_out") or 0) > 0:
            metrics.append(("Tokens", f"{g.get('tokens_in') or 0:,} in / {g.get('tokens_out') or 0:,} out"))
        if g.get("latency_ms"):
            metrics.append(("Latency", f"{g['latency_ms'] // 1000}s"))
        if g.get("stop_reason"):
            metrics.append(("Stop reason", g["stop_reason"]))
        if g.get("resolution"):
            metrics.append(("Resolution", g["resolution"]))
        if metrics:
            st.caption("Generation Metrics")
            _table([{"metric": k, "value": v} for k, v in metrics], status_cols=())

    # ── Operations ───────────────────────────────────────────────────────
    ops_list = (patch_data or {}).get("operations", []) or []
    if ops_list:
        st.caption("Operations")
        ops_text = "\n".join(f"  {i+1}. {_render_op(op)}" for i, op in enumerate(ops_list))
        st.code(ops_text, language="text")
    elif picked.ops:
        st.caption("Operations")
        ops_text = "\n".join(f"  {i+1}. {op}" for i, op in enumerate(picked.ops))
        st.code(ops_text, language="text")

    # ── Gate Validation ──────────────────────────────────────────────────
    sim = q.patch_simulation_for_patch(cfg, picked.patch_id, store_dir=store_dir)
    if sim:
        st.caption("Gate Validation Results")
        _table([
            {"gate": s.gate, "status": s.status,
             "detail": s.detail or "",
             "duration_ms": f"{s.duration_ms}ms" if s.duration_ms else "\u2014"}
            for s in sim
        ], status_cols=("status",))

    # ── Before / After Diff ──────────────────────────────────────────────
    _render_patch_diff(picked, patch_data)


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


_ASSETS = Path(__file__).parent / "assets"
_TAB_ICON = _ASSETS / "favicon.svg"        # browser tab favicon
_SIDEBAR_LOGO = _ASSETS / "logo.svg"    # sidebar wordmark
_HEADER_LOGO = _ASSETS / "header.svg"     # main content area header


def main() -> None:
    st.set_page_config(
        page_title="Aqueduct Dashboard",
        page_icon=str(_TAB_ICON) if _TAB_ICON.is_file() else "\U0001f30a",
        layout="wide",
    )
    if _SIDEBAR_LOGO.is_file():
        st.logo(str(_SIDEBAR_LOGO), size="large")
        st.markdown(
            "<style>[data-testid='stSidebarHeader'] img{transform:scale(1.5);"
            "transform-origin:top left;margin-bottom:16px;}</style>",
            unsafe_allow_html=True,
        )
    config_path = os.environ.get("AQ_DASH_CONFIG") or None
    store_dir = os.environ.get("AQ_DASH_STORE_DIR") or None
    try:
        from aqueduct.cli import _load_config_with_env
        cfg = _load_config_with_env(Path(config_path) if config_path else None, quiet=True)
    except Exception as exc:  # noqa: BLE001
        st.error(f"config error: {exc}")
        return

    backend = getattr(cfg.stores.observability, "backend", "duckdb")
    if _HEADER_LOGO.is_file():
        st.image(str(_HEADER_LOGO), width=315)
    else:
        st.title("Aqueduct")
    st.caption(f"observability backend: **{backend}** · read-only viewer · F5 to refresh")
    handles = q.discover_stores(cfg, store_dir=store_dir)

    fleet, runs, quality, lineage, healing, performance, doctor, config = st.tabs(
        ["Fleet", "Runs", "Quality", "Lineage", "Healing", "Performance", "Doctor", "Config"])
    with fleet:
        _fleet_tab(cfg, store_dir)
    with runs:
        _runs_tab(handles)
    with quality:
        _quality_tab(cfg, store_dir)
    with lineage:
        _lineage_tab(handles)
    with healing:
        _heal_tab(cfg, store_dir)
    with performance:
        _performance_tab(handles, cfg, store_dir)
    with doctor:
        _doctor_tab(config_path)
    with config:
        _config_tab(cfg)


# streamlit runs the target script with __name__ == "__main__".
if __name__ == "__main__":
    main()
