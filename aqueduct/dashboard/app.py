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
from datetime import datetime
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

from aqueduct.config import load_config
from aqueduct.stores import queries as q

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
                           "props": [(f"min-width", w), ("white-space", "nowrap")]})
        else:
            styles.append({"selector": f"td.col{c}",
                           "props": [("white-space", "nowrap")]})
    if styles:
        sty = sty.set_table_styles(styles, overwrite=False)

    # formatted display (%, commas, etc.)
    if formats:
        fmtd = {c: f for c, f in formats.items() if c in df.columns}
        if fmtd:
            sty = sty.format(fmtd)

    return sty


def _table(rows: list[dict], status_cols=("status",),
           numeric_cols: frozenset[str] | None = None,
           col_width: dict[str, str] | None = None,
           formats: dict[str, str] | None = None):
    """Static, full-length table with colored status cells (no chrome / no nudge)."""
    if not rows:
        st.caption("— nothing here —")
        return
    st.table(_style(pd.DataFrame(rows), status_cols, numeric_cols, col_width, formats))


# ── Tabs ─────────────────────────────────────────────────────────────────────

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
    c3.metric("Success rate", f"{(total_succ / total_runs * 100) if total_runs else 0:.0f}%")
    c4.metric("Heal attempts", sum(s.heal_attempts for s in summ))
    c5.metric("Heal coverage \u2014 zero-token", cov_pct)

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
            fig.update_layout(legend=dict(orientation="h", y=1.02, x=0), height=400, margin=dict(l=8, r=8, t=8, b=8))
            st.plotly_chart(fig, width="stretch")

    if has_dist and rc is not None:
        with rc:
            st.caption("Failure Categories")
            fig = px.bar(
                x=list(dist.keys()), y=list(dist.values()),
                labels={"x": "", "y": "count"},
            )
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
    event = st.dataframe(
        _style(df), width="stretch", hide_index=True,
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
                    with st.expander("stack trace"):
                        st.code(fc.stack_trace, language="text")
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
                    fig.update_layout(height=240, margin=dict(l=8, r=8, t=32, b=8))
                    st.plotly_chart(fig, width="stretch")
            elif sig_type == "schema_snapshot":
                prev: dict[str, str] = {}
                for s in sigs:
                    fields = {f["name"]: f["type"] for f in (s.payload.get("fields") or [])}
                    changed = {c: (prev[c], fields[c]) for c in fields
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
    # Pick a blueprint from all stores that have lineage OR fingerprint data.
    scored = []
    for h in handles:
        has_col = q.lineage(h.store, limit=1) != []
        has_fp = q.channel_fingerprints(h.store, h.label) != []
        if has_col or has_fp:
            scored.append((h, has_col, has_fp))
    if not scored:
        st.info("No column-lineage or SQL fingerprint data yet (needs a SQL Channel).")
        return
    labels = [f"{h.label}  [col-lineage]" if c and not f else
              f"{h.label}  [sql-changelog]" if f and not c else
              f"{h.label}  [col-lineage + sql-changelog]" if c and f else
              h.label
              for h, c, f in scored]
    pick = st.selectbox("Blueprint", labels)
    handle, has_col, has_fp = scored[labels.index(pick)]

    import plotly.graph_objects as go

    if has_col:
        # Only the latest run's lineage — each run appends its own set.
        latest = q.list_runs(handle.store, blueprint_id=handle.label, limit=1)
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
        fps = q.channel_fingerprints(handle.store, handle.label)
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
    drift = q.drift_events(handle.store, handle.label)
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


def _performance_tab(handles):
    rows = []
    for h in handles:
        try:
            latest = q.list_runs(h.store, limit=5)
        except Exception:
            continue
        for r in latest:
            det = q.run_detail(h.store, r.run_id)
            if det is None or not det.profile:
                continue
            for p in det.profile:
                rows.append({
                    "blueprint": h.label, "run": r.run_id[:8],
                    "module": p.module_id,
                    "duration_ms": p.duration_ms or 0,
                    "records_read": p.records_read or 0,
                    "bytes_read": p.bytes_read or 0,
                    "records_written": p.records_written or 0,
                    "bytes_written": p.bytes_written or 0,
                })
    if not rows:
        st.info("No module profile data yet.")
        return

    df = pd.DataFrame(rows)
    total = len(df)
    avg_dur = int(df["duration_ms"].mean())
    total_bytes = int(df["bytes_read"].sum())
    slowest_row = df.loc[df["duration_ms"].idxmax()]
    slowest_label = f"{slowest_row['blueprint']} / {slowest_row['module']}"

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Modules profiled", f"{total:,}")
    c2.metric("Avg duration", f"{avg_dur:,} ms")
    c3.metric("Total bytes read", f"{total_bytes:,}")
    c4.metric("Slowest module", slowest_label)

    top_n = df.nlargest(20, "duration_ms")
    fig_bar = px.bar(top_n, x="duration_ms", y="module", color="blueprint",
                     orientation="h", title="Slowest Modules (top 20 by duration)",
                     labels={"duration_ms": "ms", "module": ""},
                     height=400)
    fig_bar.update_layout(margin=dict(l=8, r=8, t=32, b=8))
    st.plotly_chart(fig_bar, width="stretch")

    has_io = df["bytes_read"].sum() > 0
    if has_io and len(df) >= 3:
        fig_scatter = px.scatter(
            df, x="bytes_read", y="duration_ms", color="blueprint",
            hover_data=["module", "run"],
            title="Duration vs. Bytes Read",
            labels={"bytes_read": "bytes read", "duration_ms": "duration (ms)"},
            height=360)
        fig_scatter.update_layout(margin=dict(l=8, r=8, t=32, b=8))
        st.plotly_chart(fig_scatter, width="stretch")

    st.caption("Recent module profiles (latest 5 runs per blueprint)")
    st.dataframe(_style(df), width="stretch", hide_index=True)


def _heal_tab(cfg, store_dir):
    hc = q.heal_coverage(cfg, store_dir=store_dir)
    gates = q.gate_rejection_rates(cfg, store_dir=store_dir)
    detail = q.heal_attempt_details(cfg, store_dir=store_dir)
    if not detail and not hc:
        st.info("No healing data yet. Run a blueprint that fails with `agent:` configured.")
        return

    # ── KPI row — heal pipeline → patch outcomes ────────────────────────
    total = sum(hc.values())
    zero_token = hc.get("cached", 0) + hc.get("replayed", 0)
    pl = q.patch_lifecycle_counts(cfg, store_dir=store_dir)
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Heal events", total)
    c2.metric("Zero-token", f"{zero_token / total * 100:.0f}%" if total else "\u2014")
    c3.metric("Cache hit", hc.get("cached", 0))
    c4.metric("Pending patches", pl.get("pending", 0))
    c5.metric("Applied patches", pl.get("applied", 0))
    c6.metric("Rejected patches", pl.get("rejected", 0))

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

    fleet, runs, lineage, healing, performance, doctor, config = st.tabs(
        ["Fleet", "Runs", "Lineage", "Healing", "Performance", "Doctor", "Config"])
    with fleet:
        _fleet_tab(cfg, store_dir)
    with runs:
        _runs_tab(handles)
    with lineage:
        _lineage_tab(handles)
    with healing:
        _heal_tab(cfg, store_dir)
    with performance:
        _performance_tab(handles)
    with doctor:
        _doctor_tab(config_path)
    with config:
        _config_tab(cfg)


# streamlit runs the target script with __name__ == "__main__".
if __name__ == "__main__":
    main()
