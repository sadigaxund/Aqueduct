"""Streamlit observability dashboard — the script run by `aqueduct dashboard`.

Runs ONLY under `streamlit run` (never imported by the base package): it imports
`streamlit` + `plotly` at module top, which live in the optional `dashboard`
extra. The `aqueduct dashboard` CLI checks the extra is installed before
launching, and passes the config path / store dir through the environment
(`AQ_DASH_CONFIG`, `AQ_DASH_STORE_DIR`).

Read-only by design. No `st.cache_data` on the queries — every rerun (incl. the
Refresh button) re-reads with short-lived connections so the viewer never holds a
DuckDB handle that would block a running pipeline's writer.
"""

from __future__ import annotations

import os
from pathlib import Path

import streamlit as st

from aqueduct.config import load_config
from aqueduct.stores import queries as q

# status → (emoji circle for table cells, st.badge colour)
_STATUS = {
    "success": ("🟢", "green"),
    "ok": ("🟢", "green"),
    "error": ("🔴", "red"),
    "fail": ("🔴", "red"),
    "warn": ("🟡", "orange"),
    "skip": ("⚪", "gray"),
    "skipped": ("⚪", "gray"),
}


def _dot(status: str) -> str:
    return _STATUS.get(status, ("⚫", "gray"))[0]


def _full_df(rows: list[dict], **kw) -> None:
    """A dataframe sized to its content (no inner scroll / no empty gap → page scrolls)."""
    height = 38 + 36 * max(len(rows), 1)
    st.dataframe(rows, use_container_width=True, hide_index=True, height=min(height, 1600), **kw)


# ── Tabs ─────────────────────────────────────────────────────────────────────

def _fleet_tab(cfg, store_dir):
    summ = q.fleet_summary(cfg, store_dir=store_dir)
    if not summ:
        st.info("No observability stores found yet. Run a blueprint first.")
        return

    total_runs = sum(s.runs for s in summ)
    total_succ = sum(s.successes for s in summ)
    total_heal = sum(s.heal_attempts for s in summ)
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Blueprints", len(summ))
    c2.metric("Runs", total_runs)
    c3.metric("Success rate", f"{(total_succ / total_runs * 100) if total_runs else 0:.0f}%")
    c4.metric("Heal attempts", total_heal)

    _full_df([
        {
            "blueprint": s.blueprint_id,
            "runs": s.runs,
            "success %": round(s.success_rate * 100, 1),
            "errors": s.errors,
            "heal attempts": s.heal_attempts,
            "last run": s.last_run or "",
        }
        for s in summ
    ])

    rot = q.runs_over_time(cfg, store_dir=store_dir)
    if rot:
        days = sorted({d.day for d in rot})
        statuses = sorted({d.status for d in rot})
        grid = {st_: {d: 0 for d in days} for st_ in statuses}
        for d in rot:
            grid[d.status][d.day] = d.count
        st.caption("Runs over time")
        st.line_chart({st_: [grid[st_][d] for d in days] for st_ in statuses})

    dist = q.failure_categories(cfg, store_dir=store_dir)
    if dist:
        st.caption("Failure categories")
        st.bar_chart(dist)


def _runs_tab(handle):
    if handle is None:
        st.info("No observability stores found yet.")
        return
    runs = q.list_runs(handle.store, limit=200)
    if not runs:
        st.info("No runs in this store yet.")
        return

    st.caption("Select a run to inspect")
    rows = [
        {"status": _dot(r.status), "run_id": r.run_id, "blueprint": r.blueprint_id, "started": r.started_at or ""}
        for r in runs
    ]
    height = min(38 + 36 * max(len(rows), 1), 700)
    event = st.dataframe(
        rows, use_container_width=True, hide_index=True, height=height,
        on_select="rerun", selection_mode="single-row", key="runs_table",
    )
    sel = event.selection.rows if event and event.selection else []
    run = runs[sel[0]] if sel else runs[0]

    st.markdown(f"**Run** `{run.run_id}` · {run.blueprint_id}")
    st.badge(run.status, color=_STATUS.get(run.status, ("", "gray"))[1])

    det = q.run_detail(handle.store, run.run_id)
    if det is None:
        return
    prof = {p.module_id: p for p in det.profile}
    st.caption("Modules (execution order)")
    _full_df([
        {
            "status": _dot(m.status),
            "module": m.module_id,
            "duration_ms": getattr(prof.get(m.module_id), "duration_ms", None),
            "rows_out": getattr(prof.get(m.module_id), "records_written", None),
            "bytes_out": getattr(prof.get(m.module_id), "bytes_written", None),
            "error": (m.error or "")[:200],
        }
        for m in det.modules
    ])


def _lineage_tab(handle):
    if handle is None:
        st.info("No observability stores found yet.")
        return
    rows = q.lineage(handle.store)
    if not rows:
        st.info("No column-lineage rows captured for this store (needs a SQL Channel).")
        return

    import plotly.graph_objects as go

    src_labels: dict[str, int] = {}
    dst_labels: dict[str, int] = {}
    labels: list[str] = []
    colors: list[str] = []

    def node(name: str, is_src: bool) -> int:
        cache = src_labels if is_src else dst_labels
        if name not in cache:
            cache[name] = len(labels)
            labels.append(name)
            colors.append("#4C78A8" if is_src else "#54A24B")  # source=blue, output=green
        return cache[name]

    s_idx, t_idx = [], []
    for r in rows:
        s = f"{r.source_table}.{r.source_column}" if r.source_table else r.source_column
        s_idx.append(node(s, True))
        t_idx.append(node(r.output_column, False))

    fig = go.Figure(
        go.Sankey(
            arrangement="snap",
            node=dict(
                label=labels, color=colors, pad=18, thickness=16,
                line=dict(color="rgba(0,0,0,0)", width=0),
                hovertemplate="%{label}<extra></extra>",
            ),
            link=dict(
                source=s_idx, target=t_idx, value=[1] * len(s_idx),
                color="rgba(120,120,120,0.35)",
                hovertemplate="%{source.label} → %{target.label}<extra></extra>",
            ),
        )
    )
    n = max(len(src_labels), len(dst_labels))
    fig.update_layout(
        height=min(max(260, 26 * n), 800),
        margin=dict(l=10, r=10, t=10, b=10),
        font=dict(size=13, color="#222"),
        paper_bgcolor="rgba(0,0,0,0)",
    )
    st.plotly_chart(fig, use_container_width=True)

    st.caption("Lineage rows")
    _full_df([
        {"channel": r.channel_id, "output": r.output_column,
         "source_table": r.source_table, "source_column": r.source_column}
        for r in rows
    ])


def _doctor_tab(config_path):
    cols = st.columns([6, 1])
    cols[0].caption("Health checks (Spark probe skipped — read-only).")
    rerun = cols[1].button("Rerun", use_container_width=True)
    # Auto-run on first open; rerun on button.
    if "doctor_ran" not in st.session_state or rerun:
        st.session_state["doctor_ran"] = True
    try:
        from aqueduct.doctor import run_doctor

        results = run_doctor(
            config_path=Path(config_path) if config_path else None, skip_spark=True
        )
    except Exception as exc:  # noqa: BLE001 — surface to the read-only viewer
        st.error(f"doctor failed: {exc}")
        return
    for r in results:
        emoji, color = _STATUS.get(r.status, ("⚫", "gray"))
        b, name, detail = st.columns([1, 2, 7])
        b.badge(r.status, color=color)
        name.markdown(f"**{r.name}**")
        detail.markdown(r.detail or "")


def _config_tab(cfg):
    rows: list[tuple[str, str]] = []
    dep = getattr(cfg, "deployment", None)
    if dep is not None:
        for f in ("engine", "target", "master_url", "env"):
            if hasattr(dep, f):
                rows.append((f"deployment.{f}", str(getattr(dep, f))))
    stores = getattr(cfg, "stores", None)
    if stores is not None:
        for sname in ("observability", "depot", "blob", "benchmark"):
            s = getattr(stores, sname, None)
            b = getattr(s, "backend", None) if s is not None else None
            if b is not None:
                rows.append((f"stores.{sname}.backend", str(b)))
    agent = getattr(cfg, "agent", None)
    if agent is not None:
        for f in ("provider", "model"):
            if getattr(agent, f, None):
                rows.append((f"agent.{f}", str(getattr(agent, f))))
    _full_df([{"key": k, "value": v} for k, v in rows])
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

    # Header: title + right-aligned refresh (no emoji, no lonely row).
    left, right = st.columns([8, 1])
    left.title("Aqueduct")
    if right.button("Refresh", use_container_width=True):
        st.rerun()

    # Global store/blueprint picker (shared by Runs + Lineage).
    backend = getattr(cfg.stores.observability, "backend", "duckdb")
    handles = q.discover_stores(cfg, store_dir=store_dir)
    with st.sidebar:
        st.caption(f"observability backend: **{backend}**")
        handle = None
        if handles:
            labels = [h.label for h in handles]
            pick = st.selectbox("Store / blueprint", labels, key="global_store")
            handle = handles[labels.index(pick)]
        else:
            st.info("No stores found.")
        st.caption("Read-only viewer · manual refresh")

    fleet, runs, lineage, doctor, config = st.tabs(
        ["Fleet", "Runs", "Lineage", "Doctor", "Config"]
    )
    with fleet:
        _fleet_tab(cfg, store_dir)
    with runs:
        _runs_tab(handle)
    with lineage:
        _lineage_tab(handle)
    with doctor:
        _doctor_tab(config_path)
    with config:
        _config_tab(cfg)


# streamlit runs the target script with __name__ == "__main__".
if __name__ == "__main__":
    main()
