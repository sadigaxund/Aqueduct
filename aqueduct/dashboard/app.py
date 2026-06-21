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

_STATUS_ICON = {"success": "✓", "error": "✗", "skipped": "⏭"}


def _cfg():
    cp = os.environ.get("AQ_DASH_CONFIG") or None
    return load_config(Path(cp) if cp else None)


def _store_dir() -> str | None:
    return os.environ.get("AQ_DASH_STORE_DIR") or None


def _fleet_tab(cfg, store_dir):
    st.subheader("Fleet")
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

    st.dataframe(
        [
            {
                "blueprint": s.blueprint_id,
                "runs": s.runs,
                "success %": round(s.success_rate * 100, 1),
                "errors": s.errors,
                "heal attempts": s.heal_attempts,
                "last run": s.last_run or "",
            }
            for s in summ
        ],
        use_container_width=True,
        hide_index=True,
    )

    # Runs over time
    rot = q.runs_over_time(cfg, store_dir=store_dir)
    if rot:
        days = sorted({d.day for d in rot})
        statuses = sorted({d.status for d in rot})
        grid = {st_: {d: 0 for d in days} for st_ in statuses}
        for d in rot:
            grid[d.status][d.day] = d.count
        st.caption("Runs over time")
        st.line_chart({st_: [grid[st_][d] for d in days] for st_ in statuses})

    # Failure categories
    dist = q.failure_categories(cfg, store_dir=store_dir)
    if dist:
        st.caption("Failure categories")
        st.bar_chart(dist)


def _runs_tab(cfg, store_dir):
    st.subheader("Runs")
    handles = q.discover_stores(cfg, store_dir=store_dir)
    if not handles:
        st.info("No observability stores found yet.")
        return
    labels = [h.label for h in handles]
    pick = st.selectbox("Store / blueprint", labels, key="runs_store")
    handle = handles[labels.index(pick)]

    runs = q.list_runs(handle.store, limit=100)
    if not runs:
        st.info("No runs in this store yet.")
        return
    st.dataframe(
        [
            {
                "": _STATUS_ICON.get(r.status, "?"),
                "run_id": r.run_id,
                "blueprint": r.blueprint_id,
                "status": r.status,
                "started": r.started_at or "",
            }
            for r in runs
        ],
        use_container_width=True,
        hide_index=True,
    )

    run_ids = [r.run_id for r in runs]
    sel = st.selectbox("Inspect run", run_ids, key="runs_detail")
    det = q.run_detail(handle.store, sel)
    if det is None:
        return
    # Merged per-module metrics table, execution order.
    prof = {p.module_id: p for p in det.profile}
    st.caption("Modules (execution order)")
    st.dataframe(
        [
            {
                "": _STATUS_ICON.get(m.status, "?"),
                "module": m.module_id,
                "status": m.status,
                "duration_ms": getattr(prof.get(m.module_id), "duration_ms", None),
                "rows_out": getattr(prof.get(m.module_id), "records_written", None),
                "bytes_out": getattr(prof.get(m.module_id), "bytes_written", None),
                "error": (m.error or "")[:200],
            }
            for m in det.modules
        ],
        use_container_width=True,
        hide_index=True,
    )


def _lineage_tab(cfg, store_dir):
    st.subheader("Column lineage")
    handles = q.discover_stores(cfg, store_dir=store_dir)
    if not handles:
        st.info("No observability stores found yet.")
        return
    labels = [h.label for h in handles]
    pick = st.selectbox("Store / blueprint", labels, key="lin_store")
    handle = handles[labels.index(pick)]
    rows = q.lineage(handle.store)
    if not rows:
        st.info("No column-lineage rows captured for this store (needs a SQL Channel).")
        return

    # Plotly Sankey: source_table.source_column → output_column.
    import plotly.graph_objects as go

    nodes: list[str] = []
    idx: dict[str, int] = {}

    def node(name: str) -> int:
        if name not in idx:
            idx[name] = len(nodes)
            nodes.append(name)
        return idx[name]

    src_i, dst_i = [], []
    for r in rows:
        s = f"{r.source_table}.{r.source_column}" if r.source_table else r.source_column
        src_i.append(node(s))
        dst_i.append(node(r.output_column))
    fig = go.Figure(
        go.Sankey(
            node=dict(label=nodes, pad=12, thickness=14),
            link=dict(source=src_i, target=dst_i, value=[1] * len(src_i)),
        )
    )
    fig.update_layout(height=max(300, 22 * len(nodes)), margin=dict(l=0, r=0, t=10, b=10))
    st.plotly_chart(fig, use_container_width=True)
    with st.expander("Lineage rows"):
        st.dataframe(
            [
                {"channel": r.channel_id, "output": r.output_column,
                 "source_table": r.source_table, "source_column": r.source_column}
                for r in rows
            ],
            use_container_width=True,
            hide_index=True,
        )


def _doctor_tab(cfg, config_path):
    st.subheader("Doctor")
    if st.button("Run checks"):
        try:
            from aqueduct.doctor import run_doctor

            results = run_doctor(
                config_path=Path(config_path) if config_path else None,
                skip_spark=True,
            )
            st.dataframe(
                [
                    {"": _STATUS_ICON.get(r.status, r.status), "check": r.name, "detail": r.detail}
                    for r in results
                ],
                use_container_width=True,
                hide_index=True,
            )
        except Exception as exc:  # noqa: BLE001 — surface to the read-only viewer
            st.error(f"doctor failed: {exc}")
    else:
        st.caption("Read-only health checks (Spark probe skipped). Click to run.")


def _config_tab(cfg):
    st.subheader("Config")
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
    st.dataframe(
        [{"key": k, "value": v} for k, v in rows],
        use_container_width=True, hide_index=True,
    )
    st.caption("Read-only. Secret values are never read or displayed.")


def main() -> None:
    st.set_page_config(page_title="Aqueduct Dashboard", layout="wide")
    config_path = os.environ.get("AQ_DASH_CONFIG") or None
    store_dir = _store_dir()
    try:
        cfg = _cfg()
    except Exception as exc:  # noqa: BLE001
        st.error(f"config error: {exc}")
        return

    st.title("Aqueduct")
    backend = getattr(cfg.stores.observability, "backend", "duckdb")
    st.caption(f"observability backend: **{backend}**  ·  read-only viewer")
    if st.button("🔄 Refresh"):
        st.rerun()

    fleet, runs, lineage, doctor, config = st.tabs(
        ["Fleet", "Runs", "Lineage", "Doctor", "Config"]
    )
    with fleet:
        _fleet_tab(cfg, store_dir)
    with runs:
        _runs_tab(cfg, store_dir)
    with lineage:
        _lineage_tab(cfg, store_dir)
    with doctor:
        _doctor_tab(cfg, config_path)
    with config:
        _config_tab(cfg)


# streamlit runs the target script with __name__ == "__main__".
if __name__ == "__main__":
    main()
