"""`aqueduct studio` — the textual application (Phase 67).

Imports ``textual`` (the optional ``tui`` extra). The CLI checks the dependency is
installed BEFORE importing this module, so a base install never hits the import.
All data comes from the read-only helpers in ``aqueduct.tui.data`` — this file is
rendering + event wiring only.

Read-only by design: runs/profile/lineage/doctor/config are views; the SQL pane
uses a read-only DuckDB connection. (Build/run/heal actions are Phase 68.)
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual.widgets import (
    DataTable,
    Footer,
    Header,
    Input,
    Static,
    TabbedContent,
    TabPane,
)

from aqueduct.tui import data as d

_STATUS_ICON = {"success": "✓", "error": "✗", "skipped": "⏭", "ok": "✓", "warn": "⚠", "fail": "✗", "skip": "⏭"}


class StudioApp(App):
    """Read-only inspection workspace over one observability store."""

    TITLE = "aqueduct studio"
    BINDINGS = [("q", "quit", "Quit"), ("r", "refresh", "Refresh")]
    CSS = """
    #runs { width: 45%; }
    #detail { width: 55%; }
    DataTable { height: 1fr; }
    #sql_error { color: $error; height: auto; }
    """

    def __init__(self, handles: list[d.StoreHandle], cfg: Any, config_path: str | None):
        super().__init__()
        self._handles = handles
        self._cfg = cfg
        self._config_path = config_path
        self._handle = handles[0]  # first store; multi-store switch = future
        self._store = self._handle.store
        self._duckdb_path = self._handle.duckdb_path  # None for postgres → SQL pane off
        self._runs: list[d.RunRow] = []

    def compose(self) -> ComposeResult:
        yield Header()
        with TabbedContent():
            with TabPane("Runs", id="tab_runs"):
                with Horizontal():
                    yield DataTable(id="runs", cursor_type="row")
                    with Vertical(id="detail"):
                        yield Static("Modules", classes="hd")
                        yield DataTable(id="modules")
                        yield Static("Resource profile", classes="hd")
                        yield DataTable(id="profile")
            with TabPane("SQL", id="tab_sql"):
                with Vertical():
                    yield Input(placeholder="SELECT * FROM run_records LIMIT 20", id="sql")
                    yield Static("", id="sql_error")
                    yield DataTable(id="sql_results")
            with TabPane("Lineage", id="tab_lineage"):
                yield DataTable(id="lineage")
            with TabPane("Doctor", id="tab_doctor"):
                yield DataTable(id="doctor")
            with TabPane("Config", id="tab_config"):
                yield DataTable(id="config")
        yield Footer()

    # ── lifecycle ───────────────────────────────────────────────────────────
    def on_mount(self) -> None:
        loc = str(self._duckdb_path) if self._duckdb_path else "postgres"
        self.sub_title = f"{self._handle.label}  ·  {loc}"
        self._load_runs()
        self._load_lineage()
        self._load_doctor()
        self._load_config()

    def action_refresh(self) -> None:
        self._load_runs()
        self._load_lineage()

    # ── Runs ────────────────────────────────────────────────────────────────
    def _load_runs(self) -> None:
        self._runs = d.list_runs(self._store)
        t = self.query_one("#runs", DataTable)
        t.clear(columns=True)
        t.add_columns("", "run_id", "blueprint", "started")
        for r in self._runs:
            t.add_row(_STATUS_ICON.get(r.status, "?"), r.run_id, r.blueprint_id, r.started_at or "")
        if self._runs:
            self._show_detail(self._runs[0].run_id)

    def _show_detail(self, run_id: str) -> None:
        det = d.run_detail(self._store, run_id)
        mods = self.query_one("#modules", DataTable)
        prof = self.query_one("#profile", DataTable)
        mods.clear(columns=True)
        mods.add_columns("", "module", "error")
        prof.clear(columns=True)
        prof.add_columns("module", "duration_ms", "rows_out", "bytes_out")
        if det is None:
            return
        for m in det.modules:
            mods.add_row(_STATUS_ICON.get(m.status, "?"), m.module_id, (m.error or "")[:80])
        for p in det.profile:
            prof.add_row(
                p.module_id,
                "-" if p.duration_ms is None else str(p.duration_ms),
                "-" if p.records_written is None else f"{p.records_written:,}",
                "-" if p.bytes_written is None else str(p.bytes_written),
            )

    def on_data_table_row_highlighted(self, event: DataTable.RowHighlighted) -> None:
        if event.data_table.id == "runs" and 0 <= event.cursor_row < len(self._runs):
            self._show_detail(self._runs[event.cursor_row].run_id)

    # ── SQL ─────────────────────────────────────────────────────────────────
    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id != "sql":
            return
        err = self.query_one("#sql_error", Static)
        table = self.query_one("#sql_results", DataTable)
        table.clear(columns=True)
        query = event.value.strip()
        if not query:
            return
        if self._duckdb_path is None:
            err.update("ad-hoc SQL pane is available for the duckdb backend only")
            return
        try:
            cols, rows = d.run_sql_readonly(self._duckdb_path, query)
        except Exception as exc:  # surface read-only / SQL errors to the user
            err.update(f"✗ {exc}")
            return
        err.update("")
        table.add_columns(*(cols or ["(no columns)"]))
        for row in rows[:1000]:
            table.add_row(*[("" if v is None else str(v)) for v in row])

    # ── Lineage ─────────────────────────────────────────────────────────────
    def _load_lineage(self) -> None:
        t = self.query_one("#lineage", DataTable)
        t.clear(columns=True)
        t.add_columns("channel", "output_column", "source_table", "source_column")
        for r in d.lineage(self._store):
            t.add_row(r.channel_id, r.output_column, r.source_table, r.source_column)

    # ── Doctor ──────────────────────────────────────────────────────────────
    def _load_doctor(self) -> None:
        t = self.query_one("#doctor", DataTable)
        t.clear(columns=True)
        t.add_columns("", "check", "detail")
        try:
            from aqueduct.doctor import run_doctor

            results = run_doctor(
                config_path=Path(self._config_path) if self._config_path else None,
                skip_spark=True,  # never build a Spark session from the TUI
            )
        except Exception as exc:
            t.add_row("✗", "doctor", str(exc))
            return
        for r in results:
            t.add_row(_STATUS_ICON.get(r.status, "?"), r.name, r.detail)

    # ── Config ──────────────────────────────────────────────────────────────
    def _load_config(self) -> None:
        t = self.query_one("#config", DataTable)
        t.clear(columns=True)
        t.add_columns("key", "value")
        for k, v in _config_rows(self._cfg):
            t.add_row(k, v)


def _config_rows(cfg: Any) -> list[tuple[str, str]]:
    """Safe, structural config fields only — no secret values are read."""
    g = lambda obj, *names: next(  # noqa: E731 - tiny defensive getattr-chain
        (getattr(obj, n) for n in names if hasattr(obj, n)), None
    )
    rows: list[tuple[str, str]] = []
    dep = g(cfg, "deployment")
    if dep is not None:
        for f in ("engine", "target", "master_url", "env"):
            if hasattr(dep, f):
                rows.append((f"deployment.{f}", str(getattr(dep, f))))
    stores = g(cfg, "stores")
    if stores is not None:
        for sname in ("observability", "lineage", "depot"):
            s = getattr(stores, sname, None)
            b = getattr(s, "backend", None) if s is not None else None
            if b is not None:
                rows.append((f"stores.{sname}.backend", str(b)))
    agent = g(cfg, "agent")
    if agent is not None:
        for f in ("provider", "model", "approval", "approval_mode"):
            if hasattr(agent, f):
                rows.append((f"agent.{f}", str(getattr(agent, f))))
    return rows


def run_studio(
    config_path: str | None = None,
    store_dir: str | None = None,
) -> int:
    """Entry point used by the CLI. Returns a process exit code."""
    from aqueduct.cli.style import error as _style_error
    from aqueduct.config import ConfigError, load_config

    try:
        cfg = load_config(Path(config_path) if config_path else None)
    except ConfigError as exc:
        _style_error(f"config error: {exc}")
        return 1

    # Backend-aware (Phase 69): duckdb → one handle per file; postgres → one store
    # (all runs). The ad-hoc SQL pane self-disables for postgres (read-only-only).
    handles = d.discover_stores(cfg, store_dir=store_dir)
    if not handles:
        _style_error("No observability stores found. Run a blueprint first.")
        return 1

    StudioApp(handles, cfg, config_path).run()
    return 0
