"""`aqueduct dashboard` — local, read-only Streamlit observability viewer (Phase 68).

Launched on demand via `aqueduct dashboard` (which shells out to `streamlit run`
on ``app.py``). Local, read-only, optional (`dashboard` extra) — never required by
a pipeline, never the control plane. All data comes from the shared read-time
layer in ``aqueduct.stores.queries``.
"""
