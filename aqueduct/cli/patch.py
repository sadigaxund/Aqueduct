"""`patch` commands — extracted verbatim from aqueduct/cli/__init__.py.

No behaviour change. The click group + shared helpers come from the package;
commands register onto `cli` when imported at the bottom of __init__.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import click

from aqueduct import exit_codes
from aqueduct.cli import (
    cli,
    _apply_warnings_from_cfg,
    _resolve_and_load_env,
    _env_options,
    _patches_root_from_blueprint,
    _uncommitted_applied_patches,)


def _patch_index_obs_store(blueprint_path: "Path | None" = None):
    """Best-effort observability store for patch_index status updates (Phase 53).

    Postgres → the shared DSN. DuckDB → the per-blueprint store when a blueprint
    is known, else the configured default. Returns None on any failure — the
    index update is best-effort and never blocks a local patch command."""
    try:
        from aqueduct.config import load_config
        cfg = load_config(None)
        if cfg.stores.observability.backend == "postgres":
            from aqueduct.stores import get_stores
            return get_stores(cfg).observability
        from aqueduct.stores.duckdb_ import DuckDBObservabilityStore
        if blueprint_path is not None:
            from aqueduct.parser.parser import parse as _parse
            bp_id = _parse(str(blueprint_path)).id
            cand = Path(".aqueduct/observability") / bp_id / "observability.db"
            if cand.exists():
                return DuckDBObservabilityStore(cand)
        default = Path(cfg.stores.observability.path)
        return DuckDBObservabilityStore(default) if default.exists() else None
    except Exception:
        return None


# ── patch command group ───────────────────────────────────────────────────────

@cli.group()
def patch() -> None:
    """Manage Blueprint patches."""


@patch.command("preview")
@click.argument("patch_file", type=click.Path(exists=True, dir_okay=False))
@click.option(
    "--blueprint",
    "blueprint_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Blueprint YAML the patch will be applied to.",
)
@click.option(
    "--sandbox",
    is_flag=True,
    default=False,
    help="Also run the sandbox gate — replay the patched Blueprint on a sampled DataFrame.",
)
@click.option(
    "--sample",
    "sample_rows",
    type=int,
    default=1000,
    show_default=True,
    help="Per-Ingress row limit during the sandbox gate. 0 = unbounded (full data).",
)
@click.option(
    "--config",
    "config_path",
    default=None,
    type=click.Path(dir_okay=False),
    help="Path to aqueduct.yml",
)
@click.option(
    "--format",
    "out_format",
    type=click.Choice(["text", "json"], case_sensitive=False),
    default="text",
    show_default=True,
    help="Output format. `text` (default) renders diff + gate findings. `json` emits a machine-readable report.",
)
@_env_options
def patch_preview(
    patch_file: str,
    blueprint_path: str,
    sandbox: bool,
    sample_rows: int,
    config_path: str | None,
    out_format: str,
    env_file: str | None,
    cli_env: tuple[str, ...],
) -> None:
    """Validation pyramid preview for a pending patch.

    Always runs the guardrails gate (schema + post-apply Parser re-check)
    and the lineage gate (live lineage impact). With `--sandbox`, also
    runs the sandbox gate (replay the patched Blueprint on a per-Ingress
    LIMIT N, Egress modules skipped and listed in the report) and the
    explain gate (post-patch `explain()` regression
    check against the most recent baseline in `observability.explain_snapshot`).
    """
    import json as _json
    from pathlib import Path as _Path
    from aqueduct.config import ConfigError, load_config
    from aqueduct.patch.apply import (
        PatchError,
        _check_guardrails,
        _yaml_load,
        apply_patch_to_dict,
        load_patch_spec,
    )
    from aqueduct.patch.preview import (
        render_unified_diff,
        run_lineage_gate,
        run_sandbox_gate,
    )
    from aqueduct.patch.explain_gate import run_explain_gate

    bp_raw = _yaml_load(_Path(blueprint_path))
    try:
        spec = load_patch_spec(_Path(patch_file))
    except PatchError as exc:
        click.echo(f"✗ patch schema error: {exc}", err=True)
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    # Guardrails gate — deterministic. Identical enforcement used by
    # `patch apply`; surfaced here so reviewers see violations up front.
    try:
        _check_guardrails(spec, bp_raw, provenance_map=None)
    except PatchError as exc:
        click.echo(f"✗ Guardrails gate blocked: {exc}", err=True)
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    try:
        bp_after = apply_patch_to_dict(bp_raw, spec)
    except PatchError as exc:
        click.echo(f"✗ patch could not be applied in memory: {exc}", err=True)
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    diff = render_unified_diff(bp_raw, bp_after)
    lineage_res = run_lineage_gate(bp_raw, bp_after, spec)

    sandbox_res = None
    explain_res = None
    if sandbox:
        cfg = None
        try:
            _resolve_and_load_env(
                env_file,
                _Path(config_path) if config_path else _Path(blueprint_path),
                cli_env=cli_env,
            )
            cfg = load_config(_Path(config_path) if config_path else None)
            _apply_warnings_from_cfg(cfg)
        except ConfigError as exc:
            click.echo(f"✗ config error (needed for sandbox): {exc}", err=True)
            sys.exit(exit_codes.CONFIG_ERROR)
        from aqueduct.stores import get_stores
        bundle = get_stores(cfg)
        failed_module = None
        explain_after: dict[str, dict] = {}
        # patch_id is used both for run-tagging and tempfile naming
        sandbox_res = run_sandbox_gate(
            bp_after,
            blueprint_path=_Path(blueprint_path),
            patch_id=spec.patch_id,
            failed_module=failed_module,
            sample_rows=int(sample_rows),
            observability_store=bundle.observability,
            explain_capture=explain_after,
            sandbox_master_url=cfg.agent.sandbox_master_url,
        )
        # Explain gate — baseline read directly from the observability store.
        try:
            from aqueduct.stores import get_stores as _gs  # noqa
            from aqueduct.surveyor.surveyor import Surveyor
            # Compile to retrieve blueprint_id without full run.
            from aqueduct.parser.parser import parse as _parse
            from aqueduct.compiler.compiler import compile as _compile
            _bp = _parse(blueprint_path)
            _mf = _compile(_bp, blueprint_path=_Path(blueprint_path))
            _surv = Surveyor(manifest=_mf, store_dir=cfg.store_dir, stores=bundle)
            _baseline = _surv.latest_explain_snapshots(blueprint_id=_mf.blueprint_id)
        except Exception:
            _baseline = {}
        explain_res = run_explain_gate(_baseline, explain_after, touched_modules=lineage_res.touched_modules)

    if out_format.lower() == "json":
        report = {
            "patch_id": spec.patch_id,
            "blueprint_path": str(blueprint_path),
            "diff": diff,
            "lineage": {
                "status": lineage_res.status,
                "touched_modules": lineage_res.touched_modules,
                "warnings": [w.__dict__ for w in lineage_res.warnings],
                "duration_ms": lineage_res.duration_ms,
            },
        }
        if sandbox_res is not None:
            report["sandbox"] = {
                "status": sandbox_res.status,
                "detail": sandbox_res.detail,
                "sample_rows": sandbox_res.sample_rows,
                "duration_ms": sandbox_res.duration_ms,
                "egress_targets": sandbox_res.egress_targets,
            }
        if explain_res is not None:
            report["explain"] = {
                "status": explain_res.status,
                "detail": explain_res.detail,
                "duration_ms": explain_res.duration_ms,
                "baseline_run_id": explain_res.baseline_run_id,
                "regressions": [r.__dict__ for r in explain_res.regressions],
            }
        click.echo(_json.dumps(report, indent=2))
        sys.exit(0 if lineage_res.status != "fail" and (sandbox_res is None or sandbox_res.status == "pass" or sandbox_res.status == "skip") else 2)

    # Text report
    click.echo(f"Patch {spec.patch_id}")
    click.echo(f"  rationale: {spec.rationale}")
    if spec.confidence is not None:
        click.echo(f"  confidence: {spec.confidence:.0%}")
    click.echo()
    click.echo("── Blueprint diff ────────────────────────────────────────────")
    click.echo(diff if diff.strip() else "  (no textual change)")

    click.echo()
    click.echo("── Lineage gate (live sqlglot) ───────────────────────────────")
    click.echo(f"  status:          {lineage_res.status}")
    click.echo(f"  touched modules: {', '.join(lineage_res.touched_modules) or '(none)'}")
    if lineage_res.warnings:
        for w in lineage_res.warnings:
            click.echo(f"  ⚠ {w.detail}")
    else:
        click.echo("  no downstream column-consumption regressions detected")
    click.echo(f"  duration:        {lineage_res.duration_ms} ms")

    if sandbox_res is not None:
        click.echo()
        click.echo("── Sandbox gate (replay) ─────────────────────────────────────")
        click.echo(f"  status:      {sandbox_res.status}")
        click.echo(f"  detail:      {sandbox_res.detail}")
        if sandbox_res.sample_rows is not None:
            click.echo(f"  sample_rows: {sandbox_res.sample_rows}")
        click.echo(f"  duration:    {sandbox_res.duration_ms} ms")
        if sandbox_res.egress_targets:
            click.echo("  Egress operations (sandbox skipped):")
            for t in sandbox_res.egress_targets:
                click.echo(
                    f"    {t.get('id')}  → {t.get('format')}  {t.get('path')}"
                    + (f"  (mode={t.get('mode')})" if t.get("mode") else "")
                )

    if explain_res is not None:
        click.echo()
        click.echo("── Explain gate (plan regression) ────────────────────────────")
        click.echo(f"  status:   {explain_res.status}")
        click.echo(f"  detail:   {explain_res.detail}")
        if explain_res.baseline_run_id:
            click.echo(f"  baseline: run {explain_res.baseline_run_id}")
        if explain_res.regressions:
            for r in explain_res.regressions:
                click.echo(f"  ⚠ {r.detail}")
        click.echo(f"  duration: {explain_res.duration_ms} ms")

    exit_code = 0
    if lineage_res.status == "fail":
        exit_code = 2
    if sandbox_res is not None and sandbox_res.status == "fail":
        exit_code = 2
    sys.exit(exit_code)


@patch.command("apply")
@click.argument("patch_file", type=click.Path(exists=True, dir_okay=False))
@click.option(
    "--blueprint",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Blueprint YAML file to patch",
)
@click.option(
    "--patches-dir",
    default=None,
    help="Root directory for patch lifecycle subdirs (default: <blueprint-dir>/patches)",
)
def patch_apply(patch_file: str, blueprint: str, patches_dir: str | None) -> None:
    """Validate and apply a PatchSpec JSON file to a Blueprint YAML.

    Backs up the original Blueprint, applies all operations atomically,
    verifies the result parses cleanly, then archives the patch.
    """
    from pathlib import Path

    from aqueduct.patch.apply import PatchError, apply_patch_file

    blueprint_path = Path(blueprint)
    patches_root = Path(patches_dir) if patches_dir else _patches_root_from_blueprint(blueprint_path)

    try:
        result = apply_patch_file(
            blueprint_path=blueprint_path,
            patch_path=Path(patch_file),
            patches_dir=patches_root,
            obs_store=_patch_index_obs_store(blueprint_path),
        )
    except PatchError as exc:
        click.echo(f"✗ patch failed: {exc}", err=True)
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    click.echo(f"✓ patch applied  id={result.patch_id}")
    click.echo(f"  blueprint  → {result.blueprint_path}")
    click.echo(f"  archived   → {result.archive_path}")
    click.echo(f"  operations   {result.operations_applied} applied")
    click.echo(f"  commit with: aqueduct patch commit --blueprint {blueprint}")


@patch.command("import")
@click.argument("patch_file", type=click.Path(exists=True, dir_okay=False))
@click.option(
    "--blueprint",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Blueprint YAML file to patch",
)
@click.option(
    "--patches-dir",
    default=None,
    help="Root directory for patch lifecycle subdirs (default: <blueprint-dir>/patches)",
)
@click.option(
    "--no-commit",
    is_flag=True,
    default=False,
    help="Apply only, skip the git commit (leave the change staged for review).",
)
def patch_import(patch_file: str, blueprint: str, patches_dir: str | None, no_commit: bool) -> None:
    """Apply a received patch JSON and commit it — the CI entry point (Phase 54).

    `approval_mode: ci` reference flow: a cluster run heals, stages the patch,
    and fires the on_patch_pending webhook; a CI runner obtains the patch body
    and calls this to apply + commit it on a fresh checkout, then opens a PR
    (see docs/templates/ci-heal-workflow.yml). Equivalent to
    `patch apply` + `patch commit` in one atomic step.
    """
    import subprocess
    import tempfile
    from pathlib import Path

    from aqueduct.patch.apply import PatchError, apply_patch_file
    from aqueduct.patch.ci import build_commit_message, validate_ci_payload

    blueprint_path = Path(blueprint)
    patches_root = Path(patches_dir) if patches_dir else _patches_root_from_blueprint(blueprint_path)

    # Pre-flight: if we are going to commit, fail BEFORE mutating the Blueprint
    # when we are not inside a git work tree (so a non-repo checkout doesn't end
    # up with an applied-but-uncommittable change).
    if not no_commit:
        _check = subprocess.run(
            ["git", "rev-parse", "--is-inside-work-tree"],
            capture_output=True, text=True, cwd=blueprint_path.parent or None,
        )
        if _check.returncode != 0 or _check.stdout.strip() != "true":
            click.echo(
                "✗ not inside a git work tree — `patch import` commits the change. "
                "Run inside the repo, or pass --no-commit to stage only.",
                err=True,
            )
            sys.exit(exit_codes.DATA_OR_RUNTIME)

    # The input may be a bare PatchSpec OR a CI webhook envelope that wraps the
    # body under a `patch` key (patch + `_aq_meta` + envelope fields). When it is
    # an envelope, validate the envelope schema and unwrap the body to a tempfile.
    _apply_path = Path(patch_file)
    _tmp_unwrapped: Path | None = None
    try:
        _raw = json.loads(Path(patch_file).read_text(encoding="utf-8"))
    except Exception:
        _raw = None
    if isinstance(_raw, dict) and isinstance(_raw.get("patch"), dict):
        violations = validate_ci_payload(_raw)
        if violations:
            click.echo("✗ invalid CI webhook payload:\n  - " + "\n  - ".join(violations), err=True)
            sys.exit(exit_codes.DATA_OR_RUNTIME)
        fd, _tmp = tempfile.mkstemp(suffix=".json", prefix="aq_ci_patch_")
        import os as _os
        with _os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(_raw["patch"], fh)
        _tmp_unwrapped = Path(_tmp)
        _apply_path = _tmp_unwrapped

    try:
        result = apply_patch_file(
            blueprint_path=blueprint_path,
            patch_path=_apply_path,
            patches_dir=patches_root,
            obs_store=_patch_index_obs_store(blueprint_path),
        )
    except PatchError as exc:
        click.echo(f"✗ patch failed: {exc}", err=True)
        sys.exit(exit_codes.DATA_OR_RUNTIME)
    finally:
        if _tmp_unwrapped is not None and _tmp_unwrapped.exists():
            _tmp_unwrapped.unlink()

    click.echo(f"✓ patch imported  id={result.patch_id}")
    click.echo(f"  blueprint  → {result.blueprint_path}")
    click.echo(f"  operations   {result.operations_applied} applied")

    if no_commit:
        click.echo("  (--no-commit) staged only — commit with: "
                   f"aqueduct patch commit --blueprint {blueprint}")
        return

    # Resolve blueprint_id for the structured commit message.
    try:
        from aqueduct.parser.parser import parse as _parse
        blueprint_id = _parse(blueprint).id
    except Exception:
        blueprint_id = blueprint_path.stem

    # The applied body is archived under patches/applied/ — read it back for the
    # commit trailer (rationale, operations, run_id).
    try:
        body = json.loads(result.archive_path.read_text(encoding="utf-8"))
    except Exception:
        body = {"patch_id": result.patch_id}
    commit_msg = build_commit_message(blueprint_id, [body])

    add = subprocess.run(
        ["git", "add", blueprint_path.name],
        capture_output=True, text=True, cwd=blueprint_path.parent or None,
    )
    if add.returncode != 0:
        click.echo(f"✗ git add failed: {add.stderr.strip()}", err=True)
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    commit = subprocess.run(
        ["git", "commit", "-m", commit_msg],
        capture_output=True, text=True, cwd=blueprint_path.parent or None,
    )
    if commit.returncode != 0:
        click.echo(f"✗ git commit failed: {commit.stderr.strip()}", err=True)
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    short_hash = subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"],
        capture_output=True, text=True, cwd=blueprint_path.parent or None,
    ).stdout.strip()
    click.echo(f"  committed  [{short_hash}]  {blueprint_id}")


@patch.command("reject")
@click.argument("patch_ref")
@click.option("--reason", required=True, help="Rejection reason (recorded in patch file)")
@click.option(
    "--patches-dir",
    default=None,
    help="Root directory for patch lifecycle subdirs (default: derived from patch file path or CWD/patches)",
)
def patch_reject(patch_ref: str, reason: str, patches_dir: str | None) -> None:
    """Reject a pending patch and record the reason.

    PATCH_REF can be a file path (patches/pending/00001_*.json) or a bare patch_id slug.

    Moves patches/pending/<file> → patches/rejected/<file> with a rejection_reason annotation.
    """
    from pathlib import Path

    from aqueduct.patch.apply import PatchError, reject_patch

    # Accept either a file path or a bare patch_id slug.
    ref_path = Path(patch_ref)
    if ref_path.suffix == ".json" and ref_path.exists():
        # Full path given — derive patches_dir from grandparent (pending/ → patches/)
        resolved_patches_dir = ref_path.parent.parent
        patch_id = ref_path.stem
    elif ref_path.suffix == ".json" and not ref_path.exists() and ref_path.parent.name == "pending":
        # Path given but file not found via CWD — try same derivation
        resolved_patches_dir = ref_path.parent.parent
        patch_id = ref_path.stem
    else:
        resolved_patches_dir = Path(patches_dir) if patches_dir else _patches_root_from_blueprint(Path.cwd() / "_sentinel")
        patch_id = patch_ref

    try:
        rejected_path = reject_patch(
            patch_id=patch_id,
            reason=reason,
            patches_dir=resolved_patches_dir,
            obs_store=_patch_index_obs_store(),
        )
    except PatchError as exc:
        click.echo(f"✗ reject failed: {exc}", err=True)
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    click.echo(f"✓ patch rejected  id={patch_id}")
    click.echo(f"  archived → {rejected_path}")
    click.echo(f"  reason: {reason}")


@patch.command("pull")
@click.argument("patch_id")
@click.option(
    "--blueprint",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Blueprint the patch belongs to (locates the index + patches dir)",
)
@click.option(
    "--out",
    default=None,
    type=click.Path(file_okay=False),
    help="Output directory (default: <blueprint-dir>/patches/pending)",
)
def patch_pull(patch_id: str, blueprint: str, out: str | None) -> None:
    """Fetch a patch body from the object store into a local checkout for review.

    Profile C — the pipeline heals on a cluster and stages the patch to an
    object store (s3/gcs/adls); this pulls the body down so you can `git diff`
    and apply it locally. With a local object store this just copies the file.
    """
    from pathlib import Path

    from aqueduct.config import load_config
    from aqueduct.patch import index as _ix
    from aqueduct.stores.object_store import make_patch_store

    blueprint_path = Path(blueprint)
    patches_root = _patches_root_from_blueprint(blueprint_path)
    cfg = load_config(None)

    obs = _patch_index_obs_store(blueprint_path)
    if obs is None:
        click.echo("✗ no observability store found — cannot resolve the patch index", err=True)
        sys.exit(exit_codes.DATA_OR_RUNTIME)
    try:
        with obs.connect() as cur:
            row = _ix.get(cur, patch_id)
    except Exception as exc:
        click.echo(f"✗ index query failed: {exc}", err=True)
        sys.exit(exit_codes.DATA_OR_RUNTIME)
    if row is None:
        click.echo(f"✗ patch {patch_id!r} not found in the index", err=True)
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    ps = make_patch_store(cfg.stores.blob.backend, cfg.stores.blob.path, patches_root)
    try:
        body = ps.get_text(str(row["object_key"]))
    except Exception as exc:
        click.echo(f"✗ could not read patch body at {row['object_key']!r}: {exc}", err=True)
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    out_dir = Path(out) if out else patches_root / "pending"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{patch_id}.json"
    out_path.write_text(body, encoding="utf-8")

    click.echo(f"✓ patch pulled  id={patch_id}  status={row['status']}")
    click.echo(f"  → {out_path}")
    click.echo(f"  review: git diff  •  apply: aqueduct patch apply {out_path} --blueprint {blueprint}")


@patch.command("commit")
@click.option(
    "--blueprint",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Blueprint YAML file to commit",
)
@click.option(
    "--patches-dir",
    default=None,
    help="Root directory for patch lifecycle subdirs (default: <blueprint-dir>/patches)",
)
def patch_commit(blueprint: str, patches_dir: str | None) -> None:
    """Commit applied patches to git with a structured commit message.

    Finds applied patches newer than the last git commit for this Blueprint,
    then runs: git add <blueprint> && git commit.
    """
    import subprocess
    from pathlib import Path

    blueprint_path = Path(blueprint)
    patches_root = Path(patches_dir) if patches_dir else _patches_root_from_blueprint(blueprint_path)

    uncommitted = _uncommitted_applied_patches(blueprint_path, patches_root)
    if not uncommitted:
        click.echo("Nothing to commit — no applied patches since last git commit.")
        return

    # Parse blueprint_id
    try:
        from aqueduct.parser.parser import parse as _parse
        bp = _parse(blueprint)
        blueprint_id = bp.id
    except Exception:
        blueprint_id = blueprint_path.stem

    # Build commit message — label each patch line by its filename stem so
    # `aqueduct log`/`rollback` can match either the patch_id or the file name.
    from aqueduct.patch.ci import build_commit_message

    patch_bodies: list[dict] = []
    n = len(uncommitted)
    for p in uncommitted:
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            data = {}
        patch_bodies.append({**data, "patch_id": p.stem})

    commit_msg = build_commit_message(blueprint_id, patch_bodies)

    add = subprocess.run(["git", "add", blueprint_path.name], capture_output=True, cwd=blueprint_path.parent or None)
    if add.returncode != 0:
        click.echo(f"✗ git add failed: {add.stderr.decode().strip()}", err=True)
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    commit = subprocess.run(
        ["git", "commit", "-m", commit_msg],
        capture_output=True, text=True, cwd=blueprint_path.parent or None,
    )
    if commit.returncode != 0:
        click.echo(f"✗ git commit failed: {commit.stderr.strip()}", err=True)
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    short_hash = subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"],
        capture_output=True, text=True, cwd=blueprint_path.parent or None,
    ).stdout.strip()

    click.echo(f"✓ committed {n} patch(es)  [{short_hash}]  {blueprint_id}")
    for p in uncommitted:
        click.echo(f"  {p.name}")


@patch.command("discard")
@click.option(
    "--blueprint",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Blueprint YAML file to restore from git HEAD",
)
@click.option(
    "--patches-dir",
    default=None,
    help="Root directory for patch lifecycle subdirs (default: <blueprint-dir>/patches)",
)
def patch_discard(blueprint: str, patches_dir: str | None) -> None:
    """Discard applied patches — restore Blueprint to last git commit.

    Runs: git checkout HEAD -- <blueprint>
    Moves uncommitted applied patches back to patches/pending/.
    """
    import subprocess
    from pathlib import Path

    blueprint_path = Path(blueprint)
    patches_root = Path(patches_dir) if patches_dir else _patches_root_from_blueprint(blueprint_path)

    uncommitted = _uncommitted_applied_patches(blueprint_path, patches_root)

    restore = subprocess.run(
        ["git", "checkout", "HEAD", "--", blueprint_path.name],
        capture_output=True, text=True, cwd=blueprint_path.parent or None,
    )
    if restore.returncode != 0:
        click.echo(f"✗ git checkout failed: {restore.stderr.strip()}", err=True)
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    click.echo(f"✓ blueprint restored to HEAD: {blueprint_path}")

    pending_dir = patches_root / "pending"
    pending_dir.mkdir(parents=True, exist_ok=True)
    moved = 0
    for patch_file in uncommitted:
        dest = pending_dir / patch_file.name
        try:
            patch_file.rename(dest)
            moved += 1
        except OSError:
            pass

    if moved:
        click.echo(f"  moved {moved} applied patch(es) back to patches/pending/")
        click.echo(f"  re-apply with: aqueduct patch apply patches/pending/<file> --blueprint {blueprint}")


@patch.command("list")
@click.option(
    "--blueprint",
    default=None,
    type=click.Path(exists=True, dir_okay=False),
    help="Blueprint YAML file (used to locate patches/ dir)",
)
@click.option(
    "--patches-dir",
    default=None,
    help="Root directory for patch lifecycle subdirs (default: <blueprint-dir>/patches or CWD/patches)",
)
@click.option(
    "--status",
    "filter_status",
    default="pending",
    type=click.Choice(["pending", "applied", "rejected", "all"]),
    show_default=True,
    help="Which lifecycle directory to list",
)
@click.option(
    "--format", "out_format",
    type=click.Choice(["text", "json"], case_sensitive=False),
    default="text", show_default=True,
    help="Output format. `json` for machine-readable consumption (Phase 30b).",
)
def patch_list(blueprint: str | None, patches_dir: str | None, filter_status: str, out_format: str) -> None:
    """List patches, showing metadata for each.

    Defaults to showing pending patches. Use --status=applied/rejected/all for other dirs.
    """
    from pathlib import Path

    if patches_dir:
        patches_root = Path(patches_dir)
    elif blueprint:
        patches_root = _patches_root_from_blueprint(Path(blueprint))
    else:
        patches_root = _patches_root_from_blueprint(Path.cwd() / "_sentinel")

    dirs_to_show: list[tuple[str, Path]] = []
    if filter_status == "all":
        for sub in ("pending", "applied", "rejected"):
            d = patches_root / sub
            if d.exists():
                dirs_to_show.append((sub, d))
    else:
        d = patches_root / filter_status
        if d.exists():
            dirs_to_show.append((filter_status, d))

    if out_format.lower() == "json":
        from aqueduct.patch.grammar import PATCH_META_KEY

        payload: list[dict] = []
        for status_label, d in dirs_to_show:
            for f in sorted(d.glob("*.json"), key=lambda x: x.name):
                try:
                    data = json.loads(f.read_text(encoding="utf-8"))
                except Exception:
                    data = {}
                meta = data.get(PATCH_META_KEY) or {}
                payload.append({
                    "status": status_label,
                    "file": str(f),
                    "patch_id": data.get("patch_id", f.stem),
                    "rationale": data.get("rationale"),
                    "confidence": data.get("confidence"),
                    "category": data.get("category"),
                    "run_id": meta.get("run_id"),
                    "blueprint_id": meta.get("blueprint_id"),
                    "failed_module": meta.get("failed_module"),
                })
        click.echo(json.dumps(payload, indent=2))
        return

    total = 0
    for status_label, d in dirs_to_show:
        files = sorted(d.glob("*.json"), key=lambda f: f.name)
        if not files:
            continue

        click.echo(f"\n  [{status_label}]  {d}")
        click.echo(f"  {'file':<55} {'patch_id':<36} {'rationale'}")
        click.echo(f"  {'-'*55} {'-'*36} {'-'*40}")

        for f in files:
            try:
                data = json.loads(f.read_text(encoding="utf-8"))
            except Exception:
                data = {}
            pid = data.get("patch_id", f.stem)
            rationale = (data.get("rationale") or "").replace("\n", " ")[:60]
            click.echo(f"  {f.name:<55} {pid:<36} {rationale}")
            total += 1

    if total == 0:
        click.echo(f"No {filter_status} patches found in {patches_root}")
        return

    if filter_status == "pending":
        click.echo("\n  Apply: aqueduct patch apply patches/pending/<file> --blueprint <blueprint.yml>")
        click.echo("  Reject: aqueduct patch reject patches/pending/<file> --reason '<reason>'")



# ── aqueduct log ─────────────────────────────────────────────────────────────

@patch.command("log")
@click.argument("blueprint", type=click.Path(exists=True, dir_okay=False))
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["table", "json"]),
    default="table",
    show_default=True,
)
def log_cmd(blueprint: str, fmt: str) -> None:
    """Show git commit history for a Blueprint with Aqueduct patch metadata.

    Parses ---aqueduct--- blocks from commit messages.  Manual commits (no
    block) are shown as '(manual change)'.
    """
    import re
    import subprocess

    blueprint_path = Path(blueprint)

    result = subprocess.run(
        ["git", "log", "--follow", "--format=%H\x1f%ci\x1f%s\x1f%B\x1eENDCOMMIT", "--", str(blueprint_path)],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        click.echo(f"✗ git log failed: {result.stderr.strip()}", err=True)
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    raw = result.stdout.strip()
    if not raw:
        click.echo("No git history for this blueprint.")
        return

    _AQ_BLOCK_RE = re.compile(r"---aqueduct---(.*?)---", re.DOTALL)
    _PATCH_LINE_RE = re.compile(r"^\s*-\s+(\S+):\s*(.*)", re.MULTILINE)

    entries = []
    for commit_raw in raw.split("\x1eENDCOMMIT"):
        commit_raw = commit_raw.strip()
        if not commit_raw:
            continue
        # First line is the \x1f-separated header
        header_line, _, body = commit_raw.partition("\n")
        parts = header_line.split("\x1f")
        if len(parts) < 3:
            continue
        commit_hash = parts[0].strip()
        commit_date = parts[1].strip()
        subject = parts[2].strip()

        aq_match = _AQ_BLOCK_RE.search(body)
        if aq_match:
            block = aq_match.group(1)
            patch_ids = [m.group(1) for m in _PATCH_LINE_RE.finditer(block)]
            ops_match = re.search(r"^ops:\s*(.+)", block, re.MULTILINE)
            ops = ops_match.group(1).strip() if ops_match else ""
            run_match = re.search(r"^run_id:\s*(\S+)", block, re.MULTILINE)
            run_id = run_match.group(1) if run_match else ""
        else:
            patch_ids = []
            ops = ""
            run_id = ""

        entries.append({
            "hash": commit_hash[:8],
            "date": commit_date[:19],
            "subject": subject,
            "patches": ", ".join(patch_ids) if patch_ids else "(manual change)",
            "ops": ops,
            "run_id": run_id,
        })

    if fmt == "json":
        click.echo(json.dumps(entries, indent=2))
        return

    if not entries:
        click.echo("No commits found.")
        return

    click.echo(f"  {'hash':<10} {'date':<20} {'patches':<40} {'ops'}")
    click.echo(f"  {'-'*10} {'-'*20} {'-'*40} {'-'*30}")
    for e in entries:
        patches_col = e["patches"][:38] + ".." if len(e["patches"]) > 40 else e["patches"]
        click.echo(f"  {e['hash']:<10} {e['date']:<20} {patches_col:<40} {e['ops']}")


# ── aqueduct rollback ─────────────────────────────────────────────────────────

@patch.command("rollback")
@click.argument("blueprint", type=click.Path(exists=True, dir_okay=False))
@click.option("--to", "patch_id", required=True, help="Revert the git commit containing this patch_id")
def rollback_cmd(blueprint: str, patch_id: str) -> None:
    """Revert a Blueprint file to its state before a specific patch was applied.

    Restores only the blueprint file (and any arcade blueprints touched in the
    same commit) by checking out the pre-patch file content from git history,
    then creates a new forward commit. Never rewrites history or touches other
    files in the repository.
    """
    import subprocess

    blueprint_path = Path(blueprint)
    cwd = blueprint_path.parent or Path.cwd()

    # Walk git log scoped to this blueprint file to find the target commit
    result = subprocess.run(
        ["git", "log", "--follow", "--format=%H\x1f%B\x1eENDCOMMIT", "--", str(blueprint_path)],
        capture_output=True, text=True, cwd=cwd,
    )
    if result.returncode != 0:
        click.echo(f"✗ git log failed: {result.stderr.strip()}", err=True)
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    target_hash: str | None = None
    for commit_raw in result.stdout.split("\x1eENDCOMMIT"):
        commit_raw = commit_raw.strip()
        if not commit_raw:
            continue
        header_line, _, body = commit_raw.partition("\n")
        commit_hash = header_line.split("\x1f")[0].strip()
        if patch_id in body:
            target_hash = commit_hash
            break

    if not target_hash:
        click.echo(
            f"✗ patch_id {patch_id!r} not found in git history for {blueprint}\n"
            "  Use 'aqueduct log <blueprint>' to list available patch_ids.",
            err=True,
        )
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    # Resolve the commit immediately before the patch
    parent = subprocess.run(
        ["git", "rev-parse", f"{target_hash}~1"],
        capture_output=True, text=True, cwd=cwd,
    )
    if parent.returncode != 0:
        click.echo(f"✗ could not resolve parent commit: {parent.stderr.strip()}", err=True)
        sys.exit(exit_codes.DATA_OR_RUNTIME)
    parent_hash = parent.stdout.strip()

    # Discover all blueprint files touched by the patch commit (handles arcades)
    diff_files = subprocess.run(
        ["git", "diff-tree", "--no-commit-id", "-r", "--name-only", target_hash],
        capture_output=True, text=True, cwd=cwd,
    )
    if diff_files.returncode != 0:
        click.echo(f"✗ could not list files in commit: {diff_files.stderr.strip()}", err=True)
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    touched_files = [f.strip() for f in diff_files.stdout.splitlines() if f.strip()]
    if not touched_files:
        click.echo(f"✗ commit {target_hash[:8]} has no file changes", err=True)
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    # Restore each file to its pre-patch state (file-scoped, non-destructive)
    for rel_path in touched_files:
        restore = subprocess.run(
            ["git", "checkout", parent_hash, "--", rel_path],
            capture_output=True, text=True, cwd=cwd,
        )
        if restore.returncode != 0:
            click.echo(f"✗ git checkout {rel_path} failed: {restore.stderr.strip()}", err=True)
            sys.exit(exit_codes.DATA_OR_RUNTIME)

    # Stage restored files and create a forward revert commit
    add = subprocess.run(
        ["git", "add", "--"] + touched_files,
        capture_output=True, text=True, cwd=cwd,
    )
    if add.returncode != 0:
        click.echo(f"✗ git add failed: {add.stderr.strip()}", err=True)
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    commit_msg = (
        f"revert(aqueduct): roll back patch {patch_id!r}\n\n"
        f"Restores {', '.join(touched_files)} to state before commit {target_hash[:8]}."
    )
    commit = subprocess.run(
        ["git", "commit", "-m", commit_msg],
        capture_output=True, text=True, cwd=cwd,
    )
    if commit.returncode != 0:
        click.echo(f"✗ git commit failed: {commit.stderr.strip()}", err=True)
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    short = subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"],
        capture_output=True, text=True, cwd=cwd,
    ).stdout.strip()

    click.echo(f"✓ rolled back patch {patch_id!r}  [{short}]")
    for f in touched_files:
        click.echo(f"  restored  {f}  (from {parent_hash[:8]})")


