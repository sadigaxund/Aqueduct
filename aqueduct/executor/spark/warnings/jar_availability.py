"""Warn when a Blueprint declares a format whose JAR is not on the classpath.

Scans Ingress/Egress for known JAR-required formats (jdbc, kafka, delta,
iceberg, hudi). After the session starts, reads the JVM's loaded JARs via
`spark.sparkContext._jsc.sc().listJars()`, and warns when a required JAR
fragment is missing.

The actual JAR file names vary by distribution and version, so the rule
matches on stable substring fragments (e.g. `spark-sql-kafka`, `delta-core`,
`iceberg-spark`). False negatives are possible — a distribution may ship a
renamed or shaded jar — but the warning is a hint, not an error.
"""

from __future__ import annotations

import logging
from typing import Any

from aqueduct.models import ModuleType

logger = logging.getLogger(__name__)

RULE_ID = "jar_availability"

# Format name → list of substring fragments that, if ANY are present in a
# loaded JAR name, count as "found". `None` value = format is in core Spark
# (no JAR check needed).
_FORMAT_JAR_FRAGMENTS: dict[str, list[str] | None] = {
    "jdbc": None,  # core Spark — actual driver JAR check happens per-driver below
    "kafka": ["spark-sql-kafka", "spark-streaming-kafka"],
    "delta": ["delta-core", "delta-spark"],
    "iceberg": ["iceberg-spark"],
    "hudi": ["hudi-spark"],
}


def _loaded_jar_names(spark: Any) -> list[str] | None:
    """Return loaded JAR names, or `None` if the JVM couldn't be inspected.

    `None` (inspection failed — e.g. `spark.sparkContext` raises on a Spark
    Connect session, which exposes no `_jsc`) must stay distinguishable from
    `[]` (inspection succeeded, genuinely zero JARs loaded). Collapsing both
    to `[]` made the missing-driver warning a permanent no-op under Connect —
    "no JARs loaded" reads identically to "couldn't check."
    """
    try:
        sc = spark.sparkContext
        jsc = getattr(sc, "_jsc", None)
        if jsc is None:
            return None
        jars = jsc.sc().listJars()
        # listJars returns a Scala Seq — iterate to coerce to Python strings
        out: list[str] = []
        it = jars.iterator()
        while True:
            # If spark is a MagicMock, hasNext() returns a Mock which is truthy.
            # We check isinstance(..., bool) to break the loop for mocks.
            has_next = it.hasNext()
            if not isinstance(has_next, bool) or not has_next:
                break
            out.append(str(it.next()))
        return out
    except Exception as exc:
        # Broad by design: JVM introspection via py4j internals
        # (sparkContext._jsc) can fail for many reasons across session types
        # (Connect has no sparkContext at all, older/newer py4j surfaces
        # differ) — any failure here means "couldn't inspect," not a real
        # error to propagate through a session-startup diagnostic.
        logger.debug("jar inspection unavailable: %s", exc)
        return None


def _formats_in_blueprint(manifest: Any) -> dict[str, list[str]]:
    """Return `{format: [module_ids that use it]}` for declared Ingress/Egress."""
    by_fmt: dict[str, list[str]] = {}
    for m in manifest.modules:
        if m.type not in (ModuleType.Ingress, ModuleType.Egress):
            continue
        fmt = ((m.config or {}).get("format") or "").lower()
        if not fmt:
            continue
        by_fmt.setdefault(fmt, []).append(m.id)
    return by_fmt


def _jdbc_driver_classes(manifest: Any) -> dict[str, list[str]]:
    """Return `{driver_class: [module_ids]}` from JDBC modules that pin one."""
    out: dict[str, list[str]] = {}
    for m in manifest.modules:
        if m.type not in (ModuleType.Ingress, ModuleType.Egress):
            continue
        cfg = m.config or {}
        if (cfg.get("format") or "").lower() != "jdbc":
            continue
        drv = ((cfg.get("options") or {}).get("driver") or "").strip()
        if drv:
            out.setdefault(drv, []).append(m.id)
    return out


def check(manifest: Any, spark: Any) -> list[str]:
    out: list[str] = []
    jars = _loaded_jar_names(spark)
    fmts = _formats_in_blueprint(manifest)

    if jars is None:
        # Could not inspect the JVM's loaded JARs at all (e.g. Spark Connect —
        # no `sparkContext`/`_jsc`). Say so instead of silently reporting
        # "all clear" — a real missing driver would otherwise never surface.
        needs_jar = [fmt for fmt in fmts if _FORMAT_JAR_FRAGMENTS.get(fmt) is not None]
        if needs_jar or fmts.get("jdbc"):
            fmt_list = ", ".join(sorted(set(needs_jar) | ({"jdbc"} if fmts.get("jdbc") else set())))
            out.append(
                f"Could not verify JAR availability for format(s) {fmt_list} on "
                "this Spark session (JVM introspection unavailable — e.g. Spark "
                "Connect). If a required driver JAR is missing, the connector "
                "will fail at first read/write with no advance warning from "
                "this check."
            )
        return out

    for fmt, module_ids in fmts.items():
        fragments = _FORMAT_JAR_FRAGMENTS.get(fmt)
        if fragments is None:
            continue
        if any(any(frag in j for frag in fragments) for j in jars):
            continue
        ids = ", ".join(repr(i) for i in module_ids)
        frag_hint = " or ".join(fragments)
        out.append(
            f"Modules {ids} declare format={fmt!r}, but no JAR matching "
            f"{frag_hint} is loaded in this Spark session. The connector "
            "will fail at first read/write. Add the appropriate JAR via "
            "spark.jars / spark.jars.packages in your aqueduct.yml "
            "`spark_config:` block."
        )

    # JDBC drivers — `options.driver: com.mysql.cj.jdbc.Driver` style.
    # We can't see all loaded driver class names from listJars(), so only
    # warn when no JDBC-flavoured JAR is loaded at all but a JDBC module is
    # declared with a driver class.
    if fmts.get("jdbc"):
        drivers = _jdbc_driver_classes(manifest)
        any_jdbc_jar = any(
            any(token in j.lower() for token in ("jdbc", "mysql", "postgresql", "oracle", "sqlserver", "snowflake"))
            for j in jars
        )
        if drivers and not any_jdbc_jar:
            drv_list = ", ".join(repr(d) for d in drivers)
            out.append(
                f"JDBC modules declare driver(s) {drv_list} but no JDBC "
                "driver JAR appears to be loaded. Ingress will fail with "
                "`ClassNotFoundException`. Add the driver JAR via "
                "spark.jars / spark.jars.packages in your "
                "`spark_config:` block."
            )

    return out
