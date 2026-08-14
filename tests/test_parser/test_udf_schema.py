"""UDF registry schema validation (parser layer).

Locks the documented specs §5.4 UDF contract — python `module:`/`entry:` and
java/scala `jar:`/`class:` — and confirms entries are now strictly validated
(`extra="forbid"`), the gap that previously let a typo through to a late failure.
"""

from __future__ import annotations

import pathlib
import tempfile

import pytest
import yaml

pytestmark = pytest.mark.unit

from aqueduct.parser.parser import ParseError, parse


def _parse_with_udf(udf: dict):
    d = tempfile.mkdtemp()
    p = pathlib.Path(d) / "bp.yml"
    p.write_text(
        yaml.dump(
            {
                "aqueduct": "1.0",
                "id": "t.bp",
                "name": "T",
                "udf_registry": [udf],
                "modules": [
                    {
                        "id": "in",
                        "type": "Ingress",
                        "label": "In",
                        "config": {"format": "parquet", "path": "p"},
                    }
                ],
                "edges": [],
            }
        ),
        encoding="utf-8",
    )
    return parse(str(p))


def test_python_udf_documented_form_parses_to_executor_dict():
    bp = _parse_with_udf(
        {
            "id": "clean_phone",
            "lang": "python",
            "module": "my_project.udfs",
            "entry": "clean_phone",
            "return_type": "STRING",
        }
    )
    entry = bp.udf_registry[0]
    assert entry["id"] == "clean_phone"
    assert entry["module"] == "my_project.udfs"
    assert entry["entry"] == "clean_phone"
    assert entry["lang"] == "python"


def test_java_udf_class_alias_round_trips():
    bp = _parse_with_udf(
        {
            "id": "geohash",
            "lang": "java",
            "jar": "libs/geo.jar",
            "class": "com.example.GeoUDF",
        }
    )
    entry = bp.udf_registry[0]
    # `class` is the YAML key the executor reads back (aliased from class_name)
    assert entry["class"] == "com.example.GeoUDF"
    # A relative `jar:` path anchors to the Blueprint's own directory
    # (specs.md's documented contract; audit-fixed 2026-08 — it previously
    # round-tripped completely unanchored, breaking "JAR not found" the
    # moment `aqueduct run` was invoked from any other CWD).
    assert entry["jar"] == str((pathlib.Path(bp.base_dir) / "libs/geo.jar").resolve())
    assert pathlib.Path(entry["jar"]).is_absolute()


def test_java_udf_absolute_jar_path_left_untouched():
    """An already-absolute `jar:` path must not be re-anchored/mangled."""
    bp = _parse_with_udf(
        {
            "id": "geohash",
            "lang": "java",
            "jar": "/opt/jars/geo.jar",
            "class": "com.example.GeoUDF",
        }
    )
    entry = bp.udf_registry[0]
    assert entry["jar"] == "/opt/jars/geo.jar"


def test_java_udf_uri_jar_path_left_untouched():
    """A URI-shaped `jar:` (s3://, gs://, ...) is not a local filesystem
    path and must not be anchored against base_dir."""
    bp = _parse_with_udf(
        {
            "id": "geohash",
            "lang": "java",
            "jar": "s3://bucket/jars/geo.jar",
            "class": "com.example.GeoUDF",
        }
    )
    entry = bp.udf_registry[0]
    assert entry["jar"] == "s3://bucket/jars/geo.jar"


def test_udf_unknown_field_is_rejected():
    with pytest.raises(ParseError):
        _parse_with_udf({"id": "x", "lang": "python", "module": "m", "bogus": 1})


def test_udf_empty_id_is_rejected():
    with pytest.raises(ParseError):
        _parse_with_udf({"id": "", "lang": "python", "module": "m"})


def test_udf_unknown_lang_is_rejected():
    with pytest.raises(ParseError):
        _parse_with_udf({"id": "x", "lang": "pandas", "module": "m"})  # not yet supported
