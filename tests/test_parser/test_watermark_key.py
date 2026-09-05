"""Schema + graph validation for Egress `watermark_key:` (watermark
crash-consistency — see docs/specs.md)."""

from __future__ import annotations

import pytest

from aqueduct.parser.parser import ParseError, parse_dict

pytestmark = pytest.mark.unit


def _bp(modules: list[dict], edges: list[dict]) -> dict:
    return {
        "aqueduct": "1.0",
        "id": "wm_test",
        "name": "wm test",
        "modules": modules,
        "edges": edges,
    }


_INGRESS = {
    "id": "ing",
    "type": "Ingress",
    "label": "Ing",
    "config": {"format": "parquet", "path": "/tmp/in"},
}


def test_watermark_key_valid_shape_parses(tmp_path):
    bp = parse_dict(
        _bp(
            [
                _INGRESS,
                {
                    "id": "append_eg",
                    "type": "Egress",
                    "label": "AppendEg",
                    "config": {
                        "format": "parquet",
                        "path": "/tmp/out",
                        "mode": "append",
                        "watermark_key": "wk1",
                    },
                },
                {
                    "id": "wm_eg",
                    "type": "Egress",
                    "label": "WmEg",
                    "config": {"format": "depot", "key": "wk1", "value": "x"},
                },
            ],
            [{"from": "ing", "to": "append_eg"}, {"from": "ing", "to": "wm_eg"}],
        ),
        base_dir=tmp_path,
    )
    assert bp.id == "wm_test"
    append_module = next(m for m in bp.modules if m.id == "append_eg")
    assert append_module.config["watermark_key"] == "wk1"


def test_watermark_key_on_non_append_mode_rejected(tmp_path):
    with pytest.raises(ParseError, match="watermark_key"):
        parse_dict(
            _bp(
                [
                    _INGRESS,
                    {
                        "id": "eg",
                        "type": "Egress",
                        "label": "Eg",
                        "config": {
                            "format": "parquet",
                            "path": "/tmp/out",
                            "mode": "overwrite",
                            "watermark_key": "wk1",
                        },
                    },
                    {
                        "id": "wm_eg",
                        "type": "Egress",
                        "label": "WmEg",
                        "config": {"format": "depot", "key": "wk1", "value": "x"},
                    },
                ],
                [{"from": "ing", "to": "eg"}, {"from": "ing", "to": "wm_eg"}],
            ),
            base_dir=tmp_path,
        )


def test_watermark_key_on_depot_format_rejected(tmp_path):
    with pytest.raises(ParseError, match="watermark_key"):
        parse_dict(
            _bp(
                [
                    _INGRESS,
                    {
                        "id": "wm_eg",
                        "type": "Egress",
                        "label": "WmEg",
                        "config": {
                            "format": "depot",
                            "key": "wk1",
                            "value": "x",
                            "mode": "append",
                            "watermark_key": "wk1",
                        },
                    },
                ],
                [{"from": "ing", "to": "wm_eg"}],
            ),
            base_dir=tmp_path,
        )


def test_watermark_key_with_no_downstream_depot_writer_rejected(tmp_path):
    with pytest.raises(ParseError, match="watermark_key"):
        parse_dict(
            _bp(
                [
                    _INGRESS,
                    {
                        "id": "append_eg",
                        "type": "Egress",
                        "label": "AppendEg",
                        "config": {
                            "format": "parquet",
                            "path": "/tmp/out",
                            "mode": "append",
                            "watermark_key": "wk1",
                        },
                    },
                ],
                [{"from": "ing", "to": "append_eg"}],
            ),
            base_dir=tmp_path,
        )


def test_watermark_key_wrong_depot_key_rejected(tmp_path):
    """A downstream depot Egress exists, but writes a DIFFERENT key."""
    with pytest.raises(ParseError, match="watermark_key"):
        parse_dict(
            _bp(
                [
                    _INGRESS,
                    {
                        "id": "append_eg",
                        "type": "Egress",
                        "label": "AppendEg",
                        "config": {
                            "format": "parquet",
                            "path": "/tmp/out",
                            "mode": "append",
                            "watermark_key": "wk1",
                        },
                    },
                    {
                        "id": "wm_eg",
                        "type": "Egress",
                        "label": "WmEg",
                        "config": {"format": "depot", "key": "some_other_key", "value": "x"},
                    },
                ],
                [{"from": "ing", "to": "append_eg"}, {"from": "ing", "to": "wm_eg"}],
            ),
            base_dir=tmp_path,
        )
