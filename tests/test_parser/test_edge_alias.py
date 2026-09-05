"""The edge `as:` alias: schema shape plus the four graph rules.

`as` names an edge's frame inside the module the edge points at. It exists
because a Junction branch produces a dotted frame key
(`<junction_id>.<branch_id>`) that no SQL text can reference, and that neither
engine registers.
"""

from __future__ import annotations

import pytest

from aqueduct.errors import ParseError
from aqueduct.parser.parser import parse_dict

pytestmark = pytest.mark.unit


def _parse(bp: dict):
    return parse_dict(bp, ".")


def _blueprint(edges: list[dict], *, fan_in_op: str = "sql") -> dict:
    return {
        "aqueduct": "1.0",
        "id": "edge_alias_bp",
        "name": "Edge alias",
        "modules": [
            {
                "id": "ing",
                "type": "Ingress",
                "label": "In",
                "config": {"format": "parquet", "path": "/tmp/in.parquet"},
            },
            {
                "id": "j",
                "type": "Junction",
                "label": "J",
                "config": {
                    "mode": "conditional",
                    "branches": [
                        {"id": "hi", "condition": "a > 2"},
                        {"id": "lo", "condition": "_else_"},
                    ],
                },
            },
            {
                "id": "fan_in",
                "type": "Channel",
                "label": "FanIn",
                "config": {"op": fan_in_op, "query": "SELECT * FROM hi_rows"},
            },
            {
                "id": "out",
                "type": "Egress",
                "label": "Out",
                "config": {"format": "parquet", "path": "/tmp/out.parquet"},
            },
        ],
        "edges": edges,
    }


_FAN_IN_EDGES = [
    {"from": "ing", "to": "j"},
    {"from": "j", "to": "fan_in", "port": "hi", "as": "hi_rows"},
    {"from": "j", "to": "fan_in", "port": "lo", "as": "lo_rows"},
    {"from": "fan_in", "to": "out"},
]


class TestSchema:
    def test_as_lands_on_the_edge_model(self):
        bp = _parse(_blueprint(_FAN_IN_EDGES))
        aliases = {e.alias for e in bp.edges if e.to_id == "fan_in"}
        assert aliases == {"hi_rows", "lo_rows"}

    def test_an_edge_without_as_keeps_none(self):
        bp = _parse(_blueprint(_FAN_IN_EDGES))
        assert [e.alias for e in bp.edges if e.from_id == "ing"] == [None]

    @pytest.mark.parametrize("bad", ["j.hi", "two words", "", "9lives", 'quo"ted'])
    def test_as_must_be_a_bare_sql_identifier(self, bad):
        edges = [dict(e) for e in _FAN_IN_EDGES]
        edges[1]["as"] = bad
        with pytest.raises(ParseError, match="not a usable SQL name|as="):
            _parse(_blueprint(edges))


class TestRuleA:
    """A branch-port edge into a multi-input SQL Channel must carry `as`."""

    def test_missing_as_on_a_branch_into_a_multi_input_channel_is_an_error(self):
        edges = [dict(e) for e in _FAN_IN_EDGES]
        del edges[2]["as"]
        with pytest.raises(ParseError, match="needs an `as:` name"):
            _parse(_blueprint(edges))

    def test_a_main_port_edge_needs_no_as(self):
        """Only a branch port has an unwritable key; `main` is the module id."""
        edges = [
            {"from": "ing", "to": "fan_in"},
            {"from": "j", "to": "fan_in", "port": "hi", "as": "hi_rows"},
            {"from": "ing", "to": "j"},
            {"from": "fan_in", "to": "out"},
        ]
        bp = _parse(_blueprint(edges))
        assert len(bp.edges) == 4

    def test_a_non_sql_channel_op_needs_no_as(self):
        """`op: union` reads its inputs positionally, never by name."""
        edges = [dict(e) for e in _FAN_IN_EDGES]
        del edges[1]["as"]
        del edges[2]["as"]
        bp = _parse(_blueprint(edges, fan_in_op="union"))
        assert [e.alias for e in bp.edges] == [None, None, None, None]


class TestRuleB:
    def test_as_may_not_shadow_a_module_id(self):
        edges = [dict(e) for e in _FAN_IN_EDGES]
        edges[1]["as"] = "ing"
        with pytest.raises(ParseError, match="collides with a module id"):
            _parse(_blueprint(edges))

    def test_two_inputs_of_one_module_may_not_share_a_name(self):
        edges = [dict(e) for e in _FAN_IN_EDGES]
        edges[2]["as"] = "hi_rows"
        with pytest.raises(ParseError, match="is already used by edge"):
            _parse(_blueprint(edges))

    def test_the_same_name_into_two_different_modules_is_fine(self):
        bp = _blueprint(_FAN_IN_EDGES)
        bp["modules"].append(
            {
                "id": "other",
                "type": "Channel",
                "label": "Other",
                "config": {"op": "sql", "query": "SELECT * FROM hi_rows"},
            }
        )
        bp["edges"] = [
            *_FAN_IN_EDGES,
            {"from": "j", "to": "other", "port": "hi", "as": "hi_rows"},
        ]
        parsed = _parse(bp)
        assert sum(e.alias == "hi_rows" for e in parsed.edges) == 2


class TestRuleC:
    def test_as_on_a_single_input_channel_is_allowed(self):
        """It names the frame alongside the `__input__` every single-input
        Channel already gets."""
        edges = [
            {"from": "ing", "to": "j"},
            {"from": "j", "to": "fan_in", "port": "hi", "as": "hi_rows"},
            {"from": "fan_in", "to": "out"},
        ]
        bp = _parse(_blueprint(edges))
        assert [e.alias for e in bp.edges if e.to_id == "fan_in"] == ["hi_rows"]


class TestRuleD:
    def test_as_on_a_non_channel_target_is_an_error(self):
        edges = [dict(e) for e in _FAN_IN_EDGES]
        edges[3] = {"from": "fan_in", "to": "out", "as": "final"}
        with pytest.raises(ParseError, match="only valid on an edge into a Channel"):
            _parse(_blueprint(edges))
