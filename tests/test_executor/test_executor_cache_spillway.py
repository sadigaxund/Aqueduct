"""Tests for _cache_if_multi_spillway behavior."""

import pytest
pytestmark = pytest.mark.spark
from aqueduct.executor.spark.executor import _cache_if_multi_spillway
from aqueduct.parser.models import Edge


class DummyDF:
    """Simple dummy DataFrame with a cache() method that records being called."""
    def __init__(self):
        self.cached = False
    def cache(self):
        self.cached = True
        return self


def test_cache_if_multi_spillway_single_consumer():
    """When only one spillway consumer exists, no caching should happen."""
    df = DummyDF()
    edges = (Edge(from_id="mod1", to_id="c1", port="spillway"),)
    result = _cache_if_multi_spillway(df, "mod1", edges)
    assert result is df
    assert not df.cached, "cache() should not be called for a single spillway consumer"


def test_cache_if_multi_spillway_multiple_consumers():
    """When multiple spillway consumers exist, the DataFrame should be cached."""
    df = DummyDF()
    edges = (
        Edge(from_id="mod1", to_id="c1", port="spillway"),
        Edge(from_id="mod1", to_id="c2", port="spillway"),
    )
    result = _cache_if_multi_spillway(df, "mod1", edges)
    assert result is df
    assert df.cached, "cache() should be called when >1 spillway consumers exist"


def test_cache_if_multi_spillway_ignores_non_spillway_edges():
    """Edges that are not spillway should not affect caching decision."""
    df = DummyDF()
    edges = (
        Edge(from_id="mod1", to_id="c1", port="main"),
        Edge(from_id="mod1", to_id="c2", port="spillway"),
    )
    result = _cache_if_multi_spillway(df, "mod1", edges)
    # Only one spillway consumer => no cache
    assert result is df
    assert not df.cached
