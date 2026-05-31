"""Phase 37 — Numeric field bound enforcement on Pydantic schemas.

Every numeric field with a ``ge=``, ``gt=``, or ``le=`` constraint is
exercised with a boundary value that should trigger ``ValidationError``.

Schemas tested:
  - ``aqueduct/parser/schema.py``: BackoffSchema, RetryPolicySchema, AgentSchema
  - ``aqueduct/config.py``: ProbesConfig, AgentConnectionConfig, WebhookEndpointConfig
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.unit


# ── BackoffSchema bounds ───────────────────────────────────────────────────────

class TestBackoffSchemaBounds:
    def test_base_seconds_zero(self):
        from aqueduct.parser.schema import BackoffSchema
        from pydantic import ValidationError
        with pytest.raises(ValidationError, match=r"Input should be greater than or equal to 1"):
            BackoffSchema(base_seconds=0)

    def test_base_seconds_negative(self):
        from aqueduct.parser.schema import BackoffSchema
        from pydantic import ValidationError
        with pytest.raises(ValidationError, match=r"Input should be greater than or equal to 1"):
            BackoffSchema(base_seconds=-1)

    def test_max_seconds_zero(self):
        from aqueduct.parser.schema import BackoffSchema
        from pydantic import ValidationError
        with pytest.raises(ValidationError, match=r"Input should be greater than or equal to 1"):
            BackoffSchema(max_seconds=0)

    def test_max_seconds_negative(self):
        from aqueduct.parser.schema import BackoffSchema
        from pydantic import ValidationError
        with pytest.raises(ValidationError, match=r"Input should be greater than or equal to 1"):
            BackoffSchema(max_seconds=-1)

    def test_default_values_are_valid(self):
        from aqueduct.parser.schema import BackoffSchema
        b = BackoffSchema()
        assert b.base_seconds == 30
        assert b.max_seconds == 600


# ── RetryPolicySchema bounds ───────────────────────────────────────────────────

class TestRetryPolicySchemaBounds:
    def test_max_attempts_zero(self):
        from aqueduct.parser.schema import RetryPolicySchema
        from pydantic import ValidationError
        with pytest.raises(ValidationError, match=r"Input should be greater than or equal to 1"):
            RetryPolicySchema(max_attempts=0)

    def test_max_attempts_negative(self):
        from aqueduct.parser.schema import RetryPolicySchema
        from pydantic import ValidationError
        with pytest.raises(ValidationError, match=r"Input should be greater than or equal to 1"):
            RetryPolicySchema(max_attempts=-1)

    def test_deadline_seconds_zero(self):
        from aqueduct.parser.schema import RetryPolicySchema
        from pydantic import ValidationError
        with pytest.raises(ValidationError, match=r"Input should be greater than 0"):
            RetryPolicySchema(deadline_seconds=0)

    def test_deadline_seconds_negative(self):
        from aqueduct.parser.schema import RetryPolicySchema
        from pydantic import ValidationError
        with pytest.raises(ValidationError, match=r"Input should be greater than 0"):
            RetryPolicySchema(deadline_seconds=-5)

    def test_deadline_seconds_none_is_valid(self):
        from aqueduct.parser.schema import RetryPolicySchema
        r = RetryPolicySchema(deadline_seconds=None)
        assert r.deadline_seconds is None

    def test_default_retry_values(self):
        from aqueduct.parser.schema import RetryPolicySchema
        r = RetryPolicySchema()
        assert r.max_attempts == 1
        assert r.deadline_seconds is None


# ── AgentSchema bounds (from aqueduct/parser/schema.py) ────────────────────────

class TestAgentSchemaBounds:
    def test_max_patches_zero(self):
        from aqueduct.parser.schema import AgentSchema
        from pydantic import ValidationError
        with pytest.raises(ValidationError, match=r"Input should be greater than or equal to 1"):
            AgentSchema(max_patches=0)

    def test_max_patches_negative(self):
        from aqueduct.parser.schema import AgentSchema
        from pydantic import ValidationError
        with pytest.raises(ValidationError, match=r"Input should be greater than or equal to 1"):
            AgentSchema(max_patches=-1)

    def test_max_patches_valid_one(self):
        from aqueduct.parser.schema import AgentSchema
        s = AgentSchema(max_patches=1)
        assert s.max_patches == 1

    def test_max_patches_valid_many(self):
        from aqueduct.parser.schema import AgentSchema
        s = AgentSchema(max_patches=5)
        assert s.max_patches == 5

    def test_timeout_zero(self):
        from aqueduct.parser.schema import AgentSchema
        from pydantic import ValidationError
        with pytest.raises(ValidationError, match=r"Input should be greater than 0"):
            AgentSchema(timeout=0)

    def test_timeout_negative(self):
        from aqueduct.parser.schema import AgentSchema
        from pydantic import ValidationError
        with pytest.raises(ValidationError, match=r"Input should be greater than 0"):
            AgentSchema(timeout=-1.0)

    def test_timeout_none_is_valid(self):
        from aqueduct.parser.schema import AgentSchema
        s = AgentSchema(timeout=None)
        assert s.timeout is None

    def test_timeout_valid(self):
        from aqueduct.parser.schema import AgentSchema
        s = AgentSchema(timeout=120.0)
        assert s.timeout == 120.0

    def test_max_reprompts_zero(self):
        from aqueduct.parser.schema import AgentSchema
        from pydantic import ValidationError
        with pytest.raises(ValidationError, match=r"Input should be greater than or equal to 1"):
            AgentSchema(max_reprompts=0)

    @pytest.mark.parametrize("bad", [-1, -10])
    def test_max_reprompts_negative(self, bad):
        from aqueduct.parser.schema import AgentSchema
        from pydantic import ValidationError
        with pytest.raises(ValidationError, match=r"Input should be greater than or equal to 1"):
            AgentSchema(max_reprompts=bad)

    def test_max_reprompts_none_is_valid(self):
        from aqueduct.parser.schema import AgentSchema
        s = AgentSchema(max_reprompts=None)
        assert s.max_reprompts is None

    def test_confidence_threshold_negative(self):
        from aqueduct.parser.schema import AgentSchema
        from pydantic import ValidationError
        with pytest.raises(ValidationError, match=r"Input should be greater than or equal to 0"):
            AgentSchema(confidence_threshold=-0.1)

    def test_confidence_threshold_too_high(self):
        from aqueduct.parser.schema import AgentSchema
        from pydantic import ValidationError
        with pytest.raises(ValidationError, match=r"Input should be less than or equal to 1"):
            AgentSchema(confidence_threshold=1.5)

    def test_confidence_threshold_valid_zero(self):
        from aqueduct.parser.schema import AgentSchema
        s = AgentSchema(confidence_threshold=0.0)
        assert s.confidence_threshold == 0.0

    def test_confidence_threshold_valid_one(self):
        from aqueduct.parser.schema import AgentSchema
        s = AgentSchema(confidence_threshold=1.0)
        assert s.confidence_threshold == 1.0

    def test_confidence_threshold_default(self):
        from aqueduct.parser.schema import AgentSchema
        s = AgentSchema()
        assert s.confidence_threshold == 0.7

    def test_max_heal_attempts_per_hour_zero(self):
        from aqueduct.parser.schema import AgentSchema
        from pydantic import ValidationError
        with pytest.raises(ValidationError, match=r"Input should be greater than or equal to 1"):
            AgentSchema(max_heal_attempts_per_hour=0)

    def test_max_heal_attempts_per_hour_none_is_valid(self):
        from aqueduct.parser.schema import AgentSchema
        s = AgentSchema(max_heal_attempts_per_hour=None)
        assert s.max_heal_attempts_per_hour is None

    def test_max_heal_attempts_per_hour_valid(self):
        from aqueduct.parser.schema import AgentSchema
        s = AgentSchema(max_heal_attempts_per_hour=5)
        assert s.max_heal_attempts_per_hour == 5


# ── ProbesConfig bounds (from aqueduct/config.py) ──────────────────────────────
# Note: requires fix for ISSUE-030 (missing model_validator import in config.py)
# before these can be collected.

class TestProbesConfigBounds:
    def test_max_sample_rows_zero(self):
        from aqueduct.config import ProbesConfig
        from pydantic import ValidationError
        with pytest.raises(ValidationError, match=r"Input should be greater than or equal to 1"):
            ProbesConfig(max_sample_rows=0)

    def test_max_sample_rows_default(self):
        from aqueduct.config import ProbesConfig
        p = ProbesConfig()
        assert p.max_sample_rows == 100

    def test_default_sample_fraction_zero(self):
        from aqueduct.config import ProbesConfig
        from pydantic import ValidationError
        with pytest.raises(ValidationError, match=r"Input should be greater than 0"):
            ProbesConfig(default_sample_fraction=0)

    def test_default_sample_fraction_too_high(self):
        from aqueduct.config import ProbesConfig
        from pydantic import ValidationError
        with pytest.raises(ValidationError, match=r"Input should be less than or equal to 1"):
            ProbesConfig(default_sample_fraction=1.5)

    def test_default_sample_fraction_negative(self):
        from aqueduct.config import ProbesConfig
        from pydantic import ValidationError
        with pytest.raises(ValidationError, match=r"Input should be greater than 0"):
            ProbesConfig(default_sample_fraction=-0.5)

    def test_default_sample_fraction_valid(self):
        from aqueduct.config import ProbesConfig
        p = ProbesConfig(default_sample_fraction=0.5)
        assert p.default_sample_fraction == 0.5

    def test_default_sample_fraction_valid_one(self):
        from aqueduct.config import ProbesConfig
        p = ProbesConfig(default_sample_fraction=1.0)
        assert p.default_sample_fraction == 1.0


# ── AgentConnectionConfig bounds (from aqueduct/config.py) ─────────────────────

class TestAgentConnectionConfigBounds:
    def test_timeout_zero(self):
        from aqueduct.config import AgentConnectionConfig
        from pydantic import ValidationError
        with pytest.raises(ValidationError, match=r"Input should be greater than 0"):
            AgentConnectionConfig(timeout=0)

    def test_timeout_negative(self):
        from aqueduct.config import AgentConnectionConfig
        from pydantic import ValidationError
        with pytest.raises(ValidationError, match=r"Input should be greater than 0"):
            AgentConnectionConfig(timeout=-10.0)

    def test_timeout_valid(self):
        from aqueduct.config import AgentConnectionConfig
        c = AgentConnectionConfig(timeout=600.0)
        assert c.timeout == 600.0

    def test_timeout_default(self):
        from aqueduct.config import AgentConnectionConfig
        c = AgentConnectionConfig()
        assert c.timeout == 300.0

    def test_max_reprompts_zero(self):
        from aqueduct.config import AgentConnectionConfig
        from pydantic import ValidationError
        with pytest.raises(ValidationError, match=r"Input should be greater than or equal to 1"):
            AgentConnectionConfig(max_reprompts=0)

    def test_max_reprompts_negative(self):
        from aqueduct.config import AgentConnectionConfig
        from pydantic import ValidationError
        with pytest.raises(ValidationError, match=r"Input should be greater than or equal to 1"):
            AgentConnectionConfig(max_reprompts=-1)

    def test_max_reprompts_default(self):
        from aqueduct.config import AgentConnectionConfig
        c = AgentConnectionConfig()
        assert c.max_reprompts == 3

    def test_max_heal_attempts_per_hour_zero(self):
        from aqueduct.config import AgentConnectionConfig
        from pydantic import ValidationError
        with pytest.raises(ValidationError, match=r"Input should be greater than or equal to 1"):
            AgentConnectionConfig(max_heal_attempts_per_hour=0)

    def test_max_heal_attempts_per_hour_none_is_valid(self):
        from aqueduct.config import AgentConnectionConfig
        c = AgentConnectionConfig(max_heal_attempts_per_hour=None)
        assert c.max_heal_attempts_per_hour is None

    def test_max_heal_attempts_per_hour_valid(self):
        from aqueduct.config import AgentConnectionConfig
        c = AgentConnectionConfig(max_heal_attempts_per_hour=10)
        assert c.max_heal_attempts_per_hour == 10


# ── WebhookEndpointConfig bounds (from aqueduct/config.py) ─────────────────────

class TestWebhookEndpointConfigBounds:
    def test_timeout_zero(self):
        from aqueduct.config import WebhookEndpointConfig
        from pydantic import ValidationError
        with pytest.raises(ValidationError, match=r"Input should be greater than or equal to 1"):
            WebhookEndpointConfig(url="http://example.com/hook", timeout=0)

    def test_timeout_negative(self):
        from aqueduct.config import WebhookEndpointConfig
        from pydantic import ValidationError
        with pytest.raises(ValidationError, match=r"Input should be greater than or equal to 1"):
            WebhookEndpointConfig(url="http://example.com/hook", timeout=-5)

    def test_timeout_default(self):
        from aqueduct.config import WebhookEndpointConfig
        w = WebhookEndpointConfig(url="http://example.com/hook")
        assert w.timeout == 10

    def test_timeout_valid(self):
        from aqueduct.config import WebhookEndpointConfig
        w = WebhookEndpointConfig(url="http://example.com/hook", timeout=30)
        assert w.timeout == 30

    def test_url_is_required(self):
        from aqueduct.config import WebhookEndpointConfig
        from pydantic import ValidationError
        with pytest.raises(ValidationError, match=r"Field required"):
            WebhookEndpointConfig()