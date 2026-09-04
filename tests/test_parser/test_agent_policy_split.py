"""2.59 — the Blueprint `agent:` block became POLICY-ONLY (security fix): a
Blueprint's `agent:` block can no longer set CONNECTION fields (`provider`,
`base_url`, `api_key`, `model`, `provider_options`, `timeout`, `cascade`),
which live exclusively in `aqueduct.yml`'s `agent:` block
(`AgentConnectionConfig`). Otherwise a pipeline author could redirect the
healing loop's `FailureContext` (pruned manifest, provenance, error text)
to an arbitrary host on any failure.

Both `AgentSchema` (Blueprint, `aqueduct/parser/schema.py`) and
`AgentConnectionConfig` (engine, `aqueduct/config.py`) extend a shared
`AgentPolicySchema` base for the POLICY fields, so a policy field can't be
added to only one side by oversight. This file asserts that structurally —
by comparing field SETS, not by listing field names — so the guard survives
a future field addition to the base without needing an update here.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from aqueduct.config import AgentConnectionConfig
from aqueduct.parser.schema import AgentPolicySchema, AgentSchema

pytestmark = pytest.mark.unit

# CONNECTION fields — legal ONLY on AgentConnectionConfig, illegal on AgentSchema.
_CONNECTION_FIELDS = frozenset(
    {
        "provider",
        "base_url",
        "api_key",
        "model",
        "provider_options",
        "timeout",
        "cascade",
    }
)


class TestStructuralFieldSetRelationship:
    """The load-bearing guarantee: AgentPolicySchema's field set is a
    SUBSET of both AgentSchema's and AgentConnectionConfig's — so adding a
    policy field to the base makes it nameable on both sides automatically,
    and the only way to violate that is to bypass the base entirely (which
    this test would catch: the new field would be present on one side and
    absent, structurally, from AgentPolicySchema)."""

    def test_policy_schema_is_subset_of_blueprint_agent_schema(self):
        assert set(AgentPolicySchema.model_fields) <= set(AgentSchema.model_fields)

    def test_policy_schema_is_subset_of_engine_agent_connection_config(self):
        assert set(AgentPolicySchema.model_fields) <= set(AgentConnectionConfig.model_fields)

    def test_agent_schema_is_a_subclass_of_policy_schema(self):
        assert issubclass(AgentSchema, AgentPolicySchema)

    def test_agent_connection_config_is_a_subclass_of_policy_schema(self):
        assert issubclass(AgentConnectionConfig, AgentPolicySchema)

    def test_connection_fields_are_disjoint_from_policy_schema(self):
        # Connection fields must never leak into the shared policy base —
        # that would make them legal on the Blueprint again by inheritance.
        assert not (_CONNECTION_FIELDS & set(AgentPolicySchema.model_fields))


class TestBlueprintRejectsConnectionFields:
    """Every CONNECTION field is a named pydantic rejection on AgentSchema —
    `extra="forbid"` (inherited from AgentPolicySchema), never a silent
    inherit-and-ignore. Parametrized over the SAME set the structural test
    above defines, so a field added to _CONNECTION_FIELDS is automatically
    covered here too."""

    @pytest.mark.parametrize("field", sorted(_CONNECTION_FIELDS))
    def test_field_not_in_blueprint_schema(self, field):
        assert field not in AgentSchema.model_fields

    @pytest.mark.parametrize("field", sorted(_CONNECTION_FIELDS))
    def test_field_in_engine_connection_config(self, field):
        assert field in AgentConnectionConfig.model_fields

    def test_base_url_rejected_by_name(self):
        with pytest.raises(ValidationError, match="base_url"):
            AgentSchema(base_url="http://evil.example.com/v1")

    def test_api_key_rejected_by_name(self):
        with pytest.raises(ValidationError, match="api_key"):
            AgentSchema(api_key="sk-literal")

    def test_model_rejected_by_name(self):
        with pytest.raises(ValidationError, match="model"):
            AgentSchema(model="claude-sonnet-4-6")

    def test_provider_rejected_by_name(self):
        with pytest.raises(ValidationError, match="provider"):
            AgentSchema(provider="anthropic")

    def test_provider_options_rejected_by_name(self):
        with pytest.raises(ValidationError, match="provider_options"):
            AgentSchema(provider_options={"temperature": 0.7})

    def test_timeout_rejected_by_name(self):
        with pytest.raises(ValidationError, match="timeout"):
            AgentSchema(timeout=120.0)

    def test_cascade_rejected_by_name(self):
        with pytest.raises(ValidationError, match="cascade"):
            AgentSchema(cascade=[{"model": "claude"}])


class TestPolicyFieldsStillLegalOnBlueprint:
    """Policy fields — including the ones AgentSchema declares directly
    (no engine-level equivalent) and the ones shared via AgentPolicySchema —
    remain legal (and still override the aqueduct.yml value when set).
    See test_cli_agent_warning.py / resolve_agent_connection tests for the
    full engine<-blueprint merge behaviour; this only proves the Blueprint
    schema itself accepts them."""

    def test_blueprint_only_policy_fields_accepted(self):
        s = AgentSchema(
            approval="auto",
            on_pending_patches="block",
            max_patches=3,
            guardrails={"forbidden_ops": ["remove_module"]},
            confidence_threshold=0.9,
            on_heal_failure="abort",
            allow_defer=True,
            deep_loop=True,
            sandbox_mode="preflight",
        )
        assert s.approval_mode == "auto"
        assert s.max_patches == 3
        assert s.sandbox_mode == "preflight"

    def test_shared_policy_fields_accepted_and_default_to_none(self):
        s = AgentSchema()
        assert s.max_reprompts is None
        assert s.prompt_context is None
        assert s.max_heal_attempts_per_hour is None
        assert s.patch_validation is None

    def test_shared_policy_field_override_accepted(self):
        s = AgentSchema(max_reprompts=3)
        assert s.max_reprompts == 3


class TestEngineAgentConnectionConfigAcceptsFullSet:
    """aqueduct.yml's agent: block (AgentConnectionConfig) still accepts
    every connection field AND every policy field — the split narrows the
    Blueprint, not the engine config."""

    def test_full_connection_set_accepted(self):
        cfg = AgentConnectionConfig(
            provider="openai_compat",
            base_url="http://localhost:11434/v1",
            api_key="sk-literal",
            model="qwen2.5-coder:7b",
            provider_options={"temperature": 0.7},
            timeout=120.0,
            cascade=[{"model": "small"}, {"model": "big", "provider": "anthropic"}],
        )
        assert cfg.provider == "openai_compat"
        assert cfg.base_url == "http://localhost:11434/v1"
        assert cfg.api_key == "sk-literal"
        assert cfg.model == "qwen2.5-coder:7b"
        assert cfg.provider_options == {"temperature": 0.7}
        assert cfg.timeout == 120.0
        assert len(cfg.cascade) == 2

    def test_policy_field_overrides_accepted_with_concrete_defaults(self):
        # Unlike AgentSchema, AgentConnectionConfig's shared policy fields
        # have CONCRETE (non-None) engine-wide defaults — there is nothing
        # above it to inherit from.
        cfg = AgentConnectionConfig()
        assert cfg.max_reprompts == 3
        assert cfg.patch_validation == "full_run"

    def test_engine_only_policy_fields_have_no_blueprint_equivalent(self):
        # approval/on_pending_patches/max_patches/guardrails/
        # confidence_threshold/on_heal_failure/allow_defer/deep_loop/
        # sandbox_mode are Blueprint-only by design (a risk decision about
        # ONE pipeline has no coherent engine-wide default) — asserting
        # their absence here pins that design choice so a future PR can't
        # silently add a dead field to AgentConnectionConfig.
        blueprint_only = {
            "approval_mode",
            "on_pending_patches",
            "max_patches",
            "guardrails",
            "confidence_threshold",
            "on_heal_failure",
            "allow_defer",
            "deep_loop",
            "sandbox_mode",
        }
        assert not (blueprint_only & set(AgentConnectionConfig.model_fields))
