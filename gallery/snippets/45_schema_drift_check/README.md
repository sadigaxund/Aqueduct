# Schema drift check

Demonstrates `aqueduct drift`, the proactive schema-drift check. Where
`aqueduct heal` fixes a pipeline after it fails, `drift` reads the live
source schema ahead of the run and compares it against the last-seen
baseline, so a breaking upstream change is caught (and can be pre-emptively
healed) before the pipeline ever runs.

## Setup

```bash
pip install -r requirements.txt
```

## How it works

The blueprint has two independent Ingress sources, `orders_a` and
`orders_b`, each read through a passthrough Channel into its own Egress.
`aqueduct drift` checks every Ingress module's live schema against the
schema it last saw (self-owned, stored in `drift_checks`, no `schema_
snapshot` Probe required):

- A **dropped** or **type-changed** column is classified `breaking`: a
  downstream Channel naming that column would fail. `drift` builds a
  synthetic failure context and asks the agent to propose a fix, exactly
  like a real self-heal, except before anything ran.
- An **added** column is classified `benign`: a `SELECT named_cols`
  pipeline does not break on a superset source, so it's recorded for audit
  but does not trigger a heal.

`orders_a` plays out the breaking story (its `status` column is dropped
upstream); `orders_b` plays out the benign one (a new `region` column
shows up upstream). A rename would surface as a drop (breaking) plus an
add (benign) on the same module: the drop fires the heal and the added
name is offered to the agent as a rename candidate.

## How to run

```bash
python populate_data.py            # writes the baseline schema for both sources
aqueduct drift blueprint.yml       # first check: no prior baseline, so it's just recorded

python populate_data.py drift      # orders_a drops 'status', orders_b adds 'region'
aqueduct drift blueprint.yml       # second check: breaking for orders_a, benign for orders_b

python inspect_results.py          # print every recorded drift_checks row
```

Expected `aqueduct drift` output on the second run:

```
⚠ orders_a: breaking drift
    · column 'status' dropped (was string)
  → no patch (agent disabled or failed to produce one)
  ◦ orders_b: benign, column 'region' added (string) (no heal)
```

`drift` exits `0` when nothing breaking is found, a `HEAL_PENDING` code
when a patch got staged, and a data/runtime code when a source schema
could not be read at all. Schedule it ahead of your batch window (cron,
Airflow, whatever runs before the pipeline) so a breaking change is healed
before the real run ever sees it.

**Requires an LLM provider** to actually stage a heal patch for the
breaking column (set `AQ_OPENAI_API_KEY` / `AQ_OPENAI_BASE_URL`, or
configure `agent:` in `aqueduct.yml`). Without one, `drift` still detects
and reports the change correctly, it just can't propose a fix, same as
`aqueduct heal` without a provider.
