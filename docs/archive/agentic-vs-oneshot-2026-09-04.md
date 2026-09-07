# Agentic vs oneshot measurement (2026-09-04)

Empirical A/B of `agent.mode=oneshot` vs `agent.mode=agentic` over the full
`gallery/aqscenarios/` suite (19 scenarios), same cheap model, same budget,
run sequentially (never in parallel) via `aqueduct benchmark`.

- Model: `deepseek-v4-flash` (DeepSeek's OpenAI-compatible API). Discovered
  via `GET https://api.deepseek.com/models`: a "v4" + "flash" id exists, so
  the `deepseek-chat` fallback named in the brief was not needed.
- Provider: `openai_compat`, `base_url=https://api.deepseek.com/v1`.
- `agent.provider_options.max_tokens=16000`, `agent.budget.max_seconds=600`,
  `agent.timeout=600`.
- Persisted per-pair rows (incl. `tokens_in_total`/`tokens_out_total`, which
  the CLI's `--format json` does not surface) to a scratch DuckDB store per
  mode and read them back with `duckdb`.

## Results

| # | Scenario | Oneshot pass | Oneshot s | Oneshot tok in | Oneshot tok out | Agentic pass | Agentic s | Agentic tok in | Agentic tok out |
|---|----------|:---:|---:|---:|---:|:---:|---:|---:|---:|
| 1 | schema_drift_column_rename | PASS | 6.8 | 9,368 | 859 | PASS | 12.8 | 21,809 | 1,522 |
| 2 | sql_bad_column_ref | PASS | 3.9 | 9,215 | 512 | PASS | 2.6 | 10,509 | 281 |
| 3 | format_csv_read_as_parquet | PASS | 3.1 | 9,195 | 364 | PASS | 10.2 | 33,263 | 1,134 |
| 4 | bad_path_typo | PASS | 3.6 | 9,211 | 502 | PASS | 3.5 | 10,505 | 432 |
| 5 | type_string_vs_numeric | PASS | 81.0 | 9,251 | 9,920 | PASS | 63.8 | 35,950 | 7,142 |
| 6 | guardrail_forbidden_op | PASS | 14.9 | 9,212 | 1,924 | **FAIL** | 600.9 | 0 | 0 |
| 7 | spark_oom_shuffle | PASS | 107.2 | 9,459 | 12,271 | PASS | 157.6 | 54,416 | 18,922 |
| 8 | delta_schema_merge | PASS | 13.0 | 9,216 | 1,715 | PASS | 7.8 | 10,510 | 1,026 |
| 9 | broadcast_join_timeout | PASS | 10.1 | 9,411 | 1,331 | PASS | 42.6 | 26,512 | 5,119 |
| 10 | small_files | PASS | 9.2 | 9,238 | 1,384 | PASS | 5.4 | 10,532 | 651 |
| 11 | driver_max_result_size | PASS | 5.3 | 9,268 | 728 | PASS | 6.6 | 10,562 | 718 |
| 12 | engine_config_denied_key | PASS | 114.5 | 19,372 | 13,336 | PASS | 59.6 | 27,189 | 6,283 |
| 13 | engine_config_inert_write | **FAIL** | 600.9 | 0 | 0 | **FAIL** | 221.8 | 0 | 0 |
| 14 | duckdb_memory_limit | PASS | 6.8 | 9,196 | 920 | PASS | 9.2 | 21,968 | 1,036 |
| 15 | schema_hint_type_mismatch | PASS | 4.5 | 8,965 | 670 | PASS | 7.8 | 21,136 | 796 |
| 16 | field_not_found_join_ref | PASS | 3.6 | 9,198 | 483 | PASS | 5.0 | 21,443 | 540 |
| 17 | backtick_reserved_identifier | PASS | 4.1 | 8,940 | 532 | PASS | 5.0 | 21,020 | 515 |
| 18 | two_independent_bugs | PASS | 3.0 | 9,250 | 312 | PASS | 10.0 | 21,960 | 1,083 |
| 19 | agentic_vs_oneshot_lineage_field | PASS | 6.6 | 9,504 | 947 | PASS | 9.6 | 23,087 | 1,205 |
| | **Totals (19 pairs)** | **18/19 (94.7%)** | **1,002.0s** | **176,469** | **48,710** | **17/19 (89.5%)** | **1,242.0s** | **382,371** | **48,405** |

Both failures are environmental, not aqueduct-side:
- `engine_config_inert_write` failed in **both** modes. Oneshot hit the
  600s socket read timeout (`budget_seconds_exceeded`: "The read operation
  timed out"). Agentic got an `api_error`: DeepSeek returned a
  `finish_reason='length'` truncation at the 16,000-token cap before
  producing a parseable response: a token-budget problem, not a malformed
  response.
- `guardrail_forbidden_op` failed **only** in agentic mode: it burned the
  full 600s budget and hit the same read-timeout stop reason
  (`budget_seconds_exceeded`) instead of returning a PatchSpec: one slow
  DeepSeek tool-calling round trip ate the whole per-pair budget.

## Verdict

Agentic mode does **not** beat oneshot on this suite: pass rate is lower
(17/19 vs 18/19: agentic introduced one additional environmental failure
that oneshot didn't hit), total wall-clock time is ~24% higher (1,242s vs
1,002s), and total input tokens are **~2.17x** higher (382,371 vs 176,469)
for essentially the same output-token spend (48,405 vs 48,710): the tool
round trips inflate the prompt side of the bill without buying a
diagnostic-quality or pass-rate improvement on this scenario suite. Per the
cleanup decision rule, this result favors deleting agentic mode
(`agent/toolbox.py`, `aqueduct/tools/`, `agent.mode`/`max_tool_calls`/
`supports_tools`, `tool_calls_json`, schema-reader hooks, `tests/test_tools/`,
`tools-tests` lane, specs §8.10/§8.12) rather than keeping it: that deletion
is a separate, not-yet-taken action.
