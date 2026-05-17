# aqtests — planned examples (temporary)

Goal: cover every **testable module type** (`Channel`, `Junction`,
`Funnel`, `Assert`) and every **assertion type** (`row_count`,
`contains`, `sql`) plus the tricky mechanics (NULL handling, Junction
`branch:`, multi-input). Not 20 — ~5 is enough to teach the surface.

- [x] `01_channel_dedup` — Channel; `row_count` + `contains` + `sql`;
      single upstream input.
- [ ] `02_assert_quarantine` — Assert module in isolation: assert the
      **passing** DataFrame (failing rows dropped). Shows that `aqueduct
      test` returns `passing_df` for Assert.
- [ ] `03_junction_branch` — Junction with conditional split; uses
      `branch:` to assert a non-default output branch (and a second
      case omitting `branch:` to show first-branch default).
- [ ] `04_funnel_merge` — Funnel with **two** `inputs:` entries (two
      upstream ids), assert the merged row count + `contains`.
- [ ] `05_nulls_and_sql` — explicit NULL rows (`null` in `rows`),
      `contains` with a null column (IS NULL), `sql` aggregate over
      `__output__`. Pure assertion-mechanics reference.

Stretch (only if a real gap shows up):
- [ ] custom DDL types in `schema:` (`decimal(10,2)`, `array<string>`).
- [ ] failing-case example (intentionally red) to show output format.

When all checked: fold the highlights into `aqtests/README.md`, delete
this file.
