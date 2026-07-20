# Design: Deterministic Statistical Significance for Sales & Merchandising Anomalies

> **Ticket:** SAAI-XXX · **Author:** Aayush Sagar · **Status:** Draft for review — design only, no code changes in this MR
> **Reviewers requested:** Dominic Fernando, Bob (SAAI-362 owner)

## 1. Problem statement

The briefing pipeline currently asks the LLM to judge which sales changes are
significant. It cannot do this, for a structural reason: every number in the
prompt is a point-in-time snapshot (this window vs the same window last year).
The prompt contains no history, no trend, and no measure of normal variation —
so any significance judgment the model produces is necessarily invented
("vibes-based"), not computed.

Evidence from the codebase (fact-finding, 2026-07):

- The only quantitative comparison primitive anywhere is `growth_pct` — a
  single TY-vs-LY point ratio. No rolling mean, stddev, variance, or z-score
  exists in `store_data_tap` or `briefing_research`.
- The only pre-filtering is dollar floors: `MATERIAL_SALES_MIN = 100.0`,
  `DOWNSAMPLE_SIZE = 50`, `FPP_UNDER_PLAN_THRESHOLD = 0.80`. These answer
  "is it big?", never "is it unusual for this department?"
- Our own eval rubric (`briefing_research/AGENTS.md`, SAAI-261) already scores
  briefings on *"Materiality — effect big enough vs normal week-to-week
  noise?"* — i.e. we grade the model on a question the pipeline never answers.

**Goal:** compute significance deterministically (PySpark/SQL in Databricks,
plus a thin Python consumption layer), pass the computed flag through the
pipeline, and instruct the LLM to act on the flag rather than guess.

## 2. Motivating example (real data, store 385, WTD vs LY)

From `ground_truth/385-ground-truth-2026-06-17.md`:

| Department | SA08 (target) | SA29 (like) | SA23 (like) |
|---|---:|---:|---:|
| Produce | **−6.83%** | −8.89% | −15.16% |
| Blooms  | **+18.96%** | −24.04% | +0.47% |

- **Produce** looks alarming in isolation (−6.83%) — but SA08 is the *best
  performer in its peer group*. This is a market-wide dip, not a store
  problem. Surfacing it as a store-level anomaly is noise.
- **Blooms** (+18.96% against a peer at −24.04%) is a violent divergence from
  peers — genuinely anomalous and worth a recommendation.

A ranking driven by raw %-change (what the LLM effectively does today) gets
both cases wrong. Deterministic significance math gets both right.

## 3. Architecture: two tracks

Agreed direction (D. Fernando sync, 2026-07): ship a post-generation quick
fix now; build the pre-generation data-layer solution as the long-term fix.

### The decision rule that separates the tracks

> If a recommendation failing the significance test means **"sort it lower"**,
> the test may run *after* generation — it is a placement algorithm.
> If failing means **"do not show it"** (or "do not generate it"), the test
> must run *before* generation, at the data-pull stage — by then, an
> insignificant recommendation has already consumed a slot.

Track A re-ranks. Only Track B can filter or redirect generation.

### Track A — quick fix (post-generation re-ranking)

- After `sales_merchandising.research(...)` returns the in-memory `Run`
  object and **before** `database.save_run(run)` (hook point: `cron.py`,
  between pipeline steps 4 and 5), read the departments referenced by the
  generated recommendations.
- This mapping is structured, not parsed from prose: `recommendations` rows
  carry `department`, `sub_department`, `department_number`,
  `sub_department_number` columns, validated against
  `VALID_DEPARTMENT_NUMBERS` at generation time.
- For only those departments (~10 per store), compute significance per
  Section 4 — cross-sectional immediately (existing tables), time-series once
  the daily-grain source is confirmed — and attach
  `significance: pass | fail | insufficient_history` per recommendation, and
  re-sort: failures sink to the bottom of the feed.
- Storage: add a nullable JSONB or three typed columns to `recommendations`
  (`z_score`, `is_significant`, `significance_computed_at`) — additive,
  backward-compatible migration.

**Properties:** cheap (≤10 small queries/store), no prompt changes, ships
independently. **Limitation (by design):** it can demote noise the LLM wrote
about; it can never surface anomalies the LLM missed.

### Track B — long-term (pre-generation, data layer)

- A **new parallel query/computation** in `store_data_tap` (see Section 5 for
  why parallel, not modification) computes significance for *all* store ×
  department rows nightly, upstream of prompt assembly.
- New fields ride the existing path: dataclasses (`DeptPerf` gains
  `baseline`, `sigma`, `z_score`, `is_significant`) → ground-truth markdown
  (new columns in the department table) → prompt.
- `persona.py` gains an explicit rule:
  *"Statistical significance is pre-computed. Only surface department changes
  where `is_significant` is true. Never judge magnitude or significance
  yourself; cite the provided z-score when explaining why a change matters."*
- Because the flag lives in the data itself, any downstream consumer inherits
  it — including the SAAI-362 staged pipeline (Bob) when it merges: the
  fields are designed to drop into its EvidenceSet numeric corpus unchanged.

## 4. Calculation specification

### Scope (v1)

- Grain: **store × department × day**, metric = daily department sales
  (`ACTUAL_SALES` equivalent at daily grain). ~12 rows/store/day.
- Items, transactions, GP: v2 candidates once department-level is validated.

### Two complementary computations

1. **Cross-sectional z (target vs peer distribution) — no blockers, ships
   immediately.** `dsr_hist_sales_dept_dashboard_duration` already holds every
   store's TY/LY department sales per window. For a given department and
   window, pull growth across the full peer set (region slice or all ~450
   stores), and score where the target sits in that distribution using the
   same robust math below. This is exactly the Produce/Blooms logic in
   Section 2 ("−6.83% while the peer median is −9%" → not significant) and
   requires **no new tables**.
2. **Time-series robust z (target vs its own history) — needs the
   daily-grain source (open question 1).** Same-day-of-week robust z as
   specified below.

The axes are complementary: cross-sectional catches "is this store diverging
from the market?"; time-series catches "is this store diverging from its own
normal?" v1 flags on either axis and records which fired (`method` in the
output contract).

### Baseline and window (time-series axis)

- **28-day window, not 30.** 28 days = exactly four of each weekday, so the
  weekday mix inside the window is constant. A 30-day window carries an
  uneven weekday mix that shifts with the run date.
- **Primary method — same-day-of-week robust z:** compare today only against
  the same weekday in the trailing 8 weeks
  (`PARTITION BY store, dept, DAYOFWEEK(date)`), using robust statistics:

  ```
  baseline = median(same-weekday values, trailing 8 weeks)
  spread   = 1.4826 × MAD(same-weekday values)      -- MAD = median absolute deviation
  z        = (today − baseline) / spread
  ```

  Same-weekday windows eliminate day-of-week seasonality (Saturday ≠ Tuesday);
  median/MAD prevents one freak day in the history (storm, holiday) from
  inflating the spread and masking today's anomaly.
- **Shadow metric:** plain 28-day mean/stddev z computed alongside during
  rollout, for comparison and threshold tuning. Not used for flagging.

### Thresholds (named constants, `config.py` convention)

```
SIGNIFICANCE_Z_THRESHOLD   = 2.0   # |z| >= 2.0 -> significant (~<5% by chance)
SIGNIFICANCE_Z_CRITICAL    = 3.0   # |z| >= 3.0 -> critical tier
SIGNIFICANCE_MIN_WEEKS     = 8     # minimum same-weekday observations
SIGNIFICANCE_SIGMA_FLOOR   = 0.02  # spread floored at 2% of baseline
```

### Edge cases

| Case | Handling |
|---|---|
| New/remodeled store, < 8 weeks history | No flag; emit `insufficient_history = true` |
| Near-zero spread (metronomic dept) | Floor spread at `SIGNIFICANCE_SIGMA_FLOOR × baseline` to prevent z explosion |
| Zero-sales days (closure, outage) | Exclude zeros from the baseline window |
| Major holidays | Absorbed by median/MAD in v1; explicit holiday calendar exclusion is a v2 refinement |
| Department reorganization / renames | Key on department number, not name; break history on remapping |

### Output contract

One row per store × department × metric × as-of date:

```
store_id, department_number, metric, as_of_date,
actual_value, baseline_value, sigma,
z_score, is_significant (bool), significance_tier (normal|significant|critical),
method (cross_sectional | time_series), insufficient_history (bool), computed_at
```

Field names chosen to be directly insertable into the SAAI-362 EvidenceSet
numeric corpus (naming to be confirmed with Bob — open question 2).

## 5. Query strategy: parallel query, not modification of existing queries

Considered per D. Fernando: extend existing `store_data_tap` queries vs add a
standalone parallel query. Recommendation: **parallel query.**

| | Modify existing queries | **Parallel query (recommended)** |
|---|---|---|
| Feasibility | Existing queries hit `*_dashboard_duration` bucket tables that **cannot express a daily series** — modification cannot achieve the goal | New SQL against the daily-grain source table (open question 1) |
| Risk to current flows | Touches every consumer of merch ground truth | Zero — additive only |
| Runner support | n/a | `DatabricksQueryRunner.run(sql)` is fully generic (any read-only SQL string, no registration, no coupling to existing builders — verified in `query.py`) |
| Effort | High (regression surface) | One new query-builder function + one parse step, following the existing `aggregate.py` pure-function convention |

Preferred physical shape: the heavy computation (window functions over daily
history) runs **inside Databricks** — either a scheduled job materializing a
small significance table/view, or a single window-function `SELECT` executed
nightly by the runner. The Python layer only consumes the ~12 result rows per
store. Choice between materialized view vs on-the-fly query depends on the
daily table's size/latency — resolved once open question 1 is answered.

## 6. Implementation plan (code map)

### New package: `store_data_tap/src/store_data_tap/significance/`

Mirrors the existing `merch/` / `fresh_ops/` structure:

```
significance/
  __init__.py
  compute.py     # pure math - no I/O, no Databricks (unit-testable in isolation)
  queries.py     # SQL builders (plain functions returning strings)
  model.py       # SignificanceResult dataclass (the Section 4 output contract)
```

`compute.py` core (pure functions, `_shared.py` conventions — None-safe, no
side effects):

```python
def robust_z(history: list[float], today: float) -> ZResult | None:
    """Same-weekday robust z. None if len(history) < SIGNIFICANCE_MIN_WEEKS."""
    med = _median(history)
    mad = _median([abs(v - med) for v in history])
    spread = max(1.4826 * mad, SIGNIFICANCE_SIGMA_FLOOR * abs(med))
    return ZResult(z=(today - med) / spread, baseline=med, sigma=spread)

def cross_sectional_z(peer_values: list[float], target_value: float) -> ZResult | None:
    """Where does the target sit vs the peer distribution? Same math, peer axis."""
```

`queries.py` builders:

```python
def dept_peer_growth_query(dept: str, duration: str) -> str:
    # existing dept_dashboard_duration table, all stores' TY/LY for one
    # dept+window -> cross-sectional axis. No new tables.

def dept_daily_history_query(corp_id: int, days: int = 56) -> str:
    # PLACEHOLDER pending open question 1 (daily-grain table name).
```

Both plug into the generic `DatabricksQueryRunner.run(sql)` unchanged.
`config.py` gains the four `SIGNIFICANCE_*` constants (Section 4).

### Track A wiring (`briefing_research`) — three touches

1. New `significance.py` with one entry point:

   ```python
   def annotate_run(run: Run, runner) -> Run:
       depts = {r.department_number for r in run.recommendations if r.department_number}
       # compute z for only those departments -> attach -> re-sort (failures sink)
   ```

2. `cron.py` hook (between research() and save_run(), ~line 58):

   ```python
   run = sales_merchandising.research(ground_truth_set, ...)
   run = significance.annotate_run(run, runner)   # <- new line
   database.save_run(run)
   ```

3. Additive migration + matching `models.py` fields:

   ```sql
   ALTER TABLE recommendations
     ADD COLUMN z_score numeric,
     ADD COLUMN is_significant boolean,
     ADD COLUMN significance_computed_at timestamptz;
   ```

### Track B wiring — four thin touches

1. Databricks-side window-function SQL (or materialized view) over the daily
   table — written once open question 1 resolves.
2. `merch/model.py`: `DeptPerf` gains `baseline / sigma / z_score /
   is_significant`, all defaulted to `None` (existing call sites untouched).
3. `merch/aggregate.py`: one join step in `parse_departments` merging
   significance rows by department number.
4. `merch/render.py` + `persona.py`: two new department-table columns; one
   new prompt rule (Section 3, Track B).

### Build order

1. `compute.py` + unit tests — zero dependencies, zero blockers. **Start here.**
2. `config.py` constants + `model.py` dataclass.
3. `dept_peer_growth_query` + cross-sectional path — ships without waiting
   on anyone.
4. Track A hook (annotate_run, cron line, migration) — quick fix live for
   pilot stores.
5. Time-series query the day the daily table name arrives — drops into the
   already-built `compute.py`.
6. Track B render/persona changes last, behind the shadow period.

Steps 1–4 are roughly 300 lines total including tests.

## 7. Illustrative worked example (synthetic values, method demonstration)

Produce, trailing 8 Tuesdays (synthetic but realistic):
`29.8, 30.4, 28.9, 31.2, 30.1, 29.5, 44.0*, 30.6` (k$) — `*` one-off event day.

- median = 30.25 · MAD = 0.65 · spread = 1.4826 × 0.65 ≈ **0.96**
- Today = 29.36 → z = (29.36 − 30.25) / 0.96 ≈ **−0.92** → *not significant*
  (routine wobble — matches the real cross-store evidence in Section 2).
- Note the freak 44.0 day: with mean/stddev it would inflate σ to ~5.0 and
  hide real anomalies; median/MAD ignores it. Same data, spread stays honest.
- Counterexample: a day at 26.0 → z ≈ −4.4 → **critical** — flagged even
  though "only" −14% vs baseline, because Produce's normal wobble is tiny.

## 8. Rollout & validation

1. **Shadow period (1–2 weeks):** compute and log flags for pilot stores
   without changing ranking or prompts. Compare flags against the SAAI-261
   materiality rubric scores and against recommendations managers actually
   acted on.
2. **Track A live:** enable post-generation re-ranking for pilot stores.
3. **Threshold tuning:** review flag rates (expected: roughly 1–2 significant
   departments/store/day at 2σ; if every store flags 6+, threshold or method
   needs revisiting).
4. **Track B:** implement parallel query + prompt rule once the daily-grain
   source is confirmed; run both tracks briefly, then Track A's computation
   collapses into a lookup of Track B's output.

## 9. Open questions

1. **Daily-grain source table:** `prod.dsr` is a downstream analytics copy;
   no store × department × day sales table is discoverable from this repo
   (all `dsr` tables are duration-bucket grain). Need the upstream table
   name/schema from the data platform team. *Blocks the time-series
   computation on both tracks; the cross-sectional computation is unaffected
   and ships now.*
2. **Field naming with SAAI-362:** confirm output-contract names against the
   EvidenceSet numeric corpus so Bob's pipeline consumes the flag unchanged.
3. **Track A UX:** do failed recommendations sink to the bottom, or render
   with a "routine variance" annotation? (Determines whether Track A needs
   any app-side change.)
4. **Threshold governance:** are 2.0/3.0 global, or per-department overrides
   in config (e.g. inherently volatile depts like Blooms)?

## 10. Relationship to SAAI-362 (staged briefing pipeline)

Same leadership directive — deterministic rules decide, generative model
writes — different gaps: SAAI-362's verifiers guarantee stated numbers are
*true*; this work guarantees surfaced changes are *statistically real*. Its
detectors currently fire on dollar magnitude only. Because this computation
lives upstream in the data layer, the staged pipeline inherits the flag with
no rework when its branch merges; its detectors gain an "unusualness" axis to
complement the dollar-impact ranker.
