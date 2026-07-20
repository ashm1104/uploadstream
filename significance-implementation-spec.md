# Implementation Spec: Deterministic Significance — Phase 1 (Cross-Sectional) + Scaffolding

> **Audience:** Claude Code, working in the `store-leader-assist` repo.
> **Ticket:** SAAI-XXX. **Companion design doc:** `docs/significance-design.md`.
> This spec is self-contained; verify stated code facts against the repo before
> relying on them, but do not re-derive the design.

## 0. Objective and scope

Add a deterministic statistical-significance capability so the pipeline — not
the LLM — decides which department sales changes are real anomalies.

Two axes exist in the design:

- **Cross-sectional** (target store vs the distribution of all stores for the
  same department + window) — **fully implementable now**, uses only the
  existing `dsr_hist_sales_dept_dashboard_duration` table.
- **Time-series** (store vs its own 28-day same-weekday history) — **blocked**
  on an upstream daily-grain table whose name is not yet known.
  **Stub it; do not invent a table name.**

### In scope (implement fully)

1. New package `store_data_tap/src/store_data_tap/significance/` — pure math,
   models, SQL builders, with unit tests.
2. New config constants in `store_data_tap` `config.py`.
3. Track A integration in `briefing_research`: `significance.py` with
   `annotate_run()`, a one-line hook in `cron.py`, an additive Postgres
   migration, and matching dataclass fields in `models.py`.

### Out of scope (do NOT implement in this MR)

- Any change to `merch/render.py`, `persona.py`, or prompt content.
- Any change to existing query builders in `merch/queries.py` or
  `fresh_ops/queries.py`.
- The time-series query body (placeholder only).
- Databricks-side jobs, views, or any write to Databricks (runner is
  read-only; keep it that way).
- App/UI changes. Failed recommendations are re-sorted, nothing hidden.

## 1. Verified codebase facts you may rely on

- `DatabricksQueryRunner` (`store_data_tap/.../query.py`): `run(sql: str) ->
  list[list]`, guarded by `assert_read_only(sql)`, accepts an injectable
  `execute: Callable[[str], list[list]]` for tests. All values come back as
  `str | None`.
- `dsr_hist_sales_dept_dashboard_duration`: grain store × product-group ×
  category × sub-dept × dept × duration-bucket. Columns include `ID_STORE`,
  `DSC_DEPT`, `ACTUAL_SALES`, `LY_ACTUAL_SALES`, `DSC_DURATION_TYP` with
  values `"Yesterday" | "WTD" | "Last Week"` (see `DURATION_CODES` in
  `config.py`). It contains **all stores**, not just the target.
- Numeric conventions live in `store_data_tap/.../_shared.py`: `growth_pct`,
  `_f`, `_pct`-style helpers — pure, None-safe, return `None` on bad input.
- `briefing_research/cron.py` nightly sequence: like-store resolution →
  ground-truth build → `ground_truth.load` → `run =
  sales_merchandising.research(...)` (in-memory `Run`) →
  `database.save_run(run)`. The hook goes between the last two calls.
- `Run.recommendations` items carry validated `department`, `sub_department`,
  `department_number`, `sub_department_number` attributes (may be None on
  store-level recommendations).
- Postgres `recommendations` table exists per migrations
  `1704067200003/5/8`; migrations are timestamped SQL files.

If any of these differ from the actual code, stop and report the difference
instead of improvising.

## 2. New package: `store_data_tap/src/store_data_tap/significance/`

### 2.1 `model.py`

```python
from dataclasses import dataclass

@dataclass
class ZResult:
    z: float
    baseline: float   # median of the comparison set
    sigma: float      # robust spread actually used (after floor)
    n: int            # observations in the comparison set

@dataclass
class SignificanceResult:
    department_number: str | None
    department: str | None
    window: str                       # "wtd" | "yesterday" | "last-week"
    method: str                       # "cross_sectional" (only value in Phase 1)
    target_value: float | None        # target store growth_pct
    z_score: float | None
    baseline: float | None
    sigma: float | None
    peer_count: int | None
    is_significant: bool | None       # None => could not be computed
    tier: str                         # "normal" | "significant" | "critical" | "insufficient_data"
```

### 2.2 `compute.py` — pure math only

No imports from query/runner modules. No I/O. Follow `_shared.py` style.

```python
def _median(values: list[float]) -> float: ...
def _mad(values: list[float], med: float) -> float: ...

def robust_z(comparison: list[float], target: float) -> ZResult | None:
    """Robust z of `target` against `comparison` (which EXCLUDES target).
    Returns None if len(comparison) < SIGNIFICANCE_MIN_PEERS.
    spread = max(1.4826 * MAD, SIGNIFICANCE_SIGMA_FLOOR * max(abs(median), SIGMA_FLOOR_ABS_MIN))
    z = (target - median) / spread
    """

def classify(z: float | None) -> str:
    """None -> insufficient_data; |z| >= CRITICAL -> critical;
    |z| >= THRESHOLD -> significant; else normal."""
```

Notes:
- MAD = median(|x − median|). The 1.4826 factor makes MAD comparable to a
  standard deviation under normality.
- The sigma floor prevents z explosions when peers are nearly identical.
  `SIGMA_FLOOR_ABS_MIN` (see constants) guards the growth_pct≈0 case where a
  purely relative floor would collapse.
- Deterministic: no randomness, no time reads inside compute functions.

### 2.3 `queries.py`

```python
def dept_peer_growth_query(dept: str, duration_code: str) -> str:
    """All stores' TY/LY sales for one department and one duration bucket.
    SELECT ID_STORE, ACTUAL_SALES, LY_ACTUAL_SALES
    FROM {DEPT_TABLE}
    WHERE DSC_DEPT = '{dept}' AND DSC_DURATION_TYP = '{duration_code}'
    Reuse the exact table constant/reference style used by merch/queries.py.
    Escape/validate `dept` the same way existing builders handle string params.
    """

def dept_daily_history_query(corp_id: int, days: int = 56) -> str:
    """TIME-SERIES AXIS - BLOCKED. The daily-grain (store x dept x day) source
    table is not identified yet (design doc, open question 1).
    raise NotImplementedError("daily-grain source table pending - SAAI-XXX")
    """
```

### 2.4 `aggregate.py` (in the significance package)

Bridges rows → math, mirroring merch conventions (pure over parsed rows,
runner injected):

```python
def peer_significance(
    runner, dept: str, duration_code: str, target_corp_id: int,
) -> SignificanceResult:
    """1. rows = runner.run(dept_peer_growth_query(dept, duration_code))
       2. per row: growth = growth_pct(actual, ly); drop rows where either
          value is None or actual < SIGNIFICANCE_PEER_SALES_MIN (thin peers
          pollute the distribution)
       3. split target row out of peers by ID_STORE == target_corp_id;
          if target missing or its growth is None -> insufficient_data result
       4. zr = robust_z(peer_growths, target_growth)
       5. return SignificanceResult(...)  # tier via classify()
    """
```

## 3. Config constants (`store_data_tap` `config.py`)

Append, following the existing commented-constant style:

```python
# --- Statistical significance (SAAI-XXX) ---
SIGNIFICANCE_Z_THRESHOLD = 2.0      # |z| >= 2.0 -> significant
SIGNIFICANCE_Z_CRITICAL = 3.0       # |z| >= 3.0 -> critical
SIGNIFICANCE_MIN_PEERS = 20         # min peer stores for cross-sectional z
SIGNIFICANCE_PEER_SALES_MIN = 500.0 # ignore peers with < $500 dept sales in window
SIGNIFICANCE_SIGMA_FLOOR = 0.10     # spread floored at 10% of |median growth|
SIGMA_FLOOR_ABS_MIN = 1.0           # ...and never below 1.0 growth-pct point
SIGNIFICANCE_WINDOWS = ("wtd", "yesterday")  # windows checked by Track A
```

Values are initial proposals — keep them as constants so tuning is a one-line
change; do not scatter literals.

## 4. Track A integration (`briefing_research`)

### 4.1 New file `src/briefing_research/significance.py`

```python
def annotate_run(run: Run, runner) -> Run:
    """Post-generation, pre-persist significance pass. MUST NEVER raise:
    wrap the whole body; on any exception, log a warning and return `run`
    unchanged (best-effort, same philosophy as fresh-ops enrichment).

    1. depts = ordered-unique department numbers+names from
       run.recommendations where department is not None.
       Store-level recommendations (department None) are left untouched
       (fields stay None, sort treats them as pass).
    2. For each dept and each window in SIGNIFICANCE_WINDOWS: call
       store_data_tap significance.aggregate.peer_significance(...).
       Map window name -> duration code via the existing WINDOW_CODES map.
       Cache per (dept, window): one query per pair, deduped across
       recommendations.
    3. Per recommendation: significant if ANY checked window is
       significant/critical; fails only if ALL checked windows are 'normal';
       insufficient_data anywhere -> treat as pass (never demote on missing
       data). Attach: z_score (max |z| across windows), is_significant,
       significance_computed_at (utcnow).
    4. Stable re-sort: preserve the LLM's original order within groups;
       is_significant is False sinks below True/None.
    """
```

### 4.2 `cron.py`

Exactly one insertion between research and persist:

```python
run = sales_merchandising.research(...)          # existing
run = significance.annotate_run(run, runner)     # NEW
database.save_run(run)                           # existing
```

Reuse the runner instance already constructed for ground-truth pulls if one
is in scope; otherwise construct `DatabricksQueryRunner()` the same way the
ground-truth step does.

### 4.3 Migration (new timestamped SQL, additive only)

```sql
ALTER TABLE recommendations
  ADD COLUMN z_score numeric,
  ADD COLUMN is_significant boolean,
  ADD COLUMN significance_computed_at timestamptz;
```

Down migration drops the three columns. No changes to existing columns,
constraints, or the rename history.

### 4.4 `models.py` + `database.py`

- Add `z_score: float | None = None`, `is_significant: bool | None = None`,
  `significance_computed_at: datetime | None = None` to the recommendation
  dataclass (defaulted — existing constructors unchanged).
- `save_run()` writes the new columns; NULLs when absent. Reading paths must
  tolerate NULLs (old rows).

## 5. Tests (required; use the injectable executor — no live Databricks)

`store_data_tap` significance tests:

1. `robust_z`: hand-computed case — comparison
   `[-9.0, -8.5, -10.1, -9.4, -8.8, -9.9, -9.2, -8.6, ...]` (≥ MIN_PEERS
   values around −9), target `-6.83` → z positive, small → `normal`.
   (This mirrors the real store-385 Produce case: best-in-peer-group must
   NOT flag.)
2. Divergence case: same peers, target `+18.96` → `critical`. (The Blooms
   case: strong divergence MUST flag.)
3. Outlier robustness: one peer at `+44.0` among tight peers — spread must
   barely move vs the same set without the outlier (median/MAD property).
4. Sigma floor: all peers identical → z finite, no ZeroDivisionError.
5. `< SIGNIFICANCE_MIN_PEERS` peers → `None` → tier `insufficient_data`.
6. `peer_significance`: fake executor returning string rows (runner returns
   `str | None`) — verify string→float coercion, `SIGNIFICANCE_PEER_SALES_MIN`
   filtering, target-row extraction, and target-missing → insufficient_data.
7. `assert_read_only` passes for `dept_peer_growth_query` output.

`briefing_research` tests:

8. `annotate_run` with a stubbed significance function: mixed
   pass/fail/None recommendations → verify sort order (False sinks; ties
   keep original order), None treated as pass, store-level recs untouched.
9. `annotate_run` with a significance function that raises → returns the run
   unchanged, warning logged.
10. `save_run` round-trip with and without the new fields (NULL tolerance).

## 6. Acceptance checklist

- [ ] `pytest` green in both packages; no existing test modified except
      where a dataclass gained defaulted fields.
- [ ] No diff in `merch/queries.py`, `fresh_ops/queries.py`, `render.py`,
      `persona.py`, or any prompt/markdown output.
- [ ] Grep shows no new numeric literals for thresholds outside `config.py`.
- [ ] `annotate_run` failure path proven: kill the executor mid-run in a
      test — briefing still persists.
- [ ] `dept_daily_history_query` raises `NotImplementedError` with the
      ticket reference (searchable stub for the follow-up MR).
- [ ] Migration applies and rolls back cleanly on a scratch DB.
- [ ] Commit messages reference SAAI-XXX; MR opened as **Draft** against the
      ticket branch, linking `docs/significance-design.md`.

## 7. Explicit non-goals / guardrails

- Never write to Databricks; never weaken `assert_read_only`.
- Never hide or delete a recommendation — Phase 1 only re-sorts and annotates.
- Never demote on missing/insufficient data.
- Do not "helpfully" implement the time-series axis against a guessed table.
- Do not refactor unrelated code encountered along the way; note it in the MR
  description instead.
