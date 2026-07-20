# Cheat Sheet — Deterministic Significance (Z-Score) Step

## 1. Today's flow (one line per step)
`cron.py` picks store + 2 like-stores (`store_groups.yaml`) → `store_data_tap` pulls **pre-aggregated buckets** (Yesterday / WTD / Last Week) from Databricks via generic read-only runner → `aggregate.py` shapes into dataclasses → `render.py`/`groundtruth.py` write markdown docs → GCS → `sales_merchandising.py` concatenates persona + target + like-store docs into **one big text prompt** → Gemini (Vertex) returns recommendations JSON → validated → in-memory `Run` → `database.save_run()` → Postgres (`runs` / `recommendations` / `actions`) → manager's app.

## 2. The problem (one sentence)
**The LLM decides what's "significant" — and we give it zero information to decide with.**

Three facts from code research:
- Every number in the prompt is a **snapshot** (this window vs same window LY). No history, no trend, no variance anywhere.
- Only filtering in the whole pipeline = **dollar floors** ($100 item min, top-50 caps, 80% FPP). Zero statistics in the codebase.
- `AGENTS.md` eval rubric (SAAI-261) already **grades** briefings on "Materiality — effect big enough vs normal week-to-week noise?" — a question the pipeline never computes. We grade what we don't calculate.

## 3. The killer example (real store-385 data, WTD vs LY)
- **Produce**: SA08 **−6.83%** … but peers SA29 **−8.89%**, SA23 **−15.16%** → SA08 is the **best in its peer group**. Market-wide dip, not a store problem. Flagging it = noise.
- **Blooms**: SA08 **+18.96%** vs SA29 **−24.04%** → violent divergence from peers. Genuinely anomalous, worth a card.
- Naked %-change ranking gets **both wrong**. Deterministic math gets both right.

## 4. The fix (one breath)
Per store × department, from ~28 days of daily history: baseline + normal wobble + **z = (today − baseline) / wobble**. |z| ≥ 2 → significant (<5% chance it's noise). Deterministic: same data in, same flag out. Refinements: **28 days not 30** (exactly 4 of each weekday), **same-day-of-week windows** (Tue vs past Tuesdays), **median/MAD** (freak days can't poison the baseline). Prompt rule change: *significance is pre-computed — never judge magnitude yourself.*

## 5. Two tracks (agreed with Dominic)
- **Track A — quick fix, post-generation**: recommendations carry structured `department_number` columns → z-score **only the ~10 departments mentioned** → pass/fail flag → re-sort, failures sink. Hook: `cron.py`, between in-memory `Run` and `save_run()`.
- **Track B — long-term, pre-generation**: new **parallel query** in `store_data_tap` computes significance for all depts nightly, upstream of everything; fields ride data → markdown → prompt → and into Bob's pipeline when it lands.

**Dominic's decision rule (quote it):** if failing the test means *sort lower* → post-generation is enough (placement). If it means *never show / never generate* → must be pre-generation, at data pull. **A re-ranks; only B filters.**

## 6. Why parallel query, not modifying existing ones
Runner is fully generic (`run(sql)`, any read-only string, zero coupling). New query = one new function; existing flows untouched; zero risk. Modifying old queries buys nothing — bucket tables physically can't express a 28-day series.

## 7. Bob (SAAI-362) — cooperation, not collision
Same philosophy (deterministic decides, LLM writes), **different problems**: his verifiers prove numbers are *true*; our flag proves they're *worth saying*. His detectors fire on dollar size only. We're upstream — he inherits our flag for free.

## 8. The asks (close here)
1. **Daily-grain table name** — `prod.dsr` is a downstream copy; no store × dept × day sales table visible from repo. Only real blocker, only for Track B.
2. **Sync with Bob** on field naming (drop-in to his EvidenceSet later).
3. **GitLab access** (in progress) → design doc goes up as **draft MR** on ticket branch.
