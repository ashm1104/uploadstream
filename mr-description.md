# Draft: SAAI-XXX — Design: deterministic significance for sales anomalies

## What this MR is

Design proposal only — no runtime code changes. Adds
`docs/significance-design.md` describing how we move the "is this change
significant?" decision out of the LLM and into deterministic math.

## Why

Today the model judges significance from point-in-time numbers (this window
vs LY) with no history and no sense of normal variance, so the judgment is
effectively guesswork. Our own eval rubric already scores briefings on
materiality vs normal week-to-week noise — but nothing in the pipeline
computes it. Real example from store 385 in the doc: a Produce decline that
looks alarming but is actually best-in-peer-group (noise), next to a Blooms
swing that's a genuine divergence (signal). A raw %-change ranking gets both
wrong.

## What the doc proposes

- Two tracks: a post-generation re-ranking pass (quick win — z-score only
  the departments the recommendations touch, sink the ones that fail) and a
  pre-generation data-layer computation (long term — the flag rides the data
  into the prompt, and into the SAAI-362 pipeline when it lands).
- Two axes: cross-sectional (store vs peer distribution — uses existing
  dashboard tables, no blockers) and time-series (store vs its own 28-day
  same-weekday history — pending identification of the upstream daily-grain
  table, the one open dependency).
- Robust stats (median/MAD) rather than mean/stddev, thresholds as config
  constants, edge cases and rollout plan in the doc.

## What I'd like from review

- Sanity check on the calculation approach and thresholds (§ "The
  calculation")
- The open questions at the bottom — especially the daily-grain source
  table and the peer-set choice
- Whether Track A's failed recommendations should sink silently or carry a
  visible "routine variance" annotation

Implementation will follow in a separate MR once this settles.
