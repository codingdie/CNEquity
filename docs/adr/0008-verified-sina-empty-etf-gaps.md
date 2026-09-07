# ADR 0008: Verified Sina-empty ETF gaps remain explicit source limitations

- Status: Accepted
- Date: 2026-09-07
- Relates to: [ADR-0005](0005-source-routing-vs-switching.md),
  [ADR-0007](0007-two-facts-two-columns-in-trading-status.md)

## Context

`daily_bars` is normally fail-closed: an interior `symbol x trading-session`
gap keeps its worker batch failed and blocks compact. That protects stock
history from being silently shortened when TDX or EastMoney loses coverage.

Some exchange-traded funds and LOFs, represented by `asset_type = "etf"`, are
present in instruments but have historical intervals that TDX and EastMoney do
not serve. Sina can successfully parse the same request and explicitly return
no rows. That is a source limitation, not evidence that the fund was
suspended, never traded, or had zero turnover. Treating the gap as a suspension
would manufacture a trading-status fact; keeping every other dataset behind a
permanent failed core batch makes a known limitation prevent unrelated daily
rows from publishing.

## Decision

For a missing `daily_bars` key, retain TDX -> EastMoney -> Sina recovery. The
key is publishable only when all of the following are true:

1. the final Sina request succeeded, parsed normally, and produced no rows in
   the requested range;
2. `instruments` explicitly classifies the symbol as `asset_type = "etf"`;
3. the key remains absent after the normal recovery path.

Such keys emit a `daily_bars_source_unavailable` warning. Their failed worker
batches are resolved so the valid staged core rows can compact, and the run is
degraded rather than failed. No bar, zero-volume placeholder, suspension row,
or negative `source_empty` cache entry is written. A future run makes a fresh
source attempt; this is not a permanent assertion that the fund has no data.

Stocks, unclassified instruments, partial Sina responses, transport failures,
and malformed Sina payloads remain strict coverage failures. `trading_status`
bar-gap derivation excludes the explicitly classified ETF/LOF class, so an
unavailable history interval cannot become a synthetic suspension.

## Consequences

- A limited ETF/LOF source gap no longer blocks publication of unrelated core
  daily bars.
- The limitation stays visible in run findings and makes the run degraded.
- Consumers still see an actual daily-bar gap and must not infer a halt from
  its absence.
- The exception is intentionally metadata-dependent: unknown type means no
  relaxation.

## Alternatives considered

- **Suppress all fund gaps.** Rejected because a broad prefix or name rule
  would hide ordinary coverage regressions and misclassified instruments.
- **Turn every empty vendor response into no-data evidence.** Rejected because
  a vendor empty response is not independent proof of non-trading and could
  become stale.
- **Derive `suspended` from the gap.** Rejected because missing source coverage
  cannot establish an exchange trading state.
- **Accept failed or malformed Sina calls as empty.** Rejected because that
  converts a transient source incident into a permanent-looking data fact.
