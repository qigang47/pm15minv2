# Chainlink Official Fallback Design

## Goal

Make the direct oracle backfill path prefer Polymarket prices, then use the authenticated Chainlink official API to fill any missing boundary price, and only fall back to the existing free Polygon RPC streams at table-build time.

## Scope

- Keep the existing direct-oracle pipeline as the primary entrypoint.
- Fill `price_to_beat` and `final_price` independently.
- Preserve source provenance per field so downstream truth labels can tell whether a boundary came from Polymarket or Chainlink.
- Do not change experiment entrypoints or research workflow wiring.

## Source Priority

Per boundary field:

1. Polymarket direct API / event-page fallback
2. Authenticated Chainlink official Data Streams REST API
3. Existing free Polygon RPC streams fallback during oracle table build

Within the direct-oracle source parquet:

- Never drop an already-populated field just because another candidate only fills the other field.
- When the same field exists from multiple direct sources, keep the higher-priority source.
- Keep the row if a rerun only improves completeness.

## Design

### Direct Oracle Source Schema

Extend direct-oracle rows with field-level provenance:

- `source_price_to_beat`
- `source_final_price`

Retain the legacy `source` column as a row summary for compatibility.

### Chainlink Official Client

Add a dedicated authenticated Data Streams REST client that:

- Reads API credentials from environment
- Signs requests with the official HMAC header format
- Fetches a report for a specific `feedID` and timestamp
- Decodes the returned `fullReport` payload into a numeric benchmark price

### Direct Oracle Merge

Replace whole-row winner-take-all behavior with field-wise merge behavior so:

- Polymarket can fill the open while Chainlink fills the close
- Existing complete rows are not downgraded by partial reruns
- Higher-priority direct sources can still upgrade a lower-priority field

### Downstream Table Build

When building `oracle_prices` from the direct-oracle source:

- Prefer `source_price_to_beat` / `source_final_price` if present
- Fall back to legacy `source` for old rows
- Keep the existing free-streams fallback unchanged

### Truth Labeling

Treat the new official Chainlink direct source as Chainlink-backed when classifying truth source labels.

## Validation

- Unit-test Chainlink auth header generation and report decoding.
- Unit-test direct-oracle field-wise fallback: Polymarket open + Chainlink close.
- Unit-test direct-oracle canonical merge so complete rows are preserved.
- Unit-test truth-source normalization for the new official Chainlink source token.
