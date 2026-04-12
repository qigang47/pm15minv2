# Chainlink Official Fallback Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an authenticated Chainlink official fallback behind Polymarket direct oracle fetches while preserving per-field source provenance and existing downstream workflows.

**Architecture:** Extend the direct-oracle source schema with field-level source columns, add a small authenticated Chainlink Data Streams REST client, and update direct-oracle canonicalization to merge boundary fields independently. Oracle/truth builders keep their existing shape and simply consume the more precise provenance.

**Tech Stack:** Python 3.11+, pandas, requests, pytest

---

### Task 1: Add the failing client tests

**Files:**
- Create: `tests/test_chainlink_streams_api.py`
- Modify: `src/pm15min/data/sources/chainlink_streams_api.py`

- [ ] **Step 1: Write the failing test**

```python
def test_fetch_report_signs_request_and_decodes_price():
    ...
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src pytest -q tests/test_chainlink_streams_api.py`
Expected: FAIL because `pm15min.data.sources.chainlink_streams_api` does not exist yet.

- [ ] **Step 3: Write minimal implementation**

```python
class ChainlinkDataStreamsApiClient:
    ...
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTHONPATH=src pytest -q tests/test_chainlink_streams_api.py`
Expected: PASS

### Task 2: Add the failing direct-oracle fallback tests

**Files:**
- Modify: `tests/test_data_builders.py`
- Modify: `src/pm15min/data/pipelines/direct_oracle_prices.py`

- [ ] **Step 1: Write the failing tests**

```python
def test_sync_direct_oracle_fills_missing_close_from_chainlink_official():
    ...

def test_sync_direct_oracle_preserves_field_level_sources_in_canonical_row():
    ...
```

- [ ] **Step 2: Run focused tests to verify they fail**

Run: `PYTHONPATH=src pytest -q tests/test_data_builders.py -k "chainlink_official or field_level_sources"`
Expected: FAIL with missing behavior or missing columns.

- [ ] **Step 3: Write minimal implementation**

```python
def _merge_direct_candidate(...):
    ...
```

- [ ] **Step 4: Run focused tests to verify they pass**

Run: `PYTHONPATH=src pytest -q tests/test_data_builders.py -k "chainlink_official or field_level_sources"`
Expected: PASS

### Task 3: Wire the new provenance into oracle/truth builders

**Files:**
- Modify: `src/pm15min/data/pipelines/oracle_prices.py`
- Modify: `src/pm15min/data/pipelines/truth.py`
- Modify: `tests/test_data_builders.py`

- [ ] **Step 1: Write the failing test**

```python
def test_build_truth_recognizes_chainlink_official_direct_source():
    ...
```

- [ ] **Step 2: Run focused test to verify it fails**

Run: `PYTHONPATH=src pytest -q tests/test_data_builders.py -k "recognizes_chainlink_official_direct_source"`
Expected: FAIL because the new source token is treated as plain oracle data.

- [ ] **Step 3: Write minimal implementation**

```python
def _normalize_oracle_source_label(value: object) -> str:
    ...
```

- [ ] **Step 4: Run focused test to verify it passes**

Run: `PYTHONPATH=src pytest -q tests/test_data_builders.py -k "recognizes_chainlink_official_direct_source"`
Expected: PASS

### Task 4: Run verification

**Files:**
- Modify: `src/pm15min/data/sources/chainlink_streams_api.py`
- Modify: `src/pm15min/data/pipelines/direct_oracle_prices.py`
- Modify: `src/pm15min/data/pipelines/oracle_prices.py`
- Modify: `src/pm15min/data/pipelines/truth.py`
- Modify: `tests/test_chainlink_streams_api.py`
- Modify: `tests/test_data_builders.py`

- [ ] **Step 1: Run targeted verification**

Run: `PYTHONPATH=src pytest -q tests/test_chainlink_streams_api.py tests/test_data_builders.py`
Expected: PASS

- [ ] **Step 2: Run a smaller regression slice for direct-oracle callers**

Run: `PYTHONPATH=src pytest -q tests/test_data_oracle_api.py tests/test_data_direct_oracle_prices.py tests/test_data_foundation_runtime.py`
Expected: PASS
