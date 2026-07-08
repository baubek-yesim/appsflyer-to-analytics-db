# Issues #15, #17 Hygiene Batch Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a guard against AppsFlyer's undocumented-at-runtime 1M-row Pull API export cap
(#15), and fix four small non-behavioral doc/config issues found during a full repo review (#17).

**Architecture:** Two independent, non-overlapping-file tasks. Task 1 is a TDD code change
(one new branch in `fetch_events`, one new test). Task 2 is four mechanical doc/config edits
with no new tests (verified by existing `pre-commit`/`ruff`/`pytest` gates, not new test code).

**Tech Stack:** Python 3.12, polars, pytest + respx (existing conventions only — no new deps).

## Global Constraints

- Do not enable the `S` (flake8-bandit) or `RUF` rule groups in `pyproject.toml` — verified in
  brainstorming that doing so surfaces 161 new findings (mostly `S101` false positives across
  the pytest suite), a disproportionate, out-of-scope effort. Only `RUF100` (unused-noqa) is
  in scope.
- Do not make `MAX_CHUNK_DAYS` operator-tunable — YAGNI, not requested by either task.
- Full spec: `docs/superpowers/specs/2026-07-08-issues-15-17-design.md`.

---

### Task 1: 1M-row Pull API cap guard

**Files:**
- Modify: `src/appsflyer_pipeline/appsflyer_client.py:130-142`
- Test: `tests/test_appsflyer_client.py`

**Interfaces:**
- Consumes: existing `AppsFlyerAPIError` (already defined in this file), existing `fetch_events`
  signature (unchanged).
- Produces: `fetch_events` now also raises `AppsFlyerAPIError` when the parsed DataFrame has
  `height >= 1_000_000`. No new public names.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_appsflyer_client.py` (after `test_fetch_events_raises_on_malformed_csv`,
the last function in the file):

```python
class _StubDataFrame:
    """Minimal stand-in for a polars DataFrame exposing only what fetch_events
    reads (.height) -- avoids generating an actual 1M-row CSV in a unit test.
    """

    height = 1_000_000


@respx.mock
def test_fetch_events_raises_on_1m_row_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    """Issue #15: AppsFlyer's Pull API silently truncates raw-data exports beyond
    1,000,000 rows with no error. fetch_events must fail loudly instead of
    returning truncated data as if it were a complete, successful fetch.
    """
    monkeypatch.setattr(appsflyer_client.pl, "read_csv", lambda *args, **kwargs: _StubDataFrame())
    respx.get(_url("id123", "non_organic")).mock(return_value=httpx.Response(200, text=SAMPLE_CSV))
    with httpx.Client() as client, pytest.raises(AppsFlyerAPIError, match="1M-row cap"):
        fetch_events(
            client,
            app_id="id123",
            attribution_type="non_organic",
            from_date=datetime.date(2026, 5, 20),
            to_date=datetime.date(2026, 5, 20),
            api_token="token",
            media_source="Facebook Ads",
            event_names=["af_purchase"],
        )
```

No new imports needed — `appsflyer_client`, `httpx`, `pytest`, `respx`, `datetime`,
`AppsFlyerAPIError`, `fetch_events`, `SAMPLE_CSV`, and `_url` are all already imported/defined
at the top of this file.

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_appsflyer_client.py::test_fetch_events_raises_on_1m_row_cap -v`
Expected: FAIL — `fetch_events` returns the stub DataFrame instead of raising (no cap check
exists yet), so `pytest.raises(AppsFlyerAPIError, ...)` fails with
`DID NOT RAISE <class 'appsflyer_pipeline.appsflyer_client.AppsFlyerAPIError'>`.

- [ ] **Step 3: Implement the guard**

In `src/appsflyer_pipeline/appsflyer_client.py`, the `fetch_events` function currently ends
with (lines 130-142):

```python
    if not content.strip():
        return pl.DataFrame()
    # infer_schema_length=0 forces every column to Utf8: chunks are read
    # independently and later pl.concat'ed, so dtype inference (e.g. an
    # all-null column guessed as Int64 in one chunk, Utf8 in another) must
    # not be allowed to diverge between them. transform.py applies real types.
    try:
        return pl.read_csv(BytesIO(content), infer_schema_length=0)
    except pl.exceptions.ComputeError as exc:
        raise AppsFlyerAPIError(
            f"AppsFlyer returned an unparseable CSV [{attribution_type}] for {app_id} "
            f"({from_date} to {to_date}): {exc}"
        ) from exc
```

Change it to:

```python
    if not content.strip():
        return pl.DataFrame()
    # infer_schema_length=0 forces every column to Utf8: chunks are read
    # independently and later pl.concat'ed, so dtype inference (e.g. an
    # all-null column guessed as Int64 in one chunk, Utf8 in another) must
    # not be allowed to diverge between them. transform.py applies real types.
    try:
        df = pl.read_csv(BytesIO(content), infer_schema_length=0)
    except pl.exceptions.ComputeError as exc:
        raise AppsFlyerAPIError(
            f"AppsFlyer returned an unparseable CSV [{attribution_type}] for {app_id} "
            f"({from_date} to {to_date}): {exc}"
        ) from exc
    if df.height >= 1_000_000:
        raise AppsFlyerAPIError(
            f"Report for {app_id} [{attribution_type}] {from_date}..{to_date} hit the "
            f"Pull API 1M-row cap — data is likely truncated; split the window into smaller chunks."
        )
    return df
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_appsflyer_client.py -v`
Expected: all tests in this file PASS, including the new
`test_fetch_events_raises_on_1m_row_cap`.

- [ ] **Step 5: Run the full suite with coverage**

Run: `uv run pytest -q`
Expected: all tests pass; coverage stays at 100% for
`src/appsflyer_pipeline/appsflyer_client.py` (the new `if df.height >= 1_000_000:` branch is
exercised by the new test; the `False` side is already exercised by every other passing test in
this file, which all return small DataFrames).

- [ ] **Step 6: Commit**

```bash
git add src/appsflyer_pipeline/appsflyer_client.py tests/test_appsflyer_client.py
git commit -m "Guard against the AppsFlyer Pull API's silent 1M-row export cap

Fixes #15"
```

---

### Task 2: Docs/config polish

**Files:**
- Modify: `deploy/user-level/appsflyer-daily.service:4-5`
- Modify: `docs/design-spec.md:98`
- Modify: `pyproject.toml:43`
- Modify: `src/appsflyer_pipeline/loader.py:85,173,180`
- Modify: `src/appsflyer_pipeline/config.py:47`
- Modify: `tests/test_loader_integration.py:31,49,63,74,159` (unused `noqa` cleanup only —
  the `except Exception as exc:` lines themselves, and everything else in this file, are
  otherwise untouched)
- Modify: `README.md`

**Interfaces:**
- No code behavior changes anywhere in this task. No new test names, no signature changes.
  Verification is via existing `ruff check .`, `uv run mypy`, `uv run pytest -q`, and
  `uv run pre-commit run --all-files` — all must stay green.

- [ ] **Step 1: Fix the dead `network-online.target` lines**

In `deploy/user-level/appsflyer-daily.service`, replace:

```ini
[Unit]
Description=AppsFlyer -> analytics DB daily incremental load (BAF-2) [user-level stopgap, no root]
Documentation=https://yesimapp.atlassian.net/browse/BAF-2
Wants=network-online.target
After=network-online.target

[Service]
```

with:

```ini
[Unit]
Description=AppsFlyer -> analytics DB daily incremental load (BAF-2) [user-level stopgap, no root]
Documentation=https://yesimapp.atlassian.net/browse/BAF-2
# No Wants=/After=network-online.target here (unlike the root-based unit): that target does
# not exist in a systemd --user manager. The 05:00 OnCalendar fire time plus the timer's
# RandomizedDelaySec is the real guarantee the network is up by the time this runs.

[Service]
```

`deploy/appsflyer-daily.service` (the root-based unit) is NOT touched —
`network-online.target` is valid and meaningful there.

- [ ] **Step 2: Fix the design-spec.md cross-reference**

In `docs/design-spec.md`, in the `## Interfaces` section, change:

```
  retention floor (see the backfill-window risk above). An explicit `--start-date` earlier than the
```

to:

```
  retention floor (see the backfill-window risk below). An explicit `--start-date` earlier than the
```

- [ ] **Step 3: Enable `RUF100` in ruff**

In `pyproject.toml`, change:

```toml
[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B", "SIM", "C4"]
```

to:

```toml
[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B", "SIM", "C4", "RUF100"]
```

- [ ] **Step 4: Remove the decorative `noqa` comments**

In `src/appsflyer_pipeline/loader.py`:

Line 85, change:
```python
                count_query = text(f"SELECT COUNT(*) FROM `{table_name}`")  # noqa: S608
```
to:
```python
                count_query = text(f"SELECT COUNT(*) FROM `{table_name}`")
```

Line 173, change:
```python
        f"DELETE FROM `{table_name}` "  # noqa: S608 - table_name is validated above
```
to:
```python
        f"DELETE FROM `{table_name}` "
```

Line 180, change:
```python
        f"INSERT INTO `{table_name}` ({columns_sql}) VALUES ({placeholders_sql})"  # noqa: S608
```
to:
```python
        f"INSERT INTO `{table_name}` ({columns_sql}) VALUES ({placeholders_sql})"
```

In `src/appsflyer_pipeline/config.py`, line 47, change:
```python
    appsflyer_event_names: Annotated[CsvList, Field(min_length=1)] = [  # noqa: RUF012
```
to:
```python
    appsflyer_event_names: Annotated[CsvList, Field(min_length=1)] = [
```

In `tests/test_loader_integration.py`, there are 5 occurrences to fix. Four are the identical
line:
```python
    except Exception as exc:  # noqa: BLE001 - environment without a reachable/configured DB
```
at lines 31, 49, 63, and 74 — change each to:
```python
    except Exception as exc:
```
(use `replace_all` for this exact line since all four occurrences are byte-identical and all
four must change the same way).

The fifth, at line 159, is a different line with the same trailing comment:
```python
    except Exception as exc:  # noqa: BLE001 - environment without a reachable/configured DB
        pytest.skip(f"no usable database in this environment: {exc}")
```
This is inside `test_load_events_logs_rowcounts_and_warns_on_wipe` — if the `replace_all` above
already caught it (it's the same exact line text), no separate edit is needed; just confirm
with `grep -n "noqa" tests/test_loader_integration.py` that all 4 `BLE001` occurrences are gone.

The sixth removal in this file, at line 125:
```python
                    f"SELECT COUNT(*) FROM `{settings.db_table}` "  # noqa: S608
```
change to:
```python
                    f"SELECT COUNT(*) FROM `{settings.db_table}` "
```

After all edits, run `grep -rn "noqa" src/ tests/` — expected output: nothing from
`loader.py`, `config.py`, or `test_loader_integration.py` (the `S608`/`RUF012`/`BLE001` ones are
all gone). No other files in the repo currently carry a `noqa` comment.

- [ ] **Step 5: Add the missing `pre-commit` line to README.md**

In `README.md`, in the `## Development` section, change:

```markdown
## Development

```bash
uv run ruff check .          # lint
uv run ruff format .         # format
uv run mypy                  # type check
uv run pytest                # tests (unit + respx-mocked HTTP; mysql:8 service in CI)
```
```

to:

```markdown
## Development

```bash
uv run ruff check .          # lint
uv run ruff format .         # format
uv run mypy                  # type check
uv run pytest                # tests (unit + respx-mocked HTTP; mysql:8 service in CI)
uv run pre-commit run --all-files   # same checks CI gates on — run before pushing
```
```

- [ ] **Step 6: Verify everything is still green**

Run, in order:
```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy
uv run pytest -q
```
Expected: all four commands succeed with no errors (the `ruff check .` run in particular
confirms `RUF100` has nothing left to flag — if it reports a new `RUF100` finding, an
`noqa` was missed in Step 4).

- [ ] **Step 7: Commit**

```bash
git add deploy/user-level/appsflyer-daily.service docs/design-spec.md pyproject.toml \
  src/appsflyer_pipeline/loader.py src/appsflyer_pipeline/config.py \
  tests/test_loader_integration.py README.md
git commit -m "Docs/config polish: dead network-online.target, wrong cross-reference, decorative noqa codes, missing pre-commit line in README

Fixes #17"
```

---

## Final Verification

- [ ] Run `uv run pytest -q` on the branch tip — all tests pass, coverage unchanged (100% on
  `appsflyer_client.py`, overall ≥98%).
- [ ] Run `uv run pre-commit run --all-files` — clean.
- [ ] Confirm `git log --oneline` on this branch shows exactly 3 commits ahead of `main`: the
  design spec (already committed), Task 1, Task 2.
