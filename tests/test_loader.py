from __future__ import annotations

import pytest

from appsflyer_pipeline.loader import PipelineError, _validate_identifier


@pytest.mark.parametrize("name", ["appsflyer_events_fb", "Table1", "a_b_c123"])
def test_validate_identifier_accepts_safe_names(name: str) -> None:
    assert _validate_identifier(name) == name


@pytest.mark.parametrize("name", ["bad name", "table;drop table x", "table`x", "a-b", ""])
def test_validate_identifier_rejects_unsafe_names(name: str) -> None:
    with pytest.raises(PipelineError):
        _validate_identifier(name)
