from __future__ import annotations

import pytest
from typer.testing import CliRunner

from appsflyer_pipeline.cli import app
from appsflyer_pipeline.config import get_settings

runner = CliRunner()

UNREACHABLE_ENV = {
    "DB_HOST": "127.0.0.1",
    "DB_PORT": "59999",  # nothing listens here -> connection refused, fast
    "DB_USER": "user",
    "DB_PASSWORD": "pw",
    "DB_NAME": "db",
    "DB_TABLE": "some_table",
    "APPSFLYER_API_TOKEN": "token",
    "APPSFLYER_APP_IDS": "id1",
}


def test_check_connection_reports_failure_for_unreachable_db(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for key, value in UNREACHABLE_ENV.items():
        monkeypatch.setenv(key, value)
    get_settings.cache_clear()

    result = runner.invoke(app, ["check-connection"])

    get_settings.cache_clear()
    assert result.exit_code == 1
    assert "FAILED" in result.output
