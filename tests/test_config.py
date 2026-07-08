from __future__ import annotations

import pytest
from pydantic import ValidationError

from appsflyer_pipeline.config import Settings

BASE_ENV = {
    "DB_HOST": "db.example.com",
    "DB_PORT": "3306",
    "DB_USER": "user",
    "DB_PASSWORD": "secret",
    "DB_NAME": "statistics",
    "DB_TABLE": "appsflyer_events",
    "APPSFLYER_API_TOKEN": "token",
    "APPSFLYER_APP_IDS": "id1,id2",
}


def _settings(monkeypatch: pytest.MonkeyPatch, **overrides: str) -> Settings:
    for key, value in {**BASE_ENV, **overrides}.items():
        monkeypatch.setenv(key, value)
    return Settings(_env_file=None)  # type: ignore[call-arg]


def test_loads_required_fields_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = _settings(monkeypatch)
    assert settings.db_host == "db.example.com"
    assert settings.db_port == 3306
    assert settings.appsflyer_api_token == "token"


def test_splits_csv_app_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = _settings(monkeypatch, APPSFLYER_APP_IDS="id1458505230, com.yesimmobile")
    assert settings.appsflyer_app_ids == ["id1458505230", "com.yesimmobile"]


def test_default_media_source_and_event_names(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = _settings(monkeypatch)
    assert settings.appsflyer_media_source == "Facebook Ads"
    assert settings.appsflyer_event_names == ["af_purchase", "af_purchase_YC"]


def test_missing_required_field_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    env = {k: v for k, v in BASE_ENV.items() if k != "DB_HOST"}
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("DB_HOST", raising=False)

    with pytest.raises(ValidationError):
        Settings(_env_file=None)  # type: ignore[call-arg]


def test_daily_lookback_defaults_to_single_day(monkeypatch: pytest.MonkeyPatch) -> None:
    assert _settings(monkeypatch).appsflyer_daily_lookback_days == 1


def test_daily_lookback_accepts_valid_depth(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = _settings(monkeypatch, APPSFLYER_DAILY_LOOKBACK_DAYS="3")
    assert settings.appsflyer_daily_lookback_days == 3


@pytest.mark.parametrize("raw", ["0", "-3", "91", "not-a-number"])
def test_daily_lookback_out_of_bounds_rejected(monkeypatch: pytest.MonkeyPatch, raw: str) -> None:
    with pytest.raises(ValidationError):
        _settings(monkeypatch, APPSFLYER_DAILY_LOOKBACK_DAYS=raw)
