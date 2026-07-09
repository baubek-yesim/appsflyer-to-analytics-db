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


@pytest.mark.parametrize("field", ["APPSFLYER_APP_IDS", "APPSFLYER_EVENT_NAMES"])
@pytest.mark.parametrize("raw", ["", "   ", " , ,"])
def test_empty_csv_list_rejected(monkeypatch: pytest.MonkeyPatch, field: str, raw: str) -> None:
    """A truncated/fat-fingered EnvironmentFile line (issue #9) must abort startup,
    not degrade to a silent no-op run (empty app list) or an active window wipe
    (empty event list -> transform's is_in([]) drops every row before the load).
    """
    with pytest.raises(ValidationError):
        _settings(monkeypatch, **{field: raw})


@pytest.mark.parametrize(
    "field",
    [
        "DB_HOST",
        "DB_USER",
        "DB_NAME",
        "DB_TABLE",
        "APPSFLYER_API_TOKEN",
        "APPSFLYER_MEDIA_SOURCE",
    ],
)
@pytest.mark.parametrize("raw", ["", "   "])
def test_empty_scalar_rejected(monkeypatch: pytest.MonkeyPatch, field: str, raw: str) -> None:
    """Issue #29 (extending #9 to scalars): a truncated line in the hand-edited
    server EnvironmentFile must abort startup. The worst case is an empty
    APPSFLYER_MEDIA_SOURCE -- the transform's exact-match re-filter then drops
    every fetched row and each window delete-then-inserts nothing, wiping data
    at exit 0. The DB scalars fail later and murkier; same fix for uniformity.
    """
    with pytest.raises(ValidationError):
        _settings(monkeypatch, **{field: raw})


def test_empty_db_password_still_accepted(monkeypatch: pytest.MonkeyPatch) -> None:
    """Deliberate #29 exemption: an empty DB password is legitimate (CI's
    mysql:8 service container authenticates root with one).
    """
    settings = _settings(monkeypatch, DB_PASSWORD="")
    assert settings.db_password == ""


def test_scalar_values_are_stripped(monkeypatch: pytest.MonkeyPatch) -> None:
    """Edge whitespace is normalized before validation -- a trailing space in
    APPSFLYER_MEDIA_SOURCE would otherwise silently break the exact-match
    media-source filter (and 'min_length' alone would count the spaces).
    """
    settings = _settings(monkeypatch, APPSFLYER_MEDIA_SOURCE="  Facebook Ads  ")
    assert settings.appsflyer_media_source == "Facebook Ads"


def test_daily_lookback_defaults_to_single_day(monkeypatch: pytest.MonkeyPatch) -> None:
    assert _settings(monkeypatch).appsflyer_daily_lookback_days == 1


def test_daily_lookback_accepts_valid_depth(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = _settings(monkeypatch, APPSFLYER_DAILY_LOOKBACK_DAYS="3")
    assert settings.appsflyer_daily_lookback_days == 3


@pytest.mark.parametrize("raw", ["0", "-3", "91", "not-a-number"])
def test_daily_lookback_out_of_bounds_rejected(monkeypatch: pytest.MonkeyPatch, raw: str) -> None:
    with pytest.raises(ValidationError):
        _settings(monkeypatch, APPSFLYER_DAILY_LOOKBACK_DAYS=raw)
