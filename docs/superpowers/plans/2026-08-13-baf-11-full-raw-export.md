# BAF-11 — Пайплайн выгрузки сырых данных из AppsFlyer Pull API

> Тикет: https://yesimapp.atlassian.net/browse/BAF-11 (Task, In Progress, assignee Baubek Ukibassov,
> reporter Mark Malovichko). Предшественник: [BAF-2](https://yesimapp.atlassian.net/browse/BAF-2) —
> текущая реализация в этом же репозитории.
> Ветка: `baf-11-full-raw-export`.
>
> Пометка **«ПРОВЕРИТЬ»** = факт не подтверждён ни кодом репозитория, ни живым запросом.
> Планировать по таким пунктам без проверки нельзя.

---

## Что просит тикет

| № | Требование | Статус |
|---|---|---|
| 1 | Перенастроить пайплайн для выгрузки **всего массива** данных, а не только Meta (Facebook Ads) | актуально |
| 2 | Для метода `installs` — **новая таблица**; для `in-app-events` — **та же таблица** | актуально (первоначальное «результаты в новые таблицы» ~~отменено~~) |
| 3 | ~~Убрать логику преобразования времени в Ригу~~ | **ОТМЕНЁН** — `Europe/Riga` остаётся |
| 4 | Убрать фильтры `API_EVENT_NAMES="af_purchase,af_purchase_YC"` и `API_MEDIA_SOURCE="Facebook Ads"` | актуально |
| 5 | Добавить метод `installs`: **все поля**, отдельная таблица, те же app, глубина 60 дней | актуально |
| 6 | Реализация параллельно со старым скриптом; после готовности старый остановить, новый запустить. **Параллельных выгрузок быть не должно** | актуально, но внутренне противоречиво (см. «Риски») |

Пункты 1 и 4 — одно изменение с двух сторон: фильтр живёт и в запросе к API, и в клиентской
трансформации. Дальше рассматриваются вместе.

---

## Что уже есть (BAF-2)

### Модули

| Файл | Роль | Ключевые места |
|---|---|---|
| `src/appsflyer_pipeline/config.py` | Единственный источник конфига (`pydantic-settings`), `get_settings()` под `@lru_cache` | `:41-44` (`Settings`), `:64-67` (app_ids), `:70` (`media_source="Facebook Ads"`), `:71-74` (`event_names`), `:84` (lookback, `le=90`), `:86-94` (CAUTION про wipe, issue #45), `:105` (`appsflyer_timezone`) |
| `src/appsflyer_pipeline/appsflyer_client.py` | HTTP-клиент Pull API v5: URL/params, ретраи, CSV→polars, нарезка окон | `:21` (`AttributionType`), `:23-26` (`_ENDPOINT_BY_ATTRIBUTION`), `:28` (`_BASE_URL`), `:33-34` (`MAX_RETENTION_DAYS=90`, `MAX_CHUNK_DAYS=31`), `:41-46` (`_is_retryable`), `:69-78` (params), `:86-88` (`follow_redirects`), `:159-163` (1M-cap) |
| `src/appsflyer_pipeline/transform.py` | 15 сырых колонок → колонки таблицы, типизация, клиентские фильтры, дедуп | `:26-42` (`_COLUMN_MAP`), `:45` (`_REQUIRED_NOT_NULL`), `:85-106` (`_dedupe_rows`), `:131-136` (schema drift), `:146-149` (фильтры) |
| `src/appsflyer_pipeline/loader.py` | Engine, DDL, идемпотентная запись delete-then-insert | `:30-38` (`_validate_identifier`), `:101-124` (`_CREATE_TABLE_TEMPLATE`), `:139-157` (`_INSERT_COLUMNS`), `:180-201` (DELETE+INSERT в одной транзакции), `:217-229` (WARNING «wiped») |
| `src/appsflyer_pipeline/pipeline.py` | Оркестрация, матрица работ, изоляция ошибок по окну | `:44` (`ATTRIBUTION_TYPES`), `:47-49` (`_today` seam), `:55` (`WindowResult.attribution_type: AttributionType`), `:101-104` (`_iter_work_items`), `:204-222` (retention floor), `:272-274` (окно backfill) |
| `src/appsflyer_pipeline/cli.py` | Typer-CLI: `version`, `check-connection`, `create-table`, `backfill`, `daily` | `:96-106` (маскирование ValidationError), `:125,130` (`_print_summary`) |
| `scripts/load_csv.py` | Ручная загрузка CSV | `:61` импортирует `_INSERT_COLUMNS`, использует в `:133-134`, `:272-273` |

### Поток запроса

`cli.backfill|daily` → `pipeline.run_backfill|run_daily` → `_run_window` (preflight `check_connection`,
кроме `--dry-run`) → один общий `httpx.Client` → последовательный обход `_iter_work_items` =
**app_id × attribution_type × чанк ≤31 дня** → `_process_window`: `fetch_events` → `transform_events`
→ `load_events(settings.db_table, …)`.

Два эндпоинта, оба `/v5`:
`…/app/{app_id}/in_app_events_report/v5` (`non_organic`) и `…/app/{app_id}/in-app-events-retarget/v5`
(`retargeting`). Query-параметры: `from`, `to`, `event_name`, `media_source` + `timezone` (только если
задан). Заголовки `Authorization: Bearer …`, `Accept: text/csv`, `follow_redirects=True` (AppsFlyer
отдаёт 302 на `rawdata.appsflyer.com`).

### Таблица `appsflyer_events_fb`

DDL продублирован: `loader.py:101-124` (шаблон с `{table}`) и `sql/create_table.sql:15-36`
(литеральное имя). 18 колонок, три DATETIME-времени, `PRIMARY KEY (id)`, единственный индекс
`idx_app_attr_time (app_id, attribution_type, event_time)` — спроектирован ровно под предикат DELETE
(`loader.py:181-184`). **UNIQUE-ключей нет** → upsert невозможен by design.

**Боевая таблица пересоздана 10.07.2026 без `id`/PK/индекса** — миграцию
`sql/migrations/2026-07-08-add-id-pk-and-index.sql` (неидемпотентную) надо прогнать заново
(`docs/RUNBOOK.md:144-151`).

### Деплой — фактическое состояние

Канонический root-комплект (`deploy/appsflyer-daily.{service,timer}`,
`EnvironmentFile=/etc/appsflyer/appsflyer.env`) **не используется**: sudo на боевой машине нет.
Реально работает no-root стопгап `deploy/user-level/` — `systemd --user`,
`EnvironmentFile=%h/appsflyer-secrets/appsflyer.env` (`deploy/user-level/appsflyer-daily.service:14`),
`ExecStart=%h/GitHubRepos/appsflyer-to-analytics-db/.venv/bin/appsflyer-pipeline daily` (`:17`).
Каталога `/etc/appsflyer/` там нет, **и своего env-шаблона в `deploy/user-level/` тоже нет** — там
только три юнита. Все операционные шаги ниже пишутся как `systemctl [--user]`.

`TimeoutStartSec=1800` в обоих комплектах (`deploy/appsflyer-daily.service:43`,
`deploy/user-level/appsflyer-daily.service:19`). `OnFailure=appsflyer-alert@%n.service` — **заглушка**
(issue #16): пишет в journald и никого не будит.

### Гейты

`uv run ruff check .`, `ruff format`, `mypy` (strict, `files=["src","tests"]`), `pytest`,
`pre-commit run --all-files`. CI: `pytest --cov-fail-under=98` против `mysql:8`; env задаёт только
`DB_TABLE=appsflyer_purchase_events` и `APPSFLYER_APP_IDS` (`.github/workflows/ci.yml:44-52`) —
ни `APPSFLYER_MEDIA_SOURCE`, ни `APPSFLYER_EVENT_NAMES` там нет.

`CLAUDE.md` и `AGENTS.md` байт-в-байт идентичны, но **`AGENTS.md` не в git** (`?? AGENTS.md`) —
до первой правки его надо либо закоммитить, либо удалить.

---

## Анализ разрыва

| Требование | Сейчас | Что менять | Файлы | Риск | Объём |
|---|---|---|---|---|---|
| **П.1/П.4 — все источники и события** | Фильтр в ДВУХ обязательных местах: `appsflyer_client.py:73` кладёт `media_source` в params безусловно (тип `str`, `:63`/`:101`); `transform.py:146-149` делает `.eq(...)`. `config.py:70-74` — дефолты с `min_length=1` | Трёхзначная семантика (см. ниже). **Категорически нельзя** выражать «все» пустой строкой: `.eq("")` отбросит всё, а delete-then-insert сотрёт окно с exit 0 | `config.py`, `appsflyer_client.py`, `transform.py`, `pipeline.py`, оба env-шаблона, `ci.yml` | **высокий** — тихое стирание | M |
| **П.5 — метод installs** | `_ENDPOINT_BY_ATTRIBUTION` знает только два in-app-events эндпоинта (`appsflyer_client.py:23-26`); `AttributionType` — `Literal` из двух значений | Ввести `ReportSpec`; добавить installs-отчёт с собственными endpoint / retention / окном / таблицей | новый `reports.py`, все слои, `sql/`, `ci.yml` | высокий | **L** |
| **П.5 — «все поля»** | Берётся 15 колонок из ~81, остальное осознанно выбрасывается (`transform.py:22-25`) | Режим «все поля» в transform + **`additional_fields`** в запросе (см. Q3 ниже) | `transform.py`, `appsflyer_client.py`, `tests/test_appsflyer_client.py:136-163` | высокий | L |
| **П.2 — вторая таблица** | Одна таблица на прогон: `pipeline.py:159` передаёт `settings.db_table` | `db_table_installs`; `create-table` создаёт все таблицы активных отчётов; DELETE-предикат из spec | `config.py`, `loader.py`, `cli.py`, `sql/create_table_installs.sql`, `ci.yml` | средний | M |
| **installs = 60 дней** | Окно считается **один раз на весь прогон** (`pipeline.py:272-274`), `_warn_if_before_retention_floor` жёстко берёт глобальную `MAX_RETENTION_DAYS` (`:204-222`), `_iter_work_items`/`_run_window` принимают единый `[start, end]` | **Структурное изменение сигнатур**: окно вычисляется per-report. Для installs нужен **жёсткий клэмп**, а не warning — иначе дефолтный 90-дневный backfill сгенерирует ~30 дней installs-окон вне ретенции → валидный пустой ответ → стирание **новой** таблицы в первый же прогон | `pipeline.py` | **высокий** | M |
| **П.3 — Riga остаётся** | `timezone` уходит только там, где явно прокинут (`appsflyer_client.py:75-78`) | Прокинуть `timezone` и в installs-запрос + регресс-тест | `appsflyer_client.py`, тесты | средний (расхождение внешне неотличимо от нормы) | S |
| **П.6 — cutover** | В репозитории нет ни одного юнита для стороннего «старого скрипта» — только `appsflyer-daily.timer`, запускающий **этот же** пайплайн | Процедура в RUNBOOK; но сначала выяснить, что называется «старым скриптом» (Q8) | `docs/RUNBOOK.md` | средний | S |

### Что ломается «по дороге» (не следует из текста тикета)

1. **`_REQUIRED_NOT_NULL` = `("event_time", "event_name", "appsflyer_id")`** (`transform.py:45,160-162`).
   Любая строка с пустым `appsflyer_id` роняет `TransformError` → `pipeline.py:166-183` теряет **всё
   31-дневное окно**. На полном массиве (web-события, privacy-ограниченные строки) это куда вероятнее,
   чем на FB-покупках. Нужно решение того же класса, что Q6: пропускать строку со счётчиком или ронять окно.
2. **Дедуп ломается в обе стороны.** `_dedupe_rows` (`transform.py:85-106`) не только падает на
   конфликте — он ещё и **молча схлопывает** байт-идентичные строки. На `af_session`/`af_app_opened`
   два реально разных события в одну секунду с одного устройства физически неотличимы → тихий недоучёт.
3. **`scripts/load_csv.py` сломается** на рефакторинге: `:61` импортирует `_INSERT_COLUMNS` напрямую,
   использует в `:133-134`, `:272-273`. mypy strict остановит гейт.
4. **`WindowResult.attribution_type: AttributionType`** (`pipeline.py:55`) — `Literal`; `cli.py:125,130`
   форматирует `[{r.attribution_type}]`. Если у отчёта `attribution_type: str | None` — под mypy strict
   меняются все три места плюс ассерты в `tests/test_pipeline.py` / `tests/test_cli.py`.
5. **Снятие дефолтов переворачивает fail-safe.** Сейчас забытая переменная = дешёвый отфильтрованный
   прогон. После изменения забытая переменная = полный массив, выжженная квота и скачок объёма. CI
   не задаёт ни одну из них.
6. **`appsflyer_daily_lookback_days` имеет `le=90`** (`config.py:84`) — граница привязана к ретенции
   in-app events, для installs бессмысленна.

---

## Предлагаемая архитектура

### `ReportSpec` — один отчёт как данные

Новый модуль `src/appsflyer_pipeline/reports.py`:

```python
@dataclass(frozen=True)
class ReportSpec:
    name: str                              # "in_app_events" | "installs"
    endpoint: str                          # "in_app_events_report" | "installs_report" (ПРОВЕРИТЬ)
    attribution_type: AttributionType
    # запрос
    sends_event_name: bool                 # installs: False
    sends_media_source: bool
    additional_fields: tuple[str, ...]     # см. Q3 — без этого «все поля» недостижимы
    retention_days: int                    # 90 in-app-events / 60 installs (ПРОВЕРИТЬ)
    max_chunk_days: int
    # трансформация
    column_map: Mapping[str, str] | None   # None = режим "все поля"
    timestamp_columns: tuple[str, ...]
    decimal_columns: tuple[str, ...]
    required_not_null: tuple[str, ...]
    dedupe_key: tuple[str, ...]
    # загрузка
    table: Callable[[Settings], str]       # НЕ строка-имя-атрибута: getattr ломает mypy strict
    insert_columns: tuple[str, ...]
    window_column: str                     # "event_time" | "install_time"
    partition_columns: tuple[str, ...]
```

Реестр — модульная константа `REPORTS: dict[str, ReportSpec]`, аналог сегодняшнего
`ATTRIBUTION_TYPES` (`pipeline.py:44`).

### Изменения по слоям

**`appsflyer_client.py`.** Развязать две смешанные идеи в `_ENDPOINT_BY_ATTRIBUTION` (`:23-26`):
endpoint переезжает в `ReportSpec.endpoint`, `AttributionType` остаётся только значением колонки.
Params собираются условно:

```python
params = {"from": ..., "to": ...}
if spec.sends_event_name and event_names is not None:
    params["event_name"] = ",".join(event_names)
if spec.sends_media_source and media_source is not None:
    params["media_source"] = media_source
if timezone is not None:
    params["timezone"] = timezone
if spec.additional_fields:
    params["additional_fields"] = ",".join(spec.additional_fields)
```

Всё остальное (`follow_redirects`, `Accept: text/csv`, tenacity-политика, проверка пустого тела,
1M-cap) переиспользуется как есть — от типа отчёта не зависит.

**`transform.py`.** Фильтры — список предикатов, применяется только если непуст:

```python
predicates = []
if media_source_filter is not None:
    predicates.append(pl.col("Media Source").eq(media_source_filter))
if event_names_filter is not None and "Event Name" in df.columns:
    predicates.append(pl.col("Event Name").is_in(event_names_filter))
filtered = df.filter(reduce(operator.and_, predicates)) if predicates else df
```

Режим «все поля» (`column_map is None`) — отдельная ветка: берём все пришедшие колонки, нормализуем
имена (`"Install Time" → install_time`), типы по спискам из spec. Защита от schema drift меняет смысл:
вместо «упасть, если ожидаемой колонки нет» → «упасть, если пришла колонка, которой нет в целевой
таблице».

**`loader.py`.** `load_events(engine, spec, rows, …)`; предикат DELETE из `spec.partition_columns` +
`spec.window_column`; `_INSERT_COLUMNS` → `spec.insert_columns`. **`scripts/load_csv.py` мигрирует
в том же коммите.**

**`pipeline.py`.** Третье измерение: `report × app_id × чанк`. **Окно и retention считаются
per-report** — это меняет сигнатуры `_iter_work_items`/`_run_window`/`_warn_if_before_retention_floor`.

**`cli.py`.** `create-table` создаёт все таблицы активных отчётов; preflight проверяет их все;
добавить `--report` для точечного прогона (нужно при cutover и при разборе квоты).

### Конфигурация фильтров

| Значение env | Поведение |
|---|---|
| переменная отсутствует/закомментирована | `None` → параметр не уходит в API, клиентский фильтр не применяется = **весь массив** |
| `APPSFLYER_MEDIA_SOURCE=Facebook Ads` | как сегодня — фильтруем |
| `APPSFLYER_MEDIA_SOURCE=` (пусто) | **ошибка старта** — сохраняем защиту issue #9 |

Два обязательных условия:

1. `Annotated[..., Field(min_length=1)]` **остаётся внутри** `| None` — иначе обещание «задан-но-пустой
   = ошибка старта» молча исчезнет из кода. `tests/test_config.py:57-58` параметризует `["", "   ", " , ,"]`;
   значение `" , ,"` после `_split_csv` даёт `[]` — не `None` и не валидный непустой список.
2. При старте прогона логировать эффективный режим на INFO:
   `filters: media_source=<all>, event_names=<all>, reports=[...]`.

**Открытый вариант получше:** явный положительный опт-ин `APPSFLYER_FILTERS=none` вместо «отсутствие
= всё». Это ровно тот аргумент, который мы сами приводим против пустой строки: отсутствие переменной
не должно означать самый разрушительный режим. Решить на этапе 1.

Возврат фильтра = раскомментировать строку в `%h/appsflyer-secrets/appsflyer.env` и дождаться
следующего срабатывания таймера (юнит oneshot, `docs/RUNBOOK.md:335-336`). Rollback без релиза.

---

## План работ по этапам

Общие правила (`CLAUDE.md`, `docs/superpowers/plans/*`):
- Ветка на этап: `baf-11-stage-N-<slug>` от `baf-11-full-raw-export`.
- TDD: сначала падающие тесты.
- Гейты после каждой задачи: `ruff check`, `ruff format --check`, `mypy`, `pytest`. Перед merge —
  `pre-commit run --all-files` и `pytest --cov-fail-under=98`.
- Не гонять CLI против прода без нужды: квота ~6-7 скачиваний/сутки на (app_id × тип отчёта); не
  запускать dry-run и реальный прогон подряд по одной комбинации.
- `CLAUDE.md` и `AGENTS.md` править синхронно — **но сперва решить судьбу неотслеживаемого `AGENTS.md`**.

### Этап 0. Разведка и разблокировка (без кода репозитория)

**Что.** Ответы на блокирующие вопросы Q1–Q4, Q8 + измерение реального объёма.

**Важно:** измерить объём текущим кодом **невозможно** — `appsflyer_media_source` имеет дефолт и
`min_length=1`, `media_source` уходит в API безусловно (`appsflyer_client.py:73`), а `fetched_rows`
в dry-run берётся из `raw_df.height` **до** клиентского фильтра (`pipeline.py:144`), т.е. цифра уже
отфильтрована сервером. Поэтому замер делается **вне репозитория**: прямой `curl` к Pull API за один
день без `media_source`/`event_name`. Это честнее и не тратит гейты — но всё равно съедает суточную квоту.

**Нельзя использовать как базовую линию** цифру «1 285 строк за 90 дней»: тогда загрузилось **11 из 12**
окон, двенадцатое упало по квоте (`docs/design-spec.md:158-165`).

**Критерий.** В `docs/design-spec.md` записаны: строк/сутки на app без фильтров, экстраполяция на
31 день, вердикт по размеру чанка. Письменные ответы Марка по Q1, Q2, Q4, Q8.

### Этап 1. Трёхзначные фильтры media_source / event_names

**Что.** `config.py:70-74` → `| None = None` с сохранением `min_length=1` внутри. Условная сборка
params. Список предикатов в `transform.py:146-149`. INFO-лог эффективного режима. Оба env-шаблона +
`.env.example` (заодно добавить отсутствующий `APPSFLYER_TIMEZONE`) + **создать отсутствующий
env-шаблон для `deploy/user-level/`** + `ci.yml`.

**Тесты (сначала).** `media_source=None` → параметра нет в `request.url.params`; transform без фильтров
возвращает все строки, включая органику с пустым Media Source; пустое значение env валит старт.
Переписать пины `tests/test_appsflyer_client.py:103-104`, `tests/test_transform.py:132,144`,
`tests/test_config.py:40,58`.

**Критерий.** Гейты зелёные, покрытие ≥98%. Прогон со старым `.env` даёт байт-в-байт тот же набор
запросов. **Прод не переключается** — в `%h/appsflyer-secrets/appsflyer.env` фильтры остаются заданными.

### Этап 2. Управляемый чанк и бюджет квоты

**Что.** `APPSFLYER_CHUNK_DAYS: Annotated[int, Field(ge=1, le=31)] = 31`, прокидка `max_days` в
`chunk_date_range` (`pipeline.py:103`). Решение по авто-респлиту — по цифрам этапа 0.

**Критерий.** В `docs/RUNBOOK.md` — таблица **`отчёты × app × чанки` против квоты**, а не «размер чанка →
число скачиваний»: после installs комбинаций становится 4 (или 6-8, если у installs есть retargeting —
Q4), и daily-прогон перестаёт быть «4 скачивания». Текущая арифметика: 90/7 = 13 чанков × 4 комбинации
= 52 скачивания против 6-7/сутки на комбинацию → backfill за одни сутки невозможен.

### Этап 3. ReportSpec (рефакторинг без изменения поведения)

**Что.** `reports.py` + реестр на два существующих отчёта. Прокидка spec через все слои. **Плюс
структурное изменение: окно и retention считаются per-report** (`_iter_work_items`, `_run_window`,
`_warn_if_before_retention_floor`). Миграция `scripts/load_csv.py`. Ревизия `WindowResult`/`_print_summary`
под возможный `str | None`.

**Критерий.** Все существующие тесты зелёные **без изменения ожидаемых значений** (кроме сигнатур
вызовов); `git diff` не трогает ни одного ожидаемого SQL/HTTP-значения в тестах. Плюс unit-тест:
для каждого spec `insert_columns` совпадают с ключами, которые производит `transform_events`.

### Этап 4. Пропускная способность и наблюдаемость (**до включения полного режима**)

**Что.** Батчинг INSERT (сейчас один `executemany` на всё окно, `loader.py:201`), стриминг вместо
построчного цикла (`transform.py:151-164`), пересмотр `TimeoutStartSec=1800` (полномассивный daily на
4-6 отчётах с высокой вероятностью получит SIGTERM посередине; юнит oneshot — окно останется
недогруженным), **рабочий алерт вместо заглушки** (`deploy/*/appsflyer-alert@.service`, issue #16).

**Почему отдельный этап.** Единственный сигнал о тихом стирании — WARNING в journald
(`loader.py:217-229`), который никто не читает. При главном риске «тихое стирание» после переключения
детектирования нет вообще.

**Критерий.** Прогон на объёме из этапа 0 укладывается в таймаут; искусственно вызванный «wiped»
доходит до человека.

### Этап 5. Вторая таблица и маршрутизация

**Что.** `db_table_installs`; `create-table` создаёт все таблицы активных отчётов; DDL installs
(шаблон в `loader.py` + зеркало в `sql/create_table_installs.sql`); индекс под предикат DELETE.
**`DB_TABLE_INSTALLS` в `.github/workflows/ci.yml:44-52`** — без этого интеграционные тесты в CI не
поедут.

**Критерий.** Гейты зелёные, включая интеграционные против `mysql:8` в CI. `DB_TABLE` для
in-app-events не изменился — П.2 соблюдён.

### Этап 6. Отчёт installs

**Что.** Spec: endpoint `installs_report` (**ПРОВЕРИТЬ** живым запросом), `sends_event_name=False`,
`retention_days=60` (**ПРОВЕРИТЬ**) с **жёстким клэмпом**, `window_column="install_time"`,
`additional_fields` из Q3, `column_map=None`. Обязательно: `timezone` уходит и в installs-запрос.

**Тесты.** respx-мок: URL корректен; `event_name` **не** уходит; `timezone` уходит (регресс П.3);
`additional_fields` уходит с ожидаемым списком; все колонки CSV попадают в строку; DELETE строится по
`install_time`; окно клэмпится по 60 дням, а не по 90.

**Критерий.** Гейты зелёные. Один живой `--dry-run` на одном app за один день, зафиксированный в
`docs/design-spec.md`: сколько колонок реально вернул installs и совпадает ли набор с DDL.

### Этап 7. Продовая схема (**блокер включения полного режима, не параллельная дорожка**)

**Что.** Прогнать `sql/migrations/2026-07-08-add-id-pk-and-index.sql` на боевом `appsflyer_events_fb`
(сейчас без PK и индекса). Проверить права на ALTER/CREATE INDEX (`docs/RUNBOOK.md:47-48` перечисляет
только SELECT/INSERT/DELETE/CREATE — про ALTER молчит). Проверить длины VARCHAR на реальной выборке
всех источников.

**Почему блокер.** Запускать полный массив в таблицу без PK и `idx_app_attr_time` — это ровно та
нелинейная деградация DELETE, которую мы сами называем риском.

**Критерий.** `SHOW INDEX FROM appsflyer_events_fb` показывает `idx_app_attr_time`; в RUNBOOK записана
дата прогона и результат проверки прав.

### Этап 8. Политика дедупликации и NOT NULL на полном массиве

**Что.** По решению Марка и data-analytics (Q6): (а) конфликт — расширить `dedupe_key` или сменить
hard-fail на warning + загрузку обеих строк; (б) **схлопывание точных дублей больше не корректно
по умолчанию** — на `af_session` два разных события неотличимы; (в) `_REQUIRED_NOT_NULL` и
`appsflyer_id` — пропускать строку со счётчиком или ронять окно. Обновить
`docs/reference-scripts-parity.md:59-62` (устаревшее описание фильтра `Is Primary Attribution`).

**Критерий.** Гейты зелёные; решение задокументировано в `docs/design-spec.md` с автором и датой.

### Этап 9. Cutover-процедура

**Что.** Новый раздел `docs/RUNBOOK.md` §15 «BAF-11 cutover»:

1. Задеплоить (`git pull`, `uv sync --frozen --no-dev`), таймер **не трогать** — юнит oneshot,
   следующее срабатывание подхватит новый venv (`docs/RUNBOOK.md:335-336`).
2. Верификация: `systemd-run` в `--dry-run` на узком окне; затем **обязательно** один реальный
   `systemctl [--user] start appsflyer-daily.service` — транзиентные проверки не переносят hardening
   и проходят, пока реальный юнит нестартуем (`docs/RUNBOOK.md:389-394`, issue #19).
3. Остановка старого расписания: `systemctl [--user] disable --now appsflyer-daily.timer`.
4. Включение нового: `systemctl [--user] enable --now appsflyer-daily.timer` — **только таймер**,
   никогда `.service` напрямую (`docs/RUNBOOK.md:162`).
5. Контроль: `systemctl [--user] list-timers | grep appsflyer` — активен ровно один.
6. Первые сутки — journald: строка эффективного режима фильтров, `deleted=/inserted=`, отсутствие
   WARNING «wiped».

Обновить `CLAUDE.md` + `AGENTS.md` и `docs/design-spec.md` (Scope/Non-Goals — `:15-21` объявляет другие
типы событий Non-Goal).

### Этап 10. Приёмка

Эталона для сверки **нет**: сверка BAF-2 с ручным экспортом из UI до сих пор не выполнена
(`docs/design-spec.md:175-176`), а каждая сверочная выгрузка тратит ту же квоту. Поэтому либо
письменный коммит Марка на один конкретный день **до** старта этапа, либо внутренние инварианты:

- row count стабилен между двумя последовательными прогонами одного окна (идемпотентность);
- нет WARNING «wiped» за период наблюдения;
- `count(media_source='Facebook Ads')` после переключения **≥** count до переключения на
  пересекающемся дне (полный массив не может содержать меньше FB-строк, чем отфильтрованный).

---

## Риски

1. **Тихое стирание данных.** `delete-then-insert` (`loader.py:190-201`) + любой сценарий с
   пустым/отфильтрованным-в-ноль результатом стирает загруженное окно с exit code 0. Частично закрыто
   (#26, #10 — WARNING в journald), но жёсткий клэмп по availability floor (#49) не сделан. Отсюда
   абсолютный запрет выражать «все источники» пустой строкой.
2. **Объём.** Снятие обоих фильтров умножает объём на порядки. Упоры: 1M-cap без авто-респлита
   (`appsflyer_client.py:159`), построчный Python-цикл (`transform.py:152`), один `executemany`
   (`loader.py:201`), DELETE без индекса на проде, `TimeoutStartSec=1800`.
3. **Квота AppsFlyer.** ~6-7 скачиваний/сутки на (app_id × тип отчёта), HTTP 400, не ретраится
   (правильно). Первый прод-backfill уже упирался: 11/12 окон. installs добавляет 2 (или 4)
   комбинации в тот же бюджет.
4. **Потеря 31-дневного окна.** Один дедуп-конфликт или одна строка с пустым `appsflyer_id` →
   `TransformError` → окно не загружено (`pipeline.py:166-183`). На FB-объёмах — раз в год; на полном
   массиве может стать ежедневным.
5. **Timezone-расхождение.** Если installs-запрос уйдёт без `timezone` — в одной БД окажутся
   Riga-events и UTC-installs; расхождение 2-3 часа, внешне неотличимое от нормы. В git-истории зона
   уже откатывалась и возвращалась (`494436d` → `9bb02f0`).
6. **Инверсия fail-safe.** После снятия дефолтов забытая переменная = полный массив.

### Внутреннее противоречие П.6

«Реализация параллельно со старым скриптом» несовместима с «параллельных выгрузок быть не должно» при
действующей редакции П.2 (in-app-events пишутся в **ту же** таблицу): два пайплайна с delete-then-insert
по окну затрут данные друг друга и сожгут одну квоту. Практически «параллельно» может означать только
«новый код готов и проверен, но его таймер выключен». Проговорить с Марком дословно.

---

## Вопросы Mark Malovichko

### Блокирующие (без ответов не начинать)

**Q1. «Выгружаем весь массив» — это снятие ОБОИХ фильтров, или все источники, но по-прежнему только покупки?**
Разница в порядки. Только `media_source`: покупки всех сетей — единицы-десятки тысяч строк за 90 дней.
Оба: все SDK-события всех источников, включая `af_app_opened`/`af_session` — миллионы строк, упор в
1M-cap и в квоту. От ответа зависит, нужен ли адаптивный чанкинг и переписывание loader под батчи —
разница между M и L.
Варианты: (a) снять только `media_source`; (b) снять оба; (c) снять оба, но сузить app_id/окно.

**Q2. «Все поля» (П.5) — только для installs, или для in-app-events тоже? Кто утверждает DDL новой таблицы?**
Сейчас берётся 15 колонок из ~81 (`transform.py:22-25`). Если «все поля» касается и in-app-events — это
ALTER прод-таблицы на ~80 колонок, совсем другая задача. Владелец схемы уже пересоздавал прод-таблицу
самостоятельно 10.07.2026, значит DDL согласуется не только с разработчиком.

**Q3. Подтвердить состав полей installs и список `additional_fields`.**
По документации AppsFlyer недефолтные колонки выдаются **только** через `additional_fields` — значит
дефолтный ответ по определению не «все поля», и П.5 без этого параметра невыполним. Зафиксированный
в репозитории HTTP 400 «Unknown additional field» (`tests/test_appsflyer_client.py:136-163`, живая
проверка 2026-07-07) был получен на `is_primary_attribution` — **дефолтную** колонку v5, которую нельзя
запрашивать как additional. То есть урок обратный тому, как это читается с первого взгляда.
Следствия: пин `test_fetch_events_never_sends_additional_fields` придётся **сознательно развернуть**;
любое неизвестное имя в списке даёт 400 на **весь** отчёт и не ретраится (`appsflyer_client.py:41-46`).
**Список надо один раз провалидировать живьём на однодневном окне до планирования DDL.**

**Q4. Есть ли у installs аналог `attribution_type`?**
По документации существуют и `/{app-id}/installs_report/v5`, и `/{app-id}/installs-retarget/v5` — та же
пара, что уже реализована для in-app events. **ПРОВЕРИТЬ одним запросом.** Если пара сохраняется:
схема новой таблицы структурно совпадает с текущей, но количество скачиваний **удваивается**
(4 комбинации вместо 2), и бюджет квоты этапа 2 надо считать сразу с этим.

**Q8. Что конкретно называется «старым скриптом» в П.6?**
В репозитории нет ни одного юнита/крона для стороннего скрипта — единственная запланированная задача
это `appsflyer-daily.timer`, запускающий **этот же** пайплайн. Если «старый» = текущий пайплайн, то
«параллельно» означает второй экземпляр в ту же таблицу с взаимным затиранием. Если «старый» = скрипты
Марка на другой машине — нужны хост, расписание и целевая таблица, чтобы спланировать останов.

### Важные, но допускают параллельную работу

**Q5. Откуда «60 дней» и совпадает ли это с реальной границей?**
Для in-app-events зафиксированы **две** границы: документированные ~90 дней (`appsflyer_client.py:33`)
и наблюдаемая ~35-дневная, за которой API отдаёт валидный, но **пустой** ответ (`config.py:91`).
Второе опаснее: пустой ответ + delete-then-insert стирает загруженное (issue #45). Если у installs
фактическая граница молчаливой пустоты меньше 60 дней, backfill будет затирать собственные данные
с exit 0.

**Q6. Политика дедупликации и NOT NULL на полном массиве.** Три отдельных решения: (а) что делать при
конфликте ключа; (б) **схлопывание точных дублей** — на `af_session` два разных события в одну секунду
неотличимы, тихий недоучёт; (в) строки с пустым `appsflyer_id`. Адресат: Марк + data-analytics
(те же, кто принимал #46/#47).

**Q7. Гранулярность суточной квоты: на (app_id × тип отчёта) или на app_id целиком?**
От ответа зависит, реализуем ли П.6 в формулировке «параллельно».

**Q9. Нужен ли полный re-backfill после переключения и как аналитикам трактовать разрыв «до/после»?**
Идемпотентный DELETE заменит FB-строки на полный массив только в пределах перезалитых окон; за более
старые даты навсегда останутся только FB-строки. Любой запрос по всей таблице увидит скачок объёма
на дате переключения.

**Q10. Есть ли права на ALTER/CREATE INDEX?** Прод-таблица без PK и индекса; RUNBOOK про ALTER молчит.
Адресат: владелец схемы `analytics_statistics` / DBA.

**Q11. Выдан ли sudo на сервере, или деплой по-прежнему на `systemd --user`?**
Если выдан — П.6 логично совместить с миграцией на канонический root-вариант (`docs/RUNBOOK.md` §14).
Адресат: backend-команда.
