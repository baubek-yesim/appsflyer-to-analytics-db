# Замер длин колонок AppsFlyer Pull API v5 (BAF-11, этап 0)

Источник: живой замер 2026-08-13, app `com.yesimmobile`, сутки `2026-08-11`, `timezone=Europe/Riga`.
`maxlen` — максимум по непустым значениям **за один день одного приложения**: это НИЖНЯЯ граница,
а не истина. `?` = колонка пуста целиком, длина неизвестна — тип выбирается по смыслу поля из
[Raw data field dictionary](https://support.appsflyer.com/hc/en-us/articles/208387843), а не по замеру.

Лимит строки MySQL/MariaDB — 65 535 байт; `VARCHAR(n)` в utf8mb4 стоит n*4+2, `TEXT` — только указатель.

## installs_report/v5 (128 колонок)

Строк за сутки: 1872.

| Колонка | Непустых | maxlen | Предложение |
|---|---:|---:|---|
| Attributed Touch Type | 1872 | 10 | VARCHAR(32) |
| Attributed Touch Time | 1872 | 19 | VARCHAR(64) |
| Install Time | 1872 | 19 | VARCHAR(64) |
| Event Time | 1872 | 19 | VARCHAR(64) |
| Event Name | 1872 | 7 | VARCHAR(16) |
| Event Value | 0 | — | TEXT |
| Event Revenue | 0 | — | ? |
| Event Revenue Currency | 0 | — | ? |
| Event Revenue USD | 0 | — | ? |
| Event Source | 1872 | 3 | VARCHAR(16) |
| Is Receipt Validated | 0 | — | ? |
| Partner | 0 | — | ? |
| Media Source | 1872 | 20 | VARCHAR(64) |
| Channel | 1745 | 15 | VARCHAR(32) |
| Keywords | 38 | 29 | VARCHAR(64) |
| Campaign | 1869 | 42 | VARCHAR(128) |
| Campaign ID | 1570 | 18 | VARCHAR(64) |
| Adset | 1694 | 43 | VARCHAR(128) |
| Adset ID | 1563 | 18 | VARCHAR(64) |
| Ad | 271 | 34 | VARCHAR(128) |
| Ad ID | 136 | 18 | VARCHAR(64) |
| Ad Type | 1497 | 17 | VARCHAR(64) |
| Site ID | 1200 | 15 | VARCHAR(32) |
| Sub Site ID | 1 | 22 | VARCHAR(64) |
| Sub Param 1 | 13 | 3 | VARCHAR(16) |
| Sub Param 2 | 0 | — | ? |
| Sub Param 3 | 0 | — | ? |
| Sub Param 4 | 133 | 92 | VARCHAR(255) |
| Sub Param 5 | 111 | 34 | VARCHAR(128) |
| Cost Model | 0 | — | ? |
| Cost Value | 0 | — | ? |
| Cost Currency | 0 | — | ? |
| Contributor 1 Partner | 0 | — | ? |
| Contributor 1 Media Source | 190 | 17 | VARCHAR(64) |
| Contributor 1 Campaign | 190 | 39 | VARCHAR(128) |
| Contributor 1 Touch Type | 190 | 10 | VARCHAR(32) |
| Contributor 1 Touch Time | 190 | 19 | VARCHAR(64) |
| Contributor 2 Partner | 0 | — | ? |
| Contributor 2 Media Source | 13 | 17 | VARCHAR(64) |
| Contributor 2 Campaign | 13 | 28 | VARCHAR(64) |
| Contributor 2 Touch Type | 13 | 10 | VARCHAR(32) |
| Contributor 2 Touch Time | 13 | 19 | VARCHAR(64) |
| Contributor 3 Partner | 0 | — | ? |
| Contributor 3 Media Source | 1 | 9 | VARCHAR(32) |
| Contributor 3 Campaign | 1 | 4 | VARCHAR(16) |
| Contributor 3 Touch Type | 1 | 5 | VARCHAR(16) |
| Contributor 3 Touch Time | 1 | 19 | VARCHAR(64) |
| Region | 1872 | 2 | VARCHAR(16) |
| Country Code | 1872 | 2 | VARCHAR(16) |
| State | 1872 | 9 | VARCHAR(32) |
| City | 1872 | 32 | VARCHAR(64) |
| Postal Code | 1872 | 9 | VARCHAR(32) |
| DMA | 1872 | 6 | VARCHAR(16) |
| IP | 1872 | 15 | VARCHAR(32) |
| WIFI | 1872 | 5 | VARCHAR(16) |
| Operator | 1777 | 34 | VARCHAR(128) |
| Carrier | 1660 | 23 | VARCHAR(64) |
| Language | 1872 | 12 | VARCHAR(32) |
| AppsFlyer ID | 1872 | 33 | VARCHAR(128) |
| Advertising ID | 1869 | 36 | VARCHAR(128) |
| IDFA | 0 | — | ? |
| Android ID | 0 | — | ? |
| Customer User ID | 9 | 7 | VARCHAR(16) |
| IMEI | 0 | — | ? |
| IDFV | 0 | — | ? |
| Platform | 1872 | 7 | VARCHAR(16) |
| Device Type | 0 | — | ? |
| OS Version | 1872 | 2 | VARCHAR(16) |
| App Version | 1872 | 6 | VARCHAR(16) |
| SDK Version | 1872 | 7 | VARCHAR(16) |
| App ID | 1872 | 15 | VARCHAR(32) |
| App Name | 1872 | 34 | VARCHAR(128) |
| Bundle ID | 1872 | 15 | VARCHAR(32) |
| Is Retargeting | 1872 | 5 | VARCHAR(16) |
| Retargeting Conversion Type | 0 | — | ? |
| Attribution Lookback | 1872 | 3 | VARCHAR(16) |
| Reengagement Window | 0 | — | ? |
| Is Primary Attribution | 0 | — | ? |
| User Agent | 1872 | 91 | TEXT |
| HTTP Referrer | 230 | 917 | TEXT |
| Original URL | 303 | 1803 | TEXT |
| Store Reinstall | 0 | — | ? |
| Impressions | 0 | — | ? |
| Contributor 3 Match Type | 1 | 11 | VARCHAR(32) |
| Custom Dimension | 0 | — | ? |
| Conversion Type | 1872 | 7 | VARCHAR(16) |
| Google Play Click Time | 1695 | 19 | VARCHAR(64) |
| Match Type | 1872 | 13 | VARCHAR(32) |
| Mediation Network | 0 | — | ? |
| OAID | 0 | — | ? |
| Deeplink URL | 60 | 71 | TEXT |
| Blocked Reason | 0 | — | ? |
| Blocked Sub Reason | 0 | — | ? |
| Google Play Broadcast Referrer | 0 | — | TEXT |
| Google Play Install Begin Time | 1843 | 19 | VARCHAR(64) |
| Campaign Type | 1872 | 2 | VARCHAR(16) |
| Custom Data | 0 | — | TEXT |
| Rejected Reason | 0 | — | ? |
| Device Download Time | 1872 | 23 | VARCHAR(64) |
| Keyword Match Type | 38 | 1 | VARCHAR(16) |
| Contributor 1 Match Type | 190 | 13 | VARCHAR(32) |
| Contributor 2 Match Type | 13 | 13 | VARCHAR(32) |
| Device Model | 1872 | 40 | VARCHAR(128) |
| Monetization Network | 0 | — | ? |
| Segment | 0 | — | ? |
| Is LAT | 1872 | 5 | VARCHAR(16) |
| Google Play Referrer | 1865 | 1091 | TEXT |
| Blocked Reason Value | 0 | — | ? |
| Store Product Page | 0 | — | ? |
| Device Category | 1872 | 23 | VARCHAR(64) |
| App Type | 0 | — | ? |
| Rejected Reason Value | 0 | — | ? |
| Ad Unit | 0 | — | ? |
| Keyword ID | 0 | — | ? |
| Placement | 0 | — | ? |
| Network Account ID | 1570 | 19 | VARCHAR(64) |
| Install App Store | 0 | — | ? |
| Amazon Fire ID | 0 | — | ? |
| ATT | 0 | — | ? |
| Engagement Type | 1872 | 17 | VARCHAR(64) |
| Contributor 1 Engagement Type | 190 | 17 | VARCHAR(64) |
| Contributor 2 Engagement Type | 13 | 17 | VARCHAR(64) |
| Contributor 3 Engagement Type | 1 | 17 | VARCHAR(64) |
| GDPR Applies | 0 | — | ? |
| Ad User Data Enabled | 0 | — | ? |
| Ad Personalization Enabled | 0 | — | ? |
| Total Candidates | 1872 | 3 | VARCHAR(16) |
| Engagement Destination | 132 | 7 | VARCHAR(16) |

## installs-retarget/v5 (128 колонок)

Строк за сутки: 918.

| Колонка | Непустых | maxlen | Предложение |
|---|---:|---:|---|
| Attributed Touch Type | 918 | 10 | VARCHAR(32) |
| Attributed Touch Time | 918 | 19 | VARCHAR(64) |
| Install Time | 918 | 19 | VARCHAR(64) |
| Event Time | 918 | 19 | VARCHAR(64) |
| Event Name | 918 | 14 | VARCHAR(32) |
| Event Value | 0 | — | TEXT |
| Event Revenue | 0 | — | ? |
| Event Revenue Currency | 0 | — | ? |
| Event Revenue USD | 0 | — | ? |
| Event Source | 918 | 3 | VARCHAR(16) |
| Is Receipt Validated | 0 | — | ? |
| Partner | 0 | — | ? |
| Media Source | 918 | 20 | VARCHAR(64) |
| Channel | 889 | 13 | VARCHAR(32) |
| Keywords | 171 | 42 | VARCHAR(128) |
| Campaign | 918 | 41 | VARCHAR(128) |
| Campaign ID | 890 | 11 | VARCHAR(32) |
| Adset | 285 | 12 | VARCHAR(32) |
| Adset ID | 285 | 12 | VARCHAR(32) |
| Ad | 304 | 12 | VARCHAR(32) |
| Ad ID | 273 | 12 | VARCHAR(32) |
| Ad Type | 890 | 15 | VARCHAR(32) |
| Site ID | 719 | 64 | VARCHAR(128) |
| Sub Site ID | 0 | — | ? |
| Sub Param 1 | 0 | — | ? |
| Sub Param 2 | 0 | — | ? |
| Sub Param 3 | 0 | — | ? |
| Sub Param 4 | 0 | — | ? |
| Sub Param 5 | 60 | 12 | VARCHAR(32) |
| Cost Model | 0 | — | ? |
| Cost Value | 0 | — | ? |
| Cost Currency | 0 | — | ? |
| Contributor 1 Partner | 0 | — | ? |
| Contributor 1 Media Source | 3 | 20 | VARCHAR(64) |
| Contributor 1 Campaign | 3 | 28 | VARCHAR(64) |
| Contributor 1 Touch Type | 3 | 5 | VARCHAR(16) |
| Contributor 1 Touch Time | 3 | 19 | VARCHAR(64) |
| Contributor 2 Partner | 0 | — | ? |
| Contributor 2 Media Source | 0 | — | ? |
| Contributor 2 Campaign | 0 | — | ? |
| Contributor 2 Touch Type | 0 | — | ? |
| Contributor 2 Touch Time | 0 | — | ? |
| Contributor 3 Partner | 0 | — | ? |
| Contributor 3 Media Source | 0 | — | ? |
| Contributor 3 Campaign | 0 | — | ? |
| Contributor 3 Touch Type | 0 | — | ? |
| Contributor 3 Touch Time | 0 | — | ? |
| Region | 918 | 2 | VARCHAR(16) |
| Country Code | 918 | 2 | VARCHAR(16) |
| State | 918 | 9 | VARCHAR(32) |
| City | 918 | 31 | VARCHAR(64) |
| Postal Code | 918 | 9 | VARCHAR(32) |
| DMA | 918 | 6 | VARCHAR(16) |
| IP | 918 | 15 | VARCHAR(32) |
| WIFI | 918 | 5 | VARCHAR(16) |
| Operator | 902 | 31 | VARCHAR(64) |
| Carrier | 865 | 36 | VARCHAR(128) |
| Language | 918 | 11 | VARCHAR(32) |
| AppsFlyer ID | 918 | 33 | VARCHAR(128) |
| Advertising ID | 918 | 36 | VARCHAR(128) |
| IDFA | 0 | — | ? |
| Android ID | 0 | — | ? |
| Customer User ID | 825 | 7 | VARCHAR(16) |
| IMEI | 0 | — | ? |
| IDFV | 0 | — | ? |
| Platform | 918 | 7 | VARCHAR(16) |
| Device Type | 0 | — | ? |
| OS Version | 918 | 2 | VARCHAR(16) |
| App Version | 918 | 6 | VARCHAR(16) |
| SDK Version | 918 | 7 | VARCHAR(16) |
| App ID | 918 | 15 | VARCHAR(32) |
| App Name | 918 | 34 | VARCHAR(128) |
| Bundle ID | 918 | 15 | VARCHAR(32) |
| Is Retargeting | 918 | 4 | VARCHAR(16) |
| Retargeting Conversion Type | 918 | 14 | VARCHAR(32) |
| Attribution Lookback | 918 | 3 | VARCHAR(16) |
| Reengagement Window | 890 | 8 | VARCHAR(16) |
| Is Primary Attribution | 0 | — | ? |
| User Agent | 918 | 91 | TEXT |
| HTTP Referrer | 0 | — | TEXT |
| Original URL | 28 | 109 | TEXT |
| Store Reinstall | 0 | — | ? |
| Impressions | 0 | — | ? |
| Contributor 3 Match Type | 0 | — | ? |
| Custom Dimension | 0 | — | ? |
| Conversion Type | 918 | 13 | VARCHAR(32) |
| Google Play Click Time | 15 | 19 | VARCHAR(64) |
| Match Type | 918 | 8 | VARCHAR(16) |
| Mediation Network | 0 | — | ? |
| OAID | 0 | — | ? |
| Deeplink URL | 29 | 40 | TEXT |
| Blocked Reason | 0 | — | ? |
| Blocked Sub Reason | 0 | — | ? |
| Google Play Broadcast Referrer | 0 | — | TEXT |
| Google Play Install Begin Time | 20 | 19 | VARCHAR(64) |
| Campaign Type | 918 | 11 | VARCHAR(32) |
| Custom Data | 0 | — | TEXT |
| Rejected Reason | 0 | — | ? |
| Device Download Time | 918 | 23 | VARCHAR(64) |
| Keyword Match Type | 179 | 1 | VARCHAR(16) |
| Contributor 1 Match Type | 3 | 13 | VARCHAR(32) |
| Contributor 2 Match Type | 0 | — | ? |
| Device Model | 918 | 40 | VARCHAR(128) |
| Monetization Network | 0 | — | ? |
| Segment | 0 | — | ? |
| Is LAT | 918 | 5 | VARCHAR(16) |
| Google Play Referrer | 21 | 180 | TEXT |
| Blocked Reason Value | 0 | — | ? |
| Store Product Page | 0 | — | ? |
| Device Category | 21 | 12 | VARCHAR(32) |
| App Type | 0 | — | ? |
| Rejected Reason Value | 0 | — | ? |
| Ad Unit | 0 | — | ? |
| Keyword ID | 0 | — | ? |
| Placement | 0 | — | ? |
| Network Account ID | 890 | 10 | VARCHAR(32) |
| Install App Store | 0 | — | ? |
| Amazon Fire ID | 0 | — | ? |
| ATT | 0 | — | ? |
| Engagement Type | 918 | 12 | VARCHAR(32) |
| Contributor 1 Engagement Type | 3 | 17 | VARCHAR(64) |
| Contributor 2 Engagement Type | 0 | — | ? |
| Contributor 3 Engagement Type | 0 | — | ? |
| GDPR Applies | 0 | — | ? |
| Ad User Data Enabled | 0 | — | ? |
| Ad Personalization Enabled | 0 | — | ? |
| Total Candidates | 918 | 1 | VARCHAR(16) |
| Engagement Destination | 0 | — | ? |

## in_app_events_report/v5 без фильтров (81 колонка)

Строк за сутки: 7707.

| Колонка | Непустых | maxlen | Предложение |
|---|---:|---:|---|
| Attributed Touch Type | 7707 | 10 | VARCHAR(32) |
| Attributed Touch Time | 7707 | 19 | VARCHAR(64) |
| Install Time | 7707 | 19 | VARCHAR(64) |
| Event Time | 7707 | 19 | VARCHAR(64) |
| Event Name | 7707 | 25 | VARCHAR(64) |
| Event Value | 7707 | 64 | TEXT |
| Event Revenue | 592 | 5 | VARCHAR(16) |
| Event Revenue Currency | 7707 | 3 | VARCHAR(16) |
| Event Revenue USD | 592 | 18 | VARCHAR(64) |
| Event Source | 7707 | 3 | VARCHAR(16) |
| Is Receipt Validated | 0 | — | ? |
| Partner | 0 | — | ? |
| Media Source | 7707 | 20 | VARCHAR(64) |
| Channel | 6886 | 15 | VARCHAR(32) |
| Keywords | 254 | 36 | VARCHAR(128) |
| Campaign | 7702 | 42 | VARCHAR(128) |
| Campaign ID | 6053 | 18 | VARCHAR(64) |
| Adset | 6565 | 43 | VARCHAR(128) |
| Adset ID | 6036 | 18 | VARCHAR(64) |
| Ad | 1173 | 110 | VARCHAR(255) |
| Ad ID | 581 | 18 | VARCHAR(64) |
| Ad Type | 5828 | 17 | VARCHAR(64) |
| Site ID | 5013 | 71 | VARCHAR(255) |
| Sub Site ID | 2 | 22 | VARCHAR(64) |
| Sub Param 1 | 154 | 3 | VARCHAR(16) |
| Sub Param 2 | 0 | — | ? |
| Sub Param 3 | 0 | — | ? |
| Sub Param 4 | 581 | 92 | VARCHAR(255) |
| Sub Param 5 | 486 | 34 | VARCHAR(128) |
| Cost Model | 0 | — | ? |
| Cost Value | 0 | — | ? |
| Cost Currency | 0 | — | ? |
| Contributor 1 Partner | 0 | — | ? |
| Contributor 1 Media Source | 0 | — | ? |
| Contributor 1 Campaign | 0 | — | ? |
| Contributor 1 Touch Type | 0 | — | ? |
| Contributor 1 Touch Time | 0 | — | ? |
| Contributor 2 Partner | 0 | — | ? |
| Contributor 2 Media Source | 0 | — | ? |
| Contributor 2 Campaign | 0 | — | ? |
| Contributor 2 Touch Type | 0 | — | ? |
| Contributor 2 Touch Time | 0 | — | ? |
| Contributor 3 Partner | 0 | — | ? |
| Contributor 3 Media Source | 0 | — | ? |
| Contributor 3 Campaign | 0 | — | ? |
| Contributor 3 Touch Type | 0 | — | ? |
| Contributor 3 Touch Time | 0 | — | ? |
| Region | 7707 | 2 | VARCHAR(16) |
| Country Code | 7707 | 2 | VARCHAR(16) |
| State | 7707 | 9 | VARCHAR(32) |
| City | 7707 | 32 | VARCHAR(64) |
| Postal Code | 7707 | 9 | VARCHAR(32) |
| DMA | 7707 | 6 | VARCHAR(16) |
| IP | 7707 | 15 | VARCHAR(32) |
| WIFI | 7707 | 5 | VARCHAR(16) |
| Operator | 7421 | 34 | VARCHAR(128) |
| Carrier | 6919 | 23 | VARCHAR(64) |
| Language | 7707 | 12 | VARCHAR(32) |
| AppsFlyer ID | 7707 | 33 | VARCHAR(128) |
| Advertising ID | 7702 | 36 | VARCHAR(128) |
| IDFA | 0 | — | ? |
| Android ID | 0 | — | ? |
| Customer User ID | 7169 | 7 | VARCHAR(16) |
| IMEI | 0 | — | ? |
| IDFV | 0 | — | ? |
| Platform | 7707 | 7 | VARCHAR(16) |
| Device Type | 0 | — | ? |
| OS Version | 7707 | 2 | VARCHAR(16) |
| App Version | 7707 | 6 | VARCHAR(16) |
| SDK Version | 7707 | 7 | VARCHAR(16) |
| App ID | 7707 | 15 | VARCHAR(32) |
| App Name | 7707 | 34 | VARCHAR(128) |
| Bundle ID | 7707 | 15 | VARCHAR(32) |
| Is Retargeting | 7707 | 5 | VARCHAR(16) |
| Retargeting Conversion Type | 0 | — | ? |
| Attribution Lookback | 7707 | 3 | VARCHAR(16) |
| Reengagement Window | 0 | — | ? |
| Is Primary Attribution | 7707 | 5 | VARCHAR(16) |
| User Agent | 7707 | 91 | TEXT |
| HTTP Referrer | 1194 | 917 | TEXT |
| Original URL | 1656 | 1761 | TEXT |

