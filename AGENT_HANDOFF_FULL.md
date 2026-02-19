# AGENT HANDOFF (FULL CONTEXT)

Дата: 2026-02-18
Проект: `[REDACTED_PATH]/Projects/polymarket-assistant`

## 1) Что это за проект
Проект — ассистент/бот для Polymarket (Up/Down markets) с источниками данных Binance + Polymarket, с режимами `observe / paper / live`, web-интерфейсом на FastAPI + статический frontend (`web/index.html`, `web/styles.css`, `web/app.js`).

Основной backend:
- `[REDACTED_PATH]/Projects/polymarket-assistant/web_server.py`
- `[REDACTED_PATH]/Projects/polymarket-assistant/src/trading.py`
- `[REDACTED_PATH]/Projects/polymarket-assistant/src/feeds.py`
- `[REDACTED_PATH]/Projects/polymarket-assistant/src/sol_latency.py` (на ветке SOL/gRPC)

## 2) Главные цели пользователя за сессию
- Провести аудит/понимание кода и запуск.
- Настроить live-режим для реальных ставок, но максимально осторожно.
- Сделать UX удобнее: web UI, approve/reject, тосты, звук, пресеты.
- Доработать стратегию на SOL через gRPC latency (Yellowstone/Geyser), а не только BTC.
- Подготовить автономный режим и инфраструктурные команды.

## 3) Критические моменты, выявленные по ходу
- Частая ошибка `exit_error ... not enough balance / allowance` в live: проблема с conditional token balance/allowance при выходе.
- Частые `no trades` из-за жестких фильтров (`bias/obi`, price range, cooldown), а позже — из-за `sol lag` stale/малого движения.
- Конфигурационный mismatch signer/funder/sig_type изначально ломал live до корректной связки proxy signer (`sig_type=2`) и наличия collateral/allowance.
- В одном месте тестов была loop mismatch в `TestClient` при stop (ограничение тестового окружения, не пользовательский runtime path).

## 4) Состояние git на момент handoff
Текущая ветка: `main`

Сейчас в `main` есть незакоммиченные изменения:
- `[REDACTED_PATH]/Projects/polymarket-assistant/web/index.html`
- `[REDACTED_PATH]/Projects/polymarket-assistant/web/styles.css`
- `node_modules/` (untracked)

Есть stash с большим объемом работ по SOL/gRPC + runtime/UI:
- `stash@{0}: On codex/sol-grpc-probe: wip-sol-grpc-ui-before-main-switch`

Файлы внутри stash (включая untracked):
- `.traderctl`
- `README.md`
- `package.json`
- `src/feeds.py`
- `src/sol_latency.py`
- `src/trading.py`
- `tools/grpc_sanity.sh`
- `tools/run_sol_web.sh`
- `tools/yellowstone_probe.mjs`
- `web/app.js`
- `web/index.html`
- `web_server.py`
- плюс `__pycache__` файлы

Ветка с этими работами: `codex/sol-grpc-probe`.

## 5) Что сделано в main (последнее действие)
Задача пользователя: «вернуться на main и переделать интерфейс под optics.agusmayol.com.ar».

Сделано:
- Полный редизайн layout и CSS в оптико-документационном стиле (светлая палитра, sidebar, карточки, обновленные тосты/таблицы).
- Логика не ломалась: сохранены все DOM `id`, ожидаемые `web/app.js`.

Измененные файлы:
- `[REDACTED_PATH]/Projects/polymarket-assistant/web/index.html`
- `[REDACTED_PATH]/Projects/polymarket-assistant/web/styles.css`

Проверка:
- `node --check web/app.js` — OK
- Все `id` из `web/app.js` присутствуют в `web/index.html`.

## 6) Что было сделано на ветке SOL/gRPC (важно, но сейчас в stash)
Ниже — ключевые доработки, которые пользователь активно просил и использовал.

### 6.1 SOL gRPC pipeline
- Добавлена проверка/проба Yellowstone через `grpcurl` и JS probe.
- Добавлены инструменты:
  - `[REDACTED_PATH]/Projects/polymarket-assistant/tools/yellowstone_probe.mjs`
  - `[REDACTED_PATH]/Projects/polymarket-assistant/tools/grpc_sanity.sh`
- gRPC endpoint пользователя: `157.180.60.238:10000` (plaintext, без TLS).
- Reflection на сервере отключен, поэтому grpcurl должен идти с `-proto/-import-path`, а не через reflection.

### 6.2 SOL latency в runtime
- В `src/sol_latency.py` добавлен режим `grpc` (через subprocess grpcurl), парсинг stream JSON.
- Поддержка декодера `orca_whirlpool` из account data (sqrt_price).
- Поля latency в состоянии: price/source/slot/events/drops и recent events.

### 6.3 UI/UX runtime
- Тосты/звуки для pending approval, lag сигналов; позже добавлялись entry/exit уведомления.
- Кнопки approve/reject в интерфейсе и в pending toast.
- Session auth key через env `PM_WEB_ACCESS_KEY`.
- Startup preflight проверка кредов с toasts.

### 6.4 Presets и стратегии
- Добавлялись и тюнились пресеты (`safe/medium/aggressive/super/mega`, затем SOL-lag пресеты).
- Ограничения paper-only для агрессивных пресетов.

### 6.5 Логика SOL lag entry
- В `src/trading.py` (на ветке/stash) добавлялась логика входа по лагу SOL vs PM:
  - проверка движения за окно,
  - stale guard,
  - stall/mismatch с PM,
  - явные skip-причины в логах.

## 7) Расшифровка частого лога пользователя
Пример:
- `skip: sol lag: grpc stale (N.s > 3.0s)` — gRPC сигнал старый, вход блокируется.
- `skip: sol lag: move too small (+0.0xx% < 0.080%)` — движение ниже порога.
- `skip: sol lag: price out of range (1.0)` — цена PM вне лимита `min_price/max_price`.

Это не «поломка», это фильтры стратегии.

## 8) Безопасность/риск
- Реальные деньги: live требует token подтверждение `I_UNDERSTAND_REAL_MONEY_RISK`.
- Нельзя логировать/коммитить приватные ключи.
- Нужны корректные signer/funder/signature_type и allowance.
- Частая проблемная зона — выход из позиции (conditional allowance/balance).

## 9) Что важно не потерять при передаче
- Главный массив функциональных изменений не в `main`, а в stash/ветке `codex/sol-grpc-probe`.
- Если продолжать именно SOL/gRPC работу, надо сначала вернуть stash или переключиться на ветку.

Команды:
```bash
cd [REDACTED_PATH]/Projects/polymarket-assistant

git switch codex/sol-grpc-probe
# или в main применить stash:
# git stash apply stash@{0}
```

Рекомендация:
- Лучше продолжать на отдельной ветке от `codex/sol-grpc-probe` и уже потом переносить нужные куски в `main` выборочно.

## 10) Запуск (базово)
### Web
```bash
cd [REDACTED_PATH]/Projects/polymarket-assistant
export PM_WEB_ACCESS_KEY='your-strong-key-min-16-chars'
uvicorn web_server:app --host 0.0.0.0 --port 8000
```

### Live prerequisites
- `PM_PRIVATE_KEY`
- `PM_FUNDER`
- `PM_SIGNATURE_TYPE` (часто у пользователя рабочий `2` proxy)
- достаточный collateral + allowances

### gRPC sanity (из ветки/стеша, где есть script)
```bash
GRPC_API_URL=157.180.60.238:10000 \
GRPC_ACCOUNT_ADDRESSES=<account> \
GRPC_TX_REQUIRED=<program_or_account> \
bash tools/grpc_sanity.sh
```

## 11) Наблюдения по инфраструктуре
- В части окружений был ограничен DNS (ошибки резолва Binance/Polymarket в тест-клиенте), что мешает end-to-end симуляции.
- На macOS с Homebrew Python встречается PEP668 (нужен `.venv`, не системный pip).

## 12) Известные UX-факты
- Пользователь хочет максимально автономную работу и минимум ручных шагов.
- Пользователь просил явные сигналы и понятные логи «почему нет сделки».
- Пользователь предпочитает пресеты с быстрым запуском в один клик.

## 13) Что сделать следующему агенту в первую очередь
1. Решить merge-стратегию:
- либо вернуть stash в `main`,
- либо работать из `codex/sol-grpc-probe` и позже cherry-pick.

2. Зафиксировать commits атомарно:
- UI redesign в `main` отдельно.
- SOL/gRPC + lag strategy отдельно.

3. Проверить end-to-end live path:
- entry placed/fill,
- exit posted/fill,
- отсутствие `not enough balance / allowance` на exit.

4. Если цель пользователя — больше входов в live:
- ослабить stale/move thresholds,
- (по возможности) перейти к event-driven evaluation вместо чистого polling.

## 14) Текущий технический долг
- `node_modules/` untracked в репо: либо добавить в `.gitignore`, либо удалить.
- Не коммитить `__pycache__`, `.traderctl`, приватные/временные файлы.

## 15) Коротко: что сейчас реально готово
- На `main`: новый интерфейс в стиле optics (не закоммичен).
- В stash/`codex/sol-grpc-probe`: основная продвинутая логика SOL gRPC, уведомления, пресеты, диагностические скрипты, тюнинг live поведения.

---
Если нужен «чистый handoff only for coding tasks», этот файл уже можно передавать следующему агенту как единый источник контекста.
