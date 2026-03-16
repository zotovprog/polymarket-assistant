# Pair Arb Strategy — Polymarket

## Суть стратегии

Покупаем **обе стороны** бинарного рынка (UP + DN) дешевле $1.00.
Затем **merge** (слияние) on-chain → получаем ровно $1.00 USDC за пару.

**Гарантированный профит** = $1.00 - (цена UP + цена DN) - газ.

## Пример

| Сторона | Цена bid | Покупаем |
|---------|----------|----------|
| BTC UP 5m | $0.48 | 5 шар |
| BTC DN 5m | $0.49 | 5 шар |
| **Итого** | **$0.97** | |
| Merge 5 пар | → $5.00 | |
| **Профит** | **$0.15** (3 цента/пара × 5) | |

## Как работает бот

### Maker Mode (основной)
1. **Сканирует** 6 рынков (BTC/ETH/SOL × 5m/15m) каждые 1.5 сек
2. **Выбирает лучший** — где UP_bid + DN_bid наименьший (максимальный профит)
3. **Ставит maker BUY ордера** на обе стороны по best_bid (post_only=True → 0% комиссия)
4. **Ждёт fill** — когда обе стороны заполнятся, вызывает merge
5. **Merge** — on-chain через Gnosis Safe → $1.00 USDC за пару

### Защиты
- **Orphan cleanup**: если одна нога не прошла — вторую отменяем мгновенно
- **Balance pre-check**: проверяем USDC ДО размещения обеих ног
- **Min clip 5 shares**: минимум PM = 5 шар на ордер
- **Min notional $1.00**: auto-raise clip если price × size < $1.00
- **Hard drawdown $5**: стоп если потери > $5 за сессию
- **Asymmetric fill detection**: если позиции несбалансированы — алерт

## Архитектура

```
engine.py          — главный цикл, управляет market switching
  ├── scanner.py   — обнаружение рынков через PM API
  ├── maker.py     — размещение maker ордеров (обе ноги)
  ├── executor.py  — taker ордера (фоллбэк)
  ├── merger.py    — merge/redeem через Safe
  ├── risk.py      — risk gates, drawdown
  ├── config.py    — параметры стратегии
  └── types.py     — dataclasses
```

## Кошелёк

- **Gnosis Safe** (PM_FUNDER) — хранит токены и USDC
- **EOA** (PM_PRIVATE_KEY) — подписывает ордера и on-chain tx
- **signatureType=2** (POLY_GNOSIS_SAFE) для CLOB API
- **Merge/Redeem** через `Safe.execTransaction()` → CTF контракт
- **Газ**: EOA платит POL/MATIC за on-chain операции

## Известные проблемы и фиксы

### v1.0 — Критический баг: "Request exception" на второй ноге
**Причина**: `asyncio.gather()` отправлял обе ноги одновременно через `asyncio.to_thread()`.
Глобальный `httpx.Client` в py_clob_client не потокобезопасен → второй запрос падал.
**Фикс**: последовательное размещение с 0.15с паузой.

### v1.0 — Orphan cleanup
**Проблема**: до фикса одна нога проходила, вторая нет → голая направленная позиция → убыток.
**Фикс**: если одна нога None после placement — отменяем вторую мгновенно.

### v1.0 — Gas management
**Проблема**: merge/redeem молча падал когда кончался POL на EOA.
**Фикс**: pre-check баланса газа перед каждой on-chain операцией.

## Минимальные требования

- **USDC баланс**: >= $10 (лучше $15-20)
- **POL на EOA**: >= 0.05 POL для газа merge/redeem
- **Рынки**: бинарные 5-min / 15-min windows (BTC/ETH/SOL)
- **Профит**: ~1-3 цента на пару при spread < $1.00
