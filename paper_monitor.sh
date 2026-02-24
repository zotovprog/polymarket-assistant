#!/bin/bash
# Paper mode monitor — logs state snapshots every 30s
AUTH_KEY=$(cat [REDACTED_PATH]/Projects/polymarket-assistant/.web_access_key)
LOG="/tmp/paper_monitor.log"
echo "=== Paper Monitor Started $(date) ===" >> "$LOG"

while true; do
  curl -s -b "pm_web_auth=$AUTH_KEY" http://localhost:8000/api/mm/state 2>/dev/null | python3 -c "
import json, sys, time
try:
    d = json.load(sys.stdin)
except:
    print(f'{time.strftime(\"%H:%M:%S\")} [ERROR] No response')
    sys.exit(0)

inv = d.get('inventory', {})
fv = d.get('fair_value', {})
q = d.get('quotes', {})
liq = d.get('liquidation_lock', {})
tl = d.get('market', {}).get('time_remaining', 0)

up_l = inv.get('up_shares',0) * inv.get('up_avg_entry',0.5)
dn_l = inv.get('dn_shares',0) * inv.get('dn_avg_entry',0.5)
locked = up_l + dn_l
limit = d.get('session_limit', 10)

status = 'CLOSE' if d.get('is_closing') else ('PAUSE' if d.get('is_paused') else 'RUN')
liq_s = f' LIQ:{liq.get(\"chunk_index\",0)}/{liq.get(\"total_chunks\",3)}' if liq.get('active') else ''

bids = []
for k in ['up_bid','dn_bid']:
    v = q.get(k)
    if v: bids.append(f'{k[0].upper()}:{v[\"size\"]:.0f}@{v[\"price\"]:.2f}')
asks = []
for k in ['up_ask','dn_ask']:
    v = q.get(k)
    if v: asks.append(f'{k[0].upper()}:{v[\"size\"]:.0f}@{v[\"price\"]:.2f}')

mock_usdc = d.get('mock_usdc_balance', 0)

# Budget violation check
budget_ok = 'OK' if locked <= limit * 1.1 else f'OVER({locked:.2f}>{limit})'

line = (
    f'{time.strftime(\"%H:%M:%S\")} [{tl:3.0f}s] {status}{liq_s} '
    f'| UP={inv.get(\"up_shares\",0):5.1f} DN={inv.get(\"dn_shares\",0):5.1f} '
    f'| \${locked:5.2f}/\${limit:.0f} {budget_ok} '
    f'| PnL=\${d.get(\"total_pnl\",0):+.2f} '
    f'| fills={d.get(\"fill_count\",0)} ord={d.get(\"active_orders\",0)} '
    f'| BID:{\" \".join(bids) or \"-\"} ASK:{\" \".join(asks) or \"-\"} '
    f'| mock\$={mock_usdc:.2f}'
)
print(line)
" >> "$LOG" 2>&1
  sleep 30
done
