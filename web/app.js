/* Polymarket MM Dashboard — Frontend App */

const API_BASE = '';
let isRunning = false;
let pollTimer = null;

// Charts
let priceChart = null;
let priceSeries = null;
let fvSeries = null;
let spreadChart = null;
let spreadSeries = null;
let pnlChart = null;
let pnlSeries = null;
let pnlData = [];
let spreadData = [];

// ── Auth ──────────────────────────────────────────────
async function checkAuth() {
    try {
        const r = await fetch(`${API_BASE}/api/auth/check`);
        const d = await r.json();
        if (d.authenticated) {
            showDashboard();
        }
    } catch(e) {}
}

async function doLogin() {
    const key = document.getElementById('auth-key').value.trim();
    if (!key) return;
    try {
        const r = await fetch(`${API_BASE}/api/auth/login`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({key}),
        });
        if (r.ok) {
            showDashboard();
        } else {
            document.getElementById('auth-error').textContent = 'Invalid key';
            document.getElementById('auth-error').classList.remove('hidden');
        }
    } catch(e) {
        document.getElementById('auth-error').textContent = 'Connection error';
        document.getElementById('auth-error').classList.remove('hidden');
    }
}

function showDashboard() {
    document.getElementById('auth-screen').classList.add('hidden');
    document.getElementById('dashboard').classList.remove('hidden');
    initCharts();
    startPolling();
}

// ── Charts Init ──────────────────────────────────────
function initCharts() {
    const chartOpts = {
        layout: { background: { type: 'solid', color: '#1a1f2e' }, textColor: '#94a3b8' },
        grid: { vertLines: { color: '#1e293b' }, horzLines: { color: '#1e293b' } },
        timeScale: { timeVisible: true, secondsVisible: false },
        crosshair: { mode: 0 },
    };

    // Price chart
    const priceEl = document.getElementById('price-chart');
    if (priceEl && typeof LightweightCharts !== 'undefined') {
        priceChart = LightweightCharts.createChart(priceEl, {
            ...chartOpts, width: priceEl.clientWidth, height: 242,
        });
        priceSeries = priceChart.addLineSeries({ color: '#3b82f6', lineWidth: 2 });
        fvSeries = priceChart.addLineSeries({ color: '#06b6d4', lineWidth: 1, lineStyle: 2 });
    }

    // Spread chart
    const spreadEl = document.getElementById('spread-chart');
    if (spreadEl && typeof LightweightCharts !== 'undefined') {
        spreadChart = LightweightCharts.createChart(spreadEl, {
            ...chartOpts, width: spreadEl.clientWidth, height: 142,
        });
        spreadSeries = spreadChart.addHistogramSeries({
            color: '#a855f7', priceFormat: { type: 'custom', formatter: v => v.toFixed(0) + ' bps' },
        });
    }

    // PnL chart
    const pnlEl = document.getElementById('pnl-chart');
    if (pnlEl && typeof LightweightCharts !== 'undefined') {
        pnlChart = LightweightCharts.createChart(pnlEl, {
            ...chartOpts, width: pnlEl.clientWidth, height: 142,
        });
        pnlSeries = pnlChart.addAreaSeries({
            topColor: 'rgba(34, 197, 94, 0.3)', bottomColor: 'rgba(34, 197, 94, 0.0)',
            lineColor: '#22c55e', lineWidth: 2,
        });
    }

    // Resize observer
    const ro = new ResizeObserver(() => {
        if (priceChart) priceChart.applyOptions({ width: priceEl.clientWidth });
        if (spreadChart) spreadChart.applyOptions({ width: spreadEl.clientWidth });
        if (pnlChart) pnlChart.applyOptions({ width: pnlEl.clientWidth });
    });
    if (priceEl) ro.observe(priceEl);
    if (spreadEl) ro.observe(spreadEl);
    if (pnlEl) ro.observe(pnlEl);
}

// ── Polling ──────────────────────────────────────────
function startPolling() {
    pollState();
    pollTimer = setInterval(pollState, 1500);
}

async function pollState() {
    try {
        const r = await fetch(`${API_BASE}/api/mm/state`);
        if (r.status === 401) {
            document.getElementById('dashboard').classList.add('hidden');
            document.getElementById('auth-screen').classList.remove('hidden');
            clearInterval(pollTimer);
            return;
        }
        const s = await r.json();
        updateUI(s);
    } catch(e) {
        document.getElementById('status-text').textContent = 'Connection error';
        document.getElementById('status-indicator').className = 'status-dot offline';
    }
}

// ── UI Update ────────────────────────────────────────
function updateUI(s) {
    isRunning = s.is_running || false;

    // Status
    const statusDot = document.getElementById('status-indicator');
    const statusText = document.getElementById('status-text');
    const btn = document.getElementById('btn-start');

    if (isRunning) {
        if (s.is_paused) {
            statusDot.className = 'status-dot paused';
            statusText.textContent = 'Paused: ' + (s.pause_reason || '');
        } else {
            statusDot.className = 'status-dot online';
            statusText.textContent = 'Running';
        }
        btn.innerHTML = '<i class="fas fa-stop"></i> Stop';
        btn.classList.add('running');
    } else {
        statusDot.className = 'status-dot offline';
        statusText.textContent = 'Stopped';
        btn.innerHTML = '<i class="fas fa-play"></i> Start';
        btn.classList.remove('running');
    }

    // Uptime
    if (s.uptime_sec) {
        const m = Math.floor(s.uptime_sec / 60);
        const sec = Math.floor(s.uptime_sec % 60);
        document.getElementById('uptime').textContent = `${m}m ${sec}s`;
    }

    // Binance price
    if (s.fair_value) {
        document.getElementById('binance-price').textContent =
            s.fair_value.binance_mid ? '$' + s.fair_value.binance_mid.toLocaleString() : '—';

        // Fair values
        setText('fv-up', s.fair_value.up ? s.fair_value.up.toFixed(4) : '—');
        setText('fv-dn', s.fair_value.dn ? s.fair_value.dn.toFixed(4) : '—');
    }

    // Quotes
    if (s.quotes) {
        setQuote('up-bid', s.quotes.up_bid);
        setQuote('up-ask', s.quotes.up_ask);
        setQuote('dn-bid', s.quotes.dn_bid);
        setQuote('dn-ask', s.quotes.dn_ask);
    }

    // PM prices
    if (s.pm_prices) {
        setText('pm-up', s.pm_prices.up ? s.pm_prices.up.toFixed(4) : '—');
        setText('pm-dn', s.pm_prices.dn ? s.pm_prices.dn.toFixed(4) : '—');
    }

    // Inventory
    if (s.inventory) {
        setText('inv-up', s.inventory.up_shares);
        setText('inv-dn', s.inventory.dn_shares);
        setText('inv-delta', s.inventory.net_delta);
        setText('inv-usdc', '$' + (s.inventory.usdc || 0).toFixed(2));
        updateInventoryBar(s.inventory);
    }

    // PnL
    const realized = s.realized_pnl || 0;
    const unrealized = s.unrealized_pnl || 0;
    const total = s.total_pnl || (realized + unrealized);
    setPnl('pnl-realized', realized);
    setPnl('pnl-unrealized', unrealized);
    setPnl('pnl-total', total);
    setText('pnl-fees', '$' + (s.total_fees || 0).toFixed(4));

    // Stats
    setText('stat-volume', '$' + (s.total_volume || 0).toFixed(2));
    setText('stat-fills', s.fill_count || 0);
    setText('stat-quotes', s.quote_count || 0);
    setText('stat-requotes', s.requote_count || 0);
    setText('stat-spread', (s.avg_spread_bps || 0).toFixed(0) + ' bps');
    setText('stat-vol', s.fair_value ? (s.fair_value.volatility * 100).toFixed(3) + '%' : '—');

    if (s.heartbeat) {
        setText('stat-heartbeat', s.heartbeat.running ? 'OK (' + s.heartbeat.heartbeat_count + ')' : 'OFF');
    }
    if (s.rebate) {
        setText('stat-rebate', '$' + (s.rebate.estimated_daily_rebate || 0).toFixed(4));
    }

    // Fills
    if (s.recent_fills) {
        updateFills(s.recent_fills);
        setText('fill-count', s.fill_count || s.recent_fills.length);
    }

    // Market info
    if (s.market) {
        setText('market-info', `${s.market.coin || ''} ${s.market.timeframe || ''}`);
        setText('market-strike', 'Strike: ' + (s.market.strike ? '$' + s.market.strike.toLocaleString() : '—'));
        const tr = s.market.time_remaining || 0;
        setText('market-time', 'Time: ' + Math.floor(tr / 60) + 'm ' + Math.floor(tr % 60) + 's');
    }

    // Risk status
    const risk = document.getElementById('risk-status');
    if (s.is_paused) {
        risk.className = 'risk-danger';
        risk.innerHTML = '<i class="fas fa-shield-alt"></i> ' + (s.pause_reason || 'PAUSED');
    } else if (isRunning) {
        risk.className = 'risk-ok';
        risk.innerHTML = '<i class="fas fa-shield-alt"></i> OK';
    } else {
        risk.className = '';
        risk.innerHTML = '<i class="fas fa-shield-alt"></i> Idle';
    }

    // Update config inputs (only if not focused)
    if (s.config) {
        setConfigIfNotFocused('cfg-spread', s.config.half_spread_bps);
        setConfigIfNotFocused('cfg-size', s.config.order_size_usd);
        setConfigIfNotFocused('cfg-max-inv', s.config.max_inventory_shares);
        setConfigIfNotFocused('cfg-skew', s.config.skew_bps_per_unit);
        setConfigIfNotFocused('cfg-requote', s.config.requote_interval_sec);
        setConfigIfNotFocused('cfg-drawdown', s.config.max_drawdown_usd);
    }

    // Charts
    updateCharts(s);
}

// ── Helpers ──────────────────────────────────────────
function setText(id, val) {
    const el = document.getElementById(id);
    if (el) el.textContent = val;
}

function setQuote(prefix, data) {
    if (data) {
        setText(prefix + '-price', data.price.toFixed(2));
        setText(prefix + '-size', data.size.toFixed(1));
    } else {
        setText(prefix + '-price', '—');
        setText(prefix + '-size', '—');
    }
}

function setPnl(id, val) {
    const el = document.getElementById(id);
    if (!el) return;
    el.textContent = '$' + val.toFixed(4);
    el.classList.remove('pnl-positive', 'pnl-negative');
    el.classList.add(val >= 0 ? 'pnl-positive' : 'pnl-negative');
}

function setConfigIfNotFocused(id, val) {
    const el = document.getElementById(id);
    if (el && document.activeElement !== el) {
        el.value = val;
    }
}

function updateInventoryBar(inv) {
    const bar = document.getElementById('inv-bar-fill');
    if (!bar) return;
    const total = Math.max(inv.up_shares + inv.dn_shares, 1);
    const ratio = inv.up_shares / total;
    const pct = ratio * 100;
    bar.style.width = pct + '%';
    bar.style.left = '0';
    bar.style.background = pct > 55 ? 'var(--green)' : pct < 45 ? 'var(--red)' : 'var(--accent)';
}

function updateFills(fills) {
    const body = document.getElementById('fills-body');
    if (!body) return;
    body.innerHTML = fills.slice(0, 20).map(f => {
        const time = new Date(f.ts * 1000).toLocaleTimeString();
        const cls = f.side === 'BUY' ? 'fill-buy' : 'fill-sell';
        return `<tr>
            <td>${time}</td>
            <td class="${cls}">${f.side}</td>
            <td>${f.price.toFixed(2)}</td>
            <td>${f.size.toFixed(1)}</td>
            <td>${f.is_maker ? 'Maker' : 'Taker'}</td>
        </tr>`;
    }).join('');
}

function updateCharts(s) {
    const now = Math.floor(Date.now() / 1000);

    // Price chart
    if (priceSeries && s.fair_value && s.fair_value.binance_mid) {
        priceSeries.update({ time: now, value: s.fair_value.binance_mid });
    }
    if (fvSeries && s.fair_value && s.fair_value.up) {
        // Show FV as scaled value for overlay
        fvSeries.update({ time: now, value: s.fair_value.up * 100 });
    }

    // Spread chart
    if (spreadSeries && s.avg_spread_bps) {
        spreadData.push({ time: now, value: s.avg_spread_bps });
        if (spreadData.length > 500) spreadData = spreadData.slice(-300);
        spreadSeries.setData(spreadData);
    }

    // PnL chart
    if (pnlSeries) {
        const total = s.total_pnl || 0;
        pnlData.push({ time: now, value: total });
        if (pnlData.length > 500) pnlData = pnlData.slice(-300);
        pnlSeries.setData(pnlData);
        // Color based on PnL
        pnlSeries.applyOptions({
            topColor: total >= 0 ? 'rgba(34, 197, 94, 0.3)' : 'rgba(239, 68, 68, 0.3)',
            bottomColor: total >= 0 ? 'rgba(34, 197, 94, 0.0)' : 'rgba(239, 68, 68, 0.0)',
            lineColor: total >= 0 ? '#22c55e' : '#ef4444',
        });
    }
}

// ── Actions ──────────────────────────────────────────
async function toggleMM() {
    if (isRunning) {
        await fetch(`${API_BASE}/api/mm/stop`, { method: 'POST' });
    } else {
        const coin = document.getElementById('coin-select').value;
        const tf = document.getElementById('tf-select').value;
        const paper = document.getElementById('paper-mode').checked;
        await fetch(`${API_BASE}/api/mm/start`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ coin, timeframe: tf, paper_mode: paper }),
        });
    }
    setTimeout(pollState, 500);
}

async function emergency() {
    if (confirm('Emergency stop — cancel all orders?')) {
        await fetch(`${API_BASE}/api/mm/emergency`, { method: 'POST' });
        setTimeout(pollState, 500);
    }
}

async function saveConfig() {
    const cfg = {
        half_spread_bps: parseFloat(document.getElementById('cfg-spread').value),
        order_size_usd: parseFloat(document.getElementById('cfg-size').value),
        max_inventory_shares: parseFloat(document.getElementById('cfg-max-inv').value),
        skew_bps_per_unit: parseFloat(document.getElementById('cfg-skew').value),
        requote_interval_sec: parseFloat(document.getElementById('cfg-requote').value),
        max_drawdown_usd: parseFloat(document.getElementById('cfg-drawdown').value),
    };
    await fetch(`${API_BASE}/api/mm/config`, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(cfg),
    });
    setTimeout(pollState, 300);
}

// ── Enter key login ──────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
    checkAuth();
    const input = document.getElementById('auth-key');
    if (input) {
        input.addEventListener('keydown', e => {
            if (e.key === 'Enter') doLogin();
        });
    }
});
