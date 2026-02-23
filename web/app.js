/* Polymarket MM Dashboard — Frontend App v2 */

const API_BASE = '';
let isRunning = false;
let isWatching = false;
let pollTimer = null;

// Charts
let priceChart = null;
let fvUpSeries = null;
let fvDnSeries = null;
let pmUpSeries = null;
let pmDnSeries = null;
let spreadChart = null;
let spreadSeries = null;
let pnlChart = null;
let pnlSeries = null;
let pnlData = [];
let spreadData = [];
let lastStartedAt = 0;

// Last known state for order distance calc
let lastState = {};

// ── Collapsible Sections ─────────────────────────────
function toggleSection(el) {
    const content = el.nextElementSibling;
    if (!content) return;
    const isCollapsed = content.classList.toggle('collapsed');
    const chevron = el.querySelector('.chevron');
    if (chevron) chevron.textContent = isCollapsed ? '\u25b8' : '\u25be';
}

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
    // Auto-start watch mode for live feed data when no session is running
    autoWatch();
}

async function autoWatch() {
    try {
        const r = await fetch(`${API_BASE}/api/mm/state`);
        if (!r.ok) return;
        const s = await r.json();
        if (s.is_running) return; // Session active, no need for watch
        // Start watch mode for live feeds
        const coin = document.getElementById('coin-select')?.value || 'BTC';
        const tf = document.getElementById('tf-select')?.value || '5m';
        await fetch(`${API_BASE}/api/mm/watch`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ coin, timeframe: tf }),
        });
    } catch(e) {
        // Silent fail — watch is optional
    }
}

// ── Charts Init ──────────────────────────────────────
function initCharts() {
    const chartOpts = {
        layout: { background: { type: 'solid', color: '#18181b' }, textColor: '#a1a1aa' },
        grid: { vertLines: { color: '#27272a' }, horzLines: { color: '#27272a' } },
        timeScale: { timeVisible: true, secondsVisible: false },
        crosshair: { mode: 0 },
    };

    // Fair Value & PM Prices chart (all 0-1 range)
    const priceEl = document.getElementById('price-chart');
    if (priceEl && typeof LightweightCharts !== 'undefined') {
        priceChart = LightweightCharts.createChart(priceEl, {
            ...chartOpts, width: priceEl.clientWidth, height: 242,
            rightPriceScale: { autoScale: true, scaleMargins: { top: 0.05, bottom: 0.05 } },
        });
        fvUpSeries = priceChart.addLineSeries({ color: '#3b82f6', lineWidth: 2, title: 'FV UP' });
        fvDnSeries = priceChart.addLineSeries({ color: '#06b6d4', lineWidth: 2, title: 'FV DN' });
        pmUpSeries = priceChart.addLineSeries({ color: '#22c55e', lineWidth: 1, lineStyle: 2, title: 'PM UP' });
        pmDnSeries = priceChart.addLineSeries({ color: '#f97316', lineWidth: 1, lineStyle: 2, title: 'PM DN' });
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
            topColor: 'rgba(16, 185, 129, 0.3)', bottomColor: 'rgba(16, 185, 129, 0.0)',
            lineColor: '#10b981', lineWidth: 2,
        });
    }

    // Resize observer
    const ro = new ResizeObserver(() => {
        if (priceChart && priceEl) priceChart.applyOptions({ width: priceEl.clientWidth });
        if (spreadChart && spreadEl) spreadChart.applyOptions({ width: spreadEl.clientWidth });
        if (pnlChart && pnlEl) pnlChart.applyOptions({ width: pnlEl.clientWidth });
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
        lastState = s;
        updateUI(s);
    } catch(e) {
        document.getElementById('status-text').textContent = 'Connection error';
        document.getElementById('status-indicator').className = 'status-dot offline';
    }
}

// ── UI Update ────────────────────────────────────────
function updateUI(s) {
    isRunning = s.is_running || false;
    isWatching = !isRunning && s.feeds && Object.keys(s.feeds).length > 0;

    // Sync paper mode toggle with backend only while running
    const paperToggle = document.getElementById('paper-mode');
    if (paperToggle && isRunning && s.paper_mode !== undefined) {
        paperToggle.checked = s.paper_mode;
    }
    // Sync coin & timeframe selects with backend (only when session is running)
    if (isRunning && s.market) {
        const coinSelect = document.getElementById('coin-select');
        if (coinSelect && s.market.coin && coinSelect.value !== s.market.coin) {
            coinSelect.value = s.market.coin;
        }
        const tfSelect = document.getElementById('tf-select');
        if (tfSelect && s.market.timeframe && tfSelect.value !== s.market.timeframe) {
            tfSelect.value = s.market.timeframe;
        }
    }

    // PM balance & session limit (settings row 2)
    const pmBal = s.usdc_balance_pm;
    setText('pm-balance', pmBal != null ? '$' + pmBal.toFixed(2) : '—');
    const stakeInput = document.getElementById('stake-usdc');
    if (s.session_limit) {
        setText('session-limit', '$' + s.session_limit.toFixed(0));
        if (isRunning && stakeInput) stakeInput.value = s.session_limit;
    } else {
        setText('session-limit', stakeInput ? '$' + stakeInput.value : '—');
    }

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
    } else if (isWatching) {
        statusDot.className = 'status-dot watching';
        statusText.textContent = 'Watching';
        btn.innerHTML = '<i class="fas fa-play"></i> Start';
        btn.classList.remove('running');
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
        // Avg entry prices
        setText('inv-up-avg', s.inventory.up_avg_entry != null ? s.inventory.up_avg_entry.toFixed(4) : '—');
        setText('inv-dn-avg', s.inventory.dn_avg_entry != null ? s.inventory.dn_avg_entry.toFixed(4) : '—');
    }

    // Liquidation lock
    const lockInfo = document.getElementById('liq-lock-info');
    if (lockInfo && s.liquidation_lock) {
        if (s.liquidation_lock.active) {
            lockInfo.classList.remove('hidden');
            setText('liq-up-floor', s.liquidation_lock.up_floor.toFixed(2));
            setText('liq-dn-floor', s.liquidation_lock.dn_floor.toFixed(2));
            setText('liq-chunk', s.liquidation_lock.chunk_index + '/' + s.liquidation_lock.total_chunks);
        } else {
            lockInfo.classList.add('hidden');
        }
    }

    // ── PnL (simplified: 3 big numbers) ──────────────
    const sessionPnl = s.session_pnl != null ? s.session_pnl : (s.total_pnl || 0);
    const positionsWorth = (s.inventory?.up_shares || 0) * (s.pm_prices?.up || 0)
                         + (s.inventory?.dn_shares || 0) * (s.pm_prices?.dn || 0);
    const freeUsdc = s.usdc_balance_pm || 0;
    const stake = s.session_limit || 1;
    const pnlPct = (sessionPnl / stake * 100).toFixed(1);

    // Session PnL (big)
    const pnlSessionEl = document.getElementById('pnl-session');
    if (pnlSessionEl) {
        pnlSessionEl.textContent = '$' + sessionPnl.toFixed(4);
        pnlSessionEl.classList.remove('pnl-positive', 'pnl-negative');
        pnlSessionEl.classList.add(sessionPnl >= 0 ? 'pnl-positive' : 'pnl-negative');
    }
    const pctEl = document.getElementById('pnl-pct');
    if (pctEl) {
        pctEl.textContent = (sessionPnl >= 0 ? '+' : '') + pnlPct + '%';
        pctEl.className = 'pnl-pct ' + (sessionPnl >= 0 ? 'positive' : 'negative');
    }

    // Positions worth
    const posEl = document.getElementById('pnl-positions-worth');
    if (posEl) {
        posEl.textContent = '$' + positionsWorth.toFixed(2);
    }

    // Free USDC
    const freeEl = document.getElementById('pnl-free-usdc');
    if (freeEl) {
        freeEl.textContent = '$' + freeUsdc.toFixed(2);
    }

    // Session Stats (collapsed section) — detailed PnL + stats
    const realized = s.realized_pnl || 0;
    const unrealized = s.unrealized_pnl || 0;
    setText('pnl-realized', '$' + realized.toFixed(4));
    setText('pnl-unrealized', '$' + unrealized.toFixed(4));
    setText('peak-pnl', '$' + (s.peak_pnl || 0).toFixed(4));
    setText('pnl-fees', '$' + (s.total_fees || 0).toFixed(4));

    // Stats
    setText('stat-volume', '$' + (s.total_volume || 0).toFixed(2));
    setText('stat-fills', s.fill_count || 0);
    setText('stat-quotes', s.quote_count || 0);
    setText('stat-requotes', s.requote_count || 0);
    setText('stat-spread', (s.avg_spread_bps || 0).toFixed(0) + ' bps');
    setText('stat-vol', s.fair_value ? (s.fair_value.volatility * 100).toFixed(3) + '%' : '—');
    // Current spread in collapsible header
    setText('current-spread', (s.avg_spread_bps || 0).toFixed(0) + ' bps');

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

    // Active Orders (with distance)
    updateActiveOrders(s.active_orders_detail, s);

    // Market Quality
    updateMarketQuality(s.market_quality);

    // Market info + window progress
    if (s.market) {
        setText('market-info', `${s.market.coin || ''} ${s.market.timeframe || ''}`);
        setText('market-strike', 'Strike: ' + (s.market.strike ? '$' + s.market.strike.toLocaleString() : '—'));
        const tr = s.market.time_remaining || 0;
        const mins = Math.floor(tr / 60), secs = Math.floor(tr % 60);
        setText('market-time', tr > 0 ? `${mins}m ${secs}s` : 'Expired');

        // Window remaining in settings panel
        setText('window-remaining', tr > 0 ? `${mins}m ${secs}s` : 'Expired');

        // Window state badge
        const wsEl = document.getElementById('window-state');
        const nwi = s.next_window_in || 0;
        if (wsEl) {
            if (nwi > 0) {
                const nwSec = Math.ceil(nwi);
                wsEl.innerHTML = '<span style="color:#8b5cf6;font-weight:700">\u23f3 Next window in ' + nwSec + 's</span>';
            } else if (s.is_closing) {
                wsEl.innerHTML = '<span style="color:#f59e0b;font-weight:700;animation:pulse 0.8s infinite">\u26a0 CLOSING</span>';
            } else if (tr <= 0) {
                wsEl.innerHTML = '<span style="color:#6b7280">\u23f3 RESOLVING</span>';
            } else if (tr <= 30) {
                wsEl.innerHTML = '<span style="color:#ef4444;font-weight:700">\u23f0 ' + secs + 's</span>';
            } else {
                wsEl.textContent = '';
            }
        }

        // Inline progress bar in settings panel
        const progressBar = document.getElementById('window-progress-bar-inline');
        if (progressBar && isRunning && tr > 0) {
            const tfSec = {'5m':300,'15m':900,'1h':3600,'4h':14400,'daily':86400};
            const totalSec = tfSec[s.market.timeframe] || 300;
            const pct = Math.min(100, Math.max(0, (tr / totalSec) * 100));
            progressBar.style.width = pct + '%';
            if (pct > 40) progressBar.style.background = 'var(--accent)';
            else if (pct > 15) progressBar.style.background = '#f59e0b';
            else progressBar.style.background = '#ef4444';
        } else if (progressBar) {
            progressBar.style.width = '0%';
        }
    }

    // Risk status
    const risk = document.getElementById('risk-status');
    if (s.is_paused) {
        risk.className = 'risk-danger';
        risk.innerHTML = '<i class="fas fa-shield-alt"></i> ' + (s.pause_reason || 'PAUSED');
    } else if (s.is_closing) {
        risk.className = 'risk-warn';
        risk.innerHTML = '<i class="fas fa-hourglass-end"></i> CLOSING';
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
        setConfigIfNotFocused('cfg-min-spread', s.config.min_spread_bps);
        setConfigIfNotFocused('cfg-max-spread', s.config.max_spread_bps);
        setConfigIfNotFocused('cfg-vol-mult', s.config.vol_spread_mult);
        setConfigIfNotFocused('cfg-size', s.config.order_size_usd);
        setConfigIfNotFocused('cfg-min-size', s.config.min_order_size_usd);
        setConfigIfNotFocused('cfg-max-size', s.config.max_order_size_usd);
        setConfigIfNotFocused('cfg-max-inv', s.config.max_inventory_shares);
        setConfigIfNotFocused('cfg-skew', s.config.skew_bps_per_unit);
        setConfigIfNotFocused('cfg-requote', s.config.requote_interval_sec);
        setConfigIfNotFocused('cfg-requote-thresh', s.config.requote_threshold_bps);
        setConfigIfNotFocused('cfg-gtd-dur', s.config.gtd_duration_sec);
        setConfigIfNotFocused('cfg-heartbeat', s.config.heartbeat_interval_sec);
        setCheckboxIfNotFocused('cfg-post-only', s.config.use_post_only);
        setCheckboxIfNotFocused('cfg-use-gtd', s.config.use_gtd);
        setConfigIfNotFocused('cfg-drawdown', s.config.max_drawdown_usd);
        setConfigIfNotFocused('cfg-vol-pause', s.config.volatility_pause_mult);
        setConfigIfNotFocused('cfg-max-loss', s.config.max_loss_per_fill_usd);
        setConfigIfNotFocused('cfg-take-profit', s.config.take_profit_usd);
        setConfigIfNotFocused('cfg-trail-stop', (s.config.trailing_stop_pct || 0) * 100);

        // Update enabled button state
        const enabledBtn = document.getElementById('cfg-enabled-btn');
        if (enabledBtn) {
            if (s.config.enabled) {
                enabledBtn.classList.add('active');
                enabledBtn.textContent = 'Enabled';
            } else {
                enabledBtn.classList.remove('active');
                enabledBtn.textContent = 'Disabled';
            }
        }
    }

    // Feed status
    updateFeedStatus(s.feeds);

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

function setConfigIfNotFocused(id, val) {
    const el = document.getElementById(id);
    if (el && document.activeElement !== el) {
        el.value = val;
    }
}

function setCheckboxIfNotFocused(id, val) {
    const el = document.getElementById(id);
    if (el && document.activeElement !== el) {
        el.checked = !!val;
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
    bar.style.background = pct > 55 ? 'var(--success)' : pct < 45 ? 'var(--destructive)' : 'var(--accent)';
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

function updateActiveOrders(orders, s) {
    const body = document.getElementById('orders-body');
    if (!body) return;
    const countEl = document.getElementById('orders-count');
    if (countEl) countEl.textContent = orders ? orders.length : 0;
    if (!orders || orders.length === 0) {
        body.innerHTML = '<tr><td colspan="8" style="text-align:center;color:var(--text-muted)">No active orders</td></tr>';
        return;
    }
    body.innerHTML = orders.map(o => {
        const sideCls = o.side === 'BUY' ? 'order-buy' : 'order-sell';
        const typeBadge = o.type === 'liquidation'
            ? '<span class="liq-badge">LIQ</span>'
            : '';
        const age = o.age_sec < 60
            ? o.age_sec.toFixed(0) + 's'
            : (o.age_sec / 60).toFixed(1) + 'm';

        // Distance from PM mid price
        const pmMid = o.token === 'UP'
            ? (s && s.pm_prices && s.pm_prices.up ? s.pm_prices.up : 0.5)
            : (s && s.pm_prices && s.pm_prices.dn ? s.pm_prices.dn : 0.5);
        const distCents = Math.abs(o.price - pmMid) * 100;
        let distCls = 'dist-far';
        if (distCents <= 3) distCls = 'dist-close';
        else if (distCents <= 8) distCls = 'dist-mid';

        return `<tr class="${o.type === 'liquidation' ? 'liq-row' : ''}">
            <td>${o.token}</td>
            <td class="${sideCls}">${o.side}</td>
            <td>${o.price.toFixed(2)}</td>
            <td>${o.size.toFixed(1)}</td>
            <td>$${o.notional.toFixed(2)}</td>
            <td class="${distCls}">${distCents.toFixed(1)}\u00a2</td>
            <td>${age}</td>
            <td>${typeBadge || o.type}</td>
        </tr>`;
    }).join('');
}

function updateMarketQuality(mq) {
    const badge = document.getElementById('quality-badge');
    const barFill = document.getElementById('quality-bar-fill');
    const scoreVal = document.getElementById('quality-score-value');

    if (!mq || mq.overall_score == null) {
        if (badge) badge.textContent = '—';
        if (barFill) barFill.style.width = '0%';
        if (scoreVal) scoreVal.textContent = '—';
        setText('quality-liquidity', '—');
        setText('quality-spread-bps', '— bps');
        setText('quality-bid-depth', '$—');
        setText('quality-ask-depth', '$—');
        return;
    }

    const score = mq.overall_score;
    const pct = Math.round(score * 100);

    if (scoreVal) scoreVal.textContent = pct + '%';
    if (barFill) {
        barFill.style.width = pct + '%';
        barFill.style.background = score >= 0.6 ? 'var(--success)' :
                                    score >= 0.3 ? 'var(--warning)' : 'var(--destructive)';
    }
    if (badge) {
        if (mq.tradeable) {
            badge.textContent = 'GOOD';
            badge.className = 'quality-badge quality-good';
        } else {
            badge.textContent = 'POOR';
            badge.className = 'quality-badge quality-poor';
        }
    }

    setText('quality-liquidity', (mq.liquidity_score * 100).toFixed(0) + '%');
    setText('quality-spread-bps', (mq.spread_bps || 0).toFixed(0) + ' bps');
    setText('quality-bid-depth', '$' + (mq.bid_depth_usd || 0).toFixed(0));
    setText('quality-ask-depth', '$' + (mq.ask_depth_usd || 0).toFixed(0));
}

function updateFeedStatus(feeds) {
    const container = document.getElementById('feed-status');
    if (!container) return;
    if (!feeds || Object.keys(feeds).length === 0) {
        container.innerHTML = '<span style="color:var(--text-muted)">No feed data</span>';
        return;
    }

    const now = Date.now();

    function indicator(label, connected, msgCount, errCount, latencyMs) {
        let color = '#ef4444'; // red
        let icon = '\u25cf';
        if (connected) {
            if (latencyMs != null && latencyMs > 5000) {
                color = '#f59e0b'; // yellow
            } else {
                color = '#22c55e'; // green
            }
        }
        const latStr = latencyMs != null ? latencyMs + 'ms' : '—';
        return `<span class="feed-ind" style="margin-right:16px">` +
            `<span style="color:${color};font-size:12px">${icon}</span> ` +
            `<b>${label}</b> ${msgCount} msgs | ${errCount} err | ${latStr}` +
            `</span>`;
    }

    let html = '';
    const bws = feeds.binance_ws;
    if (bws) {
        html += indicator('Binance WS', bws.connected, bws.msg_count, bws.error_count, bws.latency_ms);
    }
    const bob = feeds.binance_ob;
    if (bob) {
        const readyLabel = bob.ready ? 'ready' : 'waiting';
        html += `<span class="feed-ind" style="margin-right:16px">` +
            `<span style="color:${bob.ready ? '#22c55e' : '#ef4444'};font-size:12px">\u25cf</span> ` +
            `<b>Binance OB</b> ${readyLabel} | ${bob.msg_count} msgs | ${bob.error_count} err` +
            `</span>`;
    }
    const pm = feeds.polymarket;
    if (pm) {
        html += indicator('PM WS', pm.connected, pm.msg_count, pm.error_count, pm.last_update_ms_ago);
    }
    container.innerHTML = html;
}

function updateCharts(s) {
    // Clear chart data on new session
    if (s.started_at && s.started_at !== lastStartedAt) {
        lastStartedAt = s.started_at;
        pnlData = [];
        spreadData = [];
    }

    const now = Math.floor(Date.now() / 1000);

    // Fair Value & PM Prices chart (all 0-1 range)
    if (s.fair_value) {
        if (fvUpSeries && s.fair_value.up) fvUpSeries.update({ time: now, value: s.fair_value.up });
        if (fvDnSeries && s.fair_value.dn) fvDnSeries.update({ time: now, value: s.fair_value.dn });
    }
    if (s.pm_prices) {
        if (pmUpSeries && s.pm_prices.up) pmUpSeries.update({ time: now, value: s.pm_prices.up });
        if (pmDnSeries && s.pm_prices.dn) pmDnSeries.update({ time: now, value: s.pm_prices.dn });
    }

    // Spread chart
    if (spreadSeries && s.avg_spread_bps) {
        spreadData.push({ time: now, value: s.avg_spread_bps });
        if (spreadData.length > 500) spreadData = spreadData.slice(-300);
        spreadSeries.setData(spreadData);
    }

    // PnL chart
    if (pnlSeries) {
        const total = s.session_pnl != null ? s.session_pnl : (s.total_pnl || 0);
        pnlData.push({ time: now, value: total });
        if (pnlData.length > 500) pnlData = pnlData.slice(-300);
        pnlSeries.setData(pnlData);
        // Color based on PnL
        pnlSeries.applyOptions({
            topColor: total >= 0 ? 'rgba(16, 185, 129, 0.3)' : 'rgba(239, 68, 68, 0.3)',
            bottomColor: total >= 0 ? 'rgba(16, 185, 129, 0.0)' : 'rgba(239, 68, 68, 0.0)',
            lineColor: total >= 0 ? '#10b981' : '#ef4444',
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
        const stake = parseFloat(document.getElementById('stake-usdc').value) || 10;
        const r = await fetch(`${API_BASE}/api/mm/start`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ coin, timeframe: tf, paper_mode: paper, initial_usdc: stake }),
        });
        if (!r.ok) {
            const err = await r.json().catch(() => ({ detail: 'Failed to start MM' }));
            showToast(err.detail || 'Failed to start', 'error');
            return;
        }
    }
    setTimeout(pollState, 500);
}

async function emergency() {
    if (confirm('Emergency stop \u2014 cancel all orders?')) {
        await fetch(`${API_BASE}/api/mm/emergency`, { method: 'POST' });
        setTimeout(pollState, 500);
    }
}

async function killAll() {
    if (!confirm('KILL ALL: Stop trading, sell all positions, disable auto-restart. Continue?')) return;
    try {
        const r = await fetch(`${API_BASE}/api/mm/kill`, { method: 'POST' });
        if (r.ok) {
            showToast('Kill All executed \u2014 all positions liquidated', 'warning');
        } else {
            showToast('Kill All failed', 'error');
        }
    } catch (e) {
        showToast('Kill All error: ' + e.message, 'error');
    }
    setTimeout(pollState, 500);
}

async function saveConfig() {
    const cfg = {
        half_spread_bps: parseFloat(document.getElementById('cfg-spread').value),
        min_spread_bps: parseFloat(document.getElementById('cfg-min-spread').value),
        max_spread_bps: parseFloat(document.getElementById('cfg-max-spread').value),
        vol_spread_mult: parseFloat(document.getElementById('cfg-vol-mult').value),
        order_size_usd: parseFloat(document.getElementById('cfg-size').value),
        min_order_size_usd: parseFloat(document.getElementById('cfg-min-size').value),
        max_order_size_usd: parseFloat(document.getElementById('cfg-max-size').value),
        max_inventory_shares: parseFloat(document.getElementById('cfg-max-inv').value),
        skew_bps_per_unit: parseFloat(document.getElementById('cfg-skew').value),
        requote_interval_sec: parseFloat(document.getElementById('cfg-requote').value),
        requote_threshold_bps: parseFloat(document.getElementById('cfg-requote-thresh').value),
        gtd_duration_sec: parseInt(document.getElementById('cfg-gtd-dur').value),
        heartbeat_interval_sec: parseInt(document.getElementById('cfg-heartbeat').value),
        use_post_only: document.getElementById('cfg-post-only').checked,
        use_gtd: document.getElementById('cfg-use-gtd').checked,
        max_drawdown_usd: parseFloat(document.getElementById('cfg-drawdown').value),
        volatility_pause_mult: parseFloat(document.getElementById('cfg-vol-pause').value),
        max_loss_per_fill_usd: parseFloat(document.getElementById('cfg-max-loss').value),
        take_profit_usd: parseFloat(document.getElementById('cfg-take-profit').value) || 0,
        trailing_stop_pct: (parseFloat(document.getElementById('cfg-trail-stop').value) || 0) / 100,
    };
    const r = await fetch(`${API_BASE}/api/mm/config`, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(cfg),
    });
    if (r.ok) {
        showToast('Config updated');
    } else {
        showToast('Config update failed', 'error');
    }
    setTimeout(pollState, 300);
}

async function toggleEnabled() {
    const btn = document.getElementById('cfg-enabled-btn');
    const nowEnabled = btn.classList.contains('active');
    const r = await fetch(`${API_BASE}/api/mm/config`, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ enabled: !nowEnabled }),
    });
    if (r.ok) {
        showToast(nowEnabled ? 'MM Disabled' : 'MM Enabled');
    }
    setTimeout(pollState, 300);
}

function showToast(msg, type = 'success') {
    document.querySelectorAll('.toast').forEach(t => t.remove());
    const toast = document.createElement('div');
    toast.className = 'toast ' + type;
    toast.textContent = msg;
    document.body.appendChild(toast);
    requestAnimationFrame(() => toast.classList.add('show'));
    setTimeout(() => {
        toast.classList.remove('show');
        setTimeout(() => toast.remove(), 300);
    }, 3000);
}

// ── Logs ─────────────────────────────────────────────
async function openLogs() {
    document.getElementById('logs-modal').classList.remove('hidden');
    await refreshLogs();
}

function closeLogs(e) {
    if (!e || e.target === e.currentTarget) {
        document.getElementById('logs-modal').classList.add('hidden');
    }
}

async function refreshLogs() {
    const level = document.getElementById('log-level-filter').value;
    const url = `${API_BASE}/api/logs?limit=300` + (level ? `&level=${level}` : '');
    try {
        const r = await fetch(url);
        if (!r.ok) { document.getElementById('logs-content').textContent = 'Auth error'; return; }
        const d = await r.json();
        const lines = d.logs.map(e =>
            `${e.time} [${e.level.padEnd(7)}] [${e.name}] ${e.msg}`
        ).join('\n');
        const pre = document.getElementById('logs-content');
        pre.textContent = lines || '(no logs)';
        pre.scrollTop = pre.scrollHeight;
    } catch(err) {
        document.getElementById('logs-content').textContent = 'Connection error: ' + err.message;
    }
}

async function copyLogs() {
    const text = document.getElementById('logs-content').textContent;
    try {
        await navigator.clipboard.writeText(text);
        showToast('Logs copied');
    } catch(e) {
        // Fallback
        const ta = document.createElement('textarea');
        ta.value = text;
        document.body.appendChild(ta);
        ta.select();
        document.execCommand('copy');
        ta.remove();
        showToast('Logs copied');
    }
}

// ── Credentials ──────────────────────────────────────
async function validateCredentials() {
    const badge = document.getElementById('cred-status');
    const text = document.getElementById('cred-status-text');
    if (!badge || !text) return;
    badge.className = 'cred-badge checking';
    text.textContent = 'Checking...';
    try {
        const r = await fetch(`${API_BASE}/api/mm/validate-credentials`, { method: 'POST' });
        const d = await r.json();
        if (d.valid) {
            badge.className = 'cred-badge valid';
            text.textContent = 'Valid';
        } else {
            badge.className = 'cred-badge invalid';
            text.textContent = d.detail || 'Invalid';
            showToast(d.detail || 'Invalid API credentials', 'error');
        }
    } catch (e) {
        badge.className = 'cred-badge invalid';
        text.textContent = 'Error';
    }
}

// ── Enter key login ──────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
    checkAuth();
    const paperToggle = document.getElementById('paper-mode');
    if (paperToggle) {
        paperToggle.addEventListener('change', function() {
            if (!this.checked) {
                validateCredentials();
            } else {
                const badge = document.getElementById('cred-status');
                const text = document.getElementById('cred-status-text');
                if (badge) badge.className = 'cred-badge';
                if (text) text.textContent = '—';
            }
        });
    }
    const input = document.getElementById('auth-key');
    if (input) {
        input.addEventListener('keydown', e => {
            if (e.key === 'Enter') doLogin();
        });
    }
});
