const el = (id) => document.getElementById(id);

const ui = {
  statusBadge: el("statusBadge"),
  mode: el("mode"),
  preset: el("preset"),
  coin: el("coin"),
  timeframe: el("timeframe"),
  startBtn: el("startBtn"),
  stopBtn: el("stopBtn"),
  approveBtn: el("approveBtn"),
  rejectBtn: el("rejectBtn"),
  errorBox: el("errorBox"),
  marketTitle: el("marketTitle"),
  gateText: el("gateText"),
  priceText: el("priceText"),
  pmText: el("pmText"),
  trendText: el("trendText"),
  biasText: el("biasText"),
  pendingCard: el("pendingCard"),
  pendingText: el("pendingText"),
  orderbookList: el("orderbookList"),
  flowList: el("flowList"),
  techList: el("techList"),
  signalsList: el("signalsList"),
  tradesBody: el("tradesBody"),
  logsBox: el("logsBox"),
};

const fields = [
  "size_usd",
  "min_bias",
  "min_obi",
  "min_price",
  "max_price",
  "cooldown_sec",
  "max_trades_per_day",
  "eval_interval_sec",
  "tp_pct",
  "sl_pct",
  "max_hold_sec",
  "reverse_exit_bias",
  "live_entry_fill_timeout_sec",
  "live_entry_fill_poll_sec",
];

const checks = [
  "auto_exit_enabled",
  "reverse_exit_enabled",
  "live_entry_require_fill",
  "keep_unfilled_entry_open",
];

let bootstrap = null;
let pollTimer = null;
let lastPendingSignature = "";
let audioCtx = null;

function fmtPrice(v) {
  if (v === null || v === undefined || Number.isNaN(v)) return "-";
  return `$${Number(v).toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
}

function fmtPct(v) {
  if (v === null || v === undefined || Number.isNaN(v)) return "-";
  const x = Number(v);
  return `${x >= 0 ? "+" : ""}${x.toFixed(1)}%`;
}

function fmtNum(v, digits = 2) {
  if (v === null || v === undefined || Number.isNaN(v)) return "-";
  return Number(v).toLocaleString(undefined, { maximumFractionDigits: digits });
}

function setError(msg = "") {
  ui.errorBox.textContent = msg;
}

function setStatus(running, mode) {
  ui.statusBadge.className = running ? "badge live" : "badge idle";
  ui.statusBadge.textContent = running ? `RUNNING ${String(mode || "").toUpperCase()}` : "IDLE";
}

function option(value, text) {
  const o = document.createElement("option");
  o.value = value;
  o.textContent = text;
  return o;
}

function setupCoinTimeframes() {
  ui.coin.innerHTML = "";
  (bootstrap.coins || []).forEach((c) => ui.coin.appendChild(option(c, c)));

  const refreshTimeframes = () => {
    const coin = ui.coin.value;
    const tfs = (bootstrap.coin_timeframes && bootstrap.coin_timeframes[coin]) || [];
    const current = ui.timeframe.value;
    ui.timeframe.innerHTML = "";
    tfs.forEach((tf) => ui.timeframe.appendChild(option(tf, tf)));
    if (tfs.includes(current)) {
      ui.timeframe.value = current;
    } else if (tfs.includes("15m")) {
      ui.timeframe.value = "15m";
    }
  };

  ui.coin.addEventListener("change", refreshTimeframes);
  ui.coin.value = "BTC";
  refreshTimeframes();
}

function applyPreset(name) {
  const preset = bootstrap.presets?.[name];
  if (!preset) return;
  Object.entries(preset).forEach(([k, v]) => {
    const node = el(k);
    if (node) node.value = String(v);
  });
}

function gatherStartPayload() {
  const env = {
    PM_PRIVATE_KEY: el("pm_private_key").value.trim(),
    PM_FUNDER: el("pm_funder").value.trim(),
    PM_SIGNATURE_TYPE: el("pm_signature_type").value,
  };

  const payload = {
    mode: ui.mode.value,
    preset: ui.preset.value,
    coin: ui.coin.value,
    timeframe: ui.timeframe.value,
    confirm_live_token: el("confirm_live_token").value.trim(),
    env,
    executions_log_file: "",
  };

  fields.forEach((name) => {
    const node = el(name);
    payload[name] = Number(node.value);
  });
  checks.forEach((name) => {
    payload[name] = !!el(name).checked;
  });
  return payload;
}

async function api(path, options = {}) {
  const res = await fetch(path, {
    method: options.method || "GET",
    headers: { "Content-Type": "application/json" },
    credentials: "include",
    body: options.body ? JSON.stringify(options.body) : undefined,
  });
  let data = null;
  try {
    data = await res.json();
  } catch {
    data = null;
  }
  if (!res.ok) {
    throw new Error(data?.detail || data?.error || `HTTP ${res.status}`);
  }
  return data;
}

function initAudio() {
  if (!audioCtx) {
    const Ctx = window.AudioContext || window.webkitAudioContext;
    if (Ctx) audioCtx = new Ctx();
  }
  if (audioCtx && audioCtx.state === "suspended") {
    audioCtx.resume().catch(() => {});
  }
}

function playPendingSound() {
  if (!audioCtx) return;
  const now = audioCtx.currentTime;
  [880, 1100].forEach((freq, i) => {
    const osc = audioCtx.createOscillator();
    const gain = audioCtx.createGain();
    osc.type = "triangle";
    osc.frequency.value = freq;
    gain.gain.value = 0.0001;
    osc.connect(gain);
    gain.connect(audioCtx.destination);
    const start = now + i * 0.12;
    osc.start(start);
    gain.gain.exponentialRampToValueAtTime(0.12, start + 0.01);
    gain.gain.exponentialRampToValueAtTime(0.0001, start + 0.1);
    osc.stop(start + 0.11);
  });
}

function li(text) {
  const node = document.createElement("li");
  node.textContent = text;
  return node;
}

function renderList(node, items) {
  node.innerHTML = "";
  if (!items || !items.length) {
    node.appendChild(li("-"));
    return;
  }
  items.forEach((x) => node.appendChild(li(String(x))));
}

function renderTrades(trades) {
  ui.tradesBody.innerHTML = "";
  (trades || []).slice(-40).reverse().forEach((t) => {
    const tr = document.createElement("tr");
    const when = t.ts_iso ? new Date(t.ts_iso).toLocaleTimeString() : "-";
    const pnl = t.pnl_pct !== null && t.pnl_pct !== undefined
      ? `${fmtPct(t.pnl_pct)} / $${fmtNum(t.pnl_usd, 2)}`
      : "-";
    [
      when,
      t.action || "-",
      t.side || "-",
      t.price !== undefined ? Number(t.price).toFixed(3) : "-",
      t.size_usd !== undefined ? `$${fmtNum(t.size_usd, 2)}` : "-",
      t.status || "-",
      pnl,
    ].forEach((txt) => {
      const td = document.createElement("td");
      td.textContent = txt;
      tr.appendChild(td);
    });
    ui.tradesBody.appendChild(tr);
  });
}

function renderState(state) {
  if (!state) return;

  setStatus(state.running, state.mode);
  ui.marketTitle.textContent = `${state.coin || "-"} ${state.timeframe || "-"}`;
  ui.gateText.textContent = `feed gate: ${state.feed_gate?.reason || "-"}`;

  const market = state.market || {};
  const orderbook = market.orderbook || {};
  const flow = market.flow || {};
  const tech = market.technical || {};

  ui.priceText.textContent = fmtPrice(market.price);
  ui.pmText.textContent = market.pm_up !== null && market.pm_down !== null && market.pm_up !== undefined
    ? `${Number(market.pm_up).toFixed(3)} / ${Number(market.pm_down).toFixed(3)}`
    : "-";
  ui.trendText.textContent = `${market.trend?.label || "-"} (${market.trend?.score ?? "-"})`;
  ui.biasText.textContent = `${market.bias?.label || "-"} ${fmtPct(market.bias?.value)}`;

  renderList(ui.orderbookList, [
    `OBI: ${fmtPct((orderbook.obi || 0) * 100)}`,
    `Depth 0.1%: ${fmtPrice(orderbook.depth?.[0.1])}`,
    `Depth 0.5%: ${fmtPrice(orderbook.depth?.[0.5])}`,
    `Depth 1.0%: ${fmtPrice(orderbook.depth?.[1.0])}`,
    `BUY walls: ${(orderbook.buy_walls || []).map((x) => Number(x[0]).toFixed(2)).join(", ") || "none"}`,
    `SELL walls: ${(orderbook.sell_walls || []).map((x) => Number(x[0]).toFixed(2)).join(", ") || "none"}`,
  ]);

  renderList(ui.flowList, [
    `CVD 1m: $${fmtNum(flow.cvd_1m, 2)}`,
    `CVD 3m: $${fmtNum(flow.cvd_3m, 2)}`,
    `CVD 5m: $${fmtNum(flow.cvd_5m, 2)}`,
    `Delta 1m: $${fmtNum(flow.delta_1m, 2)}`,
    `POC: ${fmtPrice(flow.poc)}`,
  ]);

  const ha = (tech.ha_last8 || []).map((x) => (x ? "▲" : "▼")).join(" ");
  renderList(ui.techList, [
    `RSI: ${fmtNum(tech.rsi, 1)}`,
    `MACD: ${fmtNum(tech.macd, 6)} | signal ${fmtNum(tech.signal, 6)}`,
    `VWAP: ${fmtPrice(tech.vwap)}`,
    `EMA5 / EMA20: ${fmtPrice(tech.ema5)} / ${fmtPrice(tech.ema20)}`,
    `Heikin Ashi: ${ha || "-"}`,
  ]);

  renderList(ui.signalsList, market.signals || []);

  const trader = state.trader || null;
  renderTrades(trader?.trades || []);

  const pending = trader?.pending_decision || null;
  if (pending) {
    ui.pendingCard.classList.remove("hidden");
    ui.pendingText.textContent = `${pending.side} @ ${Number(pending.price).toFixed(3)} | ${pending.reason || ""}`;
    const signature = `${trader.pending_key || ""}|${Number(pending.price).toFixed(3)}`;
    if (signature && signature !== lastPendingSignature) {
      lastPendingSignature = signature;
      playPendingSound();
    }
  } else {
    ui.pendingCard.classList.add("hidden");
    ui.pendingText.textContent = "-";
    lastPendingSignature = "";
  }

  ui.logsBox.textContent = (state.logs || []).join("\n");
  ui.logsBox.scrollTop = ui.logsBox.scrollHeight;
}

async function pollState() {
  try {
    const data = await api("/api/state");
    renderState(data.state);
  } catch (e) {
    setError(e.message || String(e));
  }
}

async function runCommand(command) {
  try {
    await api("/api/command", { method: "POST", body: { command } });
    await pollState();
  } catch (e) {
    setError(e.message || String(e));
  }
}

async function onStart() {
  setError("");
  initAudio();
  try {
    const payload = gatherStartPayload();
    await api("/api/start", { method: "POST", body: payload });
    await pollState();
  } catch (e) {
    setError(e.message || String(e));
  }
}

async function onStop() {
  setError("");
  try {
    await api("/api/stop", { method: "POST" });
    await pollState();
  } catch (e) {
    setError(e.message || String(e));
  }
}

async function init() {
  try {
    const data = await api("/api/bootstrap");
    bootstrap = data;
    setupCoinTimeframes();
    el("confirm_live_token").value = data.live_confirm_token || "";
    applyPreset("medium");
    renderState(data.state);

    ui.preset.addEventListener("change", () => applyPreset(ui.preset.value));
    ui.startBtn.addEventListener("click", onStart);
    ui.stopBtn.addEventListener("click", onStop);
    ui.approveBtn.addEventListener("click", () => runCommand("approve"));
    ui.rejectBtn.addEventListener("click", () => runCommand("reject"));

    document.querySelectorAll("[data-cmd]").forEach((node) => {
      node.addEventListener("click", () => runCommand(node.dataset.cmd));
    });

    if (pollTimer) clearInterval(pollTimer);
    pollTimer = setInterval(pollState, 1200);
  } catch (e) {
    setError(e.message || String(e));
  }
}

init();
