const el = (id) => document.getElementById(id);

const ui = {
  authOverlay: el("authOverlay"),
  authKeyInput: el("authKeyInput"),
  authSubmitBtn: el("authSubmitBtn"),
  authHint: el("authHint"),
  statusBadge: el("statusBadge"),
  mode: el("mode"),
  preset: el("preset"),
  coin: el("coin"),
  timeframe: el("timeframe"),
  startBtn: el("startBtn"),
  stopBtn: el("stopBtn"),
  windowBar: el("windowBar"),
  windowTimer: el("windowTimer"),
  errorBox: el("errorBox"),
  toastContainer: el("toastContainer"),
  marketTitle: el("marketTitle"),
  gateText: el("gateText"),
  summaryText: el("summaryText"),
  priceText: el("priceText"),
  pmText: el("pmText"),
  trendText: el("trendText"),
  biasText: el("biasText"),
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
  "auto_approve_live",
  "client_watchdog_enabled",
  "auto_exit_enabled",
  "reverse_exit_enabled",
  "live_entry_require_fill",
  "keep_unfilled_entry_open",
];

const FORM_STORAGE_KEY = "pm_assistant_form_v1";

let bootstrap = null;
let pollTimer = null;
let lastPendingSignature = "";
let audioCtx = null;
let lastEventId = 0;
let pendingToastEl = null;
let controlsBound = false;
let authVisible = false;
let bootstrappedOnce = false;
let coinTfBound = false;
let megaPresetNoticeShown = false;

function sentimentClass(label) {
  const raw = String(label || "").toLowerCase();
  if (raw.includes("bull")) return "bullish";
  if (raw.includes("bear")) return "bearish";
  return "neutral";
}

function signClass(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "neutral";
  const x = Number(value);
  if (x > 0) return "positive";
  if (x < 0) return "negative";
  return "neutral";
}

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

function allPersistedFieldIds() {
  return [
    "mode",
    "preset",
    "coin",
    "timeframe",
    "confirm_live_token",
    ...fields,
    ...checks,
  ];
}

function saveFormState() {
  const payload = {};
  allPersistedFieldIds().forEach((id) => {
    const node = el(id);
    if (!node) return;
    if (node.type === "checkbox") {
      payload[id] = !!node.checked;
      return;
    }
    payload[id] = node.value;
  });
  try {
    localStorage.setItem(FORM_STORAGE_KEY, JSON.stringify(payload));
  } catch {}
}

function restoreFormState() {
  let raw = "";
  try {
    raw = localStorage.getItem(FORM_STORAGE_KEY) || "";
  } catch {
    raw = "";
  }
  if (!raw) return false;

  let data = null;
  try {
    data = JSON.parse(raw);
  } catch {
    return false;
  }
  if (!data || typeof data !== "object") return false;

  if (data.coin && ui.coin.querySelector(`option[value="${String(data.coin)}"]`)) {
    ui.coin.value = String(data.coin);
    ui.coin.dispatchEvent(new Event("change"));
  }
  if (data.timeframe && ui.timeframe.querySelector(`option[value="${String(data.timeframe)}"]`)) {
    ui.timeframe.value = String(data.timeframe);
  }

  allPersistedFieldIds().forEach((id) => {
    if (id === "coin" || id === "timeframe") return;
    if (!(id in data)) return;
    const node = el(id);
    if (!node) return;
    if (node.type === "checkbox") {
      node.checked = !!data[id];
      return;
    }
    node.value = String(data[id] ?? "");
  });

  syncModeByPreset(false);
  return true;
}

function setError(msg = "") {
  ui.errorBox.textContent = msg;
}

function showToast(level, title, message, timeoutMs = 5200, sticky = false) {
  if (!ui.toastContainer) return;
  const toast = document.createElement("div");
  toast.className = `toast ${level || "info"}`;
  const h = document.createElement("h4");
  const p = document.createElement("p");
  h.textContent = title || "Notice";
  p.textContent = message || "";
  toast.appendChild(h);
  toast.appendChild(p);
  ui.toastContainer.appendChild(toast);
  if (sticky) return toast;
  window.setTimeout(() => {
    toast.style.opacity = "0";
    toast.style.transform = "translateY(-4px)";
    toast.style.transition = "opacity 0.2s ease, transform 0.2s ease";
    window.setTimeout(() => toast.remove(), 220);
  }, timeoutMs);
  return toast;
}

function removeToast(toast) {
  if (!toast) return;
  toast.style.opacity = "0";
  toast.style.transform = "translateY(-4px)";
  toast.style.transition = "opacity 0.2s ease, transform 0.2s ease";
  window.setTimeout(() => toast.remove(), 220);
}

// -- Modal System --
function showModal(title, message, confirmLabel = "Confirm", confirmClass = "danger") {
  return new Promise((resolve) => {
    const overlay = el("modalOverlay");
    const titleEl = el("modalTitle");
    const msgEl = el("modalMessage");
    const confirmBtn = el("modalConfirmBtn");
    const cancelBtn = el("modalCancelBtn");

    titleEl.textContent = title;
    msgEl.textContent = message;
    confirmBtn.textContent = confirmLabel;
    confirmBtn.className = "btn " + confirmClass;
    overlay.classList.remove("hidden");

    function cleanup() {
      overlay.classList.add("hidden");
      confirmBtn.removeEventListener("click", onConfirm);
      cancelBtn.removeEventListener("click", onCancel);
      overlay.removeEventListener("click", onOverlay);
    }
    function onConfirm() { cleanup(); resolve(true); }
    function onCancel() { cleanup(); resolve(false); }
    function onOverlay(e) { if (e.target === overlay) { cleanup(); resolve(false); } }

    confirmBtn.addEventListener("click", onConfirm);
    cancelBtn.addEventListener("click", onCancel);
    overlay.addEventListener("click", onOverlay);
  });
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

  if (!coinTfBound) {
    ui.coin.addEventListener("change", refreshTimeframes);
    coinTfBound = true;
  }
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

function syncModeByPreset(showNotice = false) {
  const isPaperOnlyAggro = ui.preset.value === "mega_aggressive" || ui.preset.value === "super_aggressive";
  if (isPaperOnlyAggro) {
    if (ui.mode.value !== "paper") {
      ui.mode.value = "paper";
      if (showNotice || !megaPresetNoticeShown) {
        showToast("warning", "Preset lock", "SUPER AGGRESSIVE is paper-only. Mode switched to PAPER.");
        megaPresetNoticeShown = true;
      }
    }
    ui.mode.setAttribute("disabled", "disabled");
    return;
  }
  ui.mode.removeAttribute("disabled");
  megaPresetNoticeShown = false;
}

function gatherStartPayload() {
  syncModeByPreset(false);
  saveFormState();

  const payload = {
    mode: ui.mode.value,
    preset: ui.preset.value,
    coin: ui.coin.value,
    timeframe: ui.timeframe.value,
    confirm_live_token: el("confirm_live_token").value.trim(),
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
    const err = new Error(data?.detail || data?.error || `HTTP ${res.status}`);
    err.status = res.status;
    err.unauthorized = res.status === 401;
    throw err;
  }
  return data;
}

function showAuthOverlay(message = "") {
  authVisible = true;
  if (ui.authOverlay) ui.authOverlay.classList.remove("hidden");
  if (ui.authHint) ui.authHint.textContent = message || "Access key required";
}

function hideAuthOverlay() {
  authVisible = false;
  if (ui.authOverlay) ui.authOverlay.classList.add("hidden");
  if (ui.authHint) ui.authHint.textContent = "";
}

async function submitAccessKey() {
  const key = String(ui.authKeyInput?.value || "").trim();
  if (!key) {
    showAuthOverlay("Enter access key");
    return false;
  }
  try {
    await api("/api/auth", { method: "POST", body: { key } });
    hideAuthOverlay();
    if (ui.authKeyInput) ui.authKeyInput.value = "";
    showToast("success", "Authorized", "Access granted");
    return true;
  } catch (e) {
    const msg = e.message || String(e);
    showAuthOverlay(msg);
    showToast("error", "Auth Error", msg, 5000);
    return false;
  }
}

function bindControlsOnce() {
  if (controlsBound) return;
  controlsBound = true;

  ui.preset.addEventListener("change", () => {
    applyPreset(ui.preset.value);
    syncModeByPreset(true);
    saveFormState();
  });
  ui.mode.addEventListener("change", () => {
    syncModeByPreset(true);
    saveFormState();
  });
  ui.startBtn.addEventListener("click", async () => {
    const mode = ui.mode.value;
    if (mode === "live") {
      const ok = await showModal(
        "Start LIVE Trading",
        "You are about to start LIVE trading with real money. Confirm?",
        "Start Live", "danger"
      );
      if (!ok) return;
    }
    await onStart();
  });
  ui.stopBtn.addEventListener("click", async () => {
    const ok = await showModal(
      "Stop Session",
      "This will stop the trading session. If a live position is open, an emergency flatten will be attempted.",
      "Stop", "danger"
    );
    if (ok) await onStop();
  });
  ui.authSubmitBtn?.addEventListener("click", async () => {
    const ok = await submitAccessKey();
    if (ok) await bootstrapApp();
  });
  ui.authKeyInput?.addEventListener("keydown", async (ev) => {
    if (ev.key !== "Enter") return;
    const ok = await submitAccessKey();
    if (ok) await bootstrapApp();
  });


  allPersistedFieldIds().forEach((id) => {
    const node = el(id);
    if (!node) return;
    const evName = node.type === "checkbox" || node.tagName === "SELECT" ? "change" : "input";
    node.addEventListener(evName, saveFormState);
  });
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
  items.forEach((item) => {
    if (typeof item === "string") {
      node.appendChild(li(item));
      return;
    }
    const nodeItem = li(String(item?.text || "-"));
    if (item?.className) nodeItem.classList.add(item.className);
    node.appendChild(nodeItem);
  });
}

function renderTrades(trades) {
  ui.tradesBody.innerHTML = "";
  (trades || []).slice(-40).reverse().forEach((t) => {
    const tr = document.createElement("tr");
    const when = t.ts_iso ? new Date(t.ts_iso).toLocaleTimeString() : "-";
    const pnl = t.pnl_pct !== null && t.pnl_pct !== undefined
      ? `${fmtPct(t.pnl_pct)} / $${fmtNum(t.pnl_usd, 2)}`
      : "-";
    const columns = [
      { text: when },
      { text: t.action || "-" },
      { text: t.side || "-", className: String(t.side || "").toLowerCase() === "up" ? "bullish" : (String(t.side || "").toLowerCase() === "down" ? "bearish" : "") },
      { text: t.price !== undefined ? Number(t.price).toFixed(3) : "-" },
      { text: t.size_usd !== undefined ? `$${fmtNum(t.size_usd, 2)}` : "-" },
      { text: t.status || "-", className: String(t.status || "").includes("error") || String(t.status || "").includes("failed") ? "bearish" : (String(t.status || "").includes("filled") || String(t.status || "").includes("posted") ? "bullish" : "") },
      { text: pnl, className: signClass(t.pnl_pct) },
    ];
    columns.forEach((col) => {
      const td = document.createElement("td");
      td.textContent = col.text;
      if (col.className) td.classList.add(col.className);
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
  ui.summaryText.textContent = market.summary || "Market summary is unavailable.";
  ui.trendText.className = sentimentClass(market.trend?.label);
  ui.biasText.className = sentimentClass(market.bias?.label);

  renderList(ui.orderbookList, [
    { text: `OBI: ${fmtPct((orderbook.obi || 0) * 100)}`, className: signClass((orderbook.obi || 0) * 100) },
    { text: `Depth 0.1%: ${fmtPrice(orderbook.depth?.[0.1])}` },
    { text: `Depth 0.5%: ${fmtPrice(orderbook.depth?.[0.5])}` },
    { text: `Depth 1.0%: ${fmtPrice(orderbook.depth?.[1.0])}` },
    { text: `BUY walls: ${(orderbook.buy_walls || []).map((x) => Number(x[0]).toFixed(2)).join(", ") || "none"}`, className: (orderbook.buy_walls || []).length ? "bullish" : "" },
    { text: `SELL walls: ${(orderbook.sell_walls || []).map((x) => Number(x[0]).toFixed(2)).join(", ") || "none"}`, className: (orderbook.sell_walls || []).length ? "bearish" : "" },
  ]);

  renderList(ui.flowList, [
    { text: `CVD 1m: $${fmtNum(flow.cvd_1m, 2)}`, className: signClass(flow.cvd_1m) },
    { text: `CVD 3m: $${fmtNum(flow.cvd_3m, 2)}`, className: signClass(flow.cvd_3m) },
    { text: `CVD 5m: $${fmtNum(flow.cvd_5m, 2)}`, className: signClass(flow.cvd_5m) },
    { text: `Delta 1m: $${fmtNum(flow.delta_1m, 2)}`, className: signClass(flow.delta_1m) },
    { text: `POC: ${fmtPrice(flow.poc)}` },
  ]);

  const ha = (tech.ha_last8 || []).map((x) => (x ? "▲" : "▼")).join(" ");
  renderList(ui.techList, [
    { text: `RSI: ${fmtNum(tech.rsi, 1)}`, className: (tech.rsi ?? 50) >= 70 ? "bearish" : ((tech.rsi ?? 50) <= 30 ? "bullish" : "neutral") },
    { text: `MACD: ${fmtNum(tech.macd, 6)} | signal ${fmtNum(tech.signal, 6)}`, className: signClass(tech.macd_hist ?? tech.macd) },
    { text: `VWAP: ${fmtPrice(tech.vwap)}` },
    { text: `EMA5 / EMA20: ${fmtPrice(tech.ema5)} / ${fmtPrice(tech.ema20)}`, className: (tech.ema5 ?? 0) > (tech.ema20 ?? 0) ? "bullish" : "bearish" },
    { text: `Heikin Ashi: ${ha || "-"}` },
  ]);

  renderList(
    ui.signalsList,
    (market.signals || []).map((s) => ({
      text: s,
      className: sentimentClass(s),
    }))
  );

  const trader = state.trader || null;
  renderTrades(trader?.trades || []);

  // Window countdown
  const win = state.window;
  if (win && ui.windowBar && ui.windowTimer) {
    const mins = Math.floor(win.remaining_sec / 60);
    const secs = win.remaining_sec % 60;
    const timeStr = `${String(mins).padStart(2, "0")}:${String(secs).padStart(2, "0")}`;
    ui.windowTimer.textContent = timeStr;
    ui.windowBar.classList.remove("hidden");
    if (win.exit_forced) {
      ui.windowTimer.className = "window-timer bearish";
    } else if (win.entry_blocked) {
      ui.windowTimer.className = "window-timer neutral";
    } else {
      ui.windowTimer.className = "window-timer";
    }
  } else if (ui.windowBar) {
    ui.windowBar.classList.add("hidden");
  }

  ui.logsBox.textContent = (state.logs || []).join("\n");
  ui.logsBox.scrollTop = ui.logsBox.scrollHeight;

  const events = state.events || [];
  if (events.length && events[events.length - 1].id < lastEventId) {
    lastEventId = 0;
  }
  events.forEach((evt) => {
    const id = Number(evt.id || 0);
    if (!id || id <= lastEventId) return;
    showToast(evt.level || "info", evt.title || "Event", evt.message || "");
    lastEventId = Math.max(lastEventId, id);
  });
}

async function pollState() {
  try {
    const data = await api("/api/state");
    renderState(data.state);
  } catch (e) {
    if (e.unauthorized) {
      showAuthOverlay("Enter access key to continue");
      return;
    }
    const msg = e.message || String(e);
    setError(msg);
    showToast("error", "State Error", msg);
  }
}

async function runCommand(command) {
  try {
    await api("/api/command", { method: "POST", body: { command } });
    await pollState();
  } catch (e) {
    if (e.unauthorized) {
      showAuthOverlay("Session is locked. Enter access key.");
      return;
    }
    const msg = e.message || String(e);
    setError(msg);
    showToast("error", "Command Error", msg);
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
    if (e.unauthorized) {
      showAuthOverlay("Session is locked. Enter access key.");
      return;
    }
    const msg = e.message || String(e);
    setError(msg);
    showToast("error", "Start Error", msg, 7000);
    try {
      await pollState();
    } catch {}
  }
}

async function onStop() {
  setError("");
  try {
    await api("/api/stop", { method: "POST" });
    await pollState();
  } catch (e) {
    if (e.unauthorized) {
      showAuthOverlay("Session is locked. Enter access key.");
      return;
    }
    const msg = e.message || String(e);
    setError(msg);
    showToast("error", "Stop Error", msg);
  }
}

async function bootstrapApp() {
  try {
    const data = await api("/api/bootstrap");
    bootstrap = data;
    setupCoinTimeframes();
    const restored = restoreFormState();
    if (!restored) {
      el("confirm_live_token").value = data.live_confirm_token || "";
      applyPreset("medium");
      el("client_watchdog_enabled").checked = !!data?.defaults?.client_watchdog_enabled;
      saveFormState();
    } else if (!el("confirm_live_token").value) {
      el("confirm_live_token").value = data.live_confirm_token || "";
      saveFormState();
    }
    // Credential status indicator
    const credAvail = !!data.credentials_available;
    const dot = el("credDot");
    const credLabel = el("credLabel");
    if (dot && credLabel) {
      dot.className = credAvail ? "cred-dot ok" : "cred-dot missing";
      credLabel.textContent = credAvail
        ? "Server credentials configured"
        : "PM_PRIVATE_KEY / PM_FUNDER not set on server";
    }

    syncModeByPreset(false);
    renderState(data.state);
    hideAuthOverlay();
    if (!bootstrappedOnce) {
      showToast("success", "Ready", "Web terminal loaded");
    }
    bootstrappedOnce = true;

    if (pollTimer) clearInterval(pollTimer);
    pollTimer = setInterval(pollState, 1200);
    return true;
  } catch (e) {
    if (e.unauthorized) {
      showAuthOverlay("Enter access key to unlock dashboard");
      return false;
    }
    const msg = e.message || String(e);
    setError(msg);
    showToast("error", "Bootstrap Error", msg, 7000);
    return false;
  }
}

async function init() {
  bindControlsOnce();
  await bootstrapApp();
}

init();
