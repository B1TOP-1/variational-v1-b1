const DEBUGGER_VERSION = "1.3";
const MAX_QUEUE_SIZE = 1000;
const AUTO_RELOAD_COOLDOWN_MS = 5000;
const QUOTE_URL_PATH = "/api/quotes/indicative";
const ORDER_URL_PATH = "/orders/new/market";
const ACTIVE_QUOTE_MARKER = "__var_active_quote";
const RUNTIME_STATE_KEY = "forwarderRuntimeState";
const CONFIG_VERSION = 2;

const DEFAULT_CONFIG = {
  configVersion: CONFIG_VERSION,
  wsEndpoint: "ws://127.0.0.1:8766",
  restEndpoint: "ws://127.0.0.1:8767",
  brokerEndpoint: "ws://127.0.0.1:8768",
  domainFilter: "variational",
  activeQuoteEnabled: true,
  activeQuoteIntervalMs: 200,
  activeQuoteNotionalUsd: 500,
  activeQuoteMaxInFlight: 4,
  activeQuoteTimeoutMs: 1500,
  activeQuoteMaxBackoffMs: 30000,
  restAllowlist: [
    "https://omni.variational.io/api/quotes/indicative",
    "https://omni.variational.io/orders/new/market"
  ],
  wsAllowlist: [
    "wss://omni-ws-server.prod.ap-northeast-1.variational.io/events",
    "wss://omni-ws-server.prod.ap-northeast-1.variational.io/portfolio"
  ]
};

const state = {
  active: false,
  attachedTabId: null,
  config: { ...DEFAULT_CONFIG },
  configLoaded: false,
  pendingResponses: new Map(),
  websocketMeta: new Map(),
  lastQuote: null,
  lastOrderResponse: null,
  pendingOrderWaiters: [],
  activeQuoteTemplate: null,
  activeQuoteStatus: "waiting_template",
  activeQuoteAsset: null,
  activeQuoteSessionId: null,
  latestAcceptedQuoteSeq: new Map(),
  activeQuoteRequests: new Map(),
  activeQuoteMetrics: null,
  activeQuoteSupervisorTimer: null,
  activeQuoteSupervisorBusy: false,
  lastError: null,
  lastAutoReloadAt: 0
};

class ForwardSocket {
  constructor(label, configKey) {
    this.label = label;
    this.configKey = configKey;
    this.ws = null;
    this.status = "disconnected";
    this.queue = [];
    this.retryTimer = null;
    this.keepaliveTimer = null;
  }

  get endpoint() {
    return state.config[this.configKey];
  }

  connect() {
    if (!state.active) {
      return;
    }

    if (this.ws && (this.ws.readyState === WebSocket.OPEN || this.ws.readyState === WebSocket.CONNECTING)) {
      return;
    }

    const endpoint = this.endpoint;
    if (!endpoint) {
      this.status = "disconnected";
      notifyStatus();
      return;
    }

    this.status = "connecting";
    notifyStatus();

    try {
      const socket = new WebSocket(endpoint);
      this.ws = socket;

      socket.onopen = () => {
        if (this.ws !== socket) {
          return;
        }
        this.status = "connected";
        this.startKeepalive();
        this.flush();
        if (this.configKey === "wsEndpoint") {
          autoReloadAttachedTab("forward receiver connected");
        }
        notifyStatus();
      };

      socket.onclose = () => {
        if (this.ws !== socket) {
          return;
        }
        this.ws = null;
        this.stopKeepalive();
        this.status = "disconnected";
        notifyStatus();
        this.scheduleReconnect();
      };

      socket.onerror = () => {
        if (this.ws !== socket) {
          return;
        }
        this.status = "error";
        notifyStatus();
      };

    } catch (error) {
      this.status = "error";
      state.lastError = `${this.label} socket connect failed: ${error.message}`;
      notifyStatus();
      this.scheduleReconnect();
    }
  }

  send(payload) {
    const data = JSON.stringify(payload);
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(data);
      return;
    }

    this.queue.push(data);
    if (this.queue.length > MAX_QUEUE_SIZE) {
      this.queue.shift();
    }
    this.connect();
  }

  flush() {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      return;
    }
    while (this.queue.length > 0) {
      this.ws.send(this.queue.shift());
    }
  }

  scheduleReconnect() {
    if (!state.active || this.retryTimer) {
      return;
    }
    this.retryTimer = setTimeout(() => {
      this.retryTimer = null;
      this.connect();
    }, 1000);
  }

  startKeepalive() {
    this.stopKeepalive();
    this.keepaliveTimer = setInterval(() => {
      if (this.ws?.readyState === WebSocket.OPEN) {
        this.ws.send(JSON.stringify({ kind: "extension_keepalive", timestamp: nowIso() }));
      }
    }, 20000);
  }

  stopKeepalive() {
    if (this.keepaliveTimer) {
      clearInterval(this.keepaliveTimer);
      this.keepaliveTimer = null;
    }
  }

  restart() {
    this.close();
    this.connect();
  }

  close() {
    if (this.retryTimer) {
      clearTimeout(this.retryTimer);
      this.retryTimer = null;
    }
    this.stopKeepalive();
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
    this.status = "disconnected";
    notifyStatus();
  }
}

const wsForwarder = new ForwardSocket("websocket", "wsEndpoint");
const restForwarder = new ForwardSocket("rest", "restEndpoint");

class BrokerSocket {
  constructor() {
    this.ws = null;
    this.status = "disconnected";
    this.retryTimer = null;
  }

  get endpoint() {
    return state.config.brokerEndpoint;
  }

  connect() {
    if (!state.active || this.ws?.readyState === WebSocket.OPEN || this.ws?.readyState === WebSocket.CONNECTING) {
      return;
    }
    if (!this.endpoint) {
      this.status = "disconnected";
      notifyStatus();
      return;
    }
    this.status = "connecting";
    notifyStatus();
    try {
      const socket = new WebSocket(this.endpoint);
      this.ws = socket;
      socket.onopen = () => {
        if (this.ws !== socket) {
          return;
        }
        this.status = "connected";
        this.send({ event: "hello", version: DEBUGGER_VERSION, at: nowIso() });
        notifyStatus();
      };
      socket.onmessage = (event) => {
        this.handleMessage(event.data).catch((error) => {
          state.lastError = `Broker command failed: ${error.message}`;
          notifyStatus();
        });
      };
      socket.onclose = () => {
        if (this.ws !== socket) {
          return;
        }
        this.ws = null;
        this.status = "disconnected";
        notifyStatus();
        this.scheduleReconnect();
      };
      socket.onerror = () => {
        if (this.ws !== socket) {
          return;
        }
        this.status = "error";
        notifyStatus();
      };
    } catch (error) {
      this.status = "error";
      state.lastError = `broker socket connect failed: ${error.message}`;
      notifyStatus();
      this.scheduleReconnect();
    }
  }

  async handleMessage(raw) {
    const message = JSON.parse(raw);
    const id = String(message.id || "");
    try {
      const result = await handleBrokerCommand(message.action, message.payload || {});
      this.send({ id, ...result });
    } catch (error) {
      this.send({ id, ok: false, error: error.message || String(error) });
    }
  }

  send(payload) {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(payload));
    }
  }

  scheduleReconnect() {
    if (!state.active || this.retryTimer) {
      return;
    }
    this.retryTimer = setTimeout(() => {
      this.retryTimer = null;
      this.connect();
    }, 1000);
  }

  restart() {
    this.close();
    this.connect();
  }

  close() {
    if (this.retryTimer) {
      clearTimeout(this.retryTimer);
      this.retryTimer = null;
    }
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
    this.status = "disconnected";
    notifyStatus();
  }
}

const brokerSocket = new BrokerSocket();

function autoReloadAttachedTab(reason) {
  if (!state.active || state.attachedTabId == null) {
    return;
  }
  const now = Date.now();
  if (now - state.lastAutoReloadAt < AUTO_RELOAD_COOLDOWN_MS) {
    return;
  }
  state.lastAutoReloadAt = now;

  chrome.tabs.reload(state.attachedTabId, {}, () => {
    const err = chrome.runtime.lastError;
    if (err) {
      state.lastError = `Auto reload failed (${reason}): ${err.message}`;
    } else {
      state.lastError = null;
    }
    notifyStatus();
  });
}

async function ensureConfigLoaded() {
  if (state.configLoaded) {
    return;
  }
  const stored = await chrome.storage.local.get("forwarderConfig");
  state.config = sanitizeConfig(stored.forwarderConfig);
  await chrome.storage.local.set({ forwarderConfig: state.config });
  state.configLoaded = true;
}

async function persistRuntimeState() {
  try {
    await chrome.storage.session.set({
      [RUNTIME_STATE_KEY]: {
        active: state.active,
        attachedTabId: state.attachedTabId,
        activeQuoteTemplate: state.activeQuoteTemplate
      }
    });
  } catch {
    // storage.session may be unavailable on older Chromium; runtime still works until worker restart.
  }
}

async function debuggerTargets() {
  return await new Promise((resolve) => {
    chrome.debugger.getTargets((targets) => resolve(targets || []));
  });
}

async function restoreRuntimeState() {
  await ensureConfigLoaded();
  let saved = null;
  try {
    const stored = await chrome.storage.session.get(RUNTIME_STATE_KEY);
    saved = stored[RUNTIME_STATE_KEY] || null;
  } catch {
    return;
  }
  if (!saved?.active || saved.attachedTabId == null) {
    return;
  }

  const tabId = saved.attachedTabId;
  try {
    await chrome.tabs.get(tabId);
    const targets = await debuggerTargets();
    const alreadyAttached = targets.some((target) => target.tabId === tabId && target.attached);
    if (!alreadyAttached) {
      await debuggerAttach(tabId);
    }
    await sendDebuggerCommand(tabId, "Network.enable");
    state.active = true;
    state.attachedTabId = tabId;
    state.activeQuoteTemplate = saved.activeQuoteTemplate || null;
    state.activeQuoteAsset = state.activeQuoteTemplate?.asset || null;
    state.activeQuoteStatus = state.config.activeQuoteEnabled
      ? (state.activeQuoteTemplate ? "restoring" : "waiting_template")
      : "disabled";
    wsForwarder.connect();
    restForwarder.connect();
    brokerSocket.connect();
    void installDomQuoteObserver(tabId);
    if (state.activeQuoteTemplate) {
      void installActiveQuotePoller(tabId, state.activeQuoteTemplate);
    }
    notifyStatus();
  } catch (error) {
    cleanupForwardingState();
    state.lastError = `Runtime restore failed: ${error.message}`;
    await persistRuntimeState();
    notifyStatus();
  }
}

function sanitizeConfig(incoming = {}) {
  const incomingVersion = asBoundedInteger(incoming.configVersion, 1, 1, CONFIG_VERSION);
  const storedInterval = asBoundedInteger(
    incoming.activeQuoteIntervalMs,
    DEFAULT_CONFIG.activeQuoteIntervalMs,
    50,
    60000
  );
  return {
    configVersion: CONFIG_VERSION,
    wsEndpoint: asStringOrDefault(incoming.wsEndpoint, DEFAULT_CONFIG.wsEndpoint),
    restEndpoint: asStringOrDefault(incoming.restEndpoint, DEFAULT_CONFIG.restEndpoint),
    brokerEndpoint: asStringOrDefault(incoming.brokerEndpoint, DEFAULT_CONFIG.brokerEndpoint),
    domainFilter: asStringOrDefault(incoming.domainFilter, DEFAULT_CONFIG.domainFilter),
    activeQuoteEnabled: incoming.activeQuoteEnabled !== false,
    activeQuoteIntervalMs: incomingVersion < 2 && storedInterval === 100 ? 200 : storedInterval,
    activeQuoteNotionalUsd: asBoundedInteger(incoming.activeQuoteNotionalUsd, DEFAULT_CONFIG.activeQuoteNotionalUsd, 10, 1000000),
    activeQuoteMaxInFlight: asBoundedInteger(incoming.activeQuoteMaxInFlight, DEFAULT_CONFIG.activeQuoteMaxInFlight, 1, 20),
    activeQuoteTimeoutMs: asBoundedInteger(incoming.activeQuoteTimeoutMs, DEFAULT_CONFIG.activeQuoteTimeoutMs, 250, 30000),
    activeQuoteMaxBackoffMs: asBoundedInteger(
      incoming.activeQuoteMaxBackoffMs,
      DEFAULT_CONFIG.activeQuoteMaxBackoffMs,
      1000,
      300000
    ),
    restAllowlist: sanitizeRestAllowlist(incoming.restAllowlist),
    wsAllowlist: sanitizeAllowlist(incoming.wsAllowlist, DEFAULT_CONFIG.wsAllowlist)
  };
}

function asBoundedInteger(value, fallback, minimum, maximum) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return fallback;
  }
  return Math.min(maximum, Math.max(minimum, Math.round(numeric)));
}

function asStringOrDefault(value, fallback) {
  if (typeof value !== "string") {
    return fallback;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : fallback;
}

function nowIso() {
  return new Date().toISOString();
}

function sanitizeAllowlist(value, fallback) {
  if (!Array.isArray(value)) {
    return [...fallback];
  }
  const cleaned = value
    .filter((item) => typeof item === "string")
    .map((item) => item.trim())
    .filter((item) => item.length > 0);
  if (!cleaned.length) {
    return [...fallback];
  }
  return cleaned;
}

function sanitizeRestAllowlist(value) {
  const cleaned = sanitizeAllowlist(value, DEFAULT_CONFIG.restAllowlist);
  const allowed = new Set(DEFAULT_CONFIG.restAllowlist);
  const strict = cleaned.filter((item) => allowed.has(item));
  return [...new Set([...strict, ...DEFAULT_CONFIG.restAllowlist])];
}

function tryParseJson(text) {
  if (typeof text !== "string") {
    return null;
  }
  try {
    return JSON.parse(text);
  } catch {
    return null;
  }
}

function decodeDebuggerBody(body, base64Encoded) {
  if (!base64Encoded) {
    return body ?? "";
  }
  try {
    return atob(body ?? "");
  } catch {
    return "";
  }
}

function isQuoteUrl(url) {
  return typeof url === "string" && url.includes(QUOTE_URL_PATH);
}

function parseActiveQuoteMeta(rawUrl) {
  try {
    const url = new URL(rawUrl);
    if (url.searchParams.get(ACTIVE_QUOTE_MARKER) !== "1") {
      return null;
    }
    const sequence = Number(url.searchParams.get("seq"));
    return {
      sessionId: url.searchParams.get("session") || "",
      sequence: Number.isSafeInteger(sequence) && sequence > 0 ? sequence : null,
      asset: (url.searchParams.get("asset") || "UNKNOWN").toUpperCase()
    };
  } catch {
    return null;
  }
}

function isOrderUrl(url) {
  return typeof url === "string" && url.includes(ORDER_URL_PATH);
}

function matchesDomainFilter(url) {
  const filter = state.config.domainFilter.trim().toLowerCase();
  if (!filter) {
    return true;
  }
  return (url || "").toLowerCase().includes(filter);
}

function normalizeUrlParts(rawUrl) {
  try {
    const parsed = new URL(rawUrl);
    return {
      originPath: `${parsed.origin}${parsed.pathname}`,
      full: parsed.toString()
    };
  } catch {
    return {
      originPath: rawUrl,
      full: rawUrl
    };
  }
}

function getMatchedRestPattern(url) {
  const patterns = state.config.restAllowlist || [];
  return getMatchedPattern(url, patterns);
}

function getMatchedWsPattern(url) {
  const patterns = state.config.wsAllowlist || [];
  return getMatchedPattern(url, patterns);
}

function getMatchedPattern(url, patterns) {
  if (!patterns.length) {
    return null;
  }

  const target = normalizeUrlParts(url);
  for (const pattern of patterns) {
    const normalizedPattern = normalizeUrlParts(pattern);
    if (target.originPath === normalizedPattern.originPath || target.full.startsWith(pattern)) {
      return pattern;
    }
  }
  return null;
}

function quoteResponseAccepted(meta, payload) {
  if (meta.status !== 200 || !payload || typeof payload !== "object" || payload.bid == null || payload.ask == null) {
    return false;
  }
  const activeMeta = meta.activeQuote;
  const asset = String(payload?.instrument?.underlying || activeMeta?.asset || "UNKNOWN").toUpperCase();
  if (!activeMeta) {
    return !(state.config.activeQuoteEnabled && state.activeQuoteStatus === "running" && state.activeQuoteAsset === asset);
  }
  if (!activeMeta.sessionId || activeMeta.sequence == null) {
    return false;
  }
  if (activeMeta.sessionId !== state.activeQuoteSessionId || asset !== state.activeQuoteAsset) {
    return false;
  }
  const key = `${activeMeta.sessionId}:${asset}`;
  const latest = state.latestAcceptedQuoteSeq.get(key) || 0;
  if (activeMeta.sequence <= latest) {
    return false;
  }
  state.latestAcceptedQuoteSeq.set(key, activeMeta.sequence);
  return true;
}

function updateLastQuote(meta, bodyText, payload, accepted) {
  if (!payload || typeof payload !== "object") {
    return;
  }
  if (!accepted) {
    return;
  }
  state.lastQuote = {
    timestamp: nowIso(),
    capturedAt: meta.capturedAt,
    requestId: meta.requestId,
    url: meta.url,
    status: meta.status,
    statusCode: meta.status,
    quoteId: payload.quote_id || "",
    bid: payload.bid != null ? Number(payload.bid) : null,
    ask: payload.ask != null ? Number(payload.ask) : null,
    markPrice: payload.mark_price != null ? Number(payload.mark_price) : null,
    bodyTimestamp: payload.timestamp || null,
    instrument: payload.instrument || null,
    activeQuote: meta.activeQuote || null,
    latencyMs: meta.sentAtMs ? Math.max(0, Date.now() - meta.sentAtMs) : null
  };
}

function makeOrderResponseSummary(meta, bodyText) {
  const payload = tryParseJson(bodyText);
  return {
    timestamp: nowIso(),
    capturedAt: meta.capturedAt,
    requestId: meta.requestId,
    url: meta.url,
    status: meta.status,
    statusCode: meta.status,
    statusText: meta.statusText,
    bodyText,
    json: payload
  };
}

function resolvePendingOrderWaiters(orderResponse) {
  const eventMs = Date.parse(orderResponse.capturedAt || orderResponse.timestamp || nowIso());
  state.pendingOrderWaiters = state.pendingOrderWaiters.filter((waiter) => {
    if (waiter.tabId != null && waiter.tabId !== orderResponse.tabId) {
      return true;
    }
    if (eventMs + 100 < waiter.startedAtMs) {
      return true;
    }
    clearTimeout(waiter.timer);
    waiter.resolve(orderResponse);
    return false;
  });
}

function waitForNextOrderResponse(tabId, timeoutMs) {
  return new Promise((resolve) => {
    const waiter = {
      tabId,
      startedAtMs: Date.now(),
      resolve,
      timer: null
    };
    waiter.timer = setTimeout(() => {
      state.pendingOrderWaiters = state.pendingOrderWaiters.filter((item) => item !== waiter);
      resolve(null);
    }, timeoutMs);
    state.pendingOrderWaiters.push(waiter);
  });
}

async function debuggerAttach(tabId) {
  await new Promise((resolve, reject) => {
    chrome.debugger.attach({ tabId }, DEBUGGER_VERSION, () => {
      const err = chrome.runtime.lastError;
      if (err) {
        reject(new Error(err.message));
        return;
      }
      resolve();
    });
  });
}

async function debuggerDetach(tabId) {
  await new Promise((resolve, reject) => {
    chrome.debugger.detach({ tabId }, () => {
      const err = chrome.runtime.lastError;
      if (err) {
        reject(new Error(err.message));
        return;
      }
      resolve();
    });
  });
}

async function sendDebuggerCommand(tabId, method, params = {}) {
  return new Promise((resolve, reject) => {
    chrome.debugger.sendCommand({ tabId }, method, params, (result) => {
      const err = chrome.runtime.lastError;
      if (err) {
        reject(new Error(err.message));
        return;
      }
      resolve(result || {});
    });
  });
}

async function dispatchTrustedClick(tabId, rect) {
  if (!rect || typeof rect.x !== "number" || typeof rect.y !== "number") {
    throw new Error("Missing click target rect");
  }
  const x = Math.round(rect.x);
  const y = Math.round(rect.y);
  await sendDebuggerCommand(tabId, "Input.dispatchMouseEvent", {
    type: "mouseMoved",
    x,
    y,
    button: "none",
    buttons: 0
  });
  await sendDebuggerCommand(tabId, "Input.dispatchMouseEvent", {
    type: "mousePressed",
    x,
    y,
    button: "left",
    buttons: 1,
    clickCount: 1
  });
  await sendDebuggerCommand(tabId, "Input.dispatchMouseEvent", {
    type: "mouseReleased",
    x,
    y,
    button: "left",
    buttons: 0,
    clickCount: 1
  });
}

async function dispatchKeyPress(tabId, windowsVirtualKeyCode, code, key, modifiers = 0) {
  await sendDebuggerCommand(tabId, "Input.dispatchKeyEvent", {
    type: "rawKeyDown",
    windowsVirtualKeyCode,
    nativeVirtualKeyCode: windowsVirtualKeyCode,
    code,
    key,
    modifiers
  });
  await sendDebuggerCommand(tabId, "Input.dispatchKeyEvent", {
    type: "keyUp",
    windowsVirtualKeyCode,
    nativeVirtualKeyCode: windowsVirtualKeyCode,
    code,
    key,
    modifiers
  });
}

async function clearAndTypeText(tabId, text) {
  await sendDebuggerCommand(tabId, "Input.dispatchKeyEvent", {
    type: "rawKeyDown",
    windowsVirtualKeyCode: 17,
    nativeVirtualKeyCode: 17,
    code: "ControlLeft",
    key: "Control",
    modifiers: 2
  });
  await dispatchKeyPress(tabId, 65, "KeyA", "a", 2);
  await sendDebuggerCommand(tabId, "Input.dispatchKeyEvent", {
    type: "keyUp",
    windowsVirtualKeyCode: 17,
    nativeVirtualKeyCode: 17,
    code: "ControlLeft",
    key: "Control",
    modifiers: 0
  });
  await dispatchKeyPress(tabId, 8, "Backspace", "Backspace", 0);
  await sendDebuggerCommand(tabId, "Input.insertText", { text: String(text || "") });
}

async function runInTab(tabId, func, args = []) {
  const results = await chrome.scripting.executeScript({
    target: { tabId },
    world: "MAIN",
    func,
    args
  });
  if (!results || !results.length) {
    throw new Error("No executeScript result");
  }
  return results[0].result;
}

// MAIN world：复用网页真实同源会话主动获取 indicative quote，不读取/导出 Cookie。
function installActiveQuotePollerInPage(config) {
  const KEY = "__variationalActiveQuotePollerV1__";
  const endpoint = "/api/quotes/indicative";
  const metricsWindowSize = 10000;

  function signatureOf(body) {
    return JSON.stringify({ instrument: body.instrument, qty: String(body.qty) });
  }

  function currentRouteAsset() {
    try {
      const raw = decodeURIComponent(location.pathname.split("/").filter(Boolean).at(-1) || "");
      return /^[A-Za-z0-9_]+$/.test(raw) ? raw.toUpperCase() : "";
    } catch {
      return "";
    }
  }

  const sourceBody = config && config.body;
  const body = sourceBody && {
    ...sourceBody,
    instrument: sourceBody.instrument ? { ...sourceBody.instrument } : sourceBody.instrument
  };
  if (!body || typeof body !== "object" || !body.instrument || body.qty == null) {
    return { ok: false, error: "invalid_quote_template" };
  }

  const signature = signatureOf(body);
  const existing = window[KEY];
  if (existing && existing.active && existing.signature === signature) {
    existing.intervalMs = config.intervalMs;
    existing.maxInFlight = config.maxInFlight;
    existing.timeoutMs = config.timeoutMs;
    existing.maxBackoffMs = config.maxBackoffMs;
    existing.quoteNotionalUsd = config.quoteNotionalUsd;
    return existing.snapshot();
  }
  if (existing && typeof existing.stop === "function") {
    existing.stop("template_changed");
  }

  const asset = String(body.instrument.underlying || "UNKNOWN").toUpperCase();
  const sessionId = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
  const state = {
    active: true,
    pausedReason: "",
    signature,
    asset,
    sessionId,
    sequence: 0,
    inFlight: 0,
    completed: 0,
    failed: 0,
    skipped: 0,
    consecutiveErrors: 0,
    nextAllowedAt: 0,
    lastStartedAt: 0,
    timer: null,
    controllers: new Set(),
    intervalMs: config.intervalMs,
    maxInFlight: config.maxInFlight,
    timeoutMs: config.timeoutMs,
    maxBackoffMs: config.maxBackoffMs,
    quoteNotionalUsd: config.quoteNotionalUsd,
    quoteQty: String(body.qty),
    nextTickAt: performance.now(),
    snapshot() {
      return {
        ok: true,
        active: this.active,
        pausedReason: this.pausedReason,
        asset: this.asset,
        sessionId: this.sessionId,
        sequence: this.sequence,
        displaySequence: this.sequence === 0 ? 0 : ((this.sequence - 1) % metricsWindowSize) + 1,
        metricsWindow: Math.floor(this.sequence / metricsWindowSize) + 1,
        quoteNotionalUsd: this.quoteNotionalUsd,
        quoteQty: this.quoteQty,
        inFlight: this.inFlight,
        completed: this.completed,
        failed: this.failed,
        skipped: this.skipped
      };
    },
    stop(reason = "stopped") {
      this.active = false;
      this.pausedReason = reason;
      if (this.timer != null) {
        clearTimeout(this.timer);
        this.timer = null;
      }
      for (const controller of this.controllers) {
        controller.abort();
      }
      this.controllers.clear();
    }
  };
  window[KEY] = state;

  function schedule() {
    if (!state.active) {
      return;
    }
    state.nextTickAt += state.intervalMs;
    if (state.nextTickAt < performance.now() - state.intervalMs) {
      state.nextTickAt = performance.now() + state.intervalMs;
    }
    state.timer = setTimeout(tick, Math.max(0, state.nextTickAt - performance.now()));
  }

  function backoff(retryAfterSeconds = null) {
    state.consecutiveErrors += 1;
    const exponential = Math.min(state.maxBackoffMs, 1000 * (2 ** Math.min(8, state.consecutiveErrors - 1)));
    const retryMs = retryAfterSeconds == null ? exponential : Math.max(exponential, retryAfterSeconds * 1000);
    state.nextAllowedAt = Date.now() + retryMs;
  }

  function updateQuoteQuantity(payload) {
    const bid = Number(payload && payload.bid);
    const ask = Number(payload && payload.ask);
    const mark = Number(payload && payload.mark_price);
    const referencePrice = Number.isFinite(mark) && mark > 0
      ? mark
      : (Number.isFinite(bid) && Number.isFinite(ask) && bid > 0 && ask > 0 ? (bid + ask) / 2 : NaN);
    if (!Number.isFinite(referencePrice) || referencePrice <= 0) {
      return;
    }
    const approximateQty = state.quoteNotionalUsd / referencePrice;
    if (!Number.isFinite(approximateQty) || approximateQty <= 0) {
      return;
    }
    // 两位有效数字足以保持约 $500 名义深度，同时避免无意义的小数尾数。
    state.quoteQty = Number(approximateQty.toPrecision(2)).toString();
    body.qty = state.quoteQty;
  }

  async function requestQuote(sequence) {
    const controller = new AbortController();
    state.controllers.add(controller);
    state.inFlight += 1;
    const timeout = setTimeout(() => controller.abort(), state.timeoutMs);
    const url = `${endpoint}?__var_active_quote=1&session=${encodeURIComponent(state.sessionId)}` +
      `&seq=${sequence}&asset=${encodeURIComponent(state.asset)}`;
    try {
      const response = await fetch(url, {
        method: "POST",
        credentials: "include",
        cache: "no-store",
        signal: controller.signal,
        headers: {
          "content-type": "application/json",
          "cache-control": "no-cache"
        },
        body: JSON.stringify(body)
      });
      if (response.status === 403) {
        state.failed += 1;
        state.stop("http_403_reverify_required");
        return;
      }
      if (response.status === 429) {
        state.failed += 1;
        const retryAfter = Number(response.headers.get("retry-after"));
        backoff(Number.isFinite(retryAfter) ? retryAfter : null);
        return;
      }
      if (response.status >= 500) {
        state.failed += 1;
        backoff();
        return;
      }
      if (!response.ok) {
        // 400/401/404/422 等同样不是有效报价，不能记入 completed。
        await response.text();
        state.failed += 1;
        backoff();
        return;
      }
      // CDP 负责读取响应正文；这里必须消费 body，保证连接可复用。
      const responseText = await response.text();
      try {
        updateQuoteQuantity(JSON.parse(responseText));
      } catch {
        // CDP 仍会处理响应；无法解析时沿用上一次有效数量。
      }
      state.completed += 1;
      state.consecutiveErrors = 0;
      state.nextAllowedAt = 0;
    } catch (error) {
      if (state.active && error && error.name !== "AbortError") {
        state.failed += 1;
        backoff();
      }
    } finally {
      clearTimeout(timeout);
      state.controllers.delete(controller);
      state.inFlight -= 1;
    }
  }

  function tick() {
    if (!state.active) {
      return;
    }
    state.kick();
    schedule();
  }

  state.kick = function kick() {
    if (!state.active) {
      return false;
    }
    const routeAsset = currentRouteAsset();
    if (routeAsset && /^[A-Z0-9_]+$/.test(state.asset) && routeAsset !== state.asset) {
      state.stop("route_asset_changed");
      return false;
    }
    const now = Date.now();
    if (now < state.nextAllowedAt || now - state.lastStartedAt < state.intervalMs) {
      return false;
    }
    if (state.inFlight >= state.maxInFlight) {
      state.skipped += 1;
      return false;
    }
    state.lastStartedAt = now;
    state.sequence += 1;
    if (state.sequence > 1 && (state.sequence - 1) % metricsWindowSize === 0) {
      state.completed = 0;
      state.failed = 0;
      state.skipped = 0;
    }
    void requestQuote(state.sequence);
    return true;
  };

  state.kick();
  schedule();
  return state.snapshot();
}

function kickActiveQuotePollerInPage() {
  const state = window.__variationalActiveQuotePollerV1__;
  if (!state || typeof state.kick !== "function") {
    return { ok: false, active: false };
  }
  const started = state.kick();
  return { ...state.snapshot(), started };
}

function stopActiveQuotePollerInPage() {
  const state = window.__variationalActiveQuotePollerV1__;
  if (!state || typeof state.stop !== "function") {
    return { ok: true, active: false };
  }
  state.stop("extension_stopped");
  return state.snapshot();
}

function activeQuoteTemplateInfo(body) {
  if (!body || typeof body !== "object" || !body.instrument || body.qty == null) {
    return null;
  }
  const asset = String(body.instrument.underlying || "UNKNOWN").toUpperCase();
  return { body, asset, signature: JSON.stringify({ instrument: body.instrument, qty: String(body.qty) }) };
}

async function installActiveQuotePoller(tabId, template = state.activeQuoteTemplate) {
  if (!state.active || !state.config.activeQuoteEnabled || tabId == null || !template) {
    return null;
  }
  const result = await runInTab(tabId, installActiveQuotePollerInPage, [{
    body: template.body,
    intervalMs: state.config.activeQuoteIntervalMs,
    maxInFlight: state.config.activeQuoteMaxInFlight,
    timeoutMs: state.config.activeQuoteTimeoutMs,
    maxBackoffMs: state.config.activeQuoteMaxBackoffMs,
    quoteNotionalUsd: state.config.activeQuoteNotionalUsd
  }]);
  if (result && result.ok) {
    state.activeQuoteStatus = result.active ? "running" : (result.pausedReason || "stopped");
    state.activeQuoteAsset = result.asset || template.asset;
    state.activeQuoteSessionId = result.sessionId || null;
    state.activeQuoteMetrics = result;
    startActiveQuoteSupervisor();
  } else {
    state.activeQuoteStatus = (result && result.error) || "install_failed";
  }
  notifyStatus();
  return result;
}

function startActiveQuoteSupervisor() {
  stopActiveQuoteSupervisor();
  if (!state.active || !state.config.activeQuoteEnabled) {
    return;
  }
  state.activeQuoteSupervisorTimer = setInterval(async () => {
    if (state.activeQuoteSupervisorBusy || state.attachedTabId == null || !state.activeQuoteTemplate) {
      return;
    }
    state.activeQuoteSupervisorBusy = true;
    try {
      const result = await runInTab(state.attachedTabId, kickActiveQuotePollerInPage);
      if (result && result.ok) {
        state.activeQuoteMetrics = result;
      }
      if (result && !result.active && result.pausedReason) {
        state.activeQuoteStatus = result.pausedReason;
        notifyStatus();
      }
    } catch {
      // Navigation races are recovered by tabs.onUpdated and the next native quote template.
    } finally {
      state.activeQuoteSupervisorBusy = false;
    }
  }, state.config.activeQuoteIntervalMs);
}

function stopActiveQuoteSupervisor() {
  if (state.activeQuoteSupervisorTimer) {
    clearInterval(state.activeQuoteSupervisorTimer);
    state.activeQuoteSupervisorTimer = null;
  }
  state.activeQuoteSupervisorBusy = false;
}

async function stopActiveQuotePoller(tabId) {
  stopActiveQuoteSupervisor();
  if (tabId == null) {
    return;
  }
  try {
    await runInTab(tabId, stopActiveQuotePollerInPage);
  } catch {
    // Tab may already be navigating or closed.
  }
  state.activeQuoteStatus = "stopped";
  state.activeQuoteSessionId = null;
  state.activeQuoteMetrics = null;
}

// 注入页面：MutationObserver 盯 bid/ask 显示，变动即通过 chrome.runtime 推给 background。
// 用 ISOLATED world（可用 chrome.runtime），只读 DOM 不下单。
function installDomQuoteObserverInPage() {
  const KEY = "__varDomQuoteObserver__";
  function parsePrice(text) {
    const cleaned = String(text == null ? "" : text).replace(/[$,\s]/g, "");
    const numeric = Number(cleaned);
    return Number.isFinite(numeric) ? numeric : null;
  }
  function read() {
    const askEl = document.querySelector("[data-testid='ask-price-display']");
    const bidEl = document.querySelector("[data-testid='bid-price-display']");
    return {
      ask: parsePrice(askEl && (askEl.innerText || askEl.textContent)),
      bid: parsePrice(bidEl && (bidEl.innerText || bidEl.textContent))
    };
  }
  let st = window[KEY];
  if (st && st.active) {
    return { reused: true };
  }
  st = { active: true, last: null, observer: null };
  window[KEY] = st;
  const emit = () => {
    const snapshot = read();
    if (snapshot.ask == null && snapshot.bid == null) {
      return;
    }
    if (st.last && st.last.ask === snapshot.ask && st.last.bid === snapshot.bid) {
      return;
    }
    st.last = snapshot;
    try {
      chrome.runtime.sendMessage({
        action: "dom_quote_event",
        payload: { bid: snapshot.bid, ask: snapshot.ask, ts: Date.now() }
      });
    } catch (e) {
      // 忽略瞬时投递失败
    }
  };
  st.observer = new MutationObserver(emit);
  st.observer.observe(document.body || document.documentElement, {
    childList: true,
    characterData: true,
    subtree: true
  });
  emit();
  return { ok: true };
}

async function installDomQuoteObserver(tabId) {
  if (tabId == null) {
    return;
  }
  try {
    await chrome.scripting.executeScript({
      target: { tabId },
      world: "ISOLATED",
      func: installDomQuoteObserverInPage
    });
  } catch (e) {
    // 页面未就绪/无权限时忽略，reload 完成或下次 attach 会重试。
  }
}

async function getActiveTabId() {
  const tabs = await chrome.tabs.query({ active: true, currentWindow: true });
  if (!tabs.length || tabs[0].id == null) {
    throw new Error("No active tab found.");
  }
  return tabs[0].id;
}

async function startForwarding(tabId = null) {
  await ensureConfigLoaded();

  if (state.active) {
    return getStatus();
  }

  const targetTabId = tabId ?? (await getActiveTabId());
  await debuggerAttach(targetTabId);

  try {
    await sendDebuggerCommand(targetTabId, "Network.enable");
  } catch (error) {
    await debuggerDetach(targetTabId);
    throw error;
  }

  state.active = true;
  state.attachedTabId = targetTabId;
  state.activeQuoteStatus = state.config.activeQuoteEnabled ? "waiting_template" : "disabled";
  state.lastError = null;
  await persistRuntimeState();
  wsForwarder.connect();
  restForwarder.connect();
  brokerSocket.connect();
  void installDomQuoteObserver(targetTabId);
  autoReloadAttachedTab("forwarder started");
  notifyStatus();
  return getStatus();
}

function locateOrderElementsInPage(side) {
  const sideNorm = String(side || "buy").toLowerCase() === "sell" ? "sell" : "buy";
  const buyWords = ["buy", "long", "买", "买入", "做多"];
  const sellWords = ["sell", "short", "卖", "卖出", "做空"];

  function isVisible(el) {
    if (!el) {
      return false;
    }
    const style = window.getComputedStyle(el);
    const rect = el.getBoundingClientRect();
    return style.display !== "none" && style.visibility !== "hidden" && rect.width > 0 && rect.height > 0;
  }

  function rectOf(el) {
    if (!el || !isVisible(el)) {
      return null;
    }
    const rect = el.getBoundingClientRect();
    return {
      x: rect.left + rect.width / 2,
      y: rect.top + rect.height / 2,
      left: rect.left,
      top: rect.top,
      width: rect.width,
      height: rect.height
    };
  }

  function textOf(el) {
    return String(el?.innerText || el?.textContent || el?.value || "").replace(/\s+/g, " ").trim();
  }

  function isDisabled(el) {
    return Boolean(el?.disabled || el?.getAttribute("aria-disabled") === "true");
  }

  function hasAnyWord(text, words) {
    const lower = text.toLowerCase();
    return words.some((word) => lower.includes(String(word).toLowerCase()));
  }

  function sideFromText(text) {
    const hasBuy = hasAnyWord(text, buyWords);
    const hasSell = hasAnyWord(text, sellWords);
    if (hasBuy && !hasSell) {
      return "buy";
    }
    if (hasSell && !hasBuy) {
      return "sell";
    }
    return "";
  }

  function describeButton(button) {
    if (!button) {
      return null;
    }
    return {
      text: textOf(button),
      disabled: isDisabled(button),
      rect: rectOf(button),
      dataTestId: button.getAttribute("data-testid") || "",
      ariaPressed: button.getAttribute("aria-pressed") || "",
      ariaLabel: button.getAttribute("aria-label") || "",
      className: String(button.className || "").replace(/\s+/g, " ").slice(0, 160)
    };
  }

  function findSideControls() {
    const switchRoot = document.querySelector("[role='switch']");
    const switchButtons = switchRoot ? Array.from(switchRoot.querySelectorAll("button")).filter(isVisible) : [];
    const allButtons = Array.from(
      document.querySelectorAll("button, [role='button'], input[type='button'], input[type='submit']")
    ).filter((button) => isVisible(button) && button.getAttribute("data-testid") !== "submit-button");
    const candidates = switchButtons.length ? switchButtons : allButtons;
    let buyButton = null;
    let sellButton = null;
    for (const button of candidates) {
      const detected = sideFromText(textOf(button) || button.getAttribute("aria-label") || "");
      if (detected === "buy" && !buyButton) {
        buyButton = button;
      } else if (detected === "sell" && !sellButton) {
        sellButton = button;
      }
    }
    const activeSide = isDisabled(buyButton) ? "buy" : isDisabled(sellButton) ? "sell" : "";
    const targetButton = sideNorm === "sell" ? sellButton : buyButton;
    return {
      buyButton,
      sellButton,
      targetButton,
      activeSide,
      targetAlreadyActive: activeSide === sideNorm
    };
  }

  const submitButton = document.querySelector("button[data-testid='submit-button']");
  const buttons = Array.from(document.querySelectorAll("button, [role='button']")).filter(isVisible);
  const submitWords = ["confirm", "submit", "place", "order", "下单", "确认", "提交"];
  const targetWords = sideNorm === "sell" ? sellWords : buyWords;
  const otherWords = sideNorm === "sell" ? buyWords : sellWords;
  const submitSide = sideFromText(textOf(submitButton));
  const sideControls = findSideControls();

  const inputs = Array.from(document.querySelectorAll("input")).filter(isVisible);
  const qtyInput = document.querySelector("input[data-testid='quantity-input']") || inputs.find((input) => {
    const hint = `${input.placeholder || ""} ${input.name || ""} ${input.getAttribute("aria-label") || ""}`.toLowerCase();
    return /qty|quantity|amount|size|数量|仓位/.test(hint) || input.type === "number" || input.inputMode === "decimal";
  }) || inputs[0] || null;

  const fallbackSubmitButton = isVisible(submitButton) ? submitButton : buttons.find((button) => {
    const text = textOf(button);
    return text.length > 0 && hasAnyWord(text, submitWords) && !isDisabled(button);
  }) || buttons.find((button) => {
    const text = textOf(button);
    return text.length > 0 && hasAnyWord(text, submitWords);
  }) || buttons.find((button) => {
    const text = textOf(button);
    const hasTarget = hasAnyWord(text, targetWords);
    const hasOther = hasAnyWord(text, otherWords);
    return hasTarget && !hasOther && text.length > 0;
  }) || sideControls.targetButton || null;
  const visibleButtons = buttons.slice(0, 30).map((button) => describeButton(button));
  const visibleInputs = inputs.slice(0, 20).map((input) => ({
    value: String(input.value || ""),
    dataTestId: input.getAttribute("data-testid") || "",
    placeholder: String(input.placeholder || ""),
    name: String(input.name || ""),
    ariaLabel: input.getAttribute("aria-label") || "",
    type: String(input.type || ""),
    inputMode: String(input.inputMode || ""),
    rect: rectOf(input)
  }));

  return {
    side: sideNorm,
    url: location.href,
    title: document.title,
    readyState: document.readyState,
    hasBody: Boolean(document.body),
    frameElement: window.frameElement ? String(window.frameElement.tagName || "") : "",
    sideButtonRect: rectOf(sideControls.targetButton),
    sideAlreadyActive: sideControls.targetAlreadyActive || submitSide === sideNorm,
    activeSide: sideControls.activeSide || submitSide,
    buyToggleMeta: describeButton(sideControls.buyButton),
    sellToggleMeta: describeButton(sideControls.sellButton),
    qtyInputRect: rectOf(qtyInput),
    qtyInputValue: qtyInput ? String(qtyInput.value || "") : "",
    submitButtonRect: rectOf(fallbackSubmitButton),
    submitButtonText: textOf(fallbackSubmitButton),
    submitButtonDisabled: isDisabled(fallbackSubmitButton),
    submitButtonMeta: describeButton(fallbackSubmitButton),
    visibleButtons,
    visibleInputs
  };
}

function prepareOrderSnapshotInPage(payload) {
  const side = String(payload?.side || "buy").toLowerCase() === "sell" ? "sell" : "buy";
  const qty = payload?.qty == null ? null : String(payload.qty);
  const buyWords = ["buy", "long", "买", "买入", "做多"];
  const sellWords = ["sell", "short", "卖", "卖出", "做空"];

  function isVisible(el) {
    if (!el) {
      return false;
    }
    const style = window.getComputedStyle(el);
    const rect = el.getBoundingClientRect();
    return style.display !== "none" && style.visibility !== "hidden" && rect.width > 0 && rect.height > 0;
  }

  function rectOf(el) {
    if (!el || !isVisible(el)) {
      return null;
    }
    const rect = el.getBoundingClientRect();
    return {
      x: rect.left + rect.width / 2,
      y: rect.top + rect.height / 2,
      left: rect.left,
      top: rect.top,
      width: rect.width,
      height: rect.height
    };
  }

  function textOf(el) {
    return String(el?.innerText || el?.textContent || el?.value || "").replace(/\s+/g, " ").trim();
  }

  function isDisabled(el) {
    return Boolean(el?.disabled || el?.getAttribute("aria-disabled") === "true");
  }

  function hasAnyWord(text, words) {
    const lower = text.toLowerCase();
    return words.some((word) => lower.includes(String(word).toLowerCase()));
  }

  function sideFromText(text) {
    const hasBuy = hasAnyWord(text, buyWords);
    const hasSell = hasAnyWord(text, sellWords);
    if (hasBuy && !hasSell) {
      return "buy";
    }
    if (hasSell && !hasBuy) {
      return "sell";
    }
    return "";
  }

  function describeButton(button) {
    if (!button) {
      return null;
    }
    const parent = button.closest("form, section, div");
    return {
      text: textOf(button),
      disabled: isDisabled(button),
      disabledAttr: button.hasAttribute("disabled"),
      rect: rectOf(button),
      dataTestId: button.getAttribute("data-testid") || "",
      ariaKeyShortcuts: button.getAttribute("aria-keyshortcuts") || "",
      ariaPressed: button.getAttribute("aria-pressed") || "",
      ariaLabel: button.getAttribute("aria-label") || "",
      title: button.getAttribute("title") || "",
      className: String(button.className || "").replace(/\s+/g, " ").slice(0, 160),
      parentText: textOf(parent).slice(0, 240)
    };
  }

  function setReactInputValue(input, value) {
    const nextValue = String(value ?? "");
    const previousValue = input.value ?? "";
    const prototype = window.HTMLInputElement?.prototype || Object.getPrototypeOf(input);
    const descriptor = Object.getOwnPropertyDescriptor(prototype, "value");
    if (descriptor && typeof descriptor.set === "function") {
      descriptor.set.call(input, nextValue);
    } else {
      input.value = nextValue;
    }
    const tracker = input._valueTracker;
    if (tracker && typeof tracker.setValue === "function") {
      tracker.setValue(previousValue);
    }
    try {
      input.dispatchEvent(new InputEvent("input", { bubbles: true, data: nextValue, inputType: "insertText" }));
    } catch {
      input.dispatchEvent(new Event("input", { bubbles: true }));
    }
    input.dispatchEvent(new Event("change", { bubbles: true }));
    input.dispatchEvent(new KeyboardEvent("keyup", { key: "Enter", code: "Enter", bubbles: true }));
  }

  function settleFormState(input, value) {
    if (!input) {
      return { ok: false, reason: "quantity_input_missing" };
    }
    if (value != null && String(input.value || "") !== String(value)) {
      setReactInputValue(input, value);
    } else {
      input.dispatchEvent(new Event("input", { bubbles: true }));
      input.dispatchEvent(new Event("change", { bubbles: true }));
    }
    input.dispatchEvent(new KeyboardEvent("keydown", { key: "Tab", code: "Tab", bubbles: true }));
    input.dispatchEvent(new KeyboardEvent("keyup", { key: "Tab", code: "Tab", bubbles: true }));
    input.dispatchEvent(new Event("blur", { bubbles: true }));
    if (document.activeElement === input && typeof input.blur === "function") {
      input.blur();
    }
    document.body?.dispatchEvent(new MouseEvent("click", { bubbles: true }));
    return {
      ok: true,
      activeTag: document.activeElement ? String(document.activeElement.tagName || "").toLowerCase() : "",
      currentValue: input.value ?? ""
    };
  }

  function findQtyInput(inputs) {
    return document.querySelector("input[data-testid='quantity-input']") || inputs.find((candidate) => {
      const hint = `${candidate.placeholder || ""} ${candidate.name || ""} ${candidate.getAttribute("aria-label") || ""}`.toLowerCase();
      return /qty|quantity|amount|size|数量|仓位/.test(hint) || candidate.type === "number" || candidate.inputMode === "decimal";
    }) || inputs[0] || null;
  }

  const visibleInputsBefore = Array.from(document.querySelectorAll("input")).filter(isVisible);
  const input = findQtyInput(visibleInputsBefore);
  let qtySettleResult = null;
  if (qty != null && input && String(input.value || "").trim() !== qty) {
    input.focus();
    setReactInputValue(input, qty);
    qtySettleResult = settleFormState(input, qty);
  } else if (qty != null && input) {
    qtySettleResult = settleFormState(input, qty);
  }

  const buttons = Array.from(document.querySelectorAll("button, [role='button']")).filter(isVisible);
  const inputs = Array.from(document.querySelectorAll("input")).filter(isVisible);
  const qtyInput = findQtyInput(inputs);
  const submitButton = document.querySelector("button[data-testid='submit-button']");
  const submitWords = ["confirm", "submit", "place", "order", "下单", "确认", "提交"];
  const targetWords = side === "sell" ? sellWords : buyWords;
  const otherWords = side === "sell" ? buyWords : sellWords;
  const fallbackSubmitButton = isVisible(submitButton) ? submitButton : buttons.find((button) => {
    const text = textOf(button);
    return text.length > 0 && hasAnyWord(text, submitWords) && !isDisabled(button);
  }) || buttons.find((button) => {
    const text = textOf(button);
    return text.length > 0 && hasAnyWord(text, submitWords);
  }) || buttons.find((button) => {
    const text = textOf(button);
    const hasTarget = hasAnyWord(text, targetWords);
    const hasOther = hasAnyWord(text, otherWords);
    return hasTarget && !hasOther && text.length > 0;
  }) || null;
  const submitSide = sideFromText(textOf(fallbackSubmitButton));
  const visibleButtons = buttons.slice(0, 30).map((button) => describeButton(button));
  const visibleInputs = inputs.slice(0, 20).map((candidate) => ({
    value: String(candidate.value || ""),
    dataTestId: candidate.getAttribute("data-testid") || "",
    placeholder: String(candidate.placeholder || ""),
    name: String(candidate.name || ""),
    ariaLabel: candidate.getAttribute("aria-label") || "",
    type: String(candidate.type || ""),
    inputMode: String(candidate.inputMode || ""),
    disabled: Boolean(candidate.disabled || candidate.getAttribute("aria-disabled") === "true"),
    readOnly: Boolean(candidate.readOnly),
    parentText: textOf(candidate.closest("[data-testid='quantity-input-container']") || candidate.closest("label, div, form, section")).slice(0, 220),
    rect: rectOf(candidate)
  }));

  return {
    side,
    url: location.href,
    title: document.title,
    readyState: document.readyState,
    hasBody: Boolean(document.body),
    frameElement: window.frameElement ? String(window.frameElement.tagName || "") : "",
    sideButtonRect: null,
    sideAlreadyActive: submitSide === side,
    activeSide: submitSide,
    buyToggleMeta: null,
    sellToggleMeta: null,
    qtyInputRect: rectOf(qtyInput),
    qtyInputValue: qtyInput ? String(qtyInput.value || "") : "",
    qtySettleResult,
    submitButtonRect: rectOf(fallbackSubmitButton),
    submitButtonText: textOf(fallbackSubmitButton),
    submitButtonDisabled: isDisabled(fallbackSubmitButton),
    submitButtonMeta: describeButton(fallbackSubmitButton),
    visibleButtons,
    visibleInputs
  };
}

function interactWithSubmitButtonInPage(payload) {
  const method = String(payload?.method || "js_click").trim();
  const button = document.querySelector("button[data-testid='submit-button']");
  if (!button) {
    return { ok: false, method, error: "submit_button_missing" };
  }
  const beforeText = String(button.innerText || button.value || "").replace(/\s+/g, " ").trim();
  if (method === "js_click") {
    button.click();
  } else if (method === "js_dispatch_mouse") {
    for (const eventName of ["pointerdown", "mousedown", "pointerup", "mouseup", "click"]) {
      const EventCtor = eventName.startsWith("pointer") && typeof PointerEvent === "function" ? PointerEvent : MouseEvent;
      button.dispatchEvent(new EventCtor(eventName, { bubbles: true, cancelable: true, composed: true, button: 0, buttons: 1 }));
    }
  } else if (method === "js_focus_enter") {
    button.focus();
    button.dispatchEvent(new KeyboardEvent("keydown", { key: "Enter", code: "Enter", bubbles: true }));
    button.dispatchEvent(new KeyboardEvent("keypress", { key: "Enter", code: "Enter", bubbles: true }));
    button.dispatchEvent(new KeyboardEvent("keyup", { key: "Enter", code: "Enter", bubbles: true }));
  } else {
    return { ok: false, method, error: "unknown_submit_method", beforeText };
  }
  return {
    ok: true,
    method,
    beforeText,
    afterText: String(button.innerText || button.value || "").replace(/\s+/g, " ").trim()
  };
}

function buildBrowserOrderTiming(timing) {
  const stages = timing?.stages || {};
  const end = performance.now();
  const duration = (to, from) => (
    typeof stages[to] === "number" && typeof stages[from] === "number"
      ? stages[to] - stages[from]
      : null
  );
  return {
    locateDuration: duration("afterLocate", "beforeLocate"),
    sideClickDuration: duration("afterSideClick", "beforeSideClick"),
    sideWaitDuration: duration("afterSideWait", "afterSideClick"),
    prepareDuration: duration("afterPrepare", "beforePrepare"),
    inputPrepareDuration: duration("afterInputPrepare", "beforeInputPrepare"),
    submitSnapshotDuration: duration("afterSubmitSnapshot", "beforeSubmitSnapshot"),
    disabledRetryWaitDuration: duration("afterDisabledRetryWait", "beforeDisabledRetryWait"),
    submitClickDuration: duration("afterSubmitClick", "beforeSubmitClick"),
    orderWaitDuration: duration("afterOrderWait", "afterSubmitClick"),
    totalDuration: end - (timing?.start || end)
  };
}

async function handlePlaceBrowserOrder(payload) {
  const timing = { start: performance.now(), stages: {} };
  const tabId = state.attachedTabId ?? (await getActiveTabId());
  const side = String(payload?.side || "buy").toLowerCase() === "sell" ? "sell" : "buy";
  const qty = payload?.qty == null ? null : String(payload.qty).trim();
  const prepareOnly = Boolean(payload?.prepareOnly);
  const dryRun = payload?.dryRun !== false;
  const timeoutMs = Math.max(1000, Number(payload?.timeoutMs ?? 20000));

  function inferSnapshotSide(snapshot) {
    const activeSide = String(snapshot?.activeSide || "").trim().toLowerCase();
    if (activeSide === "buy" || activeSide === "sell") {
      return activeSide;
    }
    const text = String(snapshot?.submitButtonText || "").replace(/\s+/g, " ").trim().toLowerCase();
    const buyWords = ["buy", "long", "买", "买入", "做多"];
    const sellWords = ["sell", "short", "卖", "卖出", "做空"];
    const hasBuy = buyWords.some((word) => text.includes(String(word).toLowerCase()));
    const hasSell = sellWords.some((word) => text.includes(String(word).toLowerCase()));
    if (hasBuy && !hasSell) {
      return "buy";
    }
    if (hasSell && !hasBuy) {
      return "sell";
    }
    return "";
  }

  timing.stages.beforeLocate = performance.now();
  const locate = await runInTab(tabId, locateOrderElementsInPage, [side]);
  timing.stages.afterLocate = performance.now();
  if (!locate?.sideAlreadyActive && !locate?.sideButtonRect) {
    return { ok: false, side, qty, dryRun, locate, error: "side_button_missing" };
  }
  timing.stages.beforeSideClick = performance.now();
  if (!locate.sideAlreadyActive) {
    await dispatchTrustedClick(tabId, locate.sideButtonRect);
    timing.stages.afterSideClick = performance.now();
    await new Promise((resolve) => setTimeout(resolve, Math.max(0, Number(payload?.waitAfterSideMs ?? 30))));
    timing.stages.afterSideWait = performance.now();
  } else {
    timing.stages.afterSideClick = timing.stages.beforeSideClick;
    timing.stages.afterSideWait = timing.stages.beforeSideClick;
  }

  timing.stages.beforePrepare = performance.now();
  let before = await runInTab(tabId, prepareOrderSnapshotInPage, [{ side, qty: null }]);
  timing.stages.afterPrepare = performance.now();
  let submitSnapshotAfterInput = null;
  if (qty && String(before?.qtyInputValue || "").trim() !== qty) {
    const waitBeforeInputMs = Math.max(0, Number(payload?.waitBeforeInputMs ?? 0));
    if (waitBeforeInputMs > 0) {
      await new Promise((resolve) => setTimeout(resolve, waitBeforeInputMs));
    }
    timing.stages.beforeInputPrepare = performance.now();
    before = await runInTab(tabId, prepareOrderSnapshotInPage, [{ side, qty }]);
    timing.stages.afterInputPrepare = performance.now();
    const waitAfterInputMs = Math.max(0, Number(payload?.waitAfterInputMs ?? 10));
    if (waitAfterInputMs > 0) {
      const deadline = performance.now() + waitAfterInputMs;
      while (true) {
        submitSnapshotAfterInput = await runInTab(tabId, prepareOrderSnapshotInPage, [{ side, qty: null }]);
        if (submitSnapshotAfterInput?.submitButtonRect && !submitSnapshotAfterInput?.submitButtonDisabled) {
          break;
        }
        const remainingMs = deadline - performance.now();
        if (remainingMs <= 0) {
          break;
        }
        await new Promise((resolve) => setTimeout(resolve, Math.min(10, remainingMs)));
      }
    }
  } else {
    timing.stages.beforeInputPrepare = timing.stages.afterPrepare;
    timing.stages.afterInputPrepare = timing.stages.afterPrepare;
  }
  timing.stages.beforeSubmitSnapshot = performance.now();
  let after = submitSnapshotAfterInput || await runInTab(tabId, prepareOrderSnapshotInPage, [{ side, qty: null }]);
  timing.stages.afterSubmitSnapshot = performance.now();
  if (!dryRun && !prepareOnly) {
    if (!after?.submitButtonRect) {
      return { ok: false, side, qty, dryRun, prepareOnly, before, after, error: "submit_button_missing" };
    }
    if (after.submitButtonDisabled) {
      const disabledRetryWaitMs = Math.max(0, Number(payload?.disabledRetryWaitMs ?? 0));
      let afterDisabledRetry = null;
      if (disabledRetryWaitMs > 0) {
        // 轮询而非死等：按钮一启用立刻点，最多等到上限，避免固定 3 秒延迟。
        timing.stages.beforeDisabledRetryWait = performance.now();
        const deadline = performance.now() + disabledRetryWaitMs;
        while (true) {
          afterDisabledRetry = await runInTab(tabId, prepareOrderSnapshotInPage, [{ side, qty: null }]);
          if (afterDisabledRetry?.submitButtonRect && !afterDisabledRetry?.submitButtonDisabled) {
            break;
          }
          const remainingMs = deadline - performance.now();
          if (remainingMs <= 0) {
            break;
          }
          await new Promise((resolve) => setTimeout(resolve, Math.min(10, remainingMs)));
        }
        timing.stages.afterDisabledRetryWait = performance.now();
      }
      if (!afterDisabledRetry || afterDisabledRetry.submitButtonDisabled) {
        return {
          ok: false,
          side,
          qty,
          dryRun,
          prepareOnly,
          before,
          after,
          afterDisabledRetry,
          timing: buildBrowserOrderTiming(timing),
          error: "submit_button_disabled"
        };
      }
      after = afterDisabledRetry;
    }
    const preparedSide = inferSnapshotSide(after);
    if (preparedSide && preparedSide !== side) {
      return { ok: false, side, qty, dryRun, prepareOnly, before, after, error: "submit_side_mismatch" };
    }
    const waitBeforeSubmitMs = Math.max(0, Number(payload?.waitBeforeSubmitMs ?? 0));
    if (waitBeforeSubmitMs > 0) {
      await new Promise((resolve) => setTimeout(resolve, waitBeforeSubmitMs));
    }
    const submitMethod = payload?.submitMethod || "js_click";
    let orderResponse = null;
    const waitForOrder = waitForNextOrderResponse(tabId, timeoutMs);
    const clickStartedAtMs = Date.now();
    const clickStartedAt = new Date(clickStartedAtMs).toISOString();
    timing.stages.beforeSubmitClick = performance.now();
    const clickResult = await runInTab(tabId, interactWithSubmitButtonInPage, [{ method: submitMethod }]);
    timing.stages.afterSubmitClick = performance.now();
    if (!clickResult?.ok) {
      return { ok: false, side, qty, dryRun, prepareOnly, before, after, clickResult, error: clickResult?.error || "submit_click_failed" };
    }
    const clickAttempts = [{
      method: submitMethod,
      clickAt: clickStartedAt,
      clickAtMs: clickStartedAtMs,
      methodResult: clickResult,
      submitButtonText: after?.submitButtonText || "",
      submitButtonDisabled: after?.submitButtonDisabled,
      submitButtonMeta: after?.submitButtonMeta || null
    }];
    orderResponse = await waitForOrder;
    const waitAfterClickMs = Math.max(0, Number(payload?.waitAfterClickMs ?? 0));
    if (waitAfterClickMs > 0) {
      await new Promise((resolve) => setTimeout(resolve, waitAfterClickMs));
    }
    timing.stages.afterOrderWait = performance.now();
    const submitted = await runInTab(tabId, prepareOrderSnapshotInPage, [{ side, qty: null }]);
    const detailedTiming = buildBrowserOrderTiming(timing);
    return {
      ok: true,
      attachedTabId: tabId,
      side,
      qty,
      dryRun,
      prepareOnly,
      submitMethod,
      clickResult,
      clickStartedAt,
      clickStartedAtMs,
      clickAttempts,
      timing: detailedTiming,
      orderResponse,
      lastQuote: state.lastQuote,
      lastOrderResponse: state.lastOrderResponse,
      before,
      after: submitted,
      blockedReason: null
    };
  }
  return {
    ok: true,
    attachedTabId: tabId,
    side,
    qty,
    dryRun,
    prepareOnly,
    submitMethod: payload?.submitMethod || "js_click",
    timing: buildBrowserOrderTiming(timing),
    lastQuote: state.lastQuote,
    lastOrderResponse: state.lastOrderResponse,
    before,
    after,
    blockedReason: prepareOnly ? "prepare_only" : "dry_run"
  };
}

async function handleBrokerCommand(action, payload) {
  if (action === "place_browser_order" || action === "prepare_browser_order") {
    return await handlePlaceBrowserOrder(payload);
  }
  if (action === "read_position") {
    return await handleReadPosition(payload);
  }
  if (action === "ping") {
    return { ok: true, attachedTabId: state.attachedTabId, status: getStatus() };
  }
  return { ok: false, error: `Unknown broker action: ${action}` };
}

function readPositionInPage() {
  const spans = Array.from(document.querySelectorAll("span"));
  const label = spans.find((s) => String(s.textContent || "").replace(/\s+/g, "") === "当前仓位");
  let valueText = "";
  if (label) {
    const sib = label.nextElementSibling;
    if (sib) {
      valueText = String(sib.innerText || sib.textContent || "").replace(/\s+/g, " ").trim();
    }
    if (!valueText && label.parentElement) {
      valueText = String(label.parentElement.innerText || label.parentElement.textContent || "")
        .replace("当前仓位", "")
        .replace(/\s+/g, " ")
        .trim();
    }
  }
  return { found: Boolean(label), valueText };
}

async function handleReadPosition(payload) {
  const tabId = state.attachedTabId ?? (await getActiveTabId());
  const result = await runInTab(tabId, readPositionInPage);
  return {
    ok: true,
    found: Boolean(result && result.found),
    valueText: String((result && result.valueText) || ""),
    attachedTabId: tabId
  };
}

async function stopForwarding() {
  const attachedTabId = state.attachedTabId;
  await stopActiveQuotePoller(attachedTabId);
  cleanupForwardingState();
  await persistRuntimeState();
  if (attachedTabId != null) {
    try {
      await debuggerDetach(attachedTabId);
    } catch (error) {
      state.lastError = `Debugger detach failed: ${error.message}`;
    }
  }
  notifyStatus();
  return getStatus();
}

function cleanupForwardingState() {
  stopActiveQuoteSupervisor();
  state.active = false;
  state.pendingResponses.clear();
  state.websocketMeta.clear();
  state.activeQuoteTemplate = null;
  state.activeQuoteStatus = "stopped";
  state.activeQuoteAsset = null;
  state.activeQuoteSessionId = null;
  state.latestAcceptedQuoteSeq.clear();
  state.activeQuoteRequests.clear();
  state.activeQuoteMetrics = null;
  state.attachedTabId = null;
  state.lastAutoReloadAt = 0;
  wsForwarder.close();
  restForwarder.close();
  brokerSocket.close();
}

function getStatus() {
  return {
    active: state.active,
    attachedTabId: state.attachedTabId,
    config: state.config,
    sockets: {
      websocket: wsForwarder.status,
      rest: restForwarder.status,
      broker: brokerSocket.status
    },
    lastQuote: state.lastQuote,
    lastOrderResponse: state.lastOrderResponse,
    pendingOrderWaiters: state.pendingOrderWaiters.length,
    activeQuote: {
      status: state.activeQuoteStatus,
      asset: state.activeQuoteAsset,
      sessionId: state.activeQuoteSessionId,
      hasTemplate: Boolean(state.activeQuoteTemplate),
      metrics: state.activeQuoteMetrics
    },
    lastError: state.lastError
  };
}

function notifyStatus() {
  chrome.runtime.sendMessage({ event: "status", status: getStatus() }).catch(() => {
    // No listeners (popup closed), safe to ignore.
  });
}

function trackResponse(params) {
  if (!params?.response?.url || !matchesDomainFilter(params.response.url)) {
    return;
  }
  if (params.type !== "Fetch" && params.type !== "XHR") {
    return;
  }

  const matchedPattern = getMatchedRestPattern(params.response.url);
  if (!matchedPattern) {
    return;
  }

  state.pendingResponses.set(params.requestId, {
    requestId: params.requestId,
    url: params.response.url,
    status: params.response.status,
    statusText: params.response.statusText,
    mimeType: params.response.mimeType,
    headers: params.response.headers,
    type: params.type,
    matchedPattern,
    activeQuote: parseActiveQuoteMeta(params.response.url),
    sentAtMs: state.activeQuoteRequests.get(params.requestId)?.sentAtMs || null,
    capturedAt: nowIso()
  });
}

async function forwardResponseBody(requestId, encodedDataLength) {
  const meta = state.pendingResponses.get(requestId);
  if (!meta || state.attachedTabId == null) {
    return;
  }
  state.pendingResponses.delete(requestId);
  state.activeQuoteRequests.delete(requestId);

  try {
    const result = await sendDebuggerCommand(state.attachedTabId, "Network.getResponseBody", { requestId });
    const bodyText = decodeDebuggerBody(result.body ?? "", Boolean(result.base64Encoded));
    let quoteAccepted = null;
    if (isQuoteUrl(meta.url)) {
      const payload = tryParseJson(bodyText);
      quoteAccepted = quoteResponseAccepted(meta, payload);
      updateLastQuote(meta, bodyText, payload, quoteAccepted);
    }
    if (isOrderUrl(meta.url)) {
      state.lastOrderResponse = {
        ...makeOrderResponseSummary(meta, bodyText),
        tabId: state.attachedTabId
      };
      resolvePendingOrderWaiters(state.lastOrderResponse);
    }
    restForwarder.send({
      kind: "rest_response",
      requestId,
      timestamp: nowIso(),
      encodedDataLength,
      ...meta,
      quoteAccepted,
      body: bodyText,
      base64Encoded: false
    });
  } catch (error) {
    restForwarder.send({
      kind: "rest_response_error",
      requestId,
      timestamp: nowIso(),
      ...meta,
      error: error.message
    });
  }
}

function forwardWebSocketFrame(direction, params) {
  const meta = state.websocketMeta.get(params.requestId);
  if (!meta) {
    return;
  }

  wsForwarder.send({
    kind: "ws_frame",
    direction,
    requestId: params.requestId,
    url: meta.url,
    matchedPattern: meta.matchedPattern || "",
    timestamp: nowIso(),
    opcode: params.response?.opcode,
    mask: params.response?.mask,
    payloadData: params.response?.payloadData ?? ""
  });
}

async function handleDebuggerEvent(source, method, params) {
  if (!state.active || source.tabId !== state.attachedTabId) {
    return;
  }

  if (method === "Network.requestWillBeSent") {
    const request = params && params.request;
    if (!request || !isQuoteUrl(request.url)) {
      return;
    }
    const activeMeta = parseActiveQuoteMeta(request.url);
    if (activeMeta) {
      state.activeQuoteRequests.set(params.requestId, { ...activeMeta, sentAtMs: Date.now() });
      return;
    }
    const parsedBody = tryParseJson(request.postData || "");
    const template = activeQuoteTemplateInfo(parsedBody);
    if (!template) {
      return;
    }
    const changed = !state.activeQuoteTemplate || state.activeQuoteTemplate.signature !== template.signature;
    state.activeQuoteTemplate = template;
    state.activeQuoteAsset = template.asset;
    await persistRuntimeState();
    if (changed || state.activeQuoteStatus !== "running") {
      state.activeQuoteStatus = "installing";
      await installActiveQuotePoller(source.tabId, template);
    }
    return;
  }

  if (method === "Network.responseReceived") {
    trackResponse(params);
    return;
  }

  if (method === "Network.loadingFinished") {
    await forwardResponseBody(params.requestId, params.encodedDataLength);
    return;
  }

  if (method === "Network.loadingFailed") {
    state.pendingResponses.delete(params.requestId);
    state.activeQuoteRequests.delete(params.requestId);
    return;
  }

  if (method === "Network.webSocketCreated") {
    const matchedPattern = getMatchedWsPattern(params.url);
    if (matchesDomainFilter(params.url) && matchedPattern) {
      state.websocketMeta.set(params.requestId, {
        url: params.url,
        matchedPattern,
        createdAt: nowIso()
      });
    }
    return;
  }

  if (method === "Network.webSocketClosed") {
    const meta = state.websocketMeta.get(params.requestId);
    if (!meta) {
      return;
    }
    wsForwarder.send({
      kind: "ws_closed",
      requestId: params.requestId,
      url: meta.url,
      matchedPattern: meta.matchedPattern || "",
      timestamp: nowIso()
    });
    state.websocketMeta.delete(params.requestId);
    return;
  }

  if (method === "Network.webSocketFrameReceived") {
    forwardWebSocketFrame("received", params);
    return;
  }

  if (method === "Network.webSocketFrameSent") {
    forwardWebSocketFrame("sent", params);
    return;
  }

  if (method === "Network.webSocketFrameError") {
    const meta = state.websocketMeta.get(params.requestId);
    if (!meta) {
      return;
    }
    wsForwarder.send({
      kind: "ws_frame_error",
      requestId: params.requestId,
      url: meta.url,
      matchedPattern: meta.matchedPattern || "",
      timestamp: nowIso(),
      errorMessage: params.errorMessage || "Unknown WebSocket frame error"
    });
  }
}

chrome.debugger.onEvent.addListener((source, method, params) => {
  handleDebuggerEvent(source, method, params).catch((error) => {
    state.lastError = `CDP event handling failed: ${error.message}`;
    notifyStatus();
  });
});

chrome.debugger.onDetach.addListener((source, reason) => {
  if (source.tabId !== state.attachedTabId) {
    return;
  }
  state.lastError = `Debugger detached: ${reason}`;
  cleanupForwardingState();
  void persistRuntimeState();
  notifyStatus();
});

chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  (async () => {
    await ensureConfigLoaded();

    if (message.action === "dom_quote_event") {
      brokerSocket.send({
        event: "dom_quote",
        bid: message.payload && message.payload.bid,
        ask: message.payload && message.payload.ask,
        ts: message.payload && message.payload.ts
      });
      return { ok: true };
    }

    if (message.action === "getStatus") {
      return { ok: true, status: getStatus() };
    }

    if (message.action === "updateConfig") {
      const wasActiveQuoteEnabled = state.config.activeQuoteEnabled;
      state.config = sanitizeConfig({ ...state.config, ...message.config });
      await chrome.storage.local.set({ forwarderConfig: state.config });
      if (state.active) {
        wsForwarder.restart();
        restForwarder.restart();
        brokerSocket.restart();
        if (!state.config.activeQuoteEnabled) {
          await stopActiveQuotePoller(state.attachedTabId);
          state.activeQuoteStatus = "disabled";
        } else if (state.activeQuoteTemplate) {
          await installActiveQuotePoller(state.attachedTabId, state.activeQuoteTemplate);
        } else if (!wasActiveQuoteEnabled) {
          state.activeQuoteStatus = "waiting_template";
        }
      }
      notifyStatus();
      return { ok: true, status: getStatus() };
    }

    if (message.action === "start") {
      const status = await startForwarding(message.tabId ?? null);
      return { ok: true, status };
    }

    if (message.action === "stop") {
      const status = await stopForwarding();
      return { ok: true, status };
    }

    return { ok: false, error: `Unknown action: ${message.action}` };
  })()
    .then((response) => sendResponse(response))
    .catch((error) => sendResponse({ ok: false, error: error.message }));

  return true;
});

chrome.tabs.onUpdated.addListener((tabId, changeInfo) => {
  if (state.active && tabId === state.attachedTabId && changeInfo.status === "complete") {
    void installDomQuoteObserver(tabId);
    if (state.activeQuoteTemplate) {
      void installActiveQuotePoller(tabId, state.activeQuoteTemplate);
    }
  }
});

chrome.tabs.onRemoved.addListener((tabId) => {
  if (state.active && tabId === state.attachedTabId) {
    cleanupForwardingState();
    state.lastError = "Attached Variational tab was closed";
    void persistRuntimeState();
    notifyStatus();
  }
});

chrome.runtime.onInstalled.addListener(() => {
  ensureConfigLoaded().catch(() => {
    // Ignore config load errors during install.
  });
});

void restoreRuntimeState();
