const DEBUGGER_VERSION = "1.3";
const MAX_QUEUE_SIZE = 1000;
const AUTO_RELOAD_COOLDOWN_MS = 5000;
const QUOTE_URL_PATH = "/api/quotes/indicative";
const ORDER_URL_PATH = "/orders/new/market";

const DEFAULT_CONFIG = {
  wsEndpoint: "ws://127.0.0.1:8766",
  restEndpoint: "ws://127.0.0.1:8767",
  brokerEndpoint: "ws://127.0.0.1:8768",
  domainFilter: "variational",
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
  state.configLoaded = true;
}

function sanitizeConfig(incoming = {}) {
  return {
    wsEndpoint: asStringOrDefault(incoming.wsEndpoint, DEFAULT_CONFIG.wsEndpoint),
    restEndpoint: asStringOrDefault(incoming.restEndpoint, DEFAULT_CONFIG.restEndpoint),
    brokerEndpoint: asStringOrDefault(incoming.brokerEndpoint, DEFAULT_CONFIG.brokerEndpoint),
    domainFilter: asStringOrDefault(incoming.domainFilter, DEFAULT_CONFIG.domainFilter),
    restAllowlist: sanitizeRestAllowlist(incoming.restAllowlist),
    wsAllowlist: sanitizeAllowlist(incoming.wsAllowlist, DEFAULT_CONFIG.wsAllowlist)
  };
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

function updateLastQuote(meta, bodyText) {
  const payload = tryParseJson(bodyText);
  if (!payload || typeof payload !== "object") {
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
    bodyTimestamp: payload.timestamp || null,
    instrument: payload.instrument || null
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
  state.lastError = null;
  wsForwarder.connect();
  restForwarder.connect();
  brokerSocket.connect();
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

  function isVisible(el) {
    if (!el) {
      return false;
    }
    const style = window.getComputedStyle(el);
    const rect = el.getBoundingClientRect();
    return style.display !== "none" && style.visibility !== "hidden" && rect.width > 0 && rect.height > 0;
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

  const input = document.querySelector("input[data-testid='quantity-input']") ||
    Array.from(document.querySelectorAll("input")).filter(isVisible)[0] ||
    null;
  if (qty != null && input && String(input.value || "").trim() !== qty) {
    input.focus();
    setReactInputValue(input, qty);
    input.dispatchEvent(new Event("blur", { bubbles: true }));
    if (document.activeElement === input && typeof input.blur === "function") {
      input.blur();
    }
  }
  return locateOrderElementsInPage(side);
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
      await new Promise((resolve) => setTimeout(resolve, waitAfterInputMs));
    }
  } else {
    timing.stages.beforeInputPrepare = timing.stages.afterPrepare;
    timing.stages.afterInputPrepare = timing.stages.afterPrepare;
  }
  timing.stages.beforeSubmitSnapshot = performance.now();
  const after = await runInTab(tabId, prepareOrderSnapshotInPage, [{ side, qty: null }]);
  timing.stages.afterSubmitSnapshot = performance.now();
  if (!dryRun && !prepareOnly) {
    if (!after?.submitButtonRect) {
      return { ok: false, side, qty, dryRun, prepareOnly, before, after, error: "submit_button_missing" };
    }
    if (after.submitButtonDisabled) {
      return { ok: false, side, qty, dryRun, prepareOnly, before, after, error: "submit_button_disabled" };
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
  if (action === "ping") {
    return { ok: true, attachedTabId: state.attachedTabId, status: getStatus() };
  }
  return { ok: false, error: `Unknown broker action: ${action}` };
}

async function stopForwarding() {
  const attachedTabId = state.attachedTabId;
  cleanupForwardingState();
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
  state.active = false;
  state.pendingResponses.clear();
  state.websocketMeta.clear();
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
    capturedAt: nowIso()
  });
}

async function forwardResponseBody(requestId, encodedDataLength) {
  const meta = state.pendingResponses.get(requestId);
  if (!meta || state.attachedTabId == null) {
    return;
  }
  state.pendingResponses.delete(requestId);

  try {
    const result = await sendDebuggerCommand(state.attachedTabId, "Network.getResponseBody", { requestId });
    const bodyText = decodeDebuggerBody(result.body ?? "", Boolean(result.base64Encoded));
    if (isQuoteUrl(meta.url)) {
      updateLastQuote(meta, bodyText);
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
  notifyStatus();
});

chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  (async () => {
    await ensureConfigLoaded();

    if (message.action === "getStatus") {
      return { ok: true, status: getStatus() };
    }

    if (message.action === "updateConfig") {
      state.config = sanitizeConfig(message.config);
      await chrome.storage.local.set({ forwarderConfig: state.config });
      if (state.active) {
        wsForwarder.restart();
        restForwarder.restart();
        brokerSocket.restart();
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

chrome.runtime.onInstalled.addListener(() => {
  ensureConfigLoaded().catch(() => {
    // Ignore config load errors during install.
  });
});
