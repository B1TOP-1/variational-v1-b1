const $ = (id) => document.getElementById(id);

const inputs = {
  domainFilter: $("domainFilter"),
  wsEndpoint: $("wsEndpoint"),
  restEndpoint: $("restEndpoint"),
  brokerEndpoint: $("brokerEndpoint"),
  activeQuoteEnabled: $("activeQuoteEnabled"),
  activeQuoteIntervalMs: $("activeQuoteIntervalMs"),
  activeQuoteNotionalUsd: $("activeQuoteNotionalUsd"),
  activeQuoteMaxInFlight: $("activeQuoteMaxInFlight"),
  activeQuoteTimeoutMs: $("activeQuoteTimeoutMs"),
  restAllowlist: $("restAllowlist")
};

let formHydrated = false;
let refreshBusy = false;

function setText(id, value) {
  $(id).textContent = value;
}

function formatPrice(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return "--";
  const digits = Math.abs(numeric) >= 1000 ? 2 : Math.abs(numeric) >= 1 ? 4 : 6;
  return numeric.toLocaleString("en-US", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
    useGrouping: true
  });
}

function formatAge(timestamp) {
  const parsed = Date.parse(timestamp || "");
  if (!Number.isFinite(parsed)) return { text: "暂无报价", stale: true };
  const ageMs = Math.max(0, Date.now() - parsed);
  return {
    text: ageMs < 1000 ? `${ageMs}毫秒前` : `${(ageMs / 1000).toFixed(1)}秒前`,
    stale: ageMs > 1500
  };
}

const STATE_TEXT = {
  running: "运行中",
  connected: "已连接",
  connecting: "连接中",
  disconnected: "未连接",
  waiting_template: "等待模板",
  installing: "正在启动",
  restoring: "正在恢复",
  stopped: "已停止",
  disabled: "已禁用",
  error: "错误",
  unknown: "未知"
};

function connectionState(value) {
  const normalized = String(value || "unknown").toLowerCase();
  if (normalized.includes("403") || normalized.includes("failed") || normalized.includes("error")) return "error";
  return normalized;
}

function renderConnection(id, value) {
  const element = $(id);
  const normalized = connectionState(value);
  element.dataset.state = normalized;
  element.querySelector("strong").textContent = STATE_TEXT[normalized] || String(value || "--").replaceAll("_", " ");
}

function hydrateForm(status) {
  if (formHydrated) return;
  inputs.domainFilter.value = status.config.domainFilter || "";
  inputs.wsEndpoint.value = status.config.wsEndpoint || "";
  inputs.restEndpoint.value = status.config.restEndpoint || "";
  inputs.brokerEndpoint.value = status.config.brokerEndpoint || "";
  inputs.activeQuoteEnabled.checked = status.config.activeQuoteEnabled !== false;
  inputs.activeQuoteIntervalMs.value = status.config.activeQuoteIntervalMs || 200;
  inputs.activeQuoteNotionalUsd.value = status.config.activeQuoteNotionalUsd || 500;
  inputs.activeQuoteMaxInFlight.value = status.config.activeQuoteMaxInFlight || 4;
  inputs.activeQuoteTimeoutMs.value = status.config.activeQuoteTimeoutMs || 1500;
  inputs.restAllowlist.value = (status.config.restAllowlist || []).join("\n");
  formHydrated = true;
}

function renderStatus(status) {
  hydrateForm(status);

  const quote = status.lastQuote || {};
  const activeQuote = status.activeQuote || {};
  const metrics = activeQuote.metrics || {};
  const bid = Number(quote.bid);
  const ask = Number(quote.ask);
  const mark = Number(quote.markPrice);
  const spread = Number.isFinite(bid) && Number.isFinite(ask) ? ask - bid : null;
  const spreadBps = Number.isFinite(spread) && Number.isFinite(mark) && mark !== 0
    ? (spread / mark) * 10000
    : null;
  const age = formatAge(quote.timestamp);

  setText("asset", quote.instrument?.underlying || activeQuote.asset || "--");
  setText("bid", formatPrice(quote.bid));
  setText("ask", formatPrice(quote.ask));
  setText("mark", formatPrice(quote.markPrice));
  setText(
    "spread",
    Number.isFinite(spread)
      ? `${formatPrice(spread)}${Number.isFinite(spreadBps) ? ` / ${spreadBps.toFixed(2)}基点` : ""}`
      : "--"
  );
  setText("latency", Number.isFinite(Number(quote.latencyMs)) ? `${Math.round(Number(quote.latencyMs))}ms` : "--");
  setText("quoteAge", age.text);
  $("quoteAge").classList.toggle("stale", age.stale);

  renderConnection("collectorConnection", activeQuote.status);
  renderConnection("restConnection", status.sockets?.rest);
  renderConnection("wsConnection", status.sockets?.websocket);
  renderConnection("brokerConnection", status.sockets?.broker);

  setText("sequence", metrics.displaySequence ?? metrics.sequence ?? 0);
  setText("completed", metrics.completed ?? 0);
  setText("failed", metrics.failed ?? 0);
  setText("skipped", metrics.skipped ?? 0);

  const collectorError = String(activeQuote.status || "").includes("403") ? activeQuote.status : "";
  setText("lastError", status.lastError || collectorError || "");

  const hasError = Boolean(status.lastError || collectorError);
  $("runDot").className = `dot ${hasError ? "error" : status.active ? "running" : ""}`.trim();
  setText("runText", hasError ? "需要处理" : status.active ? "运行中" : "已停止");
  $("start").disabled = Boolean(status.active);
  $("stop").disabled = !status.active;
}

async function send(action, payload = {}) {
  const response = await chrome.runtime.sendMessage({ action, ...payload });
  if (!response?.ok) throw new Error(response?.error || "未知插件错误");
  return response.status;
}

function readConfig() {
  return {
    domainFilter: inputs.domainFilter.value.trim(),
    wsEndpoint: inputs.wsEndpoint.value.trim(),
    restEndpoint: inputs.restEndpoint.value.trim(),
    brokerEndpoint: inputs.brokerEndpoint.value.trim(),
    activeQuoteEnabled: inputs.activeQuoteEnabled.checked,
    activeQuoteIntervalMs: Number(inputs.activeQuoteIntervalMs.value),
    activeQuoteNotionalUsd: Number(inputs.activeQuoteNotionalUsd.value),
    activeQuoteMaxInFlight: Number(inputs.activeQuoteMaxInFlight.value),
    activeQuoteTimeoutMs: Number(inputs.activeQuoteTimeoutMs.value),
    restAllowlist: inputs.restAllowlist.value.split("\n").map((line) => line.trim()).filter(Boolean)
  };
}

async function refreshStatus() {
  if (refreshBusy) return;
  refreshBusy = true;
  try {
    renderStatus(await send("getStatus"));
  } catch (error) {
    setText("lastError", `读取状态失败：${error.message}`);
  } finally {
    refreshBusy = false;
  }
}

$("saveConfig").addEventListener("click", async () => {
  try {
    renderStatus(await send("updateConfig", { config: readConfig() }));
  } catch (error) {
    setText("lastError", `保存失败：${error.message}`);
  }
});

$("start").addEventListener("click", async () => {
  try {
    await send("updateConfig", { config: readConfig() });
    renderStatus(await send("start"));
  } catch (error) {
    setText("lastError", `启动失败：${error.message}`);
  }
});

$("stop").addEventListener("click", async () => {
  try {
    renderStatus(await send("stop"));
  } catch (error) {
    setText("lastError", `停止失败：${error.message}`);
  }
});

chrome.runtime.onMessage.addListener((message) => {
  if (message.event === "status" && message.status) renderStatus(message.status);
});

void refreshStatus();
setInterval(refreshStatus, 250);
