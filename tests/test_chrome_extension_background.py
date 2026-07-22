import json
import subprocess
import textwrap
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class ChromeExtensionBackgroundTest(unittest.TestCase):
    def _run_active_quote_poller(self) -> dict:
        script = textwrap.dedent(
            r"""
            const fs = require("fs");
            const vm = require("vm");
            const source = fs.readFileSync("chrome_extension/background.js", "utf8");
            const start = source.indexOf("function installActiveQuotePollerInPage(config) {");
            if (start < 0) throw new Error("active quote poller not found");
            let depth = 0;
            let end = -1;
            for (let index = start; index < source.length; index += 1) {
              if (source[index] === "{") depth += 1;
              if (source[index] === "}") {
                depth -= 1;
                if (depth === 0) { end = index + 1; break; }
              }
            }
            const pollerSource = source.slice(start, end);
            const fetchCalls = [];
            let timerId = 0;
            const sandbox = {
              window: {},
              location: { pathname: "/perpetual/BTC" },
              performance: { now: () => 1 },
              fetch: async (url, options) => {
                fetchCalls.push({ url, options });
                return { status: 200, headers: { get: () => null }, text: async () => "{}" };
              },
              setTimeout: () => ++timerId,
              clearTimeout: () => {},
              AbortController,
              Date,
              Math,
              JSON,
              String,
              Number,
              encodeURIComponent,
              decodeURIComponent
            };
            vm.createContext(sandbox);
            vm.runInContext(pollerSource, sandbox);
            const config = {
              body: {
                instrument: {
                  underlying: "BTC",
                  instrument_type: "perpetual_future",
                  settlement_asset: "USDC",
                  funding_interval_s: 3600
                },
                qty: "0.001"
              },
              intervalMs: 100,
              maxInFlight: 4,
              timeoutMs: 1500,
              maxBackoffMs: 30000
            };
            const result = sandbox.installActiveQuotePollerInPage(config);
            setImmediate(() => process.stdout.write(JSON.stringify({ result, fetchCalls })));
            """
        )
        completed = subprocess.run(
            ["node", "-e", script],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        return json.loads(completed.stdout)

    def test_active_quote_poller_starts_same_origin_fetch_with_sequence(self):
        result = self._run_active_quote_poller()

        self.assertTrue(result["result"]["active"])
        self.assertEqual(result["result"]["asset"], "BTC")
        self.assertEqual(result["result"]["sequence"], 1)
        self.assertEqual(len(result["fetchCalls"]), 1)
        request = result["fetchCalls"][0]
        self.assertIn("/api/quotes/indicative?__var_active_quote=1", request["url"])
        self.assertIn("seq=1", request["url"])
        self.assertEqual(request["options"]["credentials"], "include")
        self.assertEqual(json.loads(request["options"]["body"])["instrument"]["underlying"], "BTC")

    def test_active_quote_collector_defaults_to_200ms_and_main_world(self):
        background = (ROOT / "chrome_extension" / "background.js").read_text()

        self.assertIn("activeQuoteIntervalMs: 200", background)
        self.assertIn("incomingVersion < 2 && storedInterval === 100 ? 200", background)
        self.assertIn("displaySequence", background)
        self.assertIn("metricsWindowSize = 10000", background)
        self.assertIn("activeQuoteNotionalUsd: 500", background)
        self.assertIn("approximateQty.toPrecision(2)", background)
        self.assertIn("if (!response.ok)", background)
        self.assertIn("state.failed += 1", background)
        self.assertIn("activeQuoteMaxInFlight: 4", background)
        self.assertIn('world: "MAIN"', background)
        self.assertIn("installActiveQuotePollerInPage", background)

    def test_active_quote_collector_learns_native_request_template(self):
        background = (ROOT / "chrome_extension" / "background.js").read_text()

        self.assertIn('method === "Network.requestWillBeSent"', background)
        self.assertIn('tryParseJson(request.postData || "")', background)
        self.assertIn("activeQuoteTemplateInfo(parsedBody)", background)
        self.assertIn("state.activeQuoteTemplate = template", background)

    def test_active_quote_requests_are_sequenced_and_old_responses_are_rejected(self):
        background = (ROOT / "chrome_extension" / "background.js").read_text()

        self.assertIn("__var_active_quote=1", background)
        self.assertIn("latestAcceptedQuoteSeq", background)
        self.assertIn("activeMeta.sequence <= latest", background)
        self.assertIn("activeMeta.sessionId !== state.activeQuoteSessionId", background)
        self.assertIn("meta.status !== 200", background)
        self.assertIn("quoteAccepted", background)
        self.assertIn("return !state.config.activeQuoteEnabled", background)

    def test_active_quote_runtime_state_is_restored(self):
        background = (ROOT / "chrome_extension" / "background.js").read_text()

        self.assertIn("chrome.storage.session", background)
        self.assertIn("restoreRuntimeState", background)
        self.assertIn("chrome.tabs.onRemoved", background)
        self.assertIn("extension_keepalive", background)

    def test_order_quantity_is_restored_after_page_reload(self):
        background = (ROOT / "chrome_extension" / "background.js").read_text()

        self.assertIn("preparedOrder: state.preparedOrder", background)
        restore_assignment = "state.preparedOrder = normalizePreparedOrder(saved?.preparedOrder)"
        self.assertIn(restore_assignment, background)
        self.assertLess(background.index(restore_assignment), background.index("if (!saved?.active"))
        self.assertIn("async function restorePreparedOrder(tabId)", background)
        self.assertIn("void restorePreparedOrder(tabId)", background)
        self.assertIn('state.preparedOrder = { side, qty }', background)
        self.assertIn('snapshot?.qtyInputValue', background)

    def test_order_response_wait_does_not_hold_strategy_for_twenty_seconds(self):
        background = (ROOT / "chrome_extension" / "background.js").read_text()

        self.assertIn('ORDER_URL_PATHS = ["/api/orders/new/market", "/orders/new/market"]', background)
        self.assertIn("ORDER_URL_PATHS.some((path) => url.includes(path))", background)
        self.assertIn("orderResponseTimeoutMs", background)
        self.assertIn("waitForNextOrderResponse(tabId, orderResponseTimeoutMs)", background)

    def test_repository_includes_silent_chrome_launcher(self):
        launcher = (ROOT / "scripts" / "start-variational-chrome.sh").read_text()

        self.assertIn("--silent-debugger-extension-api", launcher)
        self.assertIn("chrome://version", launcher)

    def test_repository_includes_long_running_memory_monitor(self):
        monitor = (ROOT / "scripts" / "monitor-memory.sh").read_text()

        self.assertIn("MEMORY_MONITOR_INTERVAL_SECONDS", monitor)
        self.assertIn("/proc/meminfo", monitor)
        self.assertIn("chrome-renderer", monitor)
        self.assertIn("variational-main", monitor)
        self.assertIn("snapshots", monitor)
        self.assertIn("--sort=-rss", monitor)

    def test_dom_quote_observer_only_watches_price_nodes(self):
        background = (ROOT / "chrome_extension" / "background.js").read_text()
        observer_start = background.index("function installDomQuoteObserverInPage()")
        observer_end = background.index("async function installDomQuoteObserver(tabId)")
        observer_source = background[observer_start:observer_end]

        self.assertIn("observer.observe(element", observer_source)
        self.assertNotIn("observer.observe(document.body", observer_source)
        self.assertIn("setInterval(bindPriceNodes, 1000)", observer_source)
        self.assertIn("disconnectPriceObservers", observer_source)
        self.assertIn("stopDomQuoteObserverInPage", background)

    def test_popup_exposes_active_quote_controls(self):
        popup = (ROOT / "chrome_extension" / "popup.html").read_text()
        popup_script = (ROOT / "chrome_extension" / "popup.js").read_text()

        self.assertIn('id="activeQuoteEnabled"', popup)
        self.assertIn('id="activeQuoteIntervalMs"', popup)
        self.assertIn('id="activeQuoteMaxInFlight"', popup)
        self.assertIn("activeQuoteIntervalMs", popup_script)
        self.assertIn("setInterval(refreshStatus, 200)", popup_script)

    def test_popup_renders_live_quote_and_collector_metrics(self):
        popup = (ROOT / "chrome_extension" / "popup.html").read_text()
        popup_script = (ROOT / "chrome_extension" / "popup.js").read_text()
        background = (ROOT / "chrome_extension" / "background.js").read_text()

        self.assertIn("报价监控", popup)
        self.assertIn("卖出价 Bid", popup)
        self.assertIn("启用主动报价采集", popup)
        for element_id in ("bid", "ask", "mark", "spread", "latency", "sequence", "completed", "failed", "skipped"):
            self.assertIn(f'id="{element_id}"', popup)
        self.assertIn("setInterval(refreshStatus, 200)", popup_script)
        self.assertIn("quote.latencyMs", popup_script)
        self.assertIn("activeQuoteMetrics", background)
        self.assertIn("sentAtMs", background)

    def _run_locate_order_elements(self, buttons_js: str, inputs_js: str = "[]", side: str = "buy") -> dict:
        script = textwrap.dedent(
            r"""
            const fs = require("fs");
            const vm = require("vm");
            const source = fs.readFileSync("chrome_extension/background.js", "utf8");
            const start = source.indexOf("function locateOrderElementsInPage(side) {");
            if (start < 0) {
              throw new Error("locateOrderElementsInPage not found");
            }
            let depth = 0;
            let end = -1;
            for (let index = start; index < source.length; index += 1) {
              const char = source[index];
              if (char === "{") {
                depth += 1;
              } else if (char === "}") {
                depth -= 1;
                if (depth === 0) {
                  end = index + 1;
                  break;
                }
              }
            }
            if (end < 0) {
              throw new Error("locateOrderElementsInPage end not found");
            }
            const locateSource = source.slice(start, end);

            function element(text, rect, attrs = {}) {
              return {
                innerText: text,
                textContent: text,
                value: attrs.value || "",
                placeholder: attrs.placeholder || "",
                name: attrs.name || "",
                type: attrs.type || "",
                inputMode: attrs.inputMode || "",
                disabled: Boolean(attrs.disabled),
                className: attrs.className || "",
                getAttribute(name) {
                  return attrs[name] || null;
                },
                getBoundingClientRect() {
                  return rect;
                }
              };
            }

            const buttons = __BUTTONS_JS__;
            const inputs = __INPUTS_JS__;
            const sandbox = {
              document: {
                title: "Variational",
                readyState: "complete",
                body: {},
                querySelector(selector) {
                  if (selector === "[data-testid='submit-button'], [data-testid=\"submit-button\"]" || selector === "button[data-testid='submit-button']") {
                    return buttons.find((button) => button.getAttribute("data-testid") === "submit-button") || null;
                  }
                  if (selector === "input[data-testid='quantity-input']") {
                    return inputs.find((input) => input.getAttribute("data-testid") === "quantity-input") || null;
                  }
                  return null;
                },
                querySelectorAll(selector) {
                  if (selector.includes("button") || selector.includes("[role='button']")) {
                    return buttons;
                  }
                  if (selector === "input") {
                    return inputs;
                  }
                  return [];
                }
              },
              window: {
                frameElement: null,
                getComputedStyle() {
                  return { display: "block", visibility: "visible" };
                }
              },
              location: {
                href: "https://omni.variational.io/trade/XAU"
              },
              console
            };
            vm.createContext(sandbox);
            vm.runInContext(`${locateSource}; this.result = locateOrderElementsInPage("__SIDE__");`, sandbox);
            process.stdout.write(JSON.stringify(sandbox.result));
            """
        )
        script = (
            script.replace("__BUTTONS_JS__", buttons_js)
            .replace("__INPUTS_JS__", inputs_js)
            .replace("__SIDE__", side)
        )
        completed = subprocess.run(
            ["node", "-e", script],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        return json.loads(completed.stdout)

    def _run_prepare_order_snapshot(self, buttons_js: str, inputs_js: str = "[]", side: str = "buy") -> dict:
        script = textwrap.dedent(
            r"""
            const fs = require("fs");
            const vm = require("vm");
            const source = fs.readFileSync("chrome_extension/background.js", "utf8");
            const start = source.indexOf("function prepareOrderSnapshotInPage(payload) {");
            if (start < 0) {
              throw new Error("prepareOrderSnapshotInPage not found");
            }
            let depth = 0;
            let end = -1;
            for (let index = start; index < source.length; index += 1) {
              const char = source[index];
              if (char === "{") {
                depth += 1;
              } else if (char === "}") {
                depth -= 1;
                if (depth === 0) {
                  end = index + 1;
                  break;
                }
              }
            }
            if (end < 0) {
              throw new Error("prepareOrderSnapshotInPage end not found");
            }
            const prepareSource = source.slice(start, end);

            function element(text, rect, attrs = {}) {
              return {
                innerText: text,
                textContent: text,
                value: attrs.value || "",
                placeholder: attrs.placeholder || "",
                name: attrs.name || "",
                type: attrs.type || "",
                inputMode: attrs.inputMode || "",
                disabled: Boolean(attrs.disabled),
                className: attrs.className || "",
                getAttribute(name) {
                  return attrs[name] || null;
                },
                hasAttribute(name) {
                  return Object.prototype.hasOwnProperty.call(attrs, name);
                },
                closest() {
                  return null;
                },
                getBoundingClientRect() {
                  return rect;
                },
                focus() {},
                blur() {},
                dispatchEvent() {
                  return true;
                }
              };
            }

            const buttons = __BUTTONS_JS__;
            const inputs = __INPUTS_JS__;
            const sandbox = {
              document: {
                title: "Variational",
                readyState: "complete",
                body: {
                  dispatchEvent() {
                    return true;
                  }
                },
                activeElement: null,
                querySelector(selector) {
                  if (selector === "[data-testid='submit-button'], [data-testid=\"submit-button\"]" || selector === "button[data-testid='submit-button']") {
                    return buttons.find((button) => button.getAttribute("data-testid") === "submit-button") || null;
                  }
                  if (selector === "input[data-testid='quantity-input']") {
                    return inputs.find((input) => input.getAttribute("data-testid") === "quantity-input") || null;
                  }
                  return null;
                },
                querySelectorAll(selector) {
                  if (selector.includes("button") || selector.includes("[role='button']")) {
                    return buttons;
                  }
                  if (selector === "input") {
                    return inputs;
                  }
                  return [];
                }
              },
              window: {
                frameElement: null,
                HTMLInputElement: function HTMLInputElement() {},
                getComputedStyle() {
                  return { display: "block", visibility: "visible" };
                }
              },
              location: {
                href: "https://omni.variational.io/trade/XAU"
              },
              Event: function Event() {},
              InputEvent: function InputEvent() {},
              KeyboardEvent: function KeyboardEvent() {},
              MouseEvent: function MouseEvent() {},
              console
            };
            vm.createContext(sandbox);
            vm.runInContext(`${prepareSource}; this.result = prepareOrderSnapshotInPage({ side: "__SIDE__", qty: null });`, sandbox);
            process.stdout.write(JSON.stringify(sandbox.result));
            """
        )
        script = (
            script.replace("__BUTTONS_JS__", buttons_js)
            .replace("__INPUTS_JS__", inputs_js)
            .replace("__SIDE__", side)
        )
        completed = subprocess.run(
            ["node", "-e", script],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        return json.loads(completed.stdout)

    def test_locates_confirm_submit_button_after_side_button_disappears(self):
        result = self._run_locate_order_elements(
            buttons_js="""
            [
              element("Market", { left: 10, top: 10, width: 80, height: 30 }),
              element("Confirm", { left: 100, top: 200, width: 160, height: 40 }),
            ]
            """,
            inputs_js="""
            [
              element("", { left: 20, top: 100, width: 120, height: 30 }, { placeholder: "Amount", value: "0.001" }),
            ]
            """,
        )

        self.assertEqual(result["submitButtonText"], "Confirm")
        self.assertIsNotNone(result["submitButtonRect"])

    def test_prefers_submit_button_testid_and_detects_active_buy_side(self):
        result = self._run_locate_order_elements(
            buttons_js="""
            [
              element("买 $4,058.98", { left: 10, top: 10, width: 140, height: 44 }, { disabled: true, className: "border-green text-green" }),
              element("卖 $4,058.45", { left: 160, top: 10, width: 140, height: 44 }, { className: "border-red text-red" }),
              element("买 XAU", { left: 20, top: 240, width: 280, height: 32 }, { "data-testid": "submit-button", className: "bg-green" }),
            ]
            """,
            side="buy",
        )

        self.assertTrue(result["sideAlreadyActive"])
        self.assertEqual(result["submitButtonText"], "买 XAU")
        self.assertEqual(result["submitButtonRect"]["left"], 20)

    def test_detects_active_sell_side_from_disabled_sell_toggle(self):
        result = self._run_locate_order_elements(
            buttons_js="""
            [
              element("买 $4,058.98", { left: 10, top: 10, width: 140, height: 44 }, { className: "border-green text-green" }),
              element("卖 $4,058.45", { left: 160, top: 10, width: 140, height: 44 }, { disabled: true, className: "border-red text-red" }),
              element("卖 XAU", { left: 20, top: 240, width: 280, height: 32 }, { "data-testid": "submit-button", className: "bg-red" }),
            ]
            """,
            side="sell",
        )

        self.assertTrue(result["sideAlreadyActive"])
        self.assertEqual(result["activeSide"], "sell")
        self.assertEqual(result["submitButtonText"], "卖 XAU")

    def test_submit_button_side_is_enough_when_switch_buttons_are_absent(self):
        result = self._run_locate_order_elements(
            buttons_js="""
            [
              element("买 XAU", { left: 20, top: 240, width: 280, height: 32 }, { "data-testid": "submit-button", className: "bg-green" }),
            ]
            """,
            side="buy",
        )

        self.assertTrue(result["sideAlreadyActive"])
        self.assertEqual(result["activeSide"], "buy")
        self.assertIsNone(result["sideButtonRect"])
        self.assertEqual(result["submitButtonText"], "买 XAU")

    def test_prepare_snapshot_is_self_contained_when_injected(self):
        result = self._run_prepare_order_snapshot(
            buttons_js="""
            [
              element("买 $4,058.98", { left: 10, top: 10, width: 140, height: 44 }, { disabled: true, className: "border-green text-green" }),
              element("卖 $4,058.45", { left: 160, top: 10, width: 140, height: 44 }, { className: "border-red text-red" }),
              element("买 XAU", { left: 20, top: 240, width: 280, height: 32 }, { "data-testid": "submit-button", className: "bg-green" }),
            ]
            """,
            inputs_js="""
            [
              element("", { left: 20, top: 100, width: 120, height: 30 }, { "data-testid": "quantity-input", value: "0.001" }),
            ]
            """,
            side="buy",
        )

        self.assertEqual(result["submitButtonText"], "买 XAU")
        self.assertEqual(result["qtyInputValue"], "0.001")


if __name__ == "__main__":
    unittest.main()
