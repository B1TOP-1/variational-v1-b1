import json
import subprocess
import textwrap
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class ChromeExtensionBackgroundTest(unittest.TestCase):
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
                body: {},
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
