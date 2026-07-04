import argparse
import asyncio
import contextlib
import csv
import json
import logging
import os
import re
import signal
import sys
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from statistics import median
from typing import Any

import requests
import websockets
from dotenv import load_dotenv
from lighter.signer_client import SignerClient
from sortedcontainers import SortedDict
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table

try:  # POSIX-only; keyboard paging falls back to disabled on Windows.
    import termios
    import tty
except ImportError:  # pragma: no cover
    termios = None
    tty = None

from variational.listener import (
    HEARTBEAT_STALE_SECONDS,
    EventSink,
    VariationalMonitor,
    run_receiver_server,
)
from variational.browser_order import BrowserOrderBroker, BrowserOrderCommand, BrowserOrderDispatchQueue, run_browser_order_broker
from variational.gradient_strategy import EditableField, GradientSignal, GradientStrategyState, StrategySection
from variational.quote_comparator import QuoteComparator

VARIATIONAL_TICKER_OVERRIDES = {
    "LIT": "LIGHTER",
}
VARIATIONAL_ASSET_TO_LIGHTER_TICKER = {v: k for k, v in VARIATIONAL_TICKER_OVERRIDES.items()}

FORWARDER_HOST = "127.0.0.1"
FORWARDER_WS_PORT = 8766
FORWARDER_REST_PORT = 8767
BROWSER_ORDER_BROKER_PORT = 8768
LOG_DIR = Path("./log")
OUTPUT_DIR = LOG_DIR
APP_LOG_FILE = LOG_DIR / "runtime.log"
TRADE_RECORDS_CSV_FILE = LOG_DIR / "trade_records.csv"
BROWSER_SMOKE_TEST_FILE = LOG_DIR / "browser_smoke_test.jsonl"
READY_TIMEOUT_SECONDS = 60.0
# broker 层等待必须 >= 页面内 timeout_ms，否则会在页面返回前假超时。
BROWSER_ORDER_BROKER_MARGIN_SECONDS = 8.0
LIGHTER_ORDER_MAP_MAX = 500
DOM_POSITION_TIMEOUT_SECONDS = 5.0
STRATEGY_EVAL_FALLBACK_SECONDS = 0.2
POSITION_FILL_REFRESH_TIMEOUT_SECONDS = 2.0
POSITION_FILL_REFRESH_POLL_SECONDS = 0.1
POSITION_BALANCE_TIMEOUT_SECONDS = 10.0
LIGHTER_WARM_RETRY_SECONDS = 15.0
# 噪音过滤：同一信号需连续出现这么多次才下单（单次视为噪音）。
SIGNAL_CONFIRM_COUNT = 3
# Var 报价断线闸（默认开）：API 或 DOM 报价超过该时长(ms)没刷新 = 断线，不下单。设 None 关闭。
VAR_QUOTE_DISCONNECT_MS: float | None = 3000.0
# 尖峰过滤（默认关）：设为 Decimal 值开启——触发价差相对近期均值的最大允许偏离。
# 默认 None=关闭：30s 滚动均值滞后，会误杀只持续几秒的真实机会。
SPIKE_BASELINE_WINDOW_SECONDS = 30.0
MAX_SPIKE_DEVIATION_PCT: Decimal | None = None
POLL_INTERVAL_SECONDS = 0.05
HEDGE_SLIPPAGE_BPS = 100.0
DASHBOARD_REFRESH_SECONDS = 1.0
# 价差走势 sparkline：滚动窗口 3 天，按此间隔取样（3天/120s ≈ 2160 点）。
SPREAD_TREND_WINDOW_SECONDS = 3 * 86400.0
SPREAD_TREND_SAMPLE_SECONDS = 30.0
DASHBOARD_ORDERS = 20
SPREAD_HISTORY_SECONDS = 3600.0
HOURLY_HISTORY_HOURS = 12
CST_TZ = timezone(timedelta(hours=8))
# Lines consumed by always-on chrome (header panel + quote/spread tables +
# the hourly/orders table frames). Remaining terminal height is split between
# the hourly-history and recent-orders rows so the dashboard fits one screen.
DASHBOARD_FIXED_OVERHEAD_LINES = 30
# Binance USDCUSDT spot price = USDT per 1 USDC. This is displayed and logged as
# a reference basis signal; it must not rewrite the raw cross-venue price spread.
BINANCE_USDCUSDT_URL = "https://api.binance.com/api/v3/ticker/price?symbol=USDCUSDT"
USDC_USDT_POLL_SECONDS = 10.0
ASSET_SWITCH_CONFIRM_TICKS = 3
PENDING_TRIGGER_SPREAD_TTL_SECONDS = 30.0
LIGHTER_WS_URL = "wss://mainnet.zklighter.elliot.ai/stream"
LIGHTER_WS_PING_INTERVAL_SECONDS = 30
LIGHTER_WS_PING_TIMEOUT_SECONDS = 30


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def now_cst_display() -> str:
    now = datetime.now(CST_TZ)
    return f"{now.month}-{now.day} {now:%H:%M:%S}"


def to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def decimal_to_str(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value, "f")


def resolve_variational_ticker(ticker: str) -> str:
    return VARIATIONAL_TICKER_OVERRIDES.get(ticker.upper(), ticker.upper())


def resolve_lighter_ticker(variational_asset: str) -> str:
    asset = variational_asset.upper()
    return VARIATIONAL_ASSET_TO_LIGHTER_TICKER.get(asset, asset)


def required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is not set")
    return value


def required_int_env(name: str) -> int:
    value = required_env(name)
    try:
        return int(value)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer, got: {value}") from exc


def optional_int_env(name: str, default: int = 0) -> int:
    value = os.getenv(name, "").strip()
    if not value:
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer, got: {value}") from exc


def env_flag(name: str) -> bool:
    value = os.getenv(name, "").strip().lower()
    return value in {"1", "true", "yes", "on"}


def spread_value(aggressive_buy_ask: Decimal | None, aggressive_sell_bid: Decimal | None) -> Decimal | None:
    if aggressive_buy_ask is None or aggressive_sell_bid is None:
        return None
    return aggressive_sell_bid - aggressive_buy_ask


def spread_percent(diff: Decimal | None, denominator: Decimal | None) -> Decimal | None:
    if diff is None or denominator is None or denominator == 0:
        return None
    return (diff / denominator) * Decimal("100")


def book_spread_percent(bid: Decimal | None, ask: Decimal | None) -> Decimal | None:
    if bid is None or ask is None:
        return None
    mid = (bid + ask) / Decimal("2")
    if mid == 0:
        return None
    return ((ask - bid) / mid) * Decimal("100")


def cross_spread_percentages(
    var_bid: Decimal | None,
    var_ask: Decimal | None,
    lighter_bid: Decimal | None,
    lighter_ask: Decimal | None,
) -> tuple[Decimal | None, Decimal | None]:
    long_var_short_lighter_pct = spread_percent(spread_value(var_ask, lighter_bid), var_ask)
    short_var_long_lighter_pct = spread_percent(spread_value(lighter_ask, var_bid), lighter_ask)
    return long_var_short_lighter_pct, short_var_long_lighter_pct


def fill_diff_by_direction(
    side: str,
    var_fill_price: Decimal | None,
    lighter_fill_price: Decimal | None,
) -> tuple[Decimal | None, Decimal | None]:
    lighter = lighter_fill_price
    side_n = side.strip().lower()
    if side_n == "buy":
        # Long Var / Short Lighter: lighter_fill - var_fill
        diff = spread_value(var_fill_price, lighter)
        pct = spread_percent(diff, var_fill_price)
        return diff, pct
    if side_n == "sell":
        # Short Var / Long Lighter: var_fill - lighter_fill
        diff = spread_value(lighter, var_fill_price)
        pct = spread_percent(diff, lighter)
        return diff, pct
    diff = spread_value(lighter, var_fill_price)
    pct = spread_percent(diff, var_fill_price)
    return diff, pct


def normalize_variational_status(status: str) -> str:
    lowered = status.strip().lower()
    if lowered == "confirmed":
        return "filled"
    return lowered


@dataclass(slots=True)
class OrderLifecycle:
    trade_key: str
    trade_id: str
    side: str
    qty: Decimal
    asset: str
    auto_hedge_enabled: bool
    last_variational_status: str

    var_fill_price: Decimal | None = None
    var_fill_ts_iso: str | None = None

    lighter_side: str | None = None
    lighter_client_order_id: int | None = None
    lighter_fill_price: Decimal | None = None
    lighter_fill_ts_iso: str | None = None
    lighter_tx_hash: str | None = None
    hedge_error: str | None = None
    var_order_error: str | None = None
    var_submit_ok: bool | None = None
    var_submit_status: int | None = None
    var_submit_order_id: str | None = None
    var_submit_error: str | None = None
    var_submit_click_started_at: str | None = None
    var_submit_click_started_at_ms: int | None = None
    var_submit_timing: dict[str, Any] | None = None
    var_submit_quote_snapshot: dict[str, Any] | None = None
    var_submit_order_response: dict[str, Any] | None = None

    trigger_spread_pct: Decimal | None = None
    # 触发信号那一刻两腿的预期价（Var 腿 / Lighter 腿），用于分腿滑点。
    var_trigger_price: Decimal | None = None
    lighter_trigger_price: Decimal | None = None
    dom_trigger_price: Decimal | None = None  # 触发时 DOM 报价，用于对比 DOM 口径滑点
    strategy_action: str | None = None
    strategy_target_qty: Decimal | None = None
    strategy_current_qty: Decimal | None = None
    slippage_recorded: bool = False
    # 信号触发时刻(monotonic秒)，用于端到端延迟；两腿延迟只记一次。
    signal_trigger_monotonic: float | None = None
    var_latency_recorded: bool = False
    lighter_latency_recorded: bool = False

    def to_payload(self) -> dict[str, Any]:
        return {
            "trade_key": self.trade_key,
            "trade_id": self.trade_id,
            "side": self.side,
            "qty": decimal_to_str(self.qty),
            "asset": self.asset,
            "variational_filled_price": decimal_to_str(self.var_fill_price),
            "variational_filled_at": self.var_fill_ts_iso,
            "lighter_order_side": self.lighter_side,
            "lighter_client_order_id": self.lighter_client_order_id,
            "lighter_filled_price": decimal_to_str(self.lighter_fill_price),
            "lighter_filled_at": self.lighter_fill_ts_iso,
            "trigger_spread_pct": decimal_to_str(self.trigger_spread_pct),
            "spread_slippage_pct": decimal_to_str(self.spread_slippage_pct()),
            "var_slippage_pct": decimal_to_str(self.var_slippage_pct()),
            "dom_slippage_pct": decimal_to_str(self.dom_slippage_pct()),
            "lighter_slippage_pct": decimal_to_str(self.lighter_slippage_pct()),
            "strategy_action": self.strategy_action,
            "strategy_target_qty": decimal_to_str(self.strategy_target_qty),
            "strategy_current_qty": decimal_to_str(self.strategy_current_qty),
            "auto_hedge_enabled": self.auto_hedge_enabled,
            "var_order_error": self.var_order_error,
            "var_submit_ok": self.var_submit_ok,
            "var_submit_status": self.var_submit_status,
            "var_submit_order_id": self.var_submit_order_id,
            "var_submit_error": self.var_submit_error,
            "var_submit_click_started_at": self.var_submit_click_started_at,
            "var_submit_click_started_at_ms": self.var_submit_click_started_at_ms,
            "var_submit_timing": self.var_submit_timing,
            "var_submit_quote_snapshot": self.var_submit_quote_snapshot,
            "var_submit_order_response": self.var_submit_order_response,
            "hedge_error": self.hedge_error,
            "last_variational_status": self.last_variational_status,
        }

    def spread_slippage_pct(self) -> Decimal | None:
        if self.trigger_spread_pct is None:
            return None
        _diff, fill_pct = fill_diff_by_direction(
            self.side,
            self.var_fill_price,
            self.lighter_fill_price,
        )
        if fill_pct is None:
            return None
        return fill_pct - self.trigger_spread_pct

    @staticmethod
    def _leg_slippage_pct(side: str, trigger: Decimal | None, fill: Decimal | None) -> Decimal | None:
        """单腿滑点%：正=有利。买腿(触发−成交)/触发；卖腿(成交−触发)/触发。"""
        if trigger is None or fill is None or trigger == 0:
            return None
        side_n = side.strip().lower()
        if side_n == "buy":
            return (trigger - fill) / trigger * Decimal("100")
        if side_n == "sell":
            return (fill - trigger) / trigger * Decimal("100")
        return None

    def var_slippage_pct(self) -> Decimal | None:
        return self._leg_slippage_pct(self.side, self.var_trigger_price, self.var_fill_price)

    def dom_slippage_pct(self) -> Decimal | None:
        return self._leg_slippage_pct(self.side, self.dom_trigger_price, self.var_fill_price)

    def lighter_slippage_pct(self) -> Decimal | None:
        return self._leg_slippage_pct(self.lighter_side or "", self.lighter_trigger_price, self.lighter_fill_price)


@dataclass(frozen=True, slots=True)
class PendingTriggerSpread:
    side: str
    spread_pct: Decimal
    created_at_monotonic: float


@dataclass(slots=True)
class RunningStat:
    """累计统计：次数/总和/最近值 → 均值。用于延迟、滑点等指标面板。"""
    n: int = 0
    total: float = 0.0
    last: float | None = None

    def add(self, value: float) -> None:
        self.n += 1
        self.total += value
        self.last = value

    def avg(self) -> float | None:
        return self.total / self.n if self.n else None


class VariationalRuntime:
    def __init__(
        self,
        host: str,
        ws_port: int,
        rest_port: int,
        output_dir: Path | None,
        quiet: bool,
    ) -> None:
        self.monitor = VariationalMonitor(trade_limit=500, snapshot_file=None)
        self.sink = EventSink(output_dir=output_dir, quiet=quiet, monitor=self.monitor)
        self.host = host
        self.ws_port = ws_port
        self.rest_port = rest_port
        self.ws_server = None
        self.rest_server = None

    async def start(self) -> None:
        self.ws_server = await run_receiver_server("ws", self.host, self.ws_port, self.sink)
        self.rest_server = await run_receiver_server("rest", self.host, self.rest_port, self.sink)

    async def stop(self) -> None:
        if self.ws_server is not None:
            self.ws_server.close()
            await self.ws_server.wait_closed()
        if self.rest_server is not None:
            self.rest_server.close()
            await self.rest_server.wait_closed()


class VariationalToLighterRuntime:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.ticker: str | None = None
        self.variational_ticker: str | None = None
        self.accepted_assets: set[str] = set()

        self.stop_flag = False
        self.logger = logging.getLogger("var_lighter_runtime")
        self.logger.setLevel(logging.INFO)
        self.logger.handlers.clear()
        self.logger.propagate = False

        LOG_DIR.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(APP_LOG_FILE, encoding="utf-8")
        file_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
        self.logger.addHandler(file_handler)
        self.dashboard_console = Console()

        output_dir = OUTPUT_DIR.expanduser().resolve()
        self.runtime = VariationalRuntime(
            host=FORWARDER_HOST,
            ws_port=FORWARDER_WS_PORT,
            rest_port=FORWARDER_REST_PORT,
            output_dir=None,
            quiet=True,
        )
        self.browser_order_broker = BrowserOrderBroker()
        self.browser_order_server = None

        self.orders_file = output_dir / "order_metrics.jsonl" if output_dir else None
        self.trade_records_csv_file = output_dir / TRADE_RECORDS_CSV_FILE.name if output_dir else None
        self._order_write_lock = asyncio.Lock()
        self._trade_csv_write_lock = asyncio.Lock()
        self._trade_records_snapshot_sig: str | None = None

        self.records: dict[str, OrderLifecycle] = {}
        self.record_order: deque[str] = deque(maxlen=500)
        self.lighter_client_order_to_trade_key: dict[int, str] = {}
        self._pending_variational_strategy_order_keys: deque[str] = deque()
        self._record_lock = asyncio.Lock()
        self.cross_spread_history: deque[tuple[float, float | None, float | None]] = deque()
        self.hourly_spread_buckets: dict[int, dict[str, list[float]]] = {}
        self._asset_switch_lock = asyncio.Lock()
        self._asset_switch_candidate: str | None = None
        self._asset_switch_candidate_hits = 0

        self.trade_event_cursor = 0

        self.lighter_base_url = "https://mainnet.zklighter.elliot.ai"
        if self.args.browser_smoke_test:
            self.account_index = optional_int_env("LIGHTER_ACCOUNT_INDEX", 0)
            self.api_key_index = optional_int_env("LIGHTER_API_KEY_INDEX", 0)
        else:
            self.account_index = required_int_env("LIGHTER_ACCOUNT_INDEX")
            self.api_key_index = required_int_env("LIGHTER_API_KEY_INDEX")
        self.lighter_client: SignerClient | None = None
        self._lighter_signer_lock = asyncio.Lock()
        # Lighter REST 连接复用 + 预热就绪标志。
        self._lighter_http_session = requests.Session()
        self._lighter_http_session.headers.update({"accept": "application/json"})
        self._lighter_ready = False
        self._lighter_warm_task: asyncio.Task[None] | None = None

        self.lighter_market_index = 0
        self.base_amount_multiplier = 0
        self.price_multiplier = 0

        # Price-keyed SortedDict per side so best bid/ask is O(log n) instead of
        # scanning every level on each book update.
        self.lighter_order_book = {"bids": SortedDict(), "asks": SortedDict()}
        self.lighter_best_bid: Decimal | None = None
        self.lighter_best_ask: Decimal | None = None
        self.lighter_order_book_offset = 0
        self.lighter_order_book_ready = False
        self.lighter_snapshot_loaded = False
        self.lighter_order_book_sequence_gap = False
        self.lighter_order_book_lock = asyncio.Lock()

        self.lighter_ws_task: asyncio.Task[None] | None = None
        self.trade_task: asyncio.Task[None] | None = None
        self.dashboard_task: asyncio.Task[None] | None = None
        self.usdc_usdt_task: asyncio.Task[None] | None = None

        # USDC/USDT basis reference (Binance USDCUSDT = USDT per 1 USDC).
        self.usdc_usdt_rate: Decimal | None = None

        # Two-page dashboard: 1 = live (quotes/signal/orders), 2 = hourly history.
        self.current_page = 1
        self._stdin_fd: int | None = None
        self._stdin_old_settings: Any = None
        self._keyboard_active = False

        self.gradient_strategy = GradientStrategyState.default()
        self._last_dom_position_qty: Decimal | None = None
        self._cached_position_qty: Decimal | None = None
        self._latest_gradient_signal: GradientSignal | None = None
        self._strategy_wake = asyncio.Event()
        self._strategy_task: asyncio.Task[None] | None = None
        self._strategy_order_in_flight = False
        # Lighter 真实账户仓位（account_all 频道，按 symbol）+ 双腿平衡闸状态。
        self._lighter_positions: dict[str, Decimal] = {}
        self._lighter_position_ready = asyncio.Event()
        self._lighter_account_task: asyncio.Task[None] | None = None
        self._strategy_halted = False
        self._halt_reason = ""
        self._last_block_log_sig: tuple[str, Any] | None = None
        self._last_leg_prices: dict[str, Decimal | None] = {}
        self._pending_signal_sig: tuple[str, str, str, str, str] | None = None
        self._pending_signal_count = 0
        self._pending_signal_started_at: float | None = None
        # Var 报价双源对比器（观测记录：API vs DOM）。
        self.quote_comparator = QuoteComparator(("api", "dom"))
        self._last_dom_bid: Decimal | None = None
        self._last_dom_ask: Decimal | None = None
        self._last_dom_at: float | None = None
        self._last_quote_wall: dict[str, float | None] = {"api": None, "dom": None}
        self._last_quote_delay: dict[str, float | None] = {"api": None, "dom": None}
        self._spread_stats_cache: dict[str, float | None] = {}
        # 近3天价差走势取样：(mono_ts, long_float, short_float)
        self._spread_trend: deque[tuple[float, float | None, float | None]] = deque()
        self._last_trend_sample_ts = 0.0
        self.browser_order_broker.on_dom_quote = self._on_dom_quote
        self.runtime.monitor.on_quote_update = self._on_api_quote
        # 统计面板（第2页）：延迟/分腿滑点/信号确认时长/下单成交数。
        self._stat_fired = 0
        self._stat_both_filled = 0
        self._stat_var_latency = RunningStat()       # 信号触发→Var成交(端到端)
        self._stat_lighter_latency = RunningStat()   # 信号触发→Lighter WS成交(端到端)
        self._stat_var_side_switch = RunningStat()   # Var 方向切换耗时(不切=0)
        self._stat_signal_confirm = RunningStat()
        self._stat_var_slip = RunningStat()       # Var 滑点(对 API 触发价)
        self._stat_var_slip_dom = RunningStat()   # Var 滑点(对 DOM 触发价)
        self._stat_lighter_slip = RunningStat()
        self._last_gradient_signal_sig: tuple[str, str, str, str, str] | None = None
        self._last_prepared_order_sig: tuple[str, str] | None = None
        self._prepared_order_side: str = "buy"
        self._pending_trigger_spreads: list[PendingTriggerSpread] = []
        self._browser_order_queue: BrowserOrderDispatchQueue[BrowserOrderCommand] = (
            BrowserOrderDispatchQueue(self._send_browser_order_task)
        )

    def print_startup_next_steps(self) -> None:
        is_zh = self.args.lang == "zh"
        if is_zh:
            lines = [
                "Python 脚本已就位，请回到 Chrome 加载并启动扩展。若 Chrome 插件已启动，请刷新网页。",
                "Use `python main.py --lang en` for the English dashboard.",
            ]
            title = "启动指引"
        else:
            lines = [
                "Python runtime is ready. Go back to Chrome and load/start the extension.",
                "If the Chrome extension has already started, please refresh the webpage."
            ]
            title = "Startup Guide"
        self.dashboard_console.print(Panel("\n".join(lines), title=title, border_style="yellow"))

    def setup_signal_handlers(self) -> None:
        try:
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, self.shutdown)
        except (NotImplementedError, RuntimeError):
            # Windows / no running loop: fall back to synchronous handlers.
            signal.signal(signal.SIGINT, self.shutdown)
            signal.signal(signal.SIGTERM, self.shutdown)

    def shutdown(self, signum=None, frame=None) -> None:
        if self.stop_flag:
            # Second interrupt: restore default handler so a stuck shutdown
            # can still be force-quit with another Ctrl+C.
            with contextlib.suppress(Exception):
                signal.signal(signal.SIGINT, signal.SIG_DFL)
            return
        self.stop_flag = True

    def setup_keyboard(self) -> None:
        """Put stdin in cbreak/no-echo mode and watch single keypresses for paging."""
        if termios is None or tty is None or not sys.stdin.isatty():
            return
        try:
            self._stdin_fd = sys.stdin.fileno()
            self._stdin_old_settings = termios.tcgetattr(self._stdin_fd)
            mode = termios.tcgetattr(self._stdin_fd)
            mode[3] = mode[3] & ~(termios.ICANON | termios.ECHO)
            mode[6][termios.VMIN] = 1
            mode[6][termios.VTIME] = 0
            termios.tcsetattr(self._stdin_fd, termios.TCSADRAIN, mode)
            asyncio.get_running_loop().add_reader(self._stdin_fd, self._on_keypress)
            self._keyboard_active = True
        except Exception:
            self.restore_keyboard()

    def _on_keypress(self) -> None:
        try:
            data = os.read(self._stdin_fd, 1)
        except (OSError, TypeError):
            return
        if not data:
            return
        key = data.decode("utf-8", errors="ignore")
        if key in ("q", "Q", "\x03"):  # q / Ctrl-C
            self.shutdown()
        elif key == "\t":
            self.current_page = 2 if self.current_page == 1 else 1
            if self.current_page == 2:
                self._schedule_prepare_browser_order()
        elif self.current_page == 2:
            was_enabled = self.gradient_strategy.enabled
            if self.gradient_strategy.handle_key(key):
                self._schedule_prepare_browser_order()
            # 由禁用切到启用 → 视为手动确认，解除硬停。
            if self.gradient_strategy.enabled and not was_enabled and self._strategy_halted:
                self._strategy_halted = False
                self._halt_reason = ""
                self.logger.info("策略硬停已由手动重新启用解除")
        elif key == "1":
            self.current_page = 1
        elif key == "2":
            self.current_page = 2
            self._schedule_prepare_browser_order()

    def restore_keyboard(self) -> None:
        if self._stdin_fd is None:
            return
        with contextlib.suppress(Exception):
            asyncio.get_running_loop().remove_reader(self._stdin_fd)
        if self._stdin_old_settings is not None and termios is not None:
            with contextlib.suppress(Exception):
                termios.tcsetattr(self._stdin_fd, termios.TCSADRAIN, self._stdin_old_settings)
        self._keyboard_active = False
        self._stdin_fd = None

    async def update_usdc_usdt_rate_loop(self) -> None:
        """Poll Binance USDCUSDT (USDT per 1 USDC) as an independent basis signal."""
        while not self.stop_flag:
            try:
                rate = await asyncio.to_thread(self._fetch_usdc_usdt_rate)
                if rate is not None and rate > 0:
                    self.usdc_usdt_rate = rate
            except Exception as exc:
                self.logger.warning("Failed to fetch USDCUSDT rate: %s", exc)
            await asyncio.sleep(USDC_USDT_POLL_SECONDS)

    @staticmethod
    def _fetch_usdc_usdt_rate() -> Decimal | None:
        response = requests.get(BINANCE_USDCUSDT_URL, timeout=5)
        response.raise_for_status()
        price = response.json().get("price")
        return Decimal(str(price)) if price is not None else None

    def initialize_lighter_client(self) -> SignerClient:
        if self.lighter_client is None:
            api_key_private_key = os.getenv("API_KEY_PRIVATE_KEY", "").strip() or required_env("LIGHTER_PRIVATE_KEY")
            client = SignerClient(
                url=self.lighter_base_url,
                account_index=self.account_index,
                api_private_keys={self.api_key_index: api_key_private_key},
            )
            err = client.check_client()
            if err is not None:
                # 校验不过不落地半初始化的 client，否则重试会跳过校验。
                raise RuntimeError(f"CheckClient error: {err}")
            self.lighter_client = client
        return self.lighter_client

    async def warm_lighter(self) -> None:
        """Lighter 预热（分步 + 重试）：建签名客户端 + 校验 + 暖一次 REST（复用连接）。"""
        if not self.args.auto_hedge:
            return
        has_pk = bool(os.getenv("API_KEY_PRIVATE_KEY", "").strip() or os.getenv("LIGHTER_PRIVATE_KEY", "").strip())
        self.logger.info(
            "Lighter 预热开始: url=%s account_index=%s api_key_index=%s 私钥=%s",
            self.lighter_base_url,
            self.account_index,
            self.api_key_index,
            "已设置" if has_pk else "缺失(检查 LIGHTER_PRIVATE_KEY / API_KEY_PRIVATE_KEY)",
        )
        while not self.stop_flag and not self._lighter_ready:
            step = "建签名客户端/check_client"
            try:
                # SignerClient 内部依赖运行中的事件循环，必须在主循环里建，勿丢线程。
                self.initialize_lighter_client()
                step = "REST account 查询"
                await asyncio.to_thread(self._rest_get_lighter_account)
                self._lighter_ready = True
                self.logger.info("Lighter 预热完成：签名客户端就绪 + REST 连接复用")
                return
            except Exception as exc:
                self.logger.warning(
                    "Lighter 预热失败[%s]，%.0fs 后重试: %r",
                    step,
                    LIGHTER_WARM_RETRY_SECONDS,
                    exc,
                )
                await asyncio.sleep(LIGHTER_WARM_RETRY_SECONDS)

    def get_lighter_market_config(self) -> tuple[int, int, int]:
        if not self.ticker:
            raise RuntimeError("Ticker is not resolved yet")
        response = requests.get(
            f"{self.lighter_base_url}/api/v1/orderBooks",
            headers={"accept": "application/json"},
            timeout=10,
        )
        response.raise_for_status()
        data = response.json()

        for market in data.get("order_books", []):
            if market.get("symbol") == self.ticker:
                price_decimals = int(market["supported_price_decimals"])
                size_decimals = int(market["supported_size_decimals"])
                return int(market["market_id"]), pow(10, size_decimals), pow(10, price_decimals)

        raise RuntimeError(f"Ticker {self.ticker} not found in Lighter order books")

    async def detect_current_variational_asset(self) -> str | None:
        async with self.runtime.monitor._lock:
            if self.runtime.monitor.current_quote_asset:
                asset = str(self.runtime.monitor.current_quote_asset).strip().upper()
                quote = self.runtime.monitor.quotes.get(asset)
                if (
                    asset
                    and asset != "UNKNOWN"
                    and isinstance(quote, dict)
                    and to_decimal(quote.get("bid")) is not None
                    and to_decimal(quote.get("ask")) is not None
                ):
                    return asset

        return None

    async def wait_for_ticker_resolution(self) -> str:
        deadline = time.time() + READY_TIMEOUT_SECONDS
        while not self.stop_flag and time.time() < deadline:
            asset = await self.detect_current_variational_asset()
            if asset:
                return asset
            await asyncio.sleep(POLL_INTERVAL_SECONDS)

        raise RuntimeError("Timed out deriving ticker from Variational quote/trade messages")

    async def _reset_state_for_asset_switch(self) -> None:
        async with self._record_lock:
            self.records.clear()
            self.record_order.clear()
            self.lighter_client_order_to_trade_key.clear()
            self._pending_variational_strategy_order_keys.clear()
            self._pending_trigger_spreads.clear()
        self.cross_spread_history.clear()
        self.hourly_spread_buckets.clear()
        # 换标的：清缓存仓位与在途/停止状态，避免用旧标的仓位误判平衡。
        self._cached_position_qty = None
        self._strategy_order_in_flight = False
        self._strategy_halted = False
        self._halt_reason = ""
        self._last_gradient_signal_sig = None
        self._pending_signal_sig = None
        self._pending_signal_count = 0
        async with self._trade_csv_write_lock:
            self._trade_records_snapshot_sig = None

    async def activate_asset(self, variational_asset: str, reason: str) -> None:
        asset = variational_asset.strip().upper()
        if not asset or asset == "UNKNOWN":
            return

        async with self._asset_switch_lock:
            next_ticker = resolve_lighter_ticker(asset)
            if self.variational_ticker == asset and self.ticker == next_ticker:
                return

            self.variational_ticker = asset
            self.ticker = next_ticker
            self.accepted_assets = {
                asset,
                next_ticker,
                resolve_variational_ticker(next_ticker),
            }

            self.lighter_market_index, self.base_amount_multiplier, self.price_multiplier = self.get_lighter_market_config()
            await self.reset_lighter_order_book()
            await self._reset_state_for_asset_switch()

            if self.lighter_ws_task and not self.lighter_ws_task.done():
                self.lighter_ws_task.cancel()
                await asyncio.gather(self.lighter_ws_task, return_exceptions=True)

            self.lighter_ws_task = asyncio.create_task(self.handle_lighter_ws())
            await self.wait_for_lighter_order_book_ready()
            self.logger.info(
                "Switched market (%s): variational_asset=%s -> lighter_ticker=%s market_id=%s",
                reason,
                self.variational_ticker,
                self.ticker,
                self.lighter_market_index,
            )

    async def wait_for_variational_ready(self) -> None:
        deadline = time.time() + READY_TIMEOUT_SECONDS
        while not self.stop_flag and time.time() < deadline:
            state = await self.runtime.monitor.get_trading_state()
            hb_age = state.get("heartbeat_age")
            if hb_age is not None and hb_age <= HEARTBEAT_STALE_SECONDS:
                return
            await asyncio.sleep(POLL_INTERVAL_SECONDS)
        raise RuntimeError("Timed out waiting for Variational events stream heartbeat")

    async def wait_for_lighter_order_book_ready(self) -> None:
        deadline = time.time() + READY_TIMEOUT_SECONDS
        while not self.stop_flag and time.time() < deadline:
            if self.lighter_order_book_ready:
                return
            await asyncio.sleep(0.2)
        raise RuntimeError("Timed out waiting for Lighter order book")

    async def reset_lighter_order_book(self) -> None:
        async with self.lighter_order_book_lock:
            self.lighter_order_book["bids"].clear()
            self.lighter_order_book["asks"].clear()
            self.lighter_order_book_offset = 0
            self.lighter_order_book_ready = False
            self.lighter_snapshot_loaded = False
            self.lighter_order_book_sequence_gap = False
            self.lighter_best_bid = None
            self.lighter_best_ask = None

    def update_lighter_order_book(self, side: str, levels: list[Any]) -> None:
        for level in levels:
            if isinstance(level, list) and len(level) >= 2:
                price = Decimal(str(level[0]))
                size = Decimal(str(level[1]))
            elif isinstance(level, dict):
                price = Decimal(str(level.get("price", 0)))
                size = Decimal(str(level.get("size", 0)))
            else:
                continue

            if size > 0:
                self.lighter_order_book[side][price] = size
            else:
                self.lighter_order_book[side].pop(price, None)

    def _refresh_lighter_best(self) -> None:
        """Recompute best bid/ask from the SortedDict books. Caller holds the lock."""
        bids = self.lighter_order_book["bids"]
        asks = self.lighter_order_book["asks"]
        self.lighter_best_bid = bids.peekitem(-1)[0] if bids else None
        self.lighter_best_ask = asks.peekitem(0)[0] if asks else None
        # 盘口变动 → 唤醒事件驱动的策略评估。
        self._strategy_wake.set()

    def validate_order_book_offset(self, new_offset: int) -> bool:
        return new_offset > self.lighter_order_book_offset

    async def request_fresh_snapshot(self, ws: Any) -> None:
        await ws.send(json.dumps({"type": "subscribe", "channel": f"order_book/{self.lighter_market_index}"}))

    async def handle_lighter_fill_update(self, order: dict[str, Any]) -> None:
        if order.get("status") != "filled":
            return

        client_order_id_raw = order.get("client_order_id")
        try:
            client_order_id = int(client_order_id_raw)
        except Exception:
            return

        fill_price: Decimal | None = None
        filled_quote = to_decimal(order.get("filled_quote_amount"))
        filled_base = to_decimal(order.get("filled_base_amount"))
        if filled_quote is not None and filled_base is not None and filled_base != 0:
            fill_price = filled_quote / filled_base

        now_iso = utc_now()

        async with self._record_lock:
            trade_key = self.lighter_client_order_to_trade_key.get(client_order_id)
            if not trade_key:
                return
            record = self.records.get(trade_key)
            if record is None:
                return
            if record.lighter_fill_ts_iso is not None:
                return

            record.lighter_fill_ts_iso = now_iso
            record.lighter_fill_price = fill_price
            self._maybe_record_slippage_stats(record)
            # Lighter 端到端延迟：信号触发 → 收到 Lighter WS 成交（仅策略单）。
            if record.signal_trigger_monotonic is not None and not record.lighter_latency_recorded:
                self._stat_lighter_latency.add((time.monotonic() - record.signal_trigger_monotonic) * 1000)
                record.lighter_latency_recorded = True
            payload = record.to_payload()
            # 成交已回填，映射项不再需要，弹出以释放。
            self.lighter_client_order_to_trade_key.pop(client_order_id, None)

        await self.append_order_log("lighter_fill", payload)

    def _maybe_record_slippage_stats(self, record: OrderLifecycle) -> None:
        """两腿都成交后累计一次分腿滑点（仅策略单有触发价；调用方持 _record_lock）。"""
        if record.slippage_recorded:
            return
        if record.var_fill_price is None or record.lighter_fill_price is None:
            return
        self._stat_both_filled += 1
        v = record.var_slippage_pct()
        vd = record.dom_slippage_pct()
        l = record.lighter_slippage_pct()
        if v is not None:
            self._stat_var_slip.add(float(v))
        if vd is not None:
            self._stat_var_slip_dom.add(float(vd))
        if l is not None:
            self._stat_lighter_slip.add(float(l))
        record.slippage_recorded = True

    def build_lighter_ws_url(self) -> str:
        params: list[str] = []
        if not self.args.auto_hedge:
            params.append("readonly=true")
        if env_flag("LIGHTER_WS_SERVER_PINGS"):
            params.append("server_pings=true")
        if params:
            return f"{LIGHTER_WS_URL}?{'&'.join(params)}"
        return LIGHTER_WS_URL

    async def handle_lighter_ws(self) -> None:
        while not self.stop_flag:
            try:
                await self.reset_lighter_order_book()
                url = self.build_lighter_ws_url()
                async with websockets.connect(
                    url,
                    ping_interval=LIGHTER_WS_PING_INTERVAL_SECONDS,
                    ping_timeout=LIGHTER_WS_PING_TIMEOUT_SECONDS,
                ) as ws:
                    await ws.send(json.dumps({"type": "subscribe", "channel": f"order_book/{self.lighter_market_index}"}))

                    if self.args.auto_hedge:
                        account_orders_channel = f"account_orders/{self.lighter_market_index}/{self.account_index}"
                        try:
                            async with self._lighter_signer_lock:
                                if not self.lighter_client:
                                    self.initialize_lighter_client()
                                auth_token, err = self.lighter_client.create_auth_token_with_expiry(
                                    api_key_index=self.api_key_index
                                )
                            if err is None:
                                await ws.send(
                                    json.dumps(
                                        {
                                            "type": "subscribe",
                                            "channel": account_orders_channel,
                                            "auth": auth_token,
                                        }
                                    )
                                )
                            else:
                                self.logger.warning("Failed to create Lighter WS auth token: %s", err)
                        except Exception as exc:
                            self.logger.warning("Error creating Lighter WS auth token: %s", exc)

                    while not self.stop_flag:
                        raw = await ws.recv()
                        if isinstance(raw, bytes):
                            raw = raw.decode("utf-8", errors="replace")
                        data = json.loads(raw)
                        msg_type = data.get("type")

                        if msg_type == "subscribed/order_book":
                            async with self.lighter_order_book_lock:
                                self.lighter_order_book["bids"].clear()
                                self.lighter_order_book["asks"].clear()
                                order_book = data.get("order_book", {})
                                self.lighter_order_book_offset = int(order_book.get("offset", 0) or 0)
                                self.update_lighter_order_book("bids", order_book.get("bids", []))
                                self.update_lighter_order_book("asks", order_book.get("asks", []))
                                self.lighter_snapshot_loaded = True
                                self.lighter_order_book_ready = True
                                self._refresh_lighter_best()

                        elif msg_type == "update/order_book" and self.lighter_snapshot_loaded:
                            order_book = data.get("order_book", {})
                            if "offset" not in order_book:
                                continue
                            new_offset = int(order_book["offset"])
                            async with self.lighter_order_book_lock:
                                if not self.validate_order_book_offset(new_offset):
                                    self.lighter_order_book_sequence_gap = True
                                else:
                                    self.update_lighter_order_book("bids", order_book.get("bids", []))
                                    self.update_lighter_order_book("asks", order_book.get("asks", []))
                                    self.lighter_order_book_offset = new_offset
                                    self._refresh_lighter_best()

                        elif msg_type == "update/account_orders":
                            orders = data.get("orders", {}).get(str(self.lighter_market_index), [])
                            for order in orders:
                                await self.handle_lighter_fill_update(order)

                        if self.lighter_order_book_sequence_gap:
                            await self.request_fresh_snapshot(ws)
                            self.lighter_order_book_sequence_gap = False

                        if msg_type == "ping":
                            await ws.send(json.dumps({"type": "pong"}))

            except asyncio.CancelledError:
                return
            except Exception as exc:
                self.logger.warning(
                    "Lighter websocket reconnect after error: %s (url=%s)",
                    exc,
                    self.build_lighter_ws_url(),
                )
                await asyncio.sleep(1)

    async def get_lighter_best_bid_ask(self) -> tuple[Decimal | None, Decimal | None]:
        async with self.lighter_order_book_lock:
            return self.lighter_best_bid, self.lighter_best_ask

    async def get_lighter_depth_quote(self, notional: Decimal) -> tuple[Decimal | None, Decimal | None]:
        """VWAP bid/ask to fill `notional` (USDT) along the Lighter book."""
        async with self.lighter_order_book_lock:
            return (
                self._lighter_vwap_locked("bids", notional),
                self._lighter_vwap_locked("asks", notional),
            )

    def _lighter_vwap_locked(self, book_side: str, notional: Decimal) -> Decimal | None:
        """Volume-weighted avg price to consume `notional` (USDT) on one book side.
        bids walked high->low (we sell), asks low->high (we buy). Caller holds the
        lock. If depth < notional, returns the VWAP of all available depth."""
        book = self.lighter_order_book[book_side]
        if not book or notional <= 0:
            return None
        prices = reversed(book) if book_side == "bids" else iter(book)
        remaining = notional
        total_base = Decimal("0")
        total_quote = Decimal("0")
        for price in prices:
            level_notional = price * book[price]
            take = level_notional if level_notional < remaining else remaining
            total_quote += take
            total_base += take / price
            remaining -= take
            if remaining <= 0:
                break
        if total_base <= 0:
            return None
        return total_quote / total_base

    async def get_variational_best_bid_ask(self, preferred_asset: str | None):
        async with self.runtime.monitor._lock:
            quote = None
            if preferred_asset:
                quote = self.runtime.monitor.quotes.get(preferred_asset)
            if quote is None and self.variational_ticker:
                quote = self.runtime.monitor.quotes.get(self.variational_ticker)
            if quote is None and self.runtime.monitor.current_quote_asset:
                quote = self.runtime.monitor.quotes.get(self.runtime.monitor.current_quote_asset)

            if quote is None:
                return None, None, None
            return to_decimal(quote.get("bid")), to_decimal(quote.get("ask")), str(quote.get("asset", ""))

    @staticmethod
    def trade_key(event: dict[str, Any]) -> str:
        trade_id = str(event.get("trade_id", "")).strip()
        if trade_id:
            return f"id:{trade_id}"
        event_seq = str(event.get("event_seq", "")).strip()
        return f"seq:{event_seq}"

    async def append_order_log(self, event_type: str, payload: dict[str, Any]) -> None:
        if self.orders_file is None:
            return
        row = {
            "event": event_type,
            "logged_at": utc_now(),
            **payload,
        }
        line = json.dumps(row, ensure_ascii=True) + "\n"
        async with self._order_write_lock:
            await asyncio.to_thread(self.orders_file.parent.mkdir, parents=True, exist_ok=True)
            await asyncio.to_thread(self._append_line, self.orders_file, line)

    @staticmethod
    def _append_line(path: Path, line: str) -> None:
        with path.open("a", encoding="utf-8") as handle:
            handle.write(line)

    async def place_lighter_order(self, record: OrderLifecycle) -> None:
        if not self.args.auto_hedge:
            return

        side = "SELL" if record.side == "buy" else "BUY"

        best_bid, best_ask = await self.get_lighter_best_bid_ask()
        if best_bid is None or best_ask is None:
            async with self._record_lock:
                record.hedge_error = "Lighter order book not ready"
                payload = record.to_payload()
            self.logger.warning("Lighter 对冲失败(盘口未就绪): key=%s side=%s qty=%s", record.trade_key, side, decimal_to_str(record.qty))
            await self.append_order_log("lighter_error", payload)
            return

        slippage = Decimal(str(HEDGE_SLIPPAGE_BPS)) / Decimal("10000")
        if side == "BUY":
            is_ask = False
            limit_price = best_ask * (Decimal("1") + slippage)
        else:
            is_ask = True
            limit_price = best_bid * (Decimal("1") - slippage)

        base_amount = int(record.qty * self.base_amount_multiplier)
        if base_amount <= 0:
            async with self._record_lock:
                record.hedge_error = f"Hedge base amount rounds to zero ({record.qty})"
                payload = record.to_payload()
            self.logger.warning("Lighter 对冲失败(数量向下取整为0): key=%s qty=%s multiplier=%s", record.trade_key, decimal_to_str(record.qty), self.base_amount_multiplier)
            await self.append_order_log("lighter_error", payload)
            return

        price_i = int(limit_price * self.price_multiplier)
        async with self._record_lock:
            client_order_id = int(time.time() * 1000)
            while client_order_id in self.lighter_client_order_to_trade_key:
                client_order_id += 1

        try:
            async with self._lighter_signer_lock:
                if not self.lighter_client:
                    self.initialize_lighter_client()
                _, tx_hash, error = await self.lighter_client.create_order(
                    market_index=self.lighter_market_index,
                    client_order_index=client_order_id,
                    base_amount=base_amount,
                    price=price_i,
                    is_ask=is_ask,
                    order_type=self.lighter_client.ORDER_TYPE_LIMIT,
                    time_in_force=self.lighter_client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
                    reduce_only=False,
                    trigger_price=0,
                )

            if error is not None:
                raise RuntimeError(f"Sign error: {error}")

            async with self._record_lock:
                record.lighter_side = side
                record.lighter_client_order_id = client_order_id
                record.lighter_tx_hash = tx_hash
                record.hedge_error = None
                self.lighter_client_order_to_trade_key[client_order_id] = record.trade_key
                # 上限淘汰最旧未成交项，避免映射表无限增长。
                while len(self.lighter_client_order_to_trade_key) > LIGHTER_ORDER_MAP_MAX:
                    oldest = next(iter(self.lighter_client_order_to_trade_key))
                    self.lighter_client_order_to_trade_key.pop(oldest, None)
            self.logger.info(
                "Lighter 对冲已下单: key=%s side=%s base_amount=%s price=%s coid=%s",
                record.trade_key, side, base_amount, price_i, client_order_id,
            )
        except Exception as exc:
            async with self._record_lock:
                record.lighter_side = side
                record.hedge_error = str(exc)
                payload = record.to_payload()
            self.logger.warning("Lighter 对冲失败(下单异常): key=%s side=%s error=%s", record.trade_key, side, exc)
            await self.append_order_log("lighter_error", payload)

    @staticmethod
    def _parse_lighter_positions(data: dict[str, Any]) -> dict[str, Decimal]:
        """从 account_all 消息解析 {symbol: 带符号仓位}（position * sign）。"""
        result: dict[str, Decimal] = {}
        positions = data.get("positions", {})
        if isinstance(positions, dict):
            pos_iter: Any = positions.values()
        elif isinstance(positions, list):
            pos_iter = positions
        else:
            return result
        for pos in pos_iter:
            if not isinstance(pos, dict):
                continue
            symbol = str(pos.get("symbol", "")).strip().upper()
            if not symbol:
                continue
            qty = to_decimal(pos.get("position"))
            if qty is None:
                continue
            try:
                sign = int(pos.get("sign", 1))
            except (TypeError, ValueError):
                sign = 1
            result[symbol] = qty * sign
        return result

    async def lighter_account_ws(self) -> None:
        """订阅 account_all/{account_index}（公开频道）实时追踪 Lighter 真实仓位。"""
        while not self.stop_flag:
            try:
                async with websockets.connect(LIGHTER_WS_URL, ping_interval=20, ping_timeout=20) as ws:
                    first = await asyncio.wait_for(ws.recv(), timeout=10)
                    if json.loads(first).get("type") != "connected":
                        self.logger.warning("Lighter account WS: 未收到 connected")
                    await ws.send(json.dumps({"type": "subscribe", "channel": f"account_all/{self.account_index}"}))
                    self.logger.info("已订阅 Lighter account_all/%s", self.account_index)
                    while not self.stop_flag:
                        raw = await ws.recv()
                        if isinstance(raw, bytes):
                            raw = raw.decode("utf-8", errors="replace")
                        data = json.loads(raw)
                        msg_type = data.get("type", "")
                        if msg_type == "ping":
                            await ws.send(json.dumps({"type": "pong"}))
                            continue
                        if msg_type in ("subscribed/account_all", "update/account_all"):
                            self._lighter_positions.update(self._parse_lighter_positions(data))
                            self._lighter_position_ready.set()
            except asyncio.CancelledError:
                return
            except Exception as exc:
                self.logger.warning("Lighter account WS 重连: %s", exc)
                await asyncio.sleep(1.0)

    def _rest_get_lighter_account(self) -> dict[str, Any]:
        response = self._lighter_http_session.get(
            f"{self.lighter_base_url}/api/v1/account",
            params={"by": "index", "value": self.account_index},
            timeout=10,
        )
        response.raise_for_status()
        return response.json() if response.text.strip() else {}

    async def _fetch_lighter_position_rest(self) -> Decimal | None:
        """REST 兜底：WS 未就绪时查询 Lighter 真实仓位。"""
        try:
            data = await asyncio.to_thread(self._rest_get_lighter_account)
        except Exception as exc:
            self.logger.warning("Lighter REST 仓位查询失败: %s", exc)
            return None
        accounts = data.get("accounts") if isinstance(data, dict) else None
        if not accounts:
            return None
        for position in accounts[0].get("positions", []):
            if str(position.get("symbol", "")).strip().upper() == (self.ticker or "").strip().upper():
                qty = to_decimal(position.get("position"))
                if qty is None:
                    return None
                try:
                    sign = int(position.get("sign", 1))
                except (TypeError, ValueError):
                    sign = 1
                return qty * sign
        return Decimal("0")

    def _current_lighter_position(self) -> Decimal:
        return self._lighter_positions.get((self.ticker or "").strip().upper(), Decimal("0"))

    def _balance_tolerance(self) -> Decimal:
        multiplier = self.base_amount_multiplier
        if not multiplier or multiplier <= 0:
            return Decimal("0")
        return Decimal("1") / Decimal(multiplier)  # 1 个最小步长

    def _positions_balanced(self) -> bool:
        """对冲两腿方向相反，净仓位应约为 0。Var 仓位未知则视为不平衡。"""
        var_pos = self._cached_position_qty
        if var_pos is None:
            return False
        net = var_pos + self._current_lighter_position()
        return abs(net) <= self._balance_tolerance()

    def _var_quote_disconnected(self) -> bool:
        """API 或 DOM 报价超过阈值没刷新 = 断线（曾收到过才算，从未收到不算）。"""
        if VAR_QUOTE_DISCONNECT_MS is None:
            return False
        now_ms = time.monotonic() * 1000.0
        for source in ("api", "dom"):
            fresh = self.quote_comparator.freshness_ms(source, now_ms)
            if fresh is not None and fresh > VAR_QUOTE_DISCONNECT_MS:
                return True
        return False

    def _strategy_order_allowed(self) -> bool:
        if self._strategy_halted:
            return False
        if self._var_quote_disconnected():
            return False
        if self.args.auto_hedge and not self._positions_balanced():
            return False
        return True

    async def _confirm_hedge_or_halt(self, prev_var_qty: Decimal | None) -> None:
        """下单后 10s 内确认两腿都到位并重新平衡；超时未平则硬停策略。"""
        if not self.args.auto_hedge:
            await self._refresh_position_cache_after_fill(prev_var_qty)
            return
        if not self._lighter_position_ready.is_set():
            rest = await self._fetch_lighter_position_rest()
            if rest is not None:
                self._lighter_positions[(self.ticker or "").strip().upper()] = rest
        deadline = time.monotonic() + POSITION_BALANCE_TIMEOUT_SECONDS
        while time.monotonic() < deadline and not self.stop_flag:
            var_pos = await self._read_listener_position()
            if var_pos is not None:
                self._cached_position_qty = var_pos
            if self._positions_balanced():
                return
            await asyncio.sleep(POSITION_FILL_REFRESH_POLL_SECONDS)
        self._strategy_halted = True
        self._halt_reason = (
            f"两腿仓位不平衡: Var={decimal_to_str(self._cached_position_qty)} "
            f"Lighter={decimal_to_str(self._current_lighter_position())}"
        )
        self.logger.error("策略已停止（需手动重新启用）：%s", self._halt_reason)

    def _make_strategy_trade_key(self) -> str:
        return f"strategy:{time.time_ns()}"

    def _create_strategy_order_from_signal(self, signal: GradientSignal, side: str, qty: Decimal) -> OrderLifecycle:
        trade_key = self._make_strategy_trade_key()
        while trade_key in self.records:
            trade_key = f"strategy:{int(time.time() * 1000) + len(self.records)}"
        asset = self.variational_ticker or self.ticker or "UNKNOWN"
        # 触发价：开仓(side=buy)→Var买@ask、Lighter卖@bid；清仓(side=sell)→Var卖@bid、Lighter买@ask。
        prices = self._last_leg_prices
        if side == "buy":
            var_trigger = prices.get("var_ask")
            lighter_trigger = prices.get("lighter_bid")
            dom_trigger = self._last_dom_ask
        else:
            var_trigger = prices.get("var_bid")
            lighter_trigger = prices.get("lighter_ask")
            dom_trigger = self._last_dom_bid
        record = OrderLifecycle(
            trade_key=trade_key,
            trade_id="",
            side=side,
            qty=qty,
            asset=asset,
            auto_hedge_enabled=self.args.auto_hedge,
            last_variational_status="strategy_submitted",
            lighter_side="SELL" if side == "buy" else "BUY",
            trigger_spread_pct=signal.spread_pct,
            var_trigger_price=var_trigger,
            lighter_trigger_price=lighter_trigger,
            dom_trigger_price=dom_trigger,
            strategy_action=signal.action,
            strategy_target_qty=signal.target_qty,
            strategy_current_qty=signal.current_qty,
            signal_trigger_monotonic=time.monotonic(),
        )
        self.records[trade_key] = record
        self.record_order.append(trade_key)
        self._queue_pending_variational_strategy_order(record)
        return record

    def _queue_pending_variational_strategy_order(self, record: OrderLifecycle) -> None:
        self._pending_variational_strategy_order_keys.append(record.trade_key)
        while len(self._pending_variational_strategy_order_keys) > 100:
            if isinstance(self._pending_variational_strategy_order_keys, deque):
                self._pending_variational_strategy_order_keys.popleft()
            else:
                self._pending_variational_strategy_order_keys.pop(0)

    def _match_pending_variational_strategy_order(self, side: str, qty: Decimal, asset: str) -> OrderLifecycle | None:
        side_n = side.strip().lower()
        asset_n = asset.strip().upper()
        for trade_key in list(self._pending_variational_strategy_order_keys):
            record = self.records.get(trade_key)
            if record is None:
                with contextlib.suppress(ValueError):
                    self._pending_variational_strategy_order_keys.remove(trade_key)
                continue
            record_assets = self._equivalent_assets(record.asset)
            if record.side == side_n and record.qty == qty and asset_n in record_assets:
                with contextlib.suppress(ValueError):
                    self._pending_variational_strategy_order_keys.remove(trade_key)
                return record
        return None

    @staticmethod
    def _equivalent_assets(asset: str) -> set[str]:
        normalized = asset.strip().upper()
        if not normalized:
            return set()
        return {
            normalized,
            resolve_lighter_ticker(normalized),
            resolve_variational_ticker(normalized),
        }

    def _quantize_to_lighter_lot(self, qty: Decimal) -> Decimal:
        """把下单量向下对齐到 Lighter 最小步长，保证两腿数量完全一致、Lighter 侧不再截断。"""
        multiplier = self.base_amount_multiplier
        if not multiplier or multiplier <= 0:
            return qty  # 未解析到 lot（Var 单边），保持原样
        units = int(qty * multiplier)  # floor 到整数 lot
        return Decimal(units) / Decimal(multiplier)

    def _handle_new_gradient_signal(self, signal: GradientSignal) -> OrderLifecycle | None:
        side = "buy" if signal.action == "open" else "sell"
        qty = self._quantize_to_lighter_lot(min(self.gradient_strategy.single_order_qty, signal.delta_qty))
        if qty <= 0:
            return None

        record = self._create_strategy_order_from_signal(signal, side, qty)
        self._browser_order_queue.submit(BrowserOrderCommand(side=side, qty=qty, dry_run=False, trade_key=record.trade_key))
        self._schedule_background_task(self._log_strategy_order_created(record))
        if self.args.auto_hedge:
            self._schedule_background_task(self.place_lighter_order(record))

        logger = getattr(self, "logger", None)
        if logger is not None:
            logger.info(
                "Strategy order created: key=%s action=%s var_side=%s lighter_side=%s qty=%s trigger_spread=%s",
                record.trade_key,
                signal.action,
                record.side,
                record.lighter_side,
                decimal_to_str(record.qty),
                decimal_to_str(record.trigger_spread_pct),
            )
        return record

    async def _log_strategy_order_created(self, record: OrderLifecycle) -> None:
        await self.append_order_log("strategy_order_created", record.to_payload())

    @staticmethod
    def _schedule_background_task(coro: Any) -> None:
        try:
            asyncio.create_task(coro)
        except RuntimeError:
            close = getattr(coro, "close", None)
            if close is not None:
                close()

    async def _record_var_order_error(self, trade_key: str | None, error: str) -> None:
        if not trade_key:
            return
        async with self._record_lock:
            record = self.records.get(trade_key)
            if record is None:
                return
            record.var_order_error = error
            payload = record.to_payload()
        await self.append_order_log("variational_order_error", payload)

    def should_track_variational_event(self, event: dict[str, Any]) -> bool:
        side = str(event.get("side", "")).strip().lower()
        if side not in {"buy", "sell"}:
            return False

        qty = to_decimal(event.get("qty"))
        if qty is None or qty <= 0:
            return False

        asset = str(event.get("asset", "")).strip().upper()
        if not asset:
            return False
        return asset in self.accepted_assets

    async def process_variational_trade_event(self, event: dict[str, Any]) -> None:
        if not self.should_track_variational_event(event):
            return

        key = self.trade_key(event)
        side = str(event.get("side", "")).strip().lower()
        qty = to_decimal(event.get("qty"))
        if qty is None:
            return

        status = normalize_variational_status(str(event.get("status", "")))
        asset = str(event.get("asset", "")).strip().upper() or self.variational_ticker
        trade_id = str(event.get("trade_id", "")).strip()

        now_iso = utc_now()
        fill_iso = str(event.get("timestamp") or now_iso)

        created = False

        async with self._record_lock:
            record = self._match_pending_variational_strategy_order(side, qty, asset if asset else "UNKNOWN")
            if record is None:
                record = self.records.get(key)
            if record is None:
                record = OrderLifecycle(
                    trade_key=key,
                    trade_id=trade_id,
                    side=side,
                    qty=qty,
                    asset=asset if asset else "UNKNOWN",
                    auto_hedge_enabled=self.args.auto_hedge,
                    last_variational_status=status,
                    trigger_spread_pct=self._consume_pending_trigger_spread(side),
                )
                self.records[key] = record
                self.record_order.append(key)
                created = True
            else:
                previous_status = record.last_variational_status
                if trade_id:
                    record.trade_id = trade_id
                record.last_variational_status = status

            if created:
                previous_status = ""

            should_set_fill = False
            if status == "filled":
                if record.var_fill_ts_iso is None:
                    should_set_fill = True
                elif previous_status != "filled":
                    should_set_fill = True

            if should_set_fill:
                record.var_fill_ts_iso = fill_iso
                record.var_fill_price = to_decimal(event.get("price"))
                self._maybe_record_slippage_stats(record)
                # Var 端到端延迟：信号触发 → 收到 Var 成交（仅策略单有触发时刻）。
                if record.signal_trigger_monotonic is not None and not record.var_latency_recorded:
                    self._stat_var_latency.add((time.monotonic() - record.signal_trigger_monotonic) * 1000)
                    record.var_latency_recorded = True
                filled_payload = record.to_payload()
            else:
                filled_payload = None

            # 老逻辑：策略未启动时，手动(非策略) Var 成交 → Lighter 自动对冲（仅一次）。
            # 策略单用 "strategy:" 前缀区分，且已由 _handle_new_gradient_signal 对冲，跳过。
            should_hedge_manual = (
                should_set_fill
                and self.args.auto_hedge
                and not record.trade_key.startswith("strategy:")
                and record.lighter_side is None
                and not self.gradient_strategy.enabled
            )

        if filled_payload is not None:
            await self.append_order_log("variational_fill", filled_payload)
        if should_hedge_manual:
            self._schedule_background_task(self.place_lighter_order(record))

    async def trade_loop(self) -> None:
        while not self.stop_flag:
            current_asset = await self.detect_current_variational_asset()
            if current_asset:
                if current_asset == self.variational_ticker:
                    self._asset_switch_candidate = None
                    self._asset_switch_candidate_hits = 0
                else:
                    if current_asset == self._asset_switch_candidate:
                        self._asset_switch_candidate_hits += 1
                    else:
                        self._asset_switch_candidate = current_asset
                        self._asset_switch_candidate_hits = 1

                    if self._asset_switch_candidate_hits >= ASSET_SWITCH_CONFIRM_TICKS:
                        await self.activate_asset(current_asset, reason="quote_stream_debounced")
                        self._asset_switch_candidate = None
                        self._asset_switch_candidate_hits = 0
            else:
                self._asset_switch_candidate = None
                self._asset_switch_candidate_hits = 0

            events = await self.runtime.monitor.get_trade_events_since(self.trade_event_cursor, limit=500)
            for event in events:
                self.trade_event_cursor = max(self.trade_event_cursor, int(event.get("event_seq", 0) or 0))
                await self.process_variational_trade_event(event)
            await asyncio.sleep(POLL_INTERVAL_SECONDS)

    def _fmt_price(self, value: Decimal | None) -> str:
        if value is None:
            return "-"
        return format(value, "f")

    def _fmt_var_cell_with_dom(self, api_value: Decimal | None, dom_value: Decimal | None) -> str:
        """行情表 Var 单元格：API 价 + 括号内 DOM 价（相等绿、不同黄，无则灰 -）。"""
        api_text = self._fmt_price(api_value)
        if dom_value is None:
            return f"{api_text} [dim](-)[/]"
        color = "green" if api_value is not None and dom_value == api_value else "yellow"
        return f"{api_text} [{color}]({self._fmt_price(dom_value)})[/]"

    @staticmethod
    def _direction_labels(side: str) -> tuple[str, str]:
        side_n = side.strip().lower()
        if side_n == "buy":
            return "做多 Var / 做空 Lighter", "Long Var / Short Lighter"
        if side_n == "sell":
            return "做空 Var / 做多 Lighter", "Short Var / Long Lighter"
        side_u = side_n.upper() if side_n else "-"
        return side_u, side_u

    def _fmt_pct(self, value: Decimal | None) -> str:
        if value is None:
            return "-"
        return f"{value:.4f}%"

    def _fmt_fill_pct_with_leg_slippage(
        self,
        fill_pct: Decimal | None,
        v_slippage_pct: Decimal | None,
        l_slippage_pct: Decimal | None,
    ) -> str:
        """成交价差% + 括号分腿滑点：V/L 各自正=有利(绿)/负=不利(红)。"""
        fill_text = self._fmt_pct(fill_pct)
        parts: list[str] = []
        for label, slip in (("V", v_slippage_pct), ("L", l_slippage_pct)):
            if slip is None:
                continue
            color = "green" if slip >= 0 else "red"
            sign = "+" if slip >= 0 else ""
            parts.append(f"{label}[{color}]{sign}{slip:.3f}%[/{color}]")
        if not parts:
            return fill_text
        return f"{fill_text} ({' '.join(parts)})"

    def _fmt_signal_pct(
        self,
        current: Decimal | None,
        book_spread_baseline: Decimal | None,
        median_5m: float | None,
        median_30m: float | None,
        median_1h: float | None,
    ) -> str:
        if current is None:
            return "-"
        if book_spread_baseline is None:
            color = "red"
            return f"[{color}]{self._fmt_pct(current)}[/{color}]"

        adjusted = current - book_spread_baseline
        adjusted_f = float(adjusted)
        thresholds = [v for v in (median_5m, median_30m, median_1h) if v is not None]
        is_green = any(adjusted_f > threshold for threshold in thresholds)
        color = "green" if is_green else "red"
        return f"[{color}]{self._fmt_pct(current)}[/{color}]"

    @staticmethod
    def _decimal_as_float(value: Decimal | None) -> float | None:
        if value is None:
            return None
        return float(value)

    @staticmethod
    def _fmt_median_pct(value: float | None) -> str:
        if value is None:
            return "-"
        return f"{value:.4f}%"

    def _record_cross_spreads(
        self,
        long_var_short_lighter_pct: Decimal | None,
        short_var_long_lighter_pct: Decimal | None,
    ) -> None:
        now = time.monotonic()
        self.cross_spread_history.append(
            (
                now,
                self._decimal_as_float(long_var_short_lighter_pct),
                self._decimal_as_float(short_var_long_lighter_pct),
            )
        )
        cutoff = now - SPREAD_HISTORY_SECONDS
        while self.cross_spread_history and self.cross_spread_history[0][0] < cutoff:
            self.cross_spread_history.popleft()

    def _cross_spread_values(self, window_seconds: float, long_side: bool) -> list[float]:
        now = time.monotonic()
        cutoff = now - window_seconds
        value_index = 1 if long_side else 2
        return [
            row[value_index]
            for row in self.cross_spread_history
            if row[0] >= cutoff and row[value_index] is not None
        ]

    def _median_cross_spread(self, window_seconds: float, long_side: bool) -> float | None:
        values = self._cross_spread_values(window_seconds, long_side)
        if not values:
            return None
        return float(median(values))

    def _percentile_cross_spread(
        self, window_seconds: float, long_side: bool, pct: float
    ) -> float | None:
        values = self._cross_spread_values(window_seconds, long_side)
        return self._percentile(values, pct)

    @staticmethod
    def _percentile(values: list[float], pct: float) -> float | None:
        if not values:
            return None
        return VariationalToLighterRuntime._percentile_sorted(sorted(values), pct)

    @staticmethod
    def _percentile_sorted(ordered: list[float], pct: float) -> float | None:
        if not ordered:
            return None
        if len(ordered) == 1:
            return float(ordered[0])
        rank = (pct / 100.0) * (len(ordered) - 1)
        low = int(rank)
        high = min(low + 1, len(ordered) - 1)
        frac = rank - low
        return float(ordered[low] + (ordered[high] - ordered[low]) * frac)

    def _window_stats(self, window_seconds: float, long_side: bool) -> tuple[float | None, float | None, float | None]:
        """一次过滤+排序，同出 (median, p90, p10)，避免同窗口重复过滤三遍。"""
        values = self._cross_spread_values(window_seconds, long_side)
        if not values:
            return None, None, None
        ordered = sorted(values)
        mid = len(ordered) // 2
        median_v = float(ordered[mid] if len(ordered) % 2 else (ordered[mid - 1] + ordered[mid]) / 2.0)
        return median_v, self._percentile_sorted(ordered, 90), self._percentile_sorted(ordered, 10)

    def _sample_spread_trend(self, long_pct: Decimal | None, short_pct: Decimal | None) -> None:
        """按取样间隔把当前价差写入近3天走势缓冲，并裁掉超窗样本。"""
        now = time.monotonic()
        if now - self._last_trend_sample_ts < SPREAD_TREND_SAMPLE_SECONDS:
            return
        long_f = self._decimal_as_float(long_pct)
        short_f = self._decimal_as_float(short_pct)
        if long_f is None and short_f is None:
            return  # 价差还没算出来(盘口未就绪)，不存空值污染走势
        self._last_trend_sample_ts = now
        self._spread_trend.append((now, long_f, short_f))
        cutoff = now - SPREAD_TREND_WINDOW_SECONDS
        while self._spread_trend and self._spread_trend[0][0] < cutoff:
            self._spread_trend.popleft()

    def _spread_trend_series(self, series_index: int, width: int, now: float) -> tuple[list[float | None], float | None, float | None]:
        """近3天该序列按时间分桶(width格)取均值；返回 (每桶值, 最小, 最大)，空桶为 None。"""
        start = now - SPREAD_TREND_WINDOW_SECONDS
        buckets: list[list[float]] = [[] for _ in range(width)]
        for ts, longv, shortv in self._spread_trend:
            if ts < start:
                continue
            value = longv if series_index == 0 else shortv
            if value is None:
                continue
            pos = int((ts - start) / SPREAD_TREND_WINDOW_SECONDS * width)
            buckets[min(width - 1, max(0, pos))].append(value)
        avgs: list[float | None] = [sum(b) / len(b) if b else None for b in buckets]
        present = [a for a in avgs if a is not None]
        if not present:
            return avgs, None, None
        return avgs, min(present), max(present)

    @staticmethod
    def _ascii_line_chart(
        values: list[float | None],
        height: int,
        x_labels: tuple[str, str] | None = None,
    ) -> list[str]:
        """asciichart 式连续折线：用 ╭╮╰╯─│ 把 values 连成折线；空值断开。左侧带轴标签，
        可选底部时间轴 x_labels=(左, 右)。"""
        present = [v for v in values if v is not None]
        if not present:
            return []
        mn, mx = min(present), max(present)
        rng = (mx - mn) or 1.0
        rows = max(2, height)
        grid = [[" "] * len(values) for _ in range(rows)]

        def level(v: float) -> int:
            return int(round((v - mn) / rng * (rows - 1)))

        prev_l: int | None = None
        for x, v in enumerate(values):
            if v is None:
                prev_l = None
                continue
            cur = level(v)
            r = rows - 1 - cur
            if prev_l is None or cur == prev_l:
                grid[r][x] = "─"
            else:
                pr = rows - 1 - prev_l
                if cur > prev_l:  # 上升
                    grid[r][x] = "╭"
                    grid[pr][x] = "╯"
                    for rr in range(r + 1, pr):
                        grid[rr][x] = "│"
                else:  # 下降
                    grid[r][x] = "╰"
                    grid[pr][x] = "╮"
                    for rr in range(pr + 1, r):
                        grid[rr][x] = "│"
            prev_l = cur

        # 小数位随范围自适应：范围越小显示越多位，看清微小波动。
        span = mx - mn
        decimals = 2
        s = span if span > 0 else 1.0
        while s < 1 and decimals < 8:
            s *= 10
            decimals += 1
        width = max(9, decimals + 4)
        lines: list[str] = []
        for i, row in enumerate(grid):
            axis = f"{mx:+.{decimals}f}" if i == 0 else (f"{mn:+.{decimals}f}" if i == rows - 1 else "")
            lines.append(f"{axis:>{width}} ┤{''.join(row)}")
        if x_labels is not None:
            left, right = x_labels
            span_pad = max(1, len(values) - len(left) - len(right))
            lines.append(" " * width + " └" + left + " " * span_pad + right)
        return lines

    def _render_spread_trend_panel(self, is_zh: bool) -> Panel:
        cols = max(20, min(200, self.dashboard_console.size.width - 14))
        long_vals, _lo, _hi = self._spread_trend_series(0, cols, time.monotonic())
        no_data = "无数据" if is_zh else "no data"
        long_label = "做多差价·近3天(整宽=3天)" if is_zh else "Long spread · 3d (full width = 3d)"

        now_wall = time.time()
        fmt = lambda t: datetime.fromtimestamp(t, CST_TZ).strftime("%m-%d %H:%M")
        x_labels = (fmt(now_wall - SPREAD_TREND_WINDOW_SECONDS), fmt(now_wall))

        grid = Table.grid()
        grid.add_column()
        chart = self._ascii_line_chart(long_vals, 7, x_labels=x_labels)
        for line in (chart or [f"  ({no_data})"]):
            grid.add_row(f"[green]{line}[/]")
        return Panel(grid, title=long_label, border_style="blue")

    def _refresh_spread_stats(self) -> None:
        """1Hz 计算并缓存 18 个窗口统计，渲染(4Hz)只读缓存，避免重复排序。"""
        cache: dict[str, float | None] = {}
        for secs, tag in ((5 * 60, "5m"), (30 * 60, "30m"), (60 * 60, "1h")):
            for long_side, side_key in ((True, "long"), (False, "short")):
                med, p90, p10 = self._window_stats(secs, long_side)
                cache[f"{side_key}_median_{tag}"] = med
                cache[f"{side_key}_p90_{tag}"] = p90
                cache[f"{side_key}_p10_{tag}"] = p10
        self._spread_stats_cache = cache

    def _record_hourly_spread(
        self,
        long_var_short_lighter_pct: Decimal | None,
        short_var_long_lighter_pct: Decimal | None,
    ) -> None:
        hour_key = int(time.time() // 3600)
        bucket = self.hourly_spread_buckets.get(hour_key)
        if bucket is None:
            bucket = {"long": [], "short": []}
            self.hourly_spread_buckets[hour_key] = bucket
        long_value = self._decimal_as_float(long_var_short_lighter_pct)
        short_value = self._decimal_as_float(short_var_long_lighter_pct)
        if long_value is not None:
            bucket["long"].append(long_value)
        if short_value is not None:
            bucket["short"].append(short_value)
        cutoff_key = hour_key - (HOURLY_HISTORY_HOURS - 1)
        for stale_key in [k for k in self.hourly_spread_buckets if k < cutoff_key]:
            del self.hourly_spread_buckets[stale_key]

    def _hourly_bucket_cells(self, values: list[float]) -> tuple[str, str, str]:
        if not values:
            return "-", "-", "-"
        return (
            self._fmt_median_pct(float(median(values))),
            self._fmt_median_pct(self._percentile(values, 90)),
            self._fmt_median_pct(self._percentile(values, 10)),
        )

    def _adaptive_row_limits(self) -> tuple[int, int]:
        """Return (hourly_rows, order_rows) that fit the current terminal height.

        Page 1 chrome (header + quote/spread tables + footer) takes a roughly
        fixed number of lines; the rest is given to the recent-orders table so a
        small window stays on one screen. The hourly history owns page 2, so it
        always renders its full window.
        """
        height = self.dashboard_console.size.height
        budget = height - DASHBOARD_FIXED_OVERHEAD_LINES
        order_rows = min(DASHBOARD_ORDERS, max(1, budget))
        # Hourly history has page 2 to itself, so always show the full window.
        return HOURLY_HISTORY_HOURS, order_rows

    def _record_unit_spread(self, record: OrderLifecycle) -> Decimal | None:
        """Direction-adjusted raw per-unit captured spread for a fully
        filled round (both legs). None if either leg is missing."""
        if record.var_fill_price is None or record.lighter_fill_price is None:
            return None
        lighter = record.lighter_fill_price
        side = record.side.strip().lower()
        if side == "buy":  # long Var / short Lighter: lighter - var
            return lighter - record.var_fill_price
        if side == "sell":  # short Var / long Lighter: var - lighter
            return record.var_fill_price - lighter
        return None

    def compute_pnl(self) -> tuple[Decimal, Decimal, int, Decimal, Decimal | None]:
        """FIFO-offset PnL (USDC) over fully-filled rounds. Caller holds _record_lock.

        Long rounds (Var buy) and short rounds (Var sell) offset each other by qty;
        an offset pair locks in both rounds' captured spreads as realized. Whatever
        stays unoffset is the live position.

        Returns (realized, unrealized, pending_rounds, position_qty, avg_open_pct):
        position_qty is signed (+ long Var / - short Var), avg_open_pct is the
        qty-weighted entry spread% of the still-open lots (the close reference).
        """
        realized = Decimal("0")
        open_lots: list[list[Any]] = []  # each: [side, remaining_qty, unit_spread, spread_pct]
        pending = 0
        for key in self.record_order:
            record = self.records.get(key)
            if record is None:
                continue
            unit = self._record_unit_spread(record)
            if unit is None:
                pending += 1
                continue
            _, pct = fill_diff_by_direction(
                record.side, record.var_fill_price, record.lighter_fill_price
            )
            side = record.side.strip().lower()
            qty = record.qty
            while qty > 0 and open_lots and open_lots[0][0] != side:
                lot = open_lots[0]
                matched = min(qty, lot[1])
                realized += lot[2] * matched + unit * matched
                lot[1] -= matched
                qty -= matched
                if lot[1] <= 0:
                    open_lots.pop(0)
            if qty > 0:
                open_lots.append([side, qty, unit, pct])
        unrealized = sum((lot[2] * lot[1] for lot in open_lots), Decimal("0"))
        open_qty = sum((lot[1] for lot in open_lots), Decimal("0"))
        open_side = open_lots[0][0] if open_lots else None
        position_qty = open_qty if open_side == "buy" else -open_qty
        avg_open_pct: Decimal | None = None
        if open_qty > 0:
            weighted = sum((lot[3] * lot[1] for lot in open_lots if lot[3] is not None), Decimal("0"))
            avg_open_pct = weighted / open_qty
        return realized, unrealized, pending, position_qty, avg_open_pct

    def _fmt_window_cell(
        self,
        median_v: float | None,
        p90_v: float | None,
        p10_v: float | None,
        labels: tuple[str, str, str],
    ) -> str:
        med_l, up_l, lo_l = labels
        return (
            f"{med_l} {self._fmt_median_pct(median_v)}\n"
            f"{up_l} {self._fmt_median_pct(p90_v)}\n"
            f"{lo_l} {self._fmt_median_pct(p10_v)}"
        )

    @staticmethod
    def _parse_dom_position_text(text: Any) -> Decimal | None:
        """解析页面"当前仓位"文本：'0.003 XAU'/'-0.01 XAU'→带符号；'-'/空→0(平仓)。"""
        if text is None:
            return None
        cleaned = str(text).strip()
        if not cleaned:
            return None
        match = re.search(r"-?\d+(?:\.\d+)?", cleaned)
        if match is None:
            # 没有数字：'-' 表示平仓(0)，其它无法识别返回 None。
            return Decimal("0") if cleaned.replace(" ", "") in {"-", "--", "—"} else None
        return to_decimal(match.group(0))

    async def _read_dom_position_qty(self) -> Decimal | None:
        """通过扩展抓取页面"当前仓位"DOM 作为后备仓位源。"""
        broker = self.browser_order_broker
        if broker is None or not broker.is_connected():
            return None
        try:
            result = await broker.read_position(timeout=DOM_POSITION_TIMEOUT_SECONDS)
        except Exception as exc:
            self.logger.debug("读取 DOM 仓位失败: %s", exc)
            return None
        if not isinstance(result, dict) or not result.get("ok") or not result.get("found"):
            return None
        qty = self._parse_dom_position_text(result.get("valueText"))
        if qty is not None:
            self._last_dom_position_qty = qty
        return qty

    async def _read_listener_position(self) -> Decimal | None:
        """从 listener 内存读真实带符号仓位（廉价，无往返）。无组合数据返回 None。"""
        try:
            state = await self.runtime.monitor.get_trading_state()
        except Exception:
            return None
        if not isinstance(state, dict) or not state.get("has_portfolio"):
            return None
        row = state.get("position_row")
        if isinstance(row, dict):
            live = to_decimal(row.get("qty"))
            if live is not None:
                return live
        # 有组合数据但当前资产无仓位 = 平仓(0)。
        return Decimal("0")

    async def _refresh_position_cache(self, *, allow_dom: bool = True) -> None:
        """刷新缓存仓位：listener 优先；allow_dom 时 listener 无数据再走 DOM 往返。"""
        pos = await self._read_listener_position()
        if pos is None and allow_dom:
            pos = await self._read_dom_position_qty()
        if pos is None:
            pos = self._last_dom_position_qty
        if pos is not None:
            self._cached_position_qty = pos

    async def _refresh_position_cache_after_fill(self, prev_qty: Decimal | None) -> None:
        """下单完成后刷新仓位：轮询真实仓位，等成交反映出来再放行下一步（有界）。"""
        deadline = time.monotonic() + POSITION_FILL_REFRESH_TIMEOUT_SECONDS
        pos: Decimal | None = None
        while time.monotonic() < deadline and not self.stop_flag:
            pos = await self._read_listener_position()
            if pos is None:
                pos = await self._read_dom_position_qty()
            if pos is not None and pos != prev_qty:
                self._cached_position_qty = pos
                return
            await asyncio.sleep(POSITION_FILL_REFRESH_POLL_SECONDS)
        # 超时：用最后读到的真实值兜底（即便未变化），避免缓存长期失真。
        if pos is not None:
            self._cached_position_qty = pos

    def _evaluate_gradient_signal(
        self,
        open_spread_pct: Decimal | None,
        close_spread_pct: Decimal | None,
        position_qty: Decimal,
    ) -> GradientSignal | None:
        signal = self.gradient_strategy.evaluate(open_spread_pct, close_spread_pct, position_qty)
        if signal is None:
            self._last_gradient_signal_sig = None
            self._pending_signal_sig = None
            self._pending_signal_count = 0
            return None
        signal_sig = signal.signature()
        if signal_sig == self._last_gradient_signal_sig or self._strategy_order_in_flight:
            return signal
        # 信号已达但被闸拦（已停止 / 两腿不平衡）→ 节流打日志，免得排查半天没线索。
        if not self._strategy_order_allowed():
            reason = "已停止" if self._strategy_halted else "两腿不平衡"
            block_sig = (reason, signal_sig)
            if block_sig != self._last_block_log_sig:
                self._last_block_log_sig = block_sig
                self.logger.warning(
                    "策略信号已达但未下单: 原因=%s halted=%s var仓=%s lighter仓=%s 容差=%s lighter_positions=%s",
                    reason,
                    self._strategy_halted,
                    decimal_to_str(self._cached_position_qty),
                    decimal_to_str(self._current_lighter_position()),
                    decimal_to_str(self._balance_tolerance()),
                    {k: decimal_to_str(v) for k, v in self._lighter_positions.items()},
                )
            return signal
        # 尖峰过滤（可选，默认关）：触发价差远超近期均值 → 瞬时噪音，不下单并清确认。
        if MAX_SPIKE_DEVIATION_PCT is not None:
            baseline = self._median_cross_spread(
                SPIKE_BASELINE_WINDOW_SECONDS, long_side=(signal.section == StrategySection.OPEN)
            )
            if baseline is not None and float(signal.spread_pct) - baseline > float(MAX_SPIKE_DEVIATION_PCT):
                self._pending_signal_sig = None
                self._pending_signal_count = 0
                spike_sig = ("spike", signal_sig)
                if spike_sig != self._last_block_log_sig:
                    self._last_block_log_sig = spike_sig
                    self.logger.warning(
                        "信号判为尖峰噪音(未下单): 触发价差=%s 近期均值=%.4f%% 允许偏离=%s",
                        decimal_to_str(signal.spread_pct),
                        baseline,
                        decimal_to_str(MAX_SPIKE_DEVIATION_PCT),
                    )
                return signal
        # 噪音过滤：同一信号需连续出现 SIGNAL_CONFIRM_COUNT 次才下单，单次(价差瞬时穿越)视为噪音。
        if signal_sig == self._pending_signal_sig:
            self._pending_signal_count += 1
        else:
            self._pending_signal_sig = signal_sig
            self._pending_signal_count = 1
            self._pending_signal_started_at = time.monotonic()
        if self._pending_signal_count < SIGNAL_CONFIRM_COUNT:
            return signal
        if self._pending_signal_started_at is not None:
            self._stat_signal_confirm.add((time.monotonic() - self._pending_signal_started_at) * 1000)
        self.logger.info(
            "GRADIENT signal: action=%s section=%s spread=%s threshold=%s current_qty=%s target_qty=%s delta_qty=%s",
            signal.action,
            signal.section.value,
            decimal_to_str(signal.spread_pct),
            decimal_to_str(signal.threshold_pct),
            decimal_to_str(signal.current_qty),
            decimal_to_str(signal.target_qty),
            decimal_to_str(signal.delta_qty),
        )
        record = self._handle_new_gradient_signal(signal)
        if record is not None:
            self._strategy_order_in_flight = True
            self._last_gradient_signal_sig = signal_sig
            self._stat_fired += 1
        return signal

    @staticmethod
    def _epoch_ms(ts: Any) -> float | None:
        """尽力把源时间戳转 epoch 毫秒（数字按秒/毫秒判断，或 ISO）；失败返回 None。"""
        if ts is None:
            return None
        if isinstance(ts, (int, float)):
            v = float(ts)
            return v if v > 1e12 else v * 1000.0
        if isinstance(ts, str):
            s = ts.strip()
            try:
                v = float(s)
                return v if v > 1e12 else v * 1000.0
            except ValueError:
                pass
            try:
                return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp() * 1000.0
            except ValueError:
                return None
        return None

    def _feed_quote_comparator(self, source: str, bid: Any, ask: Any, source_ts: Any, browser_wall_ms: float | None = None) -> None:
        b = to_decimal(bid)
        a = to_decimal(ask)
        if b is None or a is None:
            return
        acquire_ms = time.monotonic() * 1000.0            # 获取时间：本地单调钟(跨源可比)
        receive_wall_ms = time.time() * 1000.0            # 本地收到墙钟
        change_ms = self._epoch_ms(source_ts)
        if change_ms is None:
            change_ms = receive_wall_ms                   # 兜底用本地墙钟(单位一致)
        self._last_quote_wall[source] = receive_wall_ms / 1000.0  # 最近获取墙钟(秒，用于显示)
        # 传输延迟：本地收到 − 浏览器事件墙钟（同机时钟才准）。
        if browser_wall_ms is not None:
            self._last_quote_delay[source] = receive_wall_ms - browser_wall_ms
        self.quote_comparator.update(source, float(b), float(a), change_ms, acquire_ms)

    def _on_api_quote(self, asset: str, bid: Any, ask: Any, source_ts: Any, captured_at: Any = None) -> None:
        # 只喂当前活跃标的，避免与 DOM(仅当前页)错配。
        if self.variational_ticker and str(asset).strip().upper() != self.variational_ticker.strip().upper():
            return
        self._feed_quote_comparator("api", bid, ask, source_ts, browser_wall_ms=self._epoch_ms(captured_at))

    def _on_dom_quote(self, payload: dict[str, Any]) -> None:
        ts = payload.get("ts") or payload.get("capturedAtMs") or payload.get("timestamp")
        self._feed_quote_comparator("dom", payload.get("bid"), payload.get("ask"), ts, browser_wall_ms=self._epoch_ms(ts))
        self._last_dom_bid = to_decimal(payload.get("bid"))
        self._last_dom_ask = to_decimal(payload.get("ask"))
        self._last_dom_at = time.monotonic()

    async def _compute_signal_spreads(self) -> tuple[Decimal | None, Decimal | None]:
        """实时算两侧触发价差（Var 报价 × Lighter 深度 VWAP），不读仓位。"""
        var_bid, var_ask, _ = await self.get_variational_best_bid_ask(self.variational_ticker)
        lighter_bid, lighter_ask = await self.get_lighter_best_bid_ask()
        depth_notional = Decimal(str(self.args.depth_notional))
        vwap_bid, vwap_ask = await self.get_lighter_depth_quote(depth_notional)
        sig_bid = vwap_bid if vwap_bid is not None else lighter_bid
        sig_ask = vwap_ask if vwap_ask is not None else lighter_ask
        # 暂存两腿触发价，供下单时记录、算分腿滑点。
        self._last_leg_prices = {
            "var_bid": var_bid,
            "var_ask": var_ask,
            "lighter_bid": sig_bid,
            "lighter_ask": sig_ask,
        }
        return cross_spread_percentages(var_bid, var_ask, sig_bid, sig_ask)

    async def strategy_loop(self) -> None:
        """事件驱动评估：盘口刷新唤醒，外加 200ms 兜底覆盖 Var 报价变化。"""
        await self._refresh_position_cache()
        while not self.stop_flag:
            try:
                await asyncio.wait_for(self._strategy_wake.wait(), timeout=STRATEGY_EVAL_FALLBACK_SECONDS)
            except asyncio.TimeoutError:
                pass
            self._strategy_wake.clear()
            try:
                await self._run_strategy_evaluation()
            except Exception:
                self.logger.exception("策略评估异常")

    async def _run_strategy_evaluation(self) -> None:
        """单次评估：用缓存仓位（不读），触发后由下单链路刷新仓位。"""
        pos = self._cached_position_qty
        if pos is None:
            self._latest_gradient_signal = None
            return
        open_pct, close_pct = await self._compute_signal_spreads()
        self._latest_gradient_signal = self._evaluate_gradient_signal(open_pct, close_pct, pos)

    def _record_dry_run_trigger_spread(self, side: str, spread_pct: Decimal) -> None:
        logger = getattr(self, "logger", None)
        if logger is not None:
            logger.info(
                "Dry-run trigger spread not bound to orders: side=%s spread=%s",
                side,
                decimal_to_str(spread_pct),
            )

    def _record_live_trigger_spread(self, side: str, spread_pct: Decimal) -> None:
        self._drop_expired_pending_trigger_spreads()
        self._pending_trigger_spreads.append(
            PendingTriggerSpread(
                side=side.strip().lower(),
                spread_pct=spread_pct,
                created_at_monotonic=time.monotonic(),
            )
        )
        if len(self._pending_trigger_spreads) > 100:
            del self._pending_trigger_spreads[:-100]

    def _consume_pending_trigger_spread(self, side: str) -> Decimal | None:
        self._drop_expired_pending_trigger_spreads()
        side_n = side.strip().lower()
        for index, pending in enumerate(self._pending_trigger_spreads):
            if pending.side == side_n:
                return self._pending_trigger_spreads.pop(index).spread_pct
        return None

    def _drop_expired_pending_trigger_spreads(self) -> None:
        cutoff = time.monotonic() - PENDING_TRIGGER_SPREAD_TTL_SECONDS
        self._pending_trigger_spreads = [
            pending for pending in self._pending_trigger_spreads if pending.created_at_monotonic >= cutoff
        ]

    def _schedule_prepare_browser_order(self) -> None:
        qty = self.gradient_strategy.single_order_qty
        if qty <= 0:
            return
        side = self._prepared_order_side
        prepare_sig = (side, format(qty, "f"))
        if prepare_sig == self._last_prepared_order_sig:
            return
        self._browser_order_queue.submit(BrowserOrderCommand(side=side, qty=qty, dry_run=True, prepare_only=True))

    async def _send_browser_order_task(self, command: BrowserOrderCommand) -> None:
        if command.prepare_only:
            await self._send_prepare_browser_order(command)
            return
        await self._send_browser_order(command)

    @staticmethod
    def _browser_order_timeout(command: BrowserOrderCommand) -> float:
        # broker 等待 = 页面内回执上限 + 余量（选方向/输入/提交轮询），保证不早于页面返回。
        return command.timeout_ms / 1000.0 + BROWSER_ORDER_BROKER_MARGIN_SECONDS

    async def _send_prepare_browser_order(self, command: BrowserOrderCommand) -> None:
        try:
            result = await self.browser_order_broker.place_order(command, timeout=self._browser_order_timeout(command))
        except Exception as exc:
            self.logger.warning(
                "Browser order prepare failed: side=%s qty=%s error=%s",
                command.side,
                decimal_to_str(command.qty),
                exc,
            )
            return
        self.logger.info(
            "Browser order prepare result: side=%s qty=%s ok=%s blocked=%s error=%s",
            command.side,
            decimal_to_str(command.qty),
            result.get("ok"),
            result.get("blockedReason"),
            result.get("error"),
        )
        if result.get("ok"):
            side = "sell" if command.side.strip().lower() == "sell" else "buy"
            self._last_prepared_order_sig = (side, format(command.qty, "f"))
            self._prepared_order_side = side

    async def run_browser_smoke_test(self) -> None:
        qty = Decimal(str(self.args.browser_smoke_qty))
        steps = [
            ("buy_submit", BrowserOrderCommand(side="buy", qty=qty, dry_run=False, wait_after_click_ms=0)),
            ("sell_submit", BrowserOrderCommand(side="sell", qty=qty, dry_run=False, wait_after_click_ms=0)),
            ("restore_buy_prepare", BrowserOrderCommand(side="buy", qty=qty, dry_run=True, prepare_only=True)),
        ]
        total_steps = 5 + len(steps)
        await self._log_browser_smoke_progress(
            1,
            total_steps,
            "startup",
            "start",
            "打印启动指引并启动 Python 运行时",
        )
        self.print_startup_next_steps()
        await self.runtime.start()
        await self._log_browser_smoke_progress(1, total_steps, "startup", "done", "Python 运行时已启动")
        await self._log_browser_smoke_progress(
            2,
            total_steps,
            "broker_server",
            "start",
            f"启动浏览器下单 broker：ws://{FORWARDER_HOST}:{BROWSER_ORDER_BROKER_PORT}",
        )
        self.browser_order_server = await run_browser_order_broker(
            FORWARDER_HOST,
            BROWSER_ORDER_BROKER_PORT,
            self.browser_order_broker,
        )
        await self._log_browser_smoke_progress(2, total_steps, "broker_server", "done", "broker 已开始监听")
        self.logger.info(
            "Browser smoke test waiting for extension broker on ws://%s:%s",
            FORWARDER_HOST,
            BROWSER_ORDER_BROKER_PORT,
        )
        await self._log_browser_smoke_progress(
            3,
            total_steps,
            "extension_connect",
            "start",
            "等待 Chrome 扩展连接 Python",
        )
        await self._wait_for_browser_order_broker_connected(timeout=120.0)
        await self._log_browser_smoke_progress(3, total_steps, "extension_connect", "done", "Chrome 扩展已连接")
        self.logger.info("Browser smoke test broker connected; waiting 3.0s before first browser command")
        await self._log_browser_smoke_progress(
            4,
            total_steps,
            "post_connect_wait",
            "start",
            "扩展连接后等待 3.0 秒，再发送第一条浏览器命令",
        )
        await asyncio.sleep(3.0)
        await self._log_browser_smoke_progress(4, total_steps, "post_connect_wait", "done", "连接后等待完成")
        for offset, (step_name, command) in enumerate(steps, start=5):
            await self._run_browser_smoke_step(step_name, command, step_no=offset, total_steps=total_steps)
        await self._log_browser_smoke_progress(total_steps, total_steps, "complete", "done", "浏览器 smoke test 完成")
        self.logger.info("Browser smoke test completed")

    async def _wait_for_browser_order_broker_connected(self, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.browser_order_broker.is_connected():
                self.logger.info("Browser order broker connected")
                return
            await asyncio.sleep(0.25)
        raise RuntimeError("Timed out waiting for Chrome extension browser order broker connection")

    async def _run_browser_smoke_step(
        self,
        step_name: str,
        command: BrowserOrderCommand,
        step_no: int | None = None,
        total_steps: int | None = None,
    ) -> None:
        started = time.perf_counter()
        if step_no is not None and total_steps is not None:
            await self._log_browser_smoke_progress(
                step_no,
                total_steps,
                step_name,
                "start",
                f"方向={command.side} 数量={decimal_to_str(command.qty)} dry_run={command.dry_run} prepare_only={command.prepare_only}",
                command,
            )
        self.logger.info(
            "Browser smoke step start: step=%s side=%s qty=%s dry_run=%s prepare_only=%s",
            step_name,
            command.side,
            decimal_to_str(command.qty),
            command.dry_run,
            command.prepare_only,
        )
        try:
            if step_no is not None and total_steps is not None:
                await self._log_browser_smoke_progress(
                    step_no,
                    total_steps,
                    step_name,
                    "waiting_extension_result",
                    "浏览器命令已发送，等待 Chrome 扩展返回结果",
                    command,
                )
            result = await self.browser_order_broker.place_order(command, timeout=30.0)
            elapsed_ms = (time.perf_counter() - started) * 1000
            summary = self._browser_order_result_summary(result)
            self.logger.info(
                "Browser smoke step result: step=%s elapsed_ms=%.1f ok=%s blocked=%s error=%s summary=%s",
                step_name,
                elapsed_ms,
                result.get("ok"),
                result.get("blockedReason"),
                result.get("error"),
                summary,
            )
            await self._append_browser_smoke_log(step_name, command, result, elapsed_ms)
            if not result.get("ok"):
                diagnostic = self._browser_order_failure_hint(result)
                if diagnostic:
                    prefix = f"[{step_no}/{total_steps}] " if step_no is not None and total_steps is not None else ""
                    self.dashboard_console.print(f"{prefix}{step_name} diagnostic: {diagnostic}")
                    self.logger.warning("Browser smoke diagnostic hint: %s", diagnostic)
                if step_no is not None and total_steps is not None:
                    await self._log_browser_smoke_progress(
                        step_no,
                        total_steps,
                        step_name,
                        "failed",
                        f"错误={result.get('error') or result.get('blockedReason') or result}; {diagnostic}".rstrip("; "),
                        command,
                    )
                raise RuntimeError(f"{step_name} failed: {result.get('error') or result.get('blockedReason') or result}")
            if step_no is not None and total_steps is not None:
                await self._log_browser_smoke_progress(
                    step_no,
                    total_steps,
                    step_name,
                    "done",
                    f"耗时={elapsed_ms:.1f}ms",
                    command,
                )
        except Exception as exc:
            elapsed_ms = (time.perf_counter() - started) * 1000
            self.logger.warning(
                "Browser smoke step failed: step=%s elapsed_ms=%.1f error=%s",
                step_name,
                elapsed_ms,
                exc,
            )
            await self._append_browser_smoke_log(step_name, command, {"ok": False, "error": str(exc)}, elapsed_ms)
            if step_no is not None and total_steps is not None:
                await self._log_browser_smoke_progress(
                    step_no,
                    total_steps,
                    step_name,
                    "failed",
                    f"耗时={elapsed_ms:.1f}ms 错误={exc}",
                    command,
                )
            raise

    @staticmethod
    def _browser_order_failure_hint(result: dict[str, Any]) -> str:
        error = str(result.get("error") or result.get("blockedReason") or "")
        if error != "submit_button_disabled":
            return ""
        snapshot = (
            result.get("afterDisabledRetry")
            if isinstance(result.get("afterDisabledRetry"), dict)
            else result.get("after") if isinstance(result.get("after"), dict) else result.get("before")
        )
        if not isinstance(snapshot, dict):
            return "提交按钮仍是灰色，但没有拿到 DOM 快照"
        meta = snapshot.get("submitButtonMeta") if isinstance(snapshot.get("submitButtonMeta"), dict) else {}
        parent_text = str(meta.get("parentText") or "")
        submit_text = str(snapshot.get("submitButtonText") or "")
        qty_value = str(snapshot.get("qtyInputValue") or "")
        hints = [f"按钮='{submit_text}'", f"数量='{qty_value}'"]
        if isinstance(result.get("afterDisabledRetry"), dict):
            hints.append("已在 disabled 后额外等待 3 秒并重新检查")
        if "仅减仓" in parent_text and "当前仓位 -" in parent_text:
            hints.append("疑似仅减仓开启且当前无仓位，Var 页面禁止开仓")
        elif "仅减仓" in parent_text:
            hints.append("页面出现仅减仓状态，请确认 Reduce Only 是否关闭")
        return "; ".join(hints)

    async def _log_browser_smoke_progress(
        self,
        step_no: int,
        total_steps: int,
        step_name: str,
        status: str,
        detail: str = "",
        command: BrowserOrderCommand | None = None,
    ) -> None:
        status_zh = {
            "start": "开始",
            "done": "完成",
            "failed": "失败",
            "waiting_extension_result": "等待扩展返回",
        }.get(status, status)
        message = f"[{step_no}/{total_steps}] {step_name} {status_zh}: {detail}"
        self.dashboard_console.print(message)
        self.logger.info(
            "Browser smoke progress [%s/%s] step=%s status=%s detail=%s",
            step_no,
            total_steps,
            step_name,
            status,
            detail,
        )
        row = {
            "event": "browser_smoke_progress",
            "logged_at": utc_now(),
            "step_no": step_no,
            "total_steps": total_steps,
            "step": step_name,
            "status": status,
            "detail": detail,
        }
        if command is not None:
            row["command"] = command.to_payload()
        line = json.dumps(row, ensure_ascii=False, default=str) + "\n"
        async with self._order_write_lock:
            await asyncio.to_thread(BROWSER_SMOKE_TEST_FILE.parent.mkdir, parents=True, exist_ok=True)
            await asyncio.to_thread(self._append_line, BROWSER_SMOKE_TEST_FILE, line)

    async def _append_browser_smoke_log(
        self,
        step_name: str,
        command: BrowserOrderCommand,
        result: dict[str, Any],
        elapsed_ms: float,
    ) -> None:
        row = {
            "event": "browser_smoke_step",
            "logged_at": utc_now(),
            "step": step_name,
            "elapsed_ms": elapsed_ms,
            "command": command.to_payload(),
            "result": result,
            "summary": json.loads(self._browser_order_result_summary(result)),
        }
        line = json.dumps(row, ensure_ascii=False, default=str) + "\n"
        async with self._order_write_lock:
            await asyncio.to_thread(BROWSER_SMOKE_TEST_FILE.parent.mkdir, parents=True, exist_ok=True)
            await asyncio.to_thread(self._append_line, BROWSER_SMOKE_TEST_FILE, line)

    async def _send_browser_order(self, command: BrowserOrderCommand) -> None:
        side = "sell" if command.side.strip().lower() == "sell" else "buy"
        is_live_strategy_order = not command.prepare_only and not command.dry_run
        try:
            try:
                result = await self.browser_order_broker.place_order(command, timeout=self._browser_order_timeout(command))
                if is_live_strategy_order:
                    # 方向切换耗时（页面已在目标方向则为0）。
                    timing = result.get("timing") if isinstance(result.get("timing"), dict) else {}
                    switch_ms = timing.get("sideClickDuration")
                    self._stat_var_side_switch.add(float(switch_ms) if switch_ms is not None else 0.0)
            except Exception as exc:
                await self._record_var_order_error(command.trade_key, str(exc))
                self.logger.warning(
                    "Browser order failed: side=%s qty=%s dry_run=%s error=%s",
                    side,
                    decimal_to_str(command.qty),
                    command.dry_run,
                    exc,
                )
                return
            self.logger.info(
                "Browser order result: side=%s qty=%s dry_run=%s ok=%s blocked=%s error=%s",
                side,
                decimal_to_str(command.qty),
                command.dry_run,
                result.get("ok"),
                result.get("blockedReason"),
                result.get("error"),
            )
            if not result.get("ok"):
                self.logger.warning("Browser order diagnostic: %s", self._browser_order_result_summary(result))
                await self._record_var_order_error(command.trade_key, str(result.get("error") or result.get("blockedReason") or "browser_order_failed"))
            if result.get("ok"):
                await self._record_var_submit_result(command.trade_key, result)
                self._prepared_order_side = side
                self._last_prepared_order_sig = (side, format(command.qty, "f"))
                # 下单后确认：Var 单边仅刷新仓位；对冲模式在 10s 内确认两腿平衡，否则硬停。
                if is_live_strategy_order:
                    await self._confirm_hedge_or_halt(self._cached_position_qty)
        finally:
            # 无论成败都释放在途单闸并唤醒评估循环，避免卡死步进。
            if is_live_strategy_order:
                self._strategy_order_in_flight = False
                self._strategy_wake.set()

    async def _record_var_submit_result(self, trade_key: str | None, result: dict[str, Any]) -> None:
        if not trade_key:
            return
        order_response = result.get("orderResponse")
        order_json = order_response.get("json") if isinstance(order_response, dict) and isinstance(order_response.get("json"), dict) else {}
        order_id = (
            order_json.get("id")
            or order_json.get("order_id")
            or order_json.get("orderId")
            or order_json.get("trade_id")
            or order_json.get("tradeId")
        )
        submit_status = None
        if isinstance(order_response, dict) and order_response.get("status") is not None:
            with contextlib.suppress(TypeError, ValueError):
                submit_status = int(order_response.get("status"))
        async with self._record_lock:
            record = self.records.get(trade_key)
            if record is None:
                return
            record.var_submit_ok = bool(result.get("ok"))
            record.var_submit_error = str(result.get("error") or result.get("blockedReason") or "") or None
            record.var_submit_click_started_at = result.get("clickStartedAt")
            record.var_submit_click_started_at_ms = result.get("clickStartedAtMs")
            record.var_submit_timing = result.get("timing") if isinstance(result.get("timing"), dict) else None
            record.var_submit_quote_snapshot = result.get("lastQuote") if isinstance(result.get("lastQuote"), dict) else None
            record.var_submit_order_response = order_response if isinstance(order_response, dict) else None
            record.var_submit_status = submit_status
            record.var_submit_order_id = str(order_id) if order_id else None
            payload = record.to_payload()
        await self.append_order_log("variational_order_submitted", payload)

    @staticmethod
    def _browser_order_result_summary(result: dict[str, Any]) -> str:
        def snapshot_summary(snapshot: Any) -> dict[str, Any]:
            if not isinstance(snapshot, dict):
                return {}
            return {
                "url": snapshot.get("url"),
                "title": snapshot.get("title"),
                "readyState": snapshot.get("readyState"),
                "hasBody": snapshot.get("hasBody"),
                "frameElement": snapshot.get("frameElement"),
                "activeSide": snapshot.get("activeSide"),
                "sideAlreadyActive": snapshot.get("sideAlreadyActive"),
                "sideButtonRect": snapshot.get("sideButtonRect"),
                "buyToggleMeta": snapshot.get("buyToggleMeta"),
                "sellToggleMeta": snapshot.get("sellToggleMeta"),
                "qtyInputValue": snapshot.get("qtyInputValue"),
                "submitButtonText": snapshot.get("submitButtonText"),
                "submitButtonDisabled": snapshot.get("submitButtonDisabled"),
                "submitButtonMeta": snapshot.get("submitButtonMeta"),
                "visibleButtons": snapshot.get("visibleButtons"),
                "visibleInputs": snapshot.get("visibleInputs"),
            }

        summary = {
            "error": result.get("error"),
            "blockedReason": result.get("blockedReason"),
            "side": result.get("side"),
            "qty": result.get("qty"),
            "locate": snapshot_summary(result.get("locate")),
            "before": snapshot_summary(result.get("before")),
            "after": snapshot_summary(result.get("after")),
            "afterDisabledRetry": snapshot_summary(result.get("afterDisabledRetry")),
            "clickResult": result.get("clickResult"),
        }
        return json.dumps(summary, ensure_ascii=False, default=str)

    def _render_stats_panel(self, is_zh: bool) -> Panel:
        """第2页统计面板：本地会话累计的延迟 / 分腿滑点 / 信号确认时长 / 下单成交数。"""
        def fmt_ms(stat: RunningStat) -> str:
            avg = stat.avg()
            avg_t = f"{avg:.0f}ms" if avg is not None else "-"
            last_t = f"{stat.last:.0f}ms" if stat.last is not None else "-"
            return f"均 {avg_t} | 最近 {last_t} | n={stat.n}"

        def fmt_pct(stat: RunningStat) -> str:
            avg = stat.avg()
            avg_t = f"{avg:+.3f}%" if avg is not None else "-"
            last_t = f"{stat.last:+.3f}%" if stat.last is not None else "-"
            return f"均 {avg_t} | 最近 {last_t} | n={stat.n}"

        grid = Table.grid(expand=True)
        grid.add_column(ratio=1)
        if is_zh:
            grid.add_row(f"策略下单: {self._stat_fired}   两腿成交: {self._stat_both_filled}")
            grid.add_row(f"信号确认时长: {fmt_ms(self._stat_signal_confirm)}")
            grid.add_row(f"Var 延迟(信号→成交): {fmt_ms(self._stat_var_latency)}")
            grid.add_row(f"  其中方向切换: {fmt_ms(self._stat_var_side_switch)}")
            grid.add_row(f"Lighter 延迟(信号→WS成交): {fmt_ms(self._stat_lighter_latency)}")
            grid.add_row(f"V 滑点·api口径(有利+): {fmt_pct(self._stat_var_slip)}")
            grid.add_row(f"V 滑点·dom口径(有利+): {fmt_pct(self._stat_var_slip_dom)}")
            grid.add_row(f"L 滑点(有利+): {fmt_pct(self._stat_lighter_slip)}")
            grid.add_row(self._fmt_quote_compare(is_zh))
            title = "统计（本地会话）"
        else:
            grid.add_row(f"orders: {self._stat_fired}   both-filled: {self._stat_both_filled}")
            grid.add_row(f"signal confirm: {fmt_ms(self._stat_signal_confirm)}")
            grid.add_row(f"Var latency(signal->fill): {fmt_ms(self._stat_var_latency)}")
            grid.add_row(f"  side switch: {fmt_ms(self._stat_var_side_switch)}")
            grid.add_row(f"Lighter latency(signal->wsfill): {fmt_ms(self._stat_lighter_latency)}")
            grid.add_row(f"V slippage/api(+good): {fmt_pct(self._stat_var_slip)}")
            grid.add_row(f"V slippage/dom(+good): {fmt_pct(self._stat_var_slip_dom)}")
            grid.add_row(f"L slippage(+good): {fmt_pct(self._stat_lighter_slip)}")
            grid.add_row(self._fmt_quote_compare(is_zh))
            title = "Stats (session)"
        return Panel(grid, title=title, border_style="magenta")

    @staticmethod
    def _fmt_wall_ms(wall: float | None) -> str:
        if wall is None:
            return "-"
        return datetime.fromtimestamp(wall, tz=CST_TZ).strftime("%H:%M:%S.%f")[:-3]  # 精确到ms

    def _fmt_quote_compare(self, is_zh: bool) -> str:
        s = self.quote_comparator.snapshot()
        tr = s["transitions"]
        aleader = max(s["acquire_lead_counts"], key=s["acquire_lead_counts"].get) if s["acquire_lead_counts"] else "-"
        aavg = s["acquire_lead_avg_ms"]
        aavg_t = f"{aavg:.0f}ms" if aavg is not None else "-"
        dv = s["divergences"]
        def with_delay(source: str) -> str:
            t = self._fmt_wall_ms(self._last_quote_wall.get(source))
            d = self._last_quote_delay.get(source)
            return f"{t}(+{d:.0f}ms)" if d is not None else t
        api_t = with_delay("api")
        dom_t = with_delay("dom")
        if is_zh:
            return (
                f"报价对比 变动次数 api={tr.get('api', 0)}/dom={tr.get('dom', 0)} | 匹配={s['matched']}\n"
                f"  最近获取(+传输延迟) api={api_t} dom={dom_t}\n"
                f"  获取领先: {aleader} 均{aavg_t} | 背离 api={dv.get('api', 0)}/dom={dv.get('dom', 0)}"
            )
        return (
            f"quote-cmp transitions api={tr.get('api', 0)}/dom={tr.get('dom', 0)} | matched={s['matched']}\n"
            f"  last-recv(+delay) api={api_t} dom={dom_t}\n"
            f"  acquire-lead: {aleader} avg {aavg_t} | divergence api={dv.get('api', 0)}/dom={dv.get('dom', 0)}"
        )

    def _render_strategy_panel(
        self,
        open_spread_pct: Decimal | None,
        close_spread_pct: Decimal | None,
        position_qty: Decimal,
        signal: GradientSignal | None,
    ) -> Panel:
        strategy_table = Table.grid(expand=True)
        strategy_table.add_column(ratio=1)

        signal_text = "无信号"
        if signal is not None:
            action_text = "开仓" if signal.action == "open" else "清仓"
            # 显示"这一笔实际会下的量"（单次下单量与剩余的较小值，对齐 lot），
            # 剩余总差额单列，避免与单次下单量混淆。
            order_qty = self._quantize_to_lighter_lot(
                min(self.gradient_strategy.single_order_qty, signal.delta_qty)
            )
            signal_text = (
                f"[bold yellow]{action_text} {order_qty:f} {self.ticker}[/] "
                f"目标 {signal.target_qty:f} | 当前 {signal.current_qty:f} | "
                f"剩余 {signal.delta_qty:f} | 触发 {signal.threshold_pct:f}%"
            )
        strategy_table.add_row(self._strategy_enabled_row_text())
        strategy_table.add_row(self._strategy_single_order_qty_row_text())
        strategy_table.add_row(f"当前净仓位: {position_qty:f} {self.ticker} | 信号: {signal_text}")
        strategy_table.add_row("")
        strategy_table.add_row(
            "开仓区: 信号源 做多 Var / 做空 Lighter | "
            f"当前价差: {self._fmt_pct(open_spread_pct)}"
        )
        for index, _row in enumerate(self.gradient_strategy.open_rows):
            strategy_table.add_row(self._strategy_row_text(StrategySection.OPEN, index))
        strategy_table.add_row("")
        strategy_table.add_row(
            "清仓区: 信号源 做空 Var / 做多 Lighter | "
            f"当前价差: {self._fmt_pct(close_spread_pct)}"
        )
        for index, _row in enumerate(self.gradient_strategy.close_rows):
            strategy_table.add_row(self._strategy_row_text(StrategySection.CLOSE, index))
        strategy_table.add_row("")
        strategy_table.add_row("按键: ↑↓移动  ←→切字段  数字/.编辑  +/-增删梯度  Enter确认/启动  Esc取消  Tab切页  q退出")
        return Panel(strategy_table, title="触发策略", border_style="magenta")

    def _strategy_enabled_row_text(self) -> str:
        selected = self.gradient_strategy.enabled_selected()
        cursor = ">" if selected else " "
        icon = "[OK]" if self.gradient_strategy.enabled else "[ ]"
        status_text = "启动触发策略"
        row_text = f"{cursor} {icon} {status_text}"
        if selected:
            return f"[reverse]{row_text}[/]"
        return row_text

    def _strategy_single_order_qty_row_text(self) -> str:
        selected = self.gradient_strategy.order_size_selected()
        cursor = ">" if selected else " "
        qty_text = self.gradient_strategy.display_single_order_qty()
        row_text = f"{cursor} 单次下单数量: {qty_text} {self.ticker}"
        if selected:
            return f"[reverse]{row_text}[/]"
        return row_text

    def _strategy_row_text(self, section: StrategySection, index: int) -> str:
        selected = self.gradient_strategy.selected(section, index)
        cursor = ">" if selected else " "
        threshold_text = self.gradient_strategy.display_value(section, index, EditableField.THRESHOLD)
        qty_text = self.gradient_strategy.display_value(section, index, EditableField.QUANTITY)
        threshold_cell = f"价差 {threshold_text}%"
        qty_cell = f"仓位 {qty_text} {self.ticker}"
        if selected and self.gradient_strategy.cursor_field == EditableField.THRESHOLD:
            threshold_cell = f"[black on yellow]{threshold_cell}[/]"
        elif selected:
            threshold_cell = f"[bold]{threshold_cell}[/]"
        if selected and self.gradient_strategy.cursor_field == EditableField.QUANTITY:
            qty_cell = f"[black on yellow]{qty_cell}[/]"
        elif selected:
            qty_cell = f"[bold]{qty_cell}[/]"
        # ✅ 表示该行已完整输入（价差+仓位都填了）→ 会参与触发；空白=尚未填全。
        complete = self.gradient_strategy.rows_for(section)[index].is_complete()
        mark = "✅" if complete else "  "
        row_text = f"{cursor} {index + 1}. {mark} {threshold_cell} -> {qty_cell}"
        if selected:
            return f"[reverse]{row_text}[/]"
        return row_text

    async def render_dashboard(self) -> Group:
        var_bid, var_ask, quote_asset = await self.get_variational_best_bid_ask(self.variational_ticker)
        lighter_bid, lighter_ask = await self.get_lighter_best_bid_ask()
        var_book_spread = spread_value(var_bid, var_ask)
        var_book_spread_pct = book_spread_percent(var_bid, var_ask)
        lighter_book_spread_pct = book_spread_percent(lighter_bid, lighter_ask)
        spread_color_baseline: Decimal | None = None
        if var_book_spread_pct is not None and lighter_book_spread_pct is not None:
            spread_color_baseline = (var_book_spread_pct + lighter_book_spread_pct) / Decimal("2")

        # Depth-aware signal: use the raw VWAP to fill --depth-notional along the
        # Lighter book (not just top-of-book). USDC/USDT basis is displayed as a
        # separate reference signal and does not rewrite the trigger spread.
        depth_notional = Decimal(str(self.args.depth_notional))
        vwap_bid, vwap_ask = await self.get_lighter_depth_quote(depth_notional)
        sig_bid = vwap_bid if vwap_bid is not None else lighter_bid
        sig_ask = vwap_ask if vwap_ask is not None else lighter_ask
        long_var_short_lighter_pct, short_var_long_lighter_pct = cross_spread_percentages(
            var_bid,
            var_ask,
            sig_bid,
            sig_ask,
        )
        # 渲染回到 1s，每帧采样一次历史并刷新窗口统计缓存（单次过滤算 median/p90/p10）。
        self._record_cross_spreads(
            long_var_short_lighter_pct,
            short_var_long_lighter_pct,
        )
        self._record_hourly_spread(
            long_var_short_lighter_pct,
            short_var_long_lighter_pct,
        )
        self._refresh_spread_stats()
        self._sample_spread_trend(
            long_var_short_lighter_pct,
            short_var_long_lighter_pct,
        )

        st = self._spread_stats_cache  # 1Hz 缓存，避免 4Hz 重复排序
        long_pct_median_5m = st.get("long_median_5m")
        long_pct_median_30m = st.get("long_median_30m")
        long_pct_median_1h = st.get("long_median_1h")
        short_pct_median_5m = st.get("short_median_5m")
        short_pct_median_30m = st.get("short_median_30m")
        short_pct_median_1h = st.get("short_median_1h")

        long_p90_5m = st.get("long_p90_5m")
        long_p10_5m = st.get("long_p10_5m")
        long_p90_30m = st.get("long_p90_30m")
        long_p10_30m = st.get("long_p10_30m")
        long_p90_1h = st.get("long_p90_1h")
        long_p10_1h = st.get("long_p10_1h")
        short_p90_5m = st.get("short_p90_5m")
        short_p10_5m = st.get("short_p10_5m")
        short_p90_30m = st.get("short_p90_30m")
        short_p10_30m = st.get("short_p10_30m")
        short_p90_1h = st.get("short_p90_1h")
        short_p10_1h = st.get("short_p10_1h")

        hourly_row_limit, order_row_limit = self._adaptive_row_limits()
        async with self._record_lock:
            recent_keys = list(self.record_order)[-order_row_limit:]
            rows = [self.records[key] for key in reversed(recent_keys) if key in self.records]
            realized_pnl, unrealized_pnl, _pnl_pending, position_qty, avg_open_pct = self.compute_pnl()

        # 廉价兜底刷新缓存仓位（仅 listener，无 DOM 往返），防外部手动改仓。
        await self._refresh_position_cache(allow_dom=False)
        display_position_qty = self._cached_position_qty if self._cached_position_qty is not None else position_qty
        self.quote_comparator.tick(time.monotonic() * 1000.0)  # 推进背离检测

        is_zh = self.args.lang == "zh"
        header_title = "Var↔Lit"
        auto_hedge_label = "对冲" if is_zh else "hedge"
        auto_hedge_on = "开" if is_zh else "ON"
        auto_hedge_off = "关" if is_zh else "OFF"
        quote_title = "最优买一 / 卖一" if is_zh else "Best Bid / Ask"
        col_exchange = "交易所" if is_zh else "Exchange"
        col_bid = "买一" if is_zh else "Bid"
        col_ask = "卖一" if is_zh else "Ask"
        col_book_spread = "买卖价差" if is_zh else "Bid/Ask Spread"
        col_book_spread_pct = "买卖价差%" if is_zh else "Bid/Ask Spread %"
        depth_n = f"{self.args.depth_notional:g}"
        spread_title = f"价差（Lighter深度${depth_n}）" if is_zh else f"Spreads (Lighter depth ${depth_n})"
        col_metric = "指标" if is_zh else "Metric"
        col_value_pct = "当前值%" if is_zh else "Value %"
        col_win_5m = "5分钟窗口" if is_zh else "5m Window"
        col_win_30m = "30分钟窗口" if is_zh else "30m Window"
        col_win_1h = "1小时窗口" if is_zh else "1h Window"
        window_labels = ("中位", "P90↑", "P90↓") if is_zh else ("med", "P90↑", "P90↓")
        metric_long_short = "做多 Var / 做空 Lighter" if is_zh else "Long Var / Short Lighter"
        metric_short_long = "做空 Var / 做多 Lighter" if is_zh else "Short Var / Long Lighter"
        orders_title = "最近订单（最新在前）" if is_zh else "Recent Orders (latest first)"
        col_trade_id = "订单ID" if is_zh else "Trade ID"
        col_side = "方向" if is_zh else "Side"
        col_qty = "数量" if is_zh else "Qty"
        col_var_fill_px = "Var 成交价" if is_zh else "Var Fill Px"
        col_lighter_fill_px = "Lighter 成交价" if is_zh else "Lighter Fill Px"
        col_fill_diff = "成交价差" if is_zh else "Fill Diff"
        col_fill_diff_pct = "成交价差%" if is_zh else "Fill Diff %"
        no_orders_text = "（暂无订单）" if is_zh else "(no tracked orders yet)"
        variational_label = "Variational"
        lighter_label = f"Lighter (深度${depth_n})" if is_zh else f"Lighter (depth ${depth_n})"
        hedge_color = "green" if self.args.auto_hedge else "red"
        hedge_text = auto_hedge_on if self.args.auto_hedge else auto_hedge_off

        fx_text = f"{self.usdc_usdt_rate:.4f}" if self.usdc_usdt_rate else "-"
        r_color = "green" if realized_pnl >= 0 else "red"
        u_color = "green" if unrealized_pnl >= 0 else "red"
        realized_u = "0" if realized_pnl == 0 else f"{realized_pnl:.2f}u"
        unrealized_u = "0" if unrealized_pnl == 0 else f"{unrealized_pnl:.2f}u"
        if is_zh:
            pnl_text = f"收益：[{r_color}]{realized_u}[/]（[{u_color}]{unrealized_u}[/]）"
        else:
            pnl_text = f"PnL: [{r_color}]{realized_u}[/] ([{u_color}]{unrealized_u}[/])"

        pos_label = "持仓" if is_zh else "pos"
        if display_position_qty == 0:
            pos_text = f"{pos_label} 0"
        elif avg_open_pct is None:
            pos_text = f"{pos_label}{display_position_qty:+.3f}{self.ticker}"
        else:
            pos_color = "green" if avg_open_pct >= 0 else "red"
            pos_text = f"{pos_label}{display_position_qty:+.3f}{self.ticker} [{pos_color}]{avg_open_pct:+.4f}%[/]"
        lit_pos_text = ""
        if self.args.auto_hedge:
            lit_label = "Lit仓" if is_zh else "Litpos"
            lit_pos_text = f"{lit_label}{self._current_lighter_position():+.3f} | "
        halt_text = ""
        if self._strategy_halted:
            stop_label = "停止" if is_zh else "HALT"
            halt_text = f"[bold red]⛔{stop_label}: {self._halt_reason}[/] | "
        # 两腿不平衡会静默拦住策略下单，显式标出来免得找不到原因。
        imbalance_text = ""
        imbalanced = False
        if self.args.auto_hedge and not self._strategy_halted and self._cached_position_qty is not None:
            net = self._cached_position_qty + self._current_lighter_position()
            if abs(net) > self._balance_tolerance():
                imbalanced = True
                warn_label = "两腿不平衡·策略暂停下单" if is_zh else "legs unbalanced · orders paused"
                imbalance_text = (
                    f"[bold yellow]⚠️{warn_label} 净={net:+.3f}"
                    f"(Var{self._cached_position_qty:+.3f}/Lit{self._current_lighter_position():+.3f})[/] | "
                )
        disconnect_text = ""
        disconnected = self._var_quote_disconnected()
        if disconnected:
            dc_label = "报价断线·暂停下单" if is_zh else "quote disconnected · orders paused"
            disconnect_text = f"[bold red]⛔{dc_label}[/] | "
        lit_ready_text = ""
        if self.args.auto_hedge:
            if self._lighter_ready:
                ready_label = "Lighter就绪" if is_zh else "Lighter ready"
                lit_ready_text = f" | [bold green]✅{ready_label}[/]"
            else:
                warming_label = "Lighter预热中" if is_zh else "Lighter warming"
                lit_ready_text = f" | [yellow]…{warming_label}[/]"
        header = Panel(
            f"[bold]{header_title}[/bold] | [bold]{self.ticker}[/bold] | "
            f"[bold {hedge_color}]{auto_hedge_label}={hedge_text}[/] | "
            f"{halt_text}{disconnect_text}{imbalance_text}{pnl_text} | {pos_text} | {lit_pos_text}USDC/USDT={fx_text} | {now_cst_display()}{lit_ready_text}",
            border_style="red" if (self._strategy_halted or disconnected) else ("yellow" if imbalanced else "cyan"),
        )

        quote_table = Table(title=quote_title, show_header=True, expand=True)
        quote_table.add_column(col_exchange, style="bold")
        quote_table.add_column(col_bid, justify="right")
        quote_table.add_column(col_ask, justify="right")
        quote_table.add_column(col_book_spread, justify="right")
        quote_table.add_column(col_book_spread_pct, justify="right")
        quote_table.add_row(
            f"{variational_label} ({quote_asset or self.variational_ticker})",
            self._fmt_var_cell_with_dom(var_bid, self._last_dom_bid),
            self._fmt_var_cell_with_dom(var_ask, self._last_dom_ask),
            self._fmt_price(var_book_spread),
            self._fmt_pct(var_book_spread_pct),
        )
        quote_table.add_row(
            lighter_label,
            self._fmt_price(sig_bid),
            self._fmt_price(sig_ask),
            self._fmt_price(spread_value(sig_bid, sig_ask)),
            self._fmt_pct(book_spread_percent(sig_bid, sig_ask)),
        )

        spread_table = Table(title=spread_title, show_header=True, expand=True)
        spread_table.add_column(col_metric, style="bold")
        spread_table.add_column(col_value_pct, justify="right")
        spread_table.add_column(col_win_5m, justify="right")
        spread_table.add_column(col_win_30m, justify="right")
        spread_table.add_column(col_win_1h, justify="right")
        spread_table.add_row(
            f"{metric_long_short}\n[dim]lighter_vwap_bid - var_ask[/dim]",
            self._fmt_signal_pct(
                long_var_short_lighter_pct,
                spread_color_baseline,
                long_pct_median_5m,
                long_pct_median_30m,
                long_pct_median_1h,
            ),
            self._fmt_window_cell(long_pct_median_5m, long_p90_5m, long_p10_5m, window_labels),
            self._fmt_window_cell(long_pct_median_30m, long_p90_30m, long_p10_30m, window_labels),
            self._fmt_window_cell(long_pct_median_1h, long_p90_1h, long_p10_1h, window_labels),
        )
        spread_table.add_row(
            f"{metric_short_long}\n[dim]var_bid - lighter_ask[/dim]",
            self._fmt_signal_pct(
                short_var_long_lighter_pct,
                spread_color_baseline,
                short_pct_median_5m,
                short_pct_median_30m,
                short_pct_median_1h,
            ),
            self._fmt_window_cell(short_pct_median_5m, short_p90_5m, short_p10_5m, window_labels),
            self._fmt_window_cell(short_pct_median_30m, short_p90_30m, short_p10_30m, window_labels),
            self._fmt_window_cell(short_pct_median_1h, short_p90_1h, short_p10_1h, window_labels),
        )

        orders_table = Table(title=orders_title, show_header=True, expand=True)
        orders_table.add_column(col_trade_id)
        orders_table.add_column(col_side)
        orders_table.add_column(col_qty, justify="right")
        orders_table.add_column(col_var_fill_px, justify="right")
        orders_table.add_column(col_lighter_fill_px, justify="right")
        orders_table.add_column(col_fill_diff, justify="right")
        orders_table.add_column(col_fill_diff_pct, justify="right")

        if not rows:
            orders_table.add_row(
                no_orders_text,
                "-",
                "-",
                "-",
                "-",
                "-",
                "-",
            )
        else:
            for row in rows:
                payload = row.to_payload()
                trade_display = row.trade_id[:10] if row.trade_id else row.trade_key[:10]
                fill_diff, fill_diff_pct = fill_diff_by_direction(
                    row.side,
                    row.var_fill_price,
                    row.lighter_fill_price,
                )
                side_zh, side_en = self._direction_labels(row.side)
                side_display = side_zh if is_zh else side_en
                orders_table.add_row(
                    trade_display,
                    side_display,
                    self._fmt_price(row.qty),
                    payload["variational_filled_price"] or "-",
                    payload["lighter_filled_price"] or "-",
                    self._fmt_price(fill_diff),
                    self._fmt_fill_pct_with_leg_slippage(
                        fill_diff_pct, row.var_slippage_pct(), row.lighter_slippage_pct()
                    ),
                )

        hourly_title = (
            "历史价差·每小时（近12小时）"
            if is_zh
            else "Hourly Spread History (last 12h)"
        )
        col_hour = "时段" if is_zh else "Hour"
        col_h_long_med = "多·中位%" if is_zh else "Long med%"
        col_h_long_up = "多·P90↑" if is_zh else "Long P90↑"
        col_h_long_dn = "多·P90↓" if is_zh else "Long P90↓"
        col_h_short_med = "空·中位%" if is_zh else "Short med%"
        col_h_short_up = "空·P90↑" if is_zh else "Short P90↑"
        col_h_short_dn = "空·P90↓" if is_zh else "Short P90↓"
        no_hourly_text = "（暂无历史）" if is_zh else "(no history yet)"

        hourly_table = Table(title=hourly_title, show_header=True, expand=True)
        hourly_table.add_column(col_hour, style="bold")
        hourly_table.add_column(col_h_long_med, justify="right")
        hourly_table.add_column(col_h_long_up, justify="right")
        hourly_table.add_column(col_h_long_dn, justify="right")
        hourly_table.add_column(col_h_short_med, justify="right")
        hourly_table.add_column(col_h_short_up, justify="right")
        hourly_table.add_column(col_h_short_dn, justify="right")

        hour_keys = sorted(self.hourly_spread_buckets.keys(), reverse=True)[:hourly_row_limit]
        if not hour_keys:
            hourly_table.add_row(no_hourly_text, "-", "-", "-", "-", "-", "-")
        else:
            for hour_key in hour_keys:
                bucket = self.hourly_spread_buckets[hour_key]
                start = datetime.fromtimestamp(hour_key * 3600, tz=CST_TZ)
                end = start + timedelta(hours=1)
                hour_label = f"{start:%H:%M}-{end:%H:%M}"
                long_med, long_up, long_dn = self._hourly_bucket_cells(bucket["long"])
                short_med, short_up, short_dn = self._hourly_bucket_cells(bucket["short"])
                hourly_table.add_row(
                    hour_label,
                    long_med,
                    long_up,
                    long_dn,
                    short_med,
                    short_up,
                    short_dn,
                )

        # 评估与下单已移至独立的事件驱动 strategy_loop；渲染只展示其最新信号。
        signal = self._latest_gradient_signal
        strategy_panel = self._render_strategy_panel(
            long_var_short_lighter_pct,
            short_var_long_lighter_pct,
            display_position_qty,
            signal,
        )
        if is_zh:
            page_hint = "[1]实时 [2]策略 Tab切换 q退出"
            page_label = f"第{self.current_page}页"
        else:
            page_hint = "[1]Live [2]Strategy Tab q"
            page_label = f"Page {self.current_page}"
        footer = Panel(f"{page_hint}  |  {page_label}", border_style="dim")

        if self.current_page == 2:
            stats_panel = self._render_stats_panel(is_zh)
            body = Table.grid(expand=True)
            body.add_column(ratio=3)
            body.add_column(ratio=2)
            body.add_row(strategy_panel, stats_panel)
            trend_panel = self._render_spread_trend_panel(is_zh)
            return Group(header, hourly_table, body, trend_panel, footer)
        return Group(header, quote_table, spread_table, orders_table, footer)

    async def export_trade_records_csv(self) -> None:
        if self.trade_records_csv_file is None:
            return

        async with self._record_lock:
            keys = list(self.record_order)
            rows: list[dict[str, Any]] = []
            for key in keys:
                record = self.records.get(key)
                if record is None:
                    continue
                payload = record.to_payload()
                fill_diff, fill_diff_pct = fill_diff_by_direction(
                    record.side,
                    record.var_fill_price,
                    record.lighter_fill_price,
                )
                slippage_pct = record.spread_slippage_pct()
                side_zh, side_en = self._direction_labels(record.side)
                rows.append(
                    {
                        "trade_key": record.trade_key,
                        "trade_id": record.trade_id,
                        "asset": record.asset,
                        "side_raw": record.side,
                        "direction_zh": side_zh,
                        "direction_en": side_en,
                        "qty": decimal_to_str(record.qty),
                        "variational_filled_price": payload["variational_filled_price"],
                        "variational_filled_at": payload["variational_filled_at"],
                        "lighter_order_side": payload["lighter_order_side"],
                        "lighter_client_order_id": payload["lighter_client_order_id"],
                        "lighter_filled_price": payload["lighter_filled_price"],
                        "lighter_filled_at": payload["lighter_filled_at"],
                        "trigger_spread_pct": payload["trigger_spread_pct"],
                        "strategy_action": payload["strategy_action"],
                        "strategy_target_qty": payload["strategy_target_qty"],
                        "strategy_current_qty": payload["strategy_current_qty"],
                        "fill_diff": decimal_to_str(fill_diff),
                        "fill_diff_pct": decimal_to_str(fill_diff_pct),
                        "spread_slippage_pct": decimal_to_str(slippage_pct),
                        "var_slippage_pct": payload["var_slippage_pct"],
                        "dom_slippage_pct": payload["dom_slippage_pct"],
                        "lighter_slippage_pct": payload["lighter_slippage_pct"],
                        "auto_hedge_enabled": payload["auto_hedge_enabled"],
                        "var_order_error": payload["var_order_error"],
                        "var_submit_ok": payload["var_submit_ok"],
                        "var_submit_status": payload["var_submit_status"],
                        "var_submit_order_id": payload["var_submit_order_id"],
                        "var_submit_error": payload["var_submit_error"],
                        "var_submit_click_started_at": payload["var_submit_click_started_at"],
                        "var_submit_click_started_at_ms": payload["var_submit_click_started_at_ms"],
                        "var_submit_timing": json.dumps(payload["var_submit_timing"], ensure_ascii=False, default=str) if payload["var_submit_timing"] is not None else "",
                        "var_submit_quote_snapshot": json.dumps(payload["var_submit_quote_snapshot"], ensure_ascii=False, default=str) if payload["var_submit_quote_snapshot"] is not None else "",
                        "var_submit_order_response": json.dumps(payload["var_submit_order_response"], ensure_ascii=False, default=str) if payload["var_submit_order_response"] is not None else "",
                        "hedge_error": payload["hedge_error"],
                        "last_variational_status": payload["last_variational_status"],
                    }
                )

        snapshot_sig = json.dumps(rows, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        if snapshot_sig == self._trade_records_snapshot_sig:
            return

        fieldnames = [
            "trade_key",
            "trade_id",
            "asset",
            "side_raw",
            "direction_zh",
            "direction_en",
            "qty",
            "variational_filled_price",
            "variational_filled_at",
            "lighter_order_side",
            "lighter_client_order_id",
            "lighter_filled_price",
            "lighter_filled_at",
            "trigger_spread_pct",
            "strategy_action",
            "strategy_target_qty",
            "strategy_current_qty",
            "fill_diff",
            "fill_diff_pct",
            "spread_slippage_pct",
            "var_slippage_pct",
            "dom_slippage_pct",
            "lighter_slippage_pct",
            "auto_hedge_enabled",
            "var_order_error",
            "var_submit_ok",
            "var_submit_status",
            "var_submit_order_id",
            "var_submit_error",
            "var_submit_click_started_at",
            "var_submit_click_started_at_ms",
            "var_submit_timing",
            "var_submit_quote_snapshot",
            "var_submit_order_response",
            "hedge_error",
            "last_variational_status",
        ]
        async with self._trade_csv_write_lock:
            if snapshot_sig == self._trade_records_snapshot_sig:
                return
            await asyncio.to_thread(self._write_csv_rows, self.trade_records_csv_file, fieldnames, rows)
            self._trade_records_snapshot_sig = snapshot_sig

    @staticmethod
    def _write_csv_rows(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_name(f".{path.name}.tmp")
        with tmp_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        os.replace(tmp_path, path)

    async def dashboard_loop(self) -> None:
        refresh_interval = DASHBOARD_REFRESH_SECONDS
        refresh_per_second = max(1, int(round(1.0 / refresh_interval)))
        initial_render = await self.render_dashboard()
        await self.export_trade_records_csv()
        with Live(
            initial_render,
            console=self.dashboard_console,
            refresh_per_second=refresh_per_second,
            screen=True,
        ) as live:
            while not self.stop_flag:
                await asyncio.sleep(refresh_interval)
                live.update(await self.render_dashboard())
                await self.export_trade_records_csv()

    async def run(self) -> None:
        self.setup_signal_handlers()
        self.setup_keyboard()
        self.usdc_usdt_task = asyncio.create_task(self.update_usdc_usdt_rate_loop())
        await self.runtime.start()
        self.browser_order_server = await run_browser_order_broker(
            FORWARDER_HOST,
            BROWSER_ORDER_BROKER_PORT,
            self.browser_order_broker,
        )
        self._browser_order_queue.start()
        self._schedule_prepare_browser_order()
        self.print_startup_next_steps()
        self.logger.info(
            "Listening for Variational forwarder events on ws://%s:%s and ws://%s:%s; browser orders on ws://%s:%s",
            FORWARDER_HOST,
            FORWARDER_WS_PORT,
            FORWARDER_HOST,
            FORWARDER_REST_PORT,
            FORWARDER_HOST,
            BROWSER_ORDER_BROKER_PORT,
        )

        await self.wait_for_variational_ready()
        self.logger.info("Variational heartbeat is live")
        if self.args.auto_hedge:
            self._lighter_warm_task = asyncio.create_task(self.warm_lighter())
        initial_asset = await self.wait_for_ticker_resolution()
        await self.activate_asset(initial_asset, reason="startup")

        self.trade_event_cursor = await self.runtime.monitor.get_latest_trade_event_seq()
        self.logger.info("Tracking new Variational trade events from seq>%s", self.trade_event_cursor)

        self.trade_task = asyncio.create_task(self.trade_loop())
        self.dashboard_task = asyncio.create_task(self.dashboard_loop())
        self._strategy_task = asyncio.create_task(self.strategy_loop())
        if self.args.auto_hedge:
            self._lighter_account_task = asyncio.create_task(self.lighter_account_ws())

        while not self.stop_flag:
            await asyncio.sleep(0.25)

    async def close(self) -> None:
        self.stop_flag = True
        self.restore_keyboard()

        if self.usdc_usdt_task and not self.usdc_usdt_task.done():
            self.usdc_usdt_task.cancel()
            await asyncio.gather(self.usdc_usdt_task, return_exceptions=True)

        if self.dashboard_task and not self.dashboard_task.done():
            self.dashboard_task.cancel()
            await asyncio.gather(self.dashboard_task, return_exceptions=True)

        if self.trade_task and not self.trade_task.done():
            self.trade_task.cancel()
            await asyncio.gather(self.trade_task, return_exceptions=True)

        if self._strategy_task and not self._strategy_task.done():
            self._strategy_task.cancel()
            await asyncio.gather(self._strategy_task, return_exceptions=True)

        if self._lighter_account_task and not self._lighter_account_task.done():
            self._lighter_account_task.cancel()
            await asyncio.gather(self._lighter_account_task, return_exceptions=True)

        if self._lighter_warm_task and not self._lighter_warm_task.done():
            self._lighter_warm_task.cancel()
            await asyncio.gather(self._lighter_warm_task, return_exceptions=True)

        with contextlib.suppress(Exception):
            self._lighter_http_session.close()

        if self.lighter_ws_task and not self.lighter_ws_task.done():
            self.lighter_ws_task.cancel()
            await asyncio.gather(self.lighter_ws_task, return_exceptions=True)

        await self._browser_order_queue.stop()

        if self.lighter_client is not None:
            close_method = getattr(self.lighter_client, "close", None)
            if callable(close_method):
                with contextlib.suppress(Exception):
                    close_result = close_method()
                    if asyncio.iscoroutine(close_result):
                        await close_result

        if self.browser_order_server is not None:
            self.browser_order_server.close()
            await self.browser_order_server.wait_closed()
            self.browser_order_server = None

        await self.runtime.stop()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Track Variational order lifecycle and optionally auto-hedge on Lighter (ticker auto-detected)."
    )
    parser.add_argument(
        "--lang",
        choices=["zh", "en"],
        default="zh",
        help="Dashboard language: zh (Chinese) or en (English). Default: zh",
    )
    parser.add_argument(
        "--no-hedge",
        action="store_false",
        dest="auto_hedge",
        help="Disable automatic Lighter hedge placement (default: enabled)",
    )
    parser.add_argument(
        "--depth-notional",
        type=float,
        default=2000.0,
        help="Lighter VWAP depth notional in USD used as the effective bid/ask for spreads (default: 2000)",
    )
    parser.add_argument(
        "--browser-smoke-test",
        action="store_true",
        help="Run a live browser order smoke test: buy, sell, then restore buy form. Writes detailed logs.",
    )
    parser.add_argument(
        "--browser-smoke-qty",
        type=str,
        default="0.001",
        help="Quantity used by --browser-smoke-test (default: 0.001)",
    )
    parser.set_defaults(auto_hedge=True)
    return parser.parse_args(argv)


async def _amain() -> None:
    load_dotenv()
    args = parse_args()
    runtime = VariationalToLighterRuntime(args)
    try:
        if args.browser_smoke_test:
            await runtime.run_browser_smoke_test()
        else:
            await runtime.run()
    finally:
        await runtime.close()


def main() -> None:
    asyncio.run(_amain())


if __name__ == "__main__":
    main()
