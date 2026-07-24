import argparse
import asyncio
import contextlib
import csv
import json
import logging
import math
import os
import re
import signal
import subprocess
import sys
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from statistics import median
from typing import Any

import websockets
from dotenv import load_dotenv
from rich.console import Console, Group
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
from variational.lighter_rust import RustLighterGateway
from variational.browser_order import BrowserOrderBroker, BrowserOrderCommand, BrowserOrderDispatchQueue, run_browser_order_broker
from variational.differential_screen import DifferentialScreen
from variational.gradient_strategy import EditableField, GradientSignal, GradientStrategyState, StrategySection
from variational.quote_comparator import QuoteComparator
from variational.round_exit_strategy import CompletedRound, RoundExitConfig, RoundExitLedger, RoundStateError
from variational.spread_store import SpreadStore
from variational.spread_dashboard import SpreadDashboardServer

VARIATIONAL_TICKER_OVERRIDES = {
    "LIT": "LIGHTER",
    "WTI": "CL",
}
VARIATIONAL_ASSET_TO_LIGHTER_TICKER = {v: k for k, v in VARIATIONAL_TICKER_OVERRIDES.items()}

FORWARDER_HOST = "127.0.0.1"
FORWARDER_WS_PORT = 8766
FORWARDER_REST_PORT = 8767
BROWSER_ORDER_BROKER_PORT = 8768
SPREAD_DASHBOARD_PORT = 8780
PRICE_MAPPING_RATIO = Decimal("1")
LOG_DIR = Path("./log")
RUNS_DIR = LOG_DIR / "runs"
SPREAD_HISTORY_DB_FILE = LOG_DIR / "spread_history.sqlite3"
ROUND_EXIT_STATE_FILE = LOG_DIR / "round_exit_state.json"
MEMORY_MONITOR_ENABLED_ENV = "VARIATIONAL_MEMORY_MONITOR"
READY_TIMEOUT_SECONDS = 60.0
# broker 层等待必须 >= 页面内 timeout_ms，否则会在页面返回前假超时。
BROWSER_ORDER_BROKER_MARGIN_SECONDS = 8.0
BROWSER_ACTIVITY_INTERVAL_SECONDS = 300.0
LIGHTER_ORDER_MAP_MAX = 500
DOM_POSITION_TIMEOUT_SECONDS = 5.0
STRATEGY_EVAL_FALLBACK_SECONDS = 0.2
POSITION_FILL_REFRESH_TIMEOUT_SECONDS = 2.0
POSITION_FILL_REFRESH_POLL_SECONDS = 0.1
POSITION_BALANCE_TIMEOUT_SECONDS = 10.0
ROUND_POSITION_MISMATCH_CONFIRM_SECONDS = 5.0
LIGHTER_WARM_RETRY_SECONDS = 15.0
# 噪音过滤：同一信号需连续出现这么多次才下单（单次视为噪音）。
SIGNAL_CONFIRM_SECONDS = 1.0
SIGNAL_CONFIRM_MIN_QUOTES = 4
SIGNAL_FAST_CONFIRM_SECONDS = 0.4
SIGNAL_FAST_CONFIRM_MIN_QUOTES = 3
SIGNAL_FAST_MAX_EDGE_RANGE_PCT = Decimal("0.03")
# Var 报价断线闸（默认开）：API 或 DOM 报价超过该时长(ms)没刷新 = 断线，不下单。设 None 关闭。
VAR_QUOTE_DISCONNECT_MS: float | None = 3000.0
# 尖峰过滤（默认关）：设为 Decimal 值开启——触发价差相对近期均值的最大允许偏离。
# 默认 None=关闭：30s 滚动均值滞后，会误杀只持续几秒的真实机会。
SPIKE_BASELINE_WINDOW_SECONDS = 30.0
MAX_SPIKE_DEVIATION_PCT: Decimal | None = None
POLL_INTERVAL_SECONDS = 0.05
HEDGE_SLIPPAGE_BPS = 100.0
DASHBOARD_REFRESH_SECONDS = 0.2
SPREAD_SAMPLE_INTERVAL_SECONDS = 1.0
TRADE_RECORDS_EXPORT_INTERVAL_SECONDS = 1.0
DASHBOARD_RESIZE_SETTLE_SECONDS = 0.25
SPREAD_STATS_REFRESH_SECONDS = 1.0
HISTORY_CACHE_REFRESH_SECONDS = 5.0
# 价差走势 sparkline：滚动窗口 3 天，数据直接来自 SQLite。
SPREAD_TREND_WINDOW_SECONDS = 3 * 86400.0
DASHBOARD_ORDERS = 20
HOURLY_HISTORY_HOURS = 12
CST_TZ = timezone(timedelta(hours=8))
# Lines consumed by always-on chrome (header panel + quote/spread tables +
# the hourly/orders table frames). Remaining terminal height is split between
# the hourly-history and recent-orders rows so the dashboard fits one screen.
DASHBOARD_FIXED_OVERHEAD_LINES = 30
ASSET_SWITCH_CONFIRM_TICKS = 3
PENDING_TRIGGER_SPREAD_TTL_SECONDS = 30.0
BINANCE_USDCUSDT_WS_URL = "wss://data-stream.binance.vision/ws/usdcusdt@bookTicker"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def cst_now() -> datetime:
    return datetime.now(CST_TZ)


def cst_now_iso() -> str:
    return cst_now().isoformat()


class CstLogFormatter(logging.Formatter):
    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        timestamp = datetime.fromtimestamp(record.created, tz=CST_TZ)
        if datefmt:
            return timestamp.strftime(datefmt)
        return timestamp.strftime("%Y-%m-%d %H:%M:%S,") + f"{int(record.msecs):03d}"


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


def spread_value(aggressive_buy_ask: Decimal | None, aggressive_sell_bid: Decimal | None) -> Decimal | None:
    if aggressive_buy_ask is None or aggressive_sell_bid is None:
        return None
    return aggressive_sell_bid - aggressive_buy_ask


def symmetric_edge_percent(left_price: Decimal | None, mapped_right_price: Decimal | None) -> Decimal | None:
    if left_price is None or mapped_right_price is None:
        return None
    denominator = mapped_right_price + left_price
    if denominator == 0:
        return None
    return Decimal("200") * (mapped_right_price - left_price) / denominator


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
    price_mapping_ratio: Decimal = Decimal("1"),
) -> tuple[Decimal | None, Decimal | None]:
    mapped_lighter_bid = lighter_bid * price_mapping_ratio if lighter_bid is not None else None
    mapped_lighter_ask = lighter_ask * price_mapping_ratio if lighter_ask is not None else None
    long_var_short_lighter_pct = symmetric_edge_percent(var_ask, mapped_lighter_bid)
    short_var_long_lighter_pct = symmetric_edge_percent(var_bid, mapped_lighter_ask)
    return long_var_short_lighter_pct, short_var_long_lighter_pct


def edge_pnl_percent(
    entry_edge_pct: Decimal | None,
    executable_close_edge_pct: Decimal | None,
    position_qty: Decimal,
) -> Decimal | None:
    """用反方向可执行平仓 Edge 估算持仓价差收益，正数代表有利。"""
    if entry_edge_pct is None or executable_close_edge_pct is None or position_qty == 0:
        return None
    if position_qty > 0:
        return entry_edge_pct - executable_close_edge_pct
    return executable_close_edge_pct - entry_edge_pct


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
        pct = symmetric_edge_percent(var_fill_price, lighter)
        return diff, pct
    if side_n == "sell":
        # Short Var / Long Lighter: lighter_fill - var_fill，数值越低越有利。
        diff = spread_value(var_fill_price, lighter)
        pct = symmetric_edge_percent(var_fill_price, lighter)
        return diff, pct
    return None, None


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
    lighter_filled_qty: Decimal = Decimal("0")
    lighter_filled_quote: Decimal = Decimal("0")
    lighter_fill_ts_iso: str | None = None
    lighter_limit_price: Decimal | None = None
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
    first_hit_spread_pct: Decimal | None = None
    confirmed_spread_pct: Decimal | None = None
    preflight_spread_pct: Decimal | None = None
    # 触发信号那一刻两腿的预期价（Var 腿 / Lighter 腿），用于分腿滑点。
    var_trigger_price: Decimal | None = None
    lighter_trigger_price: Decimal | None = None
    dom_trigger_price: Decimal | None = None  # 网页DOM约1s报价，仅用于滑点对照
    strategy_action: str | None = None
    strategy_signal_source: str | None = None
    strategy_section: str | None = None
    strategy_threshold_pct: Decimal | None = None
    signal_long_edge_pct: Decimal | None = None
    signal_short_edge_pct: Decimal | None = None
    strategy_target_qty: Decimal | None = None
    strategy_current_qty: Decimal | None = None
    strategy_created_at_ms: int | None = None
    round_id: int | None = None
    round_fill_role: str | None = None
    round_accounted: bool = False
    slippage_recorded: bool = False
    var_fill_price_source: str | None = None
    var_fill_price_estimated: bool = False
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
            "variational_fill_price_source": self.var_fill_price_source,
            "variational_fill_price_estimated": self.var_fill_price_estimated,
            "variational_filled_at": self.var_fill_ts_iso,
            "lighter_order_side": self.lighter_side,
            "lighter_client_order_id": self.lighter_client_order_id,
            "lighter_filled_price": decimal_to_str(self.lighter_fill_price),
            "lighter_filled_qty": decimal_to_str(self.lighter_filled_qty),
            "lighter_filled_quote": decimal_to_str(self.lighter_filled_quote),
            "lighter_filled_at": self.lighter_fill_ts_iso,
            "lighter_limit_price": decimal_to_str(self.lighter_limit_price),
            "lighter_tx_hash": self.lighter_tx_hash,
            "trigger_spread_pct": decimal_to_str(self.trigger_spread_pct),
            "first_hit_spread_pct": decimal_to_str(self.first_hit_spread_pct),
            "confirmed_spread_pct": decimal_to_str(self.confirmed_spread_pct),
            "preflight_spread_pct": decimal_to_str(self.preflight_spread_pct),
            "spread_slippage_pct": decimal_to_str(self.spread_slippage_pct()),
            "var_slippage_pct": decimal_to_str(self.var_slippage_pct()),
            "dom_slippage_pct": decimal_to_str(self.dom_slippage_pct()),
            "lighter_slippage_pct": decimal_to_str(self.lighter_slippage_pct()),
            "strategy_action": self.strategy_action,
            "strategy_signal_source": self.strategy_signal_source,
            "strategy_section": self.strategy_section,
            "strategy_threshold_pct": decimal_to_str(self.strategy_threshold_pct),
            "signal_long_edge_pct": decimal_to_str(self.signal_long_edge_pct),
            "signal_short_edge_pct": decimal_to_str(self.signal_short_edge_pct),
            "strategy_target_qty": decimal_to_str(self.strategy_target_qty),
            "strategy_current_qty": decimal_to_str(self.strategy_current_qty),
            "strategy_created_at_ms": self.strategy_created_at_ms,
            "round_id": self.round_id,
            "round_fill_role": self.round_fill_role,
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
        if self.side.strip().lower() == "sell":
            return self.trigger_spread_pct - fill_pct
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
        for handler in self.logger.handlers:
            handler.close()
        self.logger.handlers.clear()
        self.logger.propagate = False

        self.run_started_at = cst_now()
        run_name = self.run_started_at.strftime("%Y-%m-%d_%H-%M-%S_%f_UTC+8")
        self.run_dir = RUNS_DIR / run_name
        self.runtime_log_file = self.run_dir / "runtime.log"
        self.orders_file = self.run_dir / "order_metrics.jsonl"
        self.trade_records_csv_file = self.run_dir / "trade_records.csv"
        self.browser_smoke_file = self.run_dir / "browser_smoke_test.jsonl"
        self._session_logging_started = False
        self.dashboard_console = Console()

        self.runtime = VariationalRuntime(
            host=FORWARDER_HOST,
            ws_port=FORWARDER_WS_PORT,
            rest_port=FORWARDER_REST_PORT,
            output_dir=None,
            quiet=True,
        )
        self.browser_order_broker = BrowserOrderBroker()
        self.browser_order_server = None

        self._order_write_lock = asyncio.Lock()
        self._trade_csv_write_lock = asyncio.Lock()
        self._trade_records_snapshot_sig: str | None = None
        self._trade_records_exported_at = 0.0

        self.records: dict[str, OrderLifecycle] = {}
        self.record_order: deque[str] = deque(maxlen=500)
        self.lighter_client_order_to_trade_key: dict[int, str] = {}
        self._pending_variational_strategy_order_keys: deque[str] = deque()
        self._record_lock = asyncio.Lock()
        spread_db_path = Path(self.args.spread_db) if self.args.spread_db else (
            Path(":memory:") if self.args.browser_smoke_test else SPREAD_HISTORY_DB_FILE
        )
        self.spread_store = SpreadStore(spread_db_path)
        self.spread_dashboard = SpreadDashboardServer(
            self.spread_store,
            FORWARDER_HOST,
            self.args.dashboard_port,
            Path(__file__).resolve().parent / "web" / "spread_dashboard.html",
        )
        self._asset_switch_lock = asyncio.Lock()
        self._asset_switch_candidate: str | None = None
        self._asset_switch_candidate_hits = 0

        self.trade_event_cursor = 0

        self.lighter_gateway = RustLighterGateway(
            execution_enabled=self.args.auto_hedge,
            event_handler=self._handle_lighter_rust_event,
            log_handler=lambda message: self.logger.warning("Lighter Rust: %s", message),
        )
        self._lighter_ready = False

        self.lighter_market_index = 0
        self.lighter_best_bid: Decimal | None = None
        self.lighter_best_ask: Decimal | None = None
        self.lighter_vwap_bid: Decimal | None = None
        self.lighter_vwap_ask: Decimal | None = None
        self.lighter_order_book_ready = False
        self.trade_task: asyncio.Task[None] | None = None
        self.dashboard_task: asyncio.Task[None] | None = None
        self.spread_sampling_task: asyncio.Task[None] | None = None
        self.binance_usdc_task: asyncio.Task[None] | None = None
        self.browser_activity_task: asyncio.Task[None] | None = None
        self.binance_usdc_bid: Decimal | None = None
        self.binance_usdc_ask: Decimal | None = None
        self.binance_usdc_updated_at: float | None = None
        self.binance_usdc_received_ms: int | None = None
        self.binance_usdc_status = "connecting"
        # Dashboard pages: 1 = live, 2 = strategy, 3 = low-frequency history.
        self.current_page = 1
        self._stdin_fd: int | None = None
        self._stdin_old_settings: Any = None
        self._keyboard_active = False
        self._dashboard_wake = asyncio.Event()
        self._dashboard_size: tuple[int, int] | None = None
        self._dashboard_resize_deadline = 0.0

        self.gradient_strategy = GradientStrategyState.from_config(os.environ)
        self.round_exit_state_file: Path | None = None if self.args.browser_smoke_test else ROUND_EXIT_STATE_FILE
        self.round_exit_ledger = self._load_round_exit_ledger()
        self._round_ledger_synced = False
        self._round_ledger_sync_warning_logged = False
        self._round_position_mismatch_sig: tuple[str, str] | None = None
        self._round_position_mismatch_since: float | None = None
        self._manual_fill_price_buffer: str | None = None
        self._manual_fill_recovery_status = ""
        self._manual_fill_recovery_busy = False
        self._last_dom_position_qty: Decimal | None = None
        self._cached_position_qty: Decimal | None = None
        self._latest_gradient_signal: GradientSignal | None = None
        self._strategy_wake = asyncio.Event()
        self._strategy_task: asyncio.Task[None] | None = None
        self._strategy_order_in_flight = False
        # Lighter 真实账户仓位（account_all 频道，按 symbol）+ 双腿平衡闸状态。
        self._lighter_positions: dict[str, Decimal] = {}
        self._lighter_position_ready = asyncio.Event()
        self._strategy_halted = False
        self._halt_reason = ""
        self._last_block_log_sig: tuple[str, Any] | None = None
        self._last_leg_prices: dict[str, Decimal | None] = {}
        self._pending_signal_sig: tuple[str, str, str, str, str, str] | None = None
        self._pending_signal_count = 0
        self._pending_signal_started_at: float | None = None
        self._pending_signal_quote_keys: set[tuple[str, int]] = set()
        self._pending_signal_edge_samples: list[Decimal] = []
        self._pending_signal_ready = False
        self._pending_signal_first_edge: Decimal | None = None
        self._pending_signal_confirmed_edge: Decimal | None = None
        # Var 报价双源对比器（观测记录：API vs DOM）。
        self.quote_comparator = QuoteComparator(("api", "dom"))
        self._last_dom_bid: Decimal | None = None
        self._last_dom_ask: Decimal | None = None
        self._last_dom_at: float | None = None
        self._last_quote_wall: dict[str, float | None] = {"api": None, "dom": None}
        self._last_quote_delay: dict[str, float | None] = {"api": None, "dom": None}
        self._spread_stats_cache: dict[str, float | None] = {}
        self._spread_stats_asset: str | None = None
        self._spread_stats_refreshed_at = 0.0
        self._history_cache_refreshed_at = 0.0
        self._history_cache_key: tuple[str, int] | None = None
        self._hourly_rows_cache: list[dict[str, Any]] = []
        self._spread_trend_cache: tuple[list[float | None], float | None, float | None] = ([], None, None)
        self.browser_order_broker.on_dom_quote = self._on_dom_quote
        self.browser_order_broker.on_dom_notice = self._on_dom_notice
        self.runtime.monitor.on_quote_update = self._on_api_quote
        # 统计面板（第2页）：延迟/分腿滑点/信号确认时长/下单成交数。
        self._stat_fired = 0
        self._stat_both_filled = 0
        self._stat_var_latency = RunningStat()       # 信号触发→Var成交(端到端)
        self._stat_lighter_latency = RunningStat()   # 信号触发→Lighter WS成交(端到端)
        self._stat_var_side_switch = RunningStat()   # Var 方向切换耗时(不切=0)
        self._stat_signal_confirm = RunningStat()
        self._stat_var_slip = RunningStat()       # Var 滑点(对200ms主动API触发价)
        self._stat_var_slip_dom = RunningStat()   # Var 滑点(对网页DOM约1s触发价)
        self._stat_lighter_slip = RunningStat()
        self._stat_long_fill_edge = RunningStat()
        self._stat_short_fill_edge = RunningStat()
        self._session_cashflow = Decimal("0")
        self._session_qty = Decimal("0")
        self._session_completed_orders = 0
        self._last_gradient_signal_sig: tuple[str, str, str, str, str, str] | None = None
        self._last_prepared_order_sig: tuple[str, str] | None = None
        self._pending_prepare_sigs: set[tuple[str, str]] = set()
        self._prepared_order_side: str = "buy"
        self._browser_activity_next_side = "sell"
        self._pending_trigger_spreads: list[PendingTriggerSpread] = []
        self._browser_order_queue: BrowserOrderDispatchQueue[BrowserOrderCommand] = (
            BrowserOrderDispatchQueue(self._send_browser_order_task)
        )

    def _start_session_logging(self) -> None:
        if self._session_logging_started:
            return
        self.run_dir.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(self.runtime_log_file, encoding="utf-8")
        file_handler.setFormatter(CstLogFormatter("%(asctime)s | %(levelname)s | %(message)s"))
        self.logger.addHandler(file_handler)
        self._session_logging_started = True

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
        dashboard_changed = False
        if key in ("q", "Q", "\x03"):  # q / Ctrl-C
            self.shutdown()
        elif key == "\t":
            self.current_page = self.current_page % 3 + 1
            if self.current_page == 2:
                self._schedule_prepare_browser_order()
            dashboard_changed = True
        elif self.current_page == 2:
            if self._handle_manual_fill_recovery_key(key):
                dashboard_changed = True
            else:
                was_enabled = self.gradient_strategy.enabled
                previous_order_qty = self.gradient_strategy.single_order_qty
                if self.gradient_strategy.handle_key(key):
                    dashboard_changed = True
                    if self.gradient_strategy.single_order_qty != previous_order_qty:
                        self._schedule_prepare_browser_order()
                if self.gradient_strategy.enabled and not was_enabled and self._strategy_halted:
                    self._manual_fill_recovery_status = "触发策略已启用；仍处于硬停，回到顶部按 Enter 恢复运行"
        elif key == "1":
            self.current_page = 1
            dashboard_changed = True
        elif key == "2":
            self.current_page = 2
            self._schedule_prepare_browser_order()
            dashboard_changed = True
        elif key == "3":
            self.current_page = 3
            dashboard_changed = True
        if dashboard_changed:
            self._dashboard_wake.set()

    def _handle_manual_fill_recovery_key(self, key: str) -> bool:
        buffer = getattr(self, "_manual_fill_price_buffer", None)
        if buffer is not None:
            if key in ("\x1b",):
                self._manual_fill_price_buffer = None
                self._manual_fill_recovery_status = "手动补记已取消"
                return True
            if key in ("\x7f", "\x08"):
                self._manual_fill_price_buffer = buffer[:-1]
                return True
            if key.isdigit() or (key == "." and "." not in buffer):
                self._manual_fill_price_buffer += key
                return True
            if key in ("\r", "\n"):
                try:
                    value = Decimal(self._manual_fill_price_buffer)
                except Exception:
                    value = Decimal("0")
                self._manual_fill_price_buffer = None
                if value <= 0:
                    self._manual_fill_recovery_status = "手动补记失败：价格必须大于 0"
                    return True
                if self._manual_fill_recovery_busy:
                    return True
                self._manual_fill_recovery_busy = True

                async def recover() -> None:
                    try:
                        await self._manual_recover_missing_var_fill(value)
                    finally:
                        self._manual_fill_recovery_busy = False
                        self._dashboard_wake.set()

                self._schedule_background_task(recover())
                self._manual_fill_recovery_status = "手动补记处理中..."
                return True
            return True

        halted = getattr(self, "_strategy_halted", False)
        if key.lower() == "r" and halted:
            self._manual_fill_price_buffer = ""
            self._manual_fill_recovery_status = "请输入漏记的 Var 成交价，Enter确认，Esc取消"
            return True
        if key in ("\r", "\n") and halted and self.gradient_strategy.enabled_selected():
            reason = self._strategy_resume_block_reason()
            if reason:
                self._manual_fill_recovery_status = f"启动失败：{reason}"
                self.logger.warning("策略手动启动失败: %s", reason)
                return True
            self.gradient_strategy.enabled = True
            self._strategy_halted = False
            self._halt_reason = ""
            self._manual_fill_recovery_status = "策略已手动启动"
            self.logger.info("策略已在程序内手动启动")
            return True
        return False

    def _strategy_resume_block_reason(self) -> str:
        errors = self.gradient_strategy.validation_errors()
        if errors:
            return errors[0]
        position_qty = self._cached_position_qty
        if position_qty is None:
            return "Var真实仓位未知"
        if not self._round_ledger_synced:
            return "本轮账本尚未同步"
        if abs(self.round_exit_ledger.position_qty - position_qty) > self._balance_tolerance():
            return (
                f"本轮账本与真实仓位不一致 ledger={decimal_to_str(self.round_exit_ledger.position_qty)} "
                f"live={decimal_to_str(position_qty)}"
            )
        if self.args.auto_hedge and not self._positions_balanced():
            return (
                f"两腿仓位不平衡 Var={decimal_to_str(position_qty)} "
                f"Lit={decimal_to_str(self._current_lighter_position())}"
            )
        if self._var_quote_disconnected():
            return "Var报价尚未恢复"
        return ""

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

    def _update_binance_usdc_book(self, payload: Any) -> bool:
        if not isinstance(payload, dict) or str(payload.get("s", "")).upper() != "USDCUSDT":
            return False
        bid = to_decimal(payload.get("b"))
        ask = to_decimal(payload.get("a"))
        if bid is None or ask is None or bid <= 0 or ask <= 0 or bid > ask:
            return False
        self.binance_usdc_bid = bid
        self.binance_usdc_ask = ask
        self.binance_usdc_updated_at = time.monotonic()
        self.binance_usdc_received_ms = int(time.time() * 1000)
        self.binance_usdc_status = "connected"
        return True

    async def binance_usdc_book_loop(self) -> None:
        """Maintain an observation-only Binance USDC/USDT best bid/ask stream."""
        retry_seconds = 1.0
        while not self.stop_flag:
            try:
                self.binance_usdc_status = "connecting"
                async with websockets.connect(
                    BINANCE_USDCUSDT_WS_URL,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=5,
                ) as ws:
                    retry_seconds = 1.0
                    async for raw in ws:
                        if self.stop_flag:
                            return
                        try:
                            self._update_binance_usdc_book(json.loads(raw))
                        except (TypeError, json.JSONDecodeError):
                            continue
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.binance_usdc_status = "disconnected"
                self.logger.warning("Binance USDC/USDT断线: %s；%.0fs后重试", exc, retry_seconds)
                await asyncio.sleep(retry_seconds)
                retry_seconds = min(30.0, retry_seconds * 2)

    async def warm_lighter(self) -> None:
        """Start the Rust-owned market-data and optional execution gateway."""
        has_pk = bool(os.getenv("API_KEY_PRIVATE_KEY", "").strip() or os.getenv("LIGHTER_PRIVATE_KEY", "").strip())
        self.logger.info(
            "Lighter启动: 执行=%s 私钥=%s | %s",
            "开" if self.args.auto_hedge else "关",
            "已设置" if has_pk else "只读模式/缺失",
            self.lighter_gateway.binary,
        )
        while not self.stop_flag and not self._lighter_ready:
            try:
                await self.lighter_gateway.start()
                self._lighter_ready = True
                self.logger.info("Lighter Rust就绪：订单簿/签名/执行/私有账户由Rust持有")
                return
            except Exception as exc:
                self.logger.warning(
                    "Lighter Rust启动失败，%.0fs 后重试: %r",
                    LIGHTER_WARM_RETRY_SECONDS,
                    exc,
                )
                await asyncio.sleep(LIGHTER_WARM_RETRY_SECONDS)

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
            self._session_cashflow = Decimal("0")
            self._session_qty = Decimal("0")
            self._session_completed_orders = 0
        # 历史价差按资产持久化，切换标的不删除数据库记录。
        # 换标的：清缓存仓位与在途/停止状态，避免用旧标的仓位误判平衡。
        self._cached_position_qty = None
        self._strategy_order_in_flight = False
        self._strategy_halted = False
        self._halt_reason = ""
        self._round_position_mismatch_sig = None
        self._round_position_mismatch_since = None
        self._last_gradient_signal_sig = None
        self._reset_pending_signal_confirmation()
        self._lighter_positions.clear()
        self._lighter_position_ready.clear()
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

            await self.reset_lighter_order_book()
            await self._reset_state_for_asset_switch()
            self.lighter_market_index = await self.lighter_gateway.set_market(
                self.ticker,
                Decimal(str(self.args.depth_notional)),
                timeout=READY_TIMEOUT_SECONDS,
            )
            self.logger.info(
                "市场切换[%s]: Var=%s Lighter=%s id=%s",
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
        await asyncio.wait_for(self.lighter_gateway.book_ready.wait(), timeout=READY_TIMEOUT_SECONDS)

    async def reset_lighter_order_book(self) -> None:
        self.lighter_gateway.book_ready.clear()
        self.lighter_order_book_ready = False
        self.lighter_best_bid = None
        self.lighter_best_ask = None
        self.lighter_vwap_bid = None
        self.lighter_vwap_ask = None

    async def _handle_lighter_rust_event(self, event: dict[str, Any]) -> None:
        event_type = event.get("type")
        if event_type == "book":
            if not event.get("ready"):
                self.lighter_order_book_ready = False
                self.lighter_best_bid = None
                self.lighter_best_ask = None
                self.lighter_vwap_bid = None
                self.lighter_vwap_ask = None
                return
            self.lighter_market_index = int(event.get("market_id", 0) or 0)
            self.lighter_best_bid = to_decimal(event.get("bid"))
            self.lighter_best_ask = to_decimal(event.get("ask"))
            self.lighter_vwap_bid = to_decimal(event.get("vwap_bid"))
            self.lighter_vwap_ask = to_decimal(event.get("vwap_ask"))
            self.lighter_order_book_ready = self.lighter_best_bid is not None and self.lighter_best_ask is not None
            self._strategy_wake.set()
            return
        if event_type == "health" and not event.get("ready"):
            self._lighter_ready = False
            self._lighter_positions.clear()
            self._lighter_position_ready.clear()
            self.lighter_order_book_ready = False
            self.lighter_best_bid = None
            self.lighter_best_ask = None
            self.lighter_vwap_bid = None
            self.lighter_vwap_ask = None
            if self.args.auto_hedge and not self.stop_flag:
                self._strategy_halted = True
                self._halt_reason = f"Lighter Rust断开: {event.get('error') or 'unknown error'}"
                self.logger.error("策略已停止（需手动检查）：%s", self._halt_reason)
            return
        if event_type == "position":
            symbol = str(event.get("symbol", "")).strip().upper()
            quantity = to_decimal(event.get("quantity"))
            if symbol and quantity is not None:
                self._lighter_positions[symbol] = quantity
                if symbol == (self.ticker or "").strip().upper():
                    self._lighter_position_ready.set()
            return
        if event_type == "error":
            self.logger.warning("Lighter Rust事件错误: source=%s error=%s", event.get("source"), event.get("error"))
            return
        if event_type != "execution":
            return
        kind = str(event.get("kind", ""))
        if kind == "fill":
            await self.handle_lighter_fill_update(event)
        elif kind == "rejected":
            self.logger.error("Lighter Rust订单拒绝: coid=%s reason=%s", event.get("client_order_index"), event.get("reason"))

    async def handle_lighter_fill_update(self, event: dict[str, Any]) -> None:
        client_order_id_raw = event.get("client_order_index") or event.get("client_order_id")
        try:
            client_order_id = int(client_order_id_raw)
        except Exception:
            return
        fill_price = to_decimal(event.get("price"))
        fill_qty = to_decimal(event.get("quantity"))
        if fill_price is None or fill_qty is None or fill_qty <= 0:
            return
        ts_event_ms = event.get("ts_event_ms")
        now_iso = (
            datetime.fromtimestamp(float(ts_event_ms) / 1000, tz=timezone.utc).isoformat()
            if ts_event_ms
            else utc_now()
        )

        async with self._record_lock:
            trade_key = self.lighter_client_order_to_trade_key.get(client_order_id)
            if not trade_key:
                self.logger.warning(
                    "收到未匹配的Lighter成交: coid=%s market=%s，可能发生映射竞态或进程重启",
                    client_order_id,
                    self.lighter_market_index,
                )
                return
            record = self.records.get(trade_key)
            if record is None:
                return
            if record.lighter_filled_qty >= record.qty:
                return
            remaining = record.qty - record.lighter_filled_qty
            applied_qty = min(fill_qty, remaining)
            record.lighter_filled_qty += applied_qty
            record.lighter_filled_quote += applied_qty * fill_price
            record.lighter_fill_price = record.lighter_filled_quote / record.lighter_filled_qty
            if record.lighter_filled_qty < record.qty:
                payload = record.to_payload()
                completed = False
            else:
                record.lighter_fill_ts_iso = now_iso
                completed = True
            if completed:
                self._maybe_record_slippage_stats(record)
            # Lighter 端到端延迟：信号触发 → 收到 Lighter WS 成交（仅策略单）。
            if record.signal_trigger_monotonic is not None and not record.lighter_latency_recorded:
                self._stat_lighter_latency.add((time.monotonic() - record.signal_trigger_monotonic) * 1000)
                record.lighter_latency_recorded = True
            payload = record.to_payload()
            if completed:
                self.lighter_client_order_to_trade_key.pop(client_order_id, None)
                self._prune_record_cache()

        await self.append_order_log("lighter_fill" if completed else "lighter_partial_fill", payload)
        logger = getattr(self, "logger", None)
        if logger is not None and not completed:
            short_id = record.trade_key.rsplit(":", 1)[-1][-8:]
            lighter_slip = record.lighter_slippage_pct()
            lighter_slip_text = "-" if lighter_slip is None else f"{lighter_slip:+.4f}%"
            logger.info(
                "L分片 #%s | 累计=%s/%s 均价=%s | L滑点=%s",
                short_id,
                decimal_to_str(record.lighter_filled_qty),
                decimal_to_str(record.qty),
                decimal_to_str(record.lighter_fill_price),
                lighter_slip_text,
            )

    def _maybe_record_slippage_stats(self, record: OrderLifecycle) -> None:
        """两腿都成交后累计一次分腿滑点（仅策略单有触发价；调用方持 _record_lock）。"""
        if record.slippage_recorded:
            return
        if record.var_fill_price is None or record.lighter_fill_price is None:
            return
        if record.lighter_filled_qty < record.qty:
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
        _fill_diff, fill_edge = fill_diff_by_direction(
            record.side,
            record.var_fill_price,
            record.lighter_fill_price,
        )
        if fill_edge is not None:
            if record.side.strip().lower() == "buy":
                self._stat_long_fill_edge.add(float(fill_edge))
            elif record.side.strip().lower() == "sell":
                self._stat_short_fill_edge.add(float(fill_edge))
            completed_rounds = self._account_round_fill(record, fill_edge)
            self._record_session_execution(record)
            logger = getattr(self, "logger", None)
            if logger is not None:
                short_id = record.trade_key.rsplit(":", 1)[-1][-8:]
                direction = "买V/卖L" if record.side.strip().lower() == "buy" else "卖V/买L"
                role = {
                    "entry": "开仓",
                    "close": "平仓",
                    "close_and_new_entry": "平仓并反向开仓",
                    "rejected_conflict": "账本冲突",
                }.get(record.round_fill_role or "", "未分类")
                round_text = (
                    f"第{record.round_id}个持仓周期 {role}"
                    if record.round_id is not None
                    else f"持仓周期未跟踪 {role}"
                )
                total_slip = record.spread_slippage_pct()
                total_slip_text = "-" if total_slip is None else f"{total_slip:+.4f}%"
                var_slip_text = "-" if v is None else f"{v:+.4f}%"
                lighter_slip_text = "-" if l is None else f"{l:+.4f}%"
                fill_source = "手动补记" if record.var_fill_price_source == "manual_input" else "成交WS"
                logger.info(
                    "成交[%s] #%s %s %s | V=%s L均价=%s Edge=%+.4f%% | "
                    "滑点(有利+) 总=%s V=%s L=%s | %s 仓=%s",
                    fill_source,
                    short_id,
                    direction,
                    decimal_to_str(record.qty),
                    decimal_to_str(record.var_fill_price),
                    decimal_to_str(record.lighter_fill_price),
                    fill_edge,
                    total_slip_text,
                    var_slip_text,
                    lighter_slip_text,
                    round_text,
                    decimal_to_str(self.round_exit_ledger.position_qty),
                )
                for item in completed_rounds:
                    cycle_quote = "-" if item.estimated_quote_pnl is None else f"{item.estimated_quote_pnl:+.4f}u"
                    cycle_quote_label = "毛收益=" if item.quote_pnl_exact else "约="
                    cumulative_edge = self.round_exit_ledger.cumulative_edge_pnl_average
                    cumulative_quote = self.round_exit_ledger.cumulative_quote_pnl
                    cumulative_edge_text = "-" if cumulative_edge is None else f"{cumulative_edge:+.4f}%"
                    cumulative_quote_text = "-" if cumulative_quote is None else f"{cumulative_quote:+.4f}u"
                    cumulative_quote_label = (
                        "毛收益="
                        if self.round_exit_ledger.cumulative_quote_pnl_exact
                        else "约="
                    )
                    logger.info(
                        "✅【仓位清零】第%s个持仓周期 %s | 入=%+.4f%% 平=%+.4f%% "
                        "Edge收益=%+.4f%% %s%s",
                        item.round_id,
                        "Long" if item.side == "long" else "Short",
                        item.entry_edge_actual,
                        item.close_edge_actual,
                        item.edge_pnl,
                        cycle_quote_label,
                        cycle_quote,
                    )
                    logger.info(
                        "📊累计清零收益 | 平均Edge=%s | %s%s",
                        cumulative_edge_text,
                        cumulative_quote_label,
                        cumulative_quote_text,
                    )
        record.slippage_recorded = True

    def _normal_close_threshold_for_entry_side(self, entry_side: str) -> Decimal | None:
        if entry_side.strip().lower() == "sell":
            values = [
                row.threshold_pct
                for row in self.gradient_strategy.open_rows
                if row.is_complete() and row.target_qty is not None and row.target_qty >= 0
            ]
            return min(values) if values else None
        values = [
            row.threshold_pct
            for row in self.gradient_strategy.close_rows
            if row.is_complete() and row.target_qty is not None and row.target_qty <= 0
        ]
        return max(values) if values else None

    def _account_round_fill(self, record: OrderLifecycle, fill_edge: Decimal) -> list[CompletedRound]:
        """Feed one fully-filled two-leg order into the live round ledger."""
        if record.round_accounted or not self._round_ledger_synced:
            return []
        ledger = self.round_exit_ledger
        side = record.side.strip().lower()
        if ledger.position_qty == 0:
            role = "entry"
        elif (ledger.side == "long" and side == "buy") or (ledger.side == "short" and side == "sell"):
            role = "entry"
        elif record.qty > abs(ledger.position_qty):
            role = "close_and_new_entry"
        else:
            role = "close"
        existing_round_id = ledger.round_id
        normal_threshold = self._normal_close_threshold_for_entry_side(side)
        try:
            reference_price = None
            if record.var_fill_price is not None and record.lighter_fill_price is not None:
                reference_price = (record.var_fill_price + record.lighter_fill_price) / Decimal("2")
            completed = ledger.apply_fill(
                side,
                record.qty,
                fill_edge,
                normal_close_threshold=normal_threshold,
                next_normal_close_threshold=normal_threshold,
                reference_price=reference_price,
                unit_spread=self._record_unit_spread(record),
            )
        except RoundStateError as exc:
            record.round_accounted = True
            record.round_fill_role = "rejected_conflict"
            self._strategy_halted = True
            self._halt_reason = f"本轮账本冲突: {exc}"
            self.logger.error("策略已停止（需手动检查）：%s", self._halt_reason)
            return []
        record.round_id = existing_round_id or (completed[0].round_id if completed else ledger.round_id)
        record.round_fill_role = role
        record.round_accounted = True
        self._persist_round_exit_ledger()
        return completed

    def _record_session_execution(self, record: OrderLifecycle) -> None:
        unit_spread = self._record_unit_spread(record)
        if unit_spread is None or record.lighter_filled_qty < record.qty:
            return
        self._session_cashflow += unit_spread * record.qty
        self._session_qty += record.qty
        self._session_completed_orders += 1

    def _record_is_referenced(self, trade_key: str) -> bool:
        return trade_key in self.lighter_client_order_to_trade_key.values() or trade_key in self._pending_variational_strategy_order_keys

    def _prune_record_cache(self) -> None:
        """Keep recent dashboard rows while releasing completed old lifecycle objects."""
        maxlen = getattr(self.record_order, "maxlen", None) or 500
        if len(self.records) <= maxlen + 100:
            return
        retained = set(self.record_order)
        for key in list(self.records):
            if len(self.records) <= maxlen:
                break
            if key in retained or self._record_is_referenced(key):
                continue
            self.records.pop(key, None)

    def _remember_record(self, trade_key: str, record: OrderLifecycle) -> None:
        maxlen = getattr(self.record_order, "maxlen", None) or 500
        evicted = self.record_order[0] if len(self.record_order) >= maxlen else None
        self.records[trade_key] = record
        self.record_order.append(trade_key)
        if evicted is not None and evicted not in self.record_order and not self._record_is_referenced(evicted):
            self.records.pop(evicted, None)

    def _load_round_exit_ledger(self) -> RoundExitLedger:
        config = RoundExitConfig(self.gradient_strategy.single_order_qty)
        path = self.round_exit_state_file
        if path is None or not path.exists():
            return RoundExitLedger(config)
        try:
            state = json.loads(path.read_text(encoding="utf-8"))
            return RoundExitLedger.from_state(config, state)
        except Exception as exc:
            self.logger.error("本轮状态恢复失败，保护模块将等待归零后重新同步: %s", exc)
            return RoundExitLedger(config)

    def _persist_round_exit_ledger(self) -> None:
        path = self.round_exit_state_file
        if path is None:
            return
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            temporary = path.with_suffix(path.suffix + ".tmp")
            temporary.write_text(
                json.dumps(self.round_exit_ledger.to_state(), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            temporary.replace(path)
        except Exception:
            self.logger.exception("本轮状态写入失败: %s", path)

    def _sync_round_ledger_with_live_position(self, live_position_qty: Decimal) -> None:
        if self._round_ledger_synced:
            return
        tracked = self.round_exit_ledger.position_qty
        if tracked != 0:
            if abs(tracked - live_position_qty) <= self._balance_tolerance():
                self._round_ledger_synced = True
                self.logger.info(
                    "本轮账本恢复成功: round=%s position=%s",
                    self.round_exit_ledger.round_id,
                    decimal_to_str(tracked),
                )
                return
            if live_position_qty == 0:
                self.round_exit_ledger = RoundExitLedger(
                    RoundExitConfig(self.gradient_strategy.single_order_qty)
                )
                self._round_ledger_synced = True
                self._persist_round_exit_ledger()
                self.logger.warning("持久化本轮仓位与实盘不一致但实盘已归零，已清空旧轮次")
                return
            self._strategy_halted = True
            self._halt_reason = (
                f"恢复本轮仓位不一致: ledger={decimal_to_str(tracked)} "
                f"live={decimal_to_str(live_position_qty)}"
            )
            if not self._round_ledger_sync_warning_logged:
                self.logger.error("策略已停止（需手动检查）：%s", self._halt_reason)
                self._round_ledger_sync_warning_logged = True
            return
        if live_position_qty == 0:
            self._round_ledger_synced = True
            self._persist_round_exit_ledger()
            self.logger.info("本轮账本已同步：当前仓位为0，下一笔实际成交将创建新轮次")
        elif not self._round_ledger_sync_warning_logged:
            self.logger.warning("启动时已有仓位但没有可恢复成本账本，本轮保护暂停至仓位归零")
            self._round_ledger_sync_warning_logged = True

    async def get_lighter_best_bid_ask(self) -> tuple[Decimal | None, Decimal | None]:
        return self.lighter_gateway.best_bid, self.lighter_gateway.best_ask

    async def get_lighter_depth_quote(self, notional: Decimal) -> tuple[Decimal | None, Decimal | None]:
        configured = Decimal(str(self.args.depth_notional))
        if notional != configured:
            raise RuntimeError(
                f"Rust Lighter gateway depth is configured for {configured}, requested {notional}"
            )
        return self.lighter_gateway.vwap_bid, self.lighter_gateway.vwap_ask

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
            "logged_at": cst_now_iso(),
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
            self._halt_strategy(f"Lighter 对冲未提交: 盘口未就绪 ({record.trade_key})")
            return

        slippage = Decimal(str(HEDGE_SLIPPAGE_BPS)) / Decimal("10000")
        if side == "BUY":
            is_ask = False
            limit_price = best_ask * (Decimal("1") + slippage)
        else:
            is_ask = True
            limit_price = best_bid * (Decimal("1") - slippage)

        async with self._record_lock:
            client_order_id = int(time.time() * 1000)
            while client_order_id in self.lighter_client_order_to_trade_key:
                client_order_id += 1
            # Register before create_order: an immediate fill WS event may arrive
            # before the request coroutine returns.
            record.lighter_side = side
            record.lighter_client_order_id = client_order_id
            record.lighter_limit_price = limit_price
            self.lighter_client_order_to_trade_key[client_order_id] = record.trade_key

        try:
            signed_quantity = -record.qty if is_ask else record.qty
            tx_hash = await self.lighter_gateway.place_order(
                symbol=self.ticker or "",
                client_order_index=client_order_id,
                signed_quantity=signed_quantity,
                limit_price=limit_price,
                reduce_only=False,
            )

            async with self._record_lock:
                record.lighter_tx_hash = tx_hash
                record.hedge_error = None
                # 上限淘汰最旧未成交项，避免映射表无限增长。
                while len(self.lighter_client_order_to_trade_key) > LIGHTER_ORDER_MAP_MAX:
                    oldest = next(iter(self.lighter_client_order_to_trade_key))
                    self.lighter_client_order_to_trade_key.pop(oldest, None)
                payload = record.to_payload()
            await self.append_order_log("lighter_submitted", payload)
            short_id = record.trade_key.rsplit(":", 1)[-1][-8:]
            self.logger.info(
                "已发 #%s L%s %s @%s | coid=%s",
                short_id,
                "卖" if side == "SELL" else "买",
                decimal_to_str(record.qty),
                decimal_to_str(limit_price),
                client_order_id,
            )
        except Exception as exc:
            async with self._record_lock:
                record.lighter_side = side
                record.hedge_error = str(exc)
                if record.lighter_fill_ts_iso is None:
                    self.lighter_client_order_to_trade_key.pop(client_order_id, None)
                payload = record.to_payload()
            self.logger.warning("Lighter 对冲失败(下单异常): key=%s side=%s error=%s", record.trade_key, side, exc)
            await self.append_order_log("lighter_error", payload)
            self._halt_strategy(f"Lighter 对冲提交失败: {exc}")

    def _current_lighter_position(self) -> Decimal:
        return self._lighter_positions.get((self.ticker or "").strip().upper(), Decimal("0"))

    def _current_lighter_position_known(self) -> bool:
        symbol = (self.ticker or "").strip().upper()
        return bool(symbol and self._lighter_position_ready.is_set() and symbol in self._lighter_positions)

    def _balance_tolerance(self) -> Decimal:
        multiplier = self.lighter_gateway.size_multiplier
        if not multiplier or multiplier <= 0:
            return Decimal("0")
        return Decimal("1") / Decimal(multiplier)  # 1 个最小步长

    def _positions_balanced(self) -> bool:
        """对冲两腿方向相反，净仓位应约为 0。Var 仓位未知则视为不平衡。"""
        var_pos = self._cached_position_qty
        if var_pos is None or not self._current_lighter_position_known():
            return False
        net = var_pos + self._current_lighter_position()
        return abs(net) <= self._balance_tolerance()

    def _var_quote_disconnected(self) -> bool:
        """主动 API 报价超过阈值没刷新 = 断线；DOM 仅用于观测对比。"""
        if VAR_QUOTE_DISCONNECT_MS is None:
            return False
        now_ms = time.monotonic() * 1000.0
        fresh = self.quote_comparator.freshness_ms("api", now_ms)
        return fresh is not None and fresh > VAR_QUOTE_DISCONNECT_MS

    def _strategy_order_allowed(self) -> bool:
        if self._strategy_halted:
            return False
        if self._var_quote_disconnected():
            return False
        if self.args.auto_hedge and not self._positions_balanced():
            return False
        return True

    def _halt_strategy(self, reason: str) -> None:
        self._strategy_halted = True
        self._halt_reason = reason
        self.logger.error("策略已停止（需手动检查）：%s", reason)

    async def _confirm_hedge_or_halt(self, prev_var_qty: Decimal | None) -> None:
        """下单后 10s 内确认两腿都到位并重新平衡；超时未平则硬停策略。"""
        if not self.args.auto_hedge:
            await self._refresh_position_cache_after_fill(prev_var_qty)
            return
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

    async def _wait_for_round_accounting(self, trade_key: str | None, timeout_seconds: float = 2.0) -> None:
        if not trade_key or not self.args.auto_hedge or not self._round_ledger_synced:
            return
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline and not self.stop_flag:
            async with self._record_lock:
                record = self.records.get(trade_key)
                if record is not None and record.round_accounted:
                    return
            await asyncio.sleep(0.05)
        self._strategy_halted = True
        self._halt_reason = f"两腿已平衡但本轮成交记账超时: {trade_key}"
        self.logger.error("策略已停止（需手动检查）：%s", self._halt_reason)

    def _make_strategy_trade_key(self) -> str:
        return f"strategy:{time.time_ns()}"

    def _create_strategy_order_from_signal(self, signal: GradientSignal, side: str, qty: Decimal) -> OrderLifecycle:
        trade_key = self._make_strategy_trade_key()
        while trade_key in self.records:
            trade_key = f"strategy:{int(time.time() * 1000) + len(self.records)}"
        asset = self.variational_ticker or self.ticker or "UNKNOWN"
        # 触发价：买 Var 时取 Var ask/Lighter bid；卖 Var 时取 Var bid/Lighter ask。
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
            first_hit_spread_pct=getattr(self, "_pending_signal_first_edge", None),
            confirmed_spread_pct=getattr(self, "_pending_signal_confirmed_edge", None),
            preflight_spread_pct=signal.spread_pct,
            var_trigger_price=var_trigger,
            lighter_trigger_price=lighter_trigger,
            dom_trigger_price=dom_trigger,
            strategy_action=signal.action,
            strategy_signal_source=signal.source,
            strategy_section=signal.section.value,
            strategy_threshold_pct=signal.threshold_pct,
            signal_long_edge_pct=getattr(self, "_latest_long_edge_pct", None),
            signal_short_edge_pct=getattr(self, "_latest_short_edge_pct", None),
            strategy_target_qty=signal.target_qty,
            strategy_current_qty=signal.current_qty,
            strategy_created_at_ms=int(time.time() * 1000),
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

    def _match_pending_variational_strategy_order(
        self,
        side: str,
        qty: Decimal,
        asset: str,
        *,
        trade_id: str = "",
        order_id: str = "",
        event_timestamp: Any = None,
    ) -> OrderLifecycle | None:
        side_n = side.strip().lower()
        asset_n = asset.strip().upper()
        candidates: list[OrderLifecycle] = []
        for trade_key in list(self._pending_variational_strategy_order_keys):
            record = self.records.get(trade_key)
            if record is None or record.var_fill_price is not None:
                with contextlib.suppress(ValueError):
                    self._pending_variational_strategy_order_keys.remove(trade_key)
                continue
            record_assets = self._equivalent_assets(record.asset)
            if record.side == side_n and record.qty == qty and asset_n in record_assets:
                candidates.append(record)

        if not candidates:
            return None

        event_ids = {value for value in (trade_id.strip(), order_id.strip()) if value}
        exact = [record for record in candidates if record.var_submit_order_id in event_ids]
        if len(exact) == 1:
            selected = exact[0]
        elif event_ids:
            logger = getattr(self, "logger", None)
            if logger is not None:
                logger.warning(
                    "Var成交ID尚未匹配到下单记录，禁止FIFO绑定: trade_id=%s order_id=%s side=%s qty=%s",
                    trade_id or "-",
                    order_id or "-",
                    side_n,
                    decimal_to_str(qty),
                )
            return None
        else:
            event_ms = self._epoch_ms(event_timestamp)
            timed: list[tuple[float, OrderLifecycle]] = []
            if event_ms is not None:
                for record in candidates:
                    submitted_ms = record.var_submit_click_started_at_ms or record.strategy_created_at_ms
                    if submitted_ms is None:
                        continue
                    distance_ms = abs(event_ms - float(submitted_ms))
                    if distance_ms <= 120_000:
                        timed.append((distance_ms, record))
            timed.sort(key=lambda item: item[0])
            if timed and (len(timed) == 1 or timed[0][0] < timed[1][0]):
                selected = timed[0][1]
            elif len(candidates) == 1:
                selected = candidates[0]
            else:
                logger = getattr(self, "logger", None)
                if logger is not None:
                    logger.error(
                        "Var成交无法唯一匹配策略单，拒绝FIFO绑定: side=%s qty=%s trade_id=%s order_id=%s candidates=%s",
                        side_n,
                        decimal_to_str(qty),
                        trade_id or "-",
                        order_id or "-",
                        [record.trade_key for record in candidates],
                    )
                return None

        with contextlib.suppress(ValueError):
            self._pending_variational_strategy_order_keys.remove(selected.trade_key)
        return selected

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
        multiplier = self.lighter_gateway.size_multiplier
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
        order_id = str(event.get("order_id", "")).strip()

        now_iso = utc_now()
        fill_iso = str(event.get("timestamp") or now_iso)

        created = False

        async with self._record_lock:
            event_ids = {value for value in (trade_id, order_id) if value}
            record = next(
                (
                    item
                    for item in self.records.values()
                    if trade_id and item.trade_id == trade_id
                ),
                None,
            )
            if record is None and event_ids:
                record = next(
                    (
                        item
                        for item in self.records.values()
                        if item.var_submit_order_id in event_ids
                    ),
                    None,
                )
            if record is None:
                record = self.records.get(key)
            if record is None:
                record = self._match_pending_variational_strategy_order(
                    side,
                    qty,
                    asset if asset else "UNKNOWN",
                    trade_id=trade_id,
                    order_id=order_id,
                    event_timestamp=event.get("timestamp"),
                )
            elif record.trade_key.startswith("strategy:"):
                with contextlib.suppress(ValueError):
                    self._pending_variational_strategy_order_keys.remove(record.trade_key)
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
                self._remember_record(key, record)
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
                record.var_fill_price_source = "trade_ws"
                record.var_fill_price_estimated = False
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

    @staticmethod
    def _compact_direction_label(side: str) -> str:
        side_n = side.strip().lower()
        if side_n == "buy":
            return "做多V/做空L"
        if side_n == "sell":
            return "做空V/做多L"
        return side_n.upper() if side_n else "-"

    @staticmethod
    def _order_time_label(record: OrderLifecycle) -> str:
        timestamp: datetime | None = None
        if record.var_submit_click_started_at_ms is not None:
            with contextlib.suppress(TypeError, ValueError, OSError):
                timestamp = datetime.fromtimestamp(
                    float(record.var_submit_click_started_at_ms) / 1000.0,
                    tz=CST_TZ,
                )
        if timestamp is None:
            raw = record.var_submit_click_started_at or record.var_fill_ts_iso
            if raw:
                with contextlib.suppress(TypeError, ValueError):
                    parsed = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
                    if parsed.tzinfo is None:
                        parsed = parsed.replace(tzinfo=timezone.utc)
                    timestamp = parsed.astimezone(CST_TZ)
        if timestamp is None:
            return "-"
        return f"{timestamp.month}/{timestamp.day} {timestamp:%H.%M}"

    def _fmt_pct(self, value: Decimal | None) -> str:
        if value is None:
            return "-"
        return f"{value:.4f}%"

    @staticmethod
    def _style_fill_value_by_direction(text: str, side: str | None) -> str:
        if str(side or "").strip().lower() == "sell":
            return f"[yellow]{text}[/yellow]"
        return text

    def _fmt_fill_pct_with_leg_slippage(
        self,
        fill_pct: Decimal | None,
        v_slippage_pct: Decimal | None,
        l_slippage_pct: Decimal | None,
        side: str | None = None,
    ) -> str:
        """成交价差% + 括号分腿滑点：V/L 各自正=有利(绿)/负=不利(红)。"""
        fill_text = self._style_fill_value_by_direction(self._fmt_pct(fill_pct), side)
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
        asset: str,
        var_bid: Decimal | None,
        var_ask: Decimal | None,
        lighter_bid: Decimal | None,
        lighter_ask: Decimal | None,
        long_var_short_lighter_pct: Decimal | None,
        short_var_long_lighter_pct: Decimal | None,
    ) -> None:
        self.spread_store.record(
            asset=asset,
            var_bid=var_bid,
            var_ask=var_ask,
            lighter_bid=lighter_bid,
            lighter_ask=lighter_ask,
            long_edge_pct=long_var_short_lighter_pct,
            short_edge_pct=short_var_long_lighter_pct,
            usdc_usdt_bid=self.binance_usdc_bid,
            usdc_usdt_ask=self.binance_usdc_ask,
            usdc_usdt_received_ms=self.binance_usdc_received_ms,
        )

    def _median_cross_spread(self, window_seconds: float, long_side: bool) -> float | None:
        asset = self.variational_ticker or self.ticker
        if not asset:
            return None
        median_value, _p90, _p10 = self.spread_store.window_stats(
            asset, window_seconds, "long" if long_side else "short"
        )
        return median_value

    def _percentile_cross_spread(
        self, window_seconds: float, long_side: bool, pct: float
    ) -> float | None:
        asset = self.variational_ticker or self.ticker
        if not asset:
            return None
        _median, p90, p10 = self.spread_store.window_stats(
            asset, window_seconds, "long" if long_side else "short"
        )
        return p90 if pct >= 50 else p10

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
        asset = self.variational_ticker or self.ticker
        if not asset:
            return None, None, None
        return self.spread_store.window_stats(asset, window_seconds, "long" if long_side else "short")

    def _spread_trend_series(
        self,
        series_index: int,
        width: int,
        now: float,
        asset: str | None = None,
    ) -> tuple[list[float | None], float | None, float | None]:
        """近3天该序列按时间分桶(width格)取均值；返回 (每桶值, 最小, 最大)，空桶为 None。"""
        asset = asset or self.variational_ticker or self.ticker
        buckets: list[list[float]] = [[] for _ in range(width)]
        if not asset:
            return [None] * width, None, None
        end_ms = int(time.time() * 1000)
        start_ms = end_ms - int(SPREAD_TREND_WINDOW_SECONDS * 1000)
        for point in self.spread_store.history(asset, SPREAD_TREND_WINDOW_SECONDS, max_points=width * 2):
            value = point["longEdge"] if series_index == 0 else point["shortEdge"]
            if value is None:
                continue
            pos = int((point["timestampMs"] - start_ms) / (end_ms - start_ms) * width)
            buckets[min(width - 1, max(0, pos))].append(float(value))
        avgs: list[float | None] = [sum(b) / len(b) if b else None for b in buckets]
        present = [a for a in avgs if a is not None]
        if not present:
            return avgs, None, None
        return avgs, min(present), max(present)

    @staticmethod
    def _nice_step(data_range: float, target_lines: int) -> float:
        """把粗略步长对齐到 1/2/5×10^n 的整齐刻度。"""
        if data_range <= 0:
            data_range = abs(data_range) or 1.0
        rough = data_range / max(1, target_lines)
        mag = 10 ** math.floor(math.log10(rough))
        norm = rough / mag
        nice = 1 if norm < 1.5 else (2 if norm < 3 else (5 if norm < 7 else 10))
        return nice * mag

    @staticmethod
    def _time_ticks(cols: int, start_wall: float, end_wall: float) -> list[tuple[int, str]]:
        """每小时一个小刻度(标签为空)，整 3/6/12h 大刻度带标签(步长自动选到放得下)。返回 [(列, 标签)]。"""
        window = end_wall - start_wall
        if window <= 0 or cols <= 0:
            return []
        label_w = 11  # "MM-DD HH:MM"
        max_labels = max(2, cols // (label_w + 2))
        total_hours = window / 3600.0
        major_h = 24
        for s in (3, 6, 12, 24):
            if total_hours / s <= max_labels:
                major_h = s
                break
        ticks: list[tuple[int, str]] = []
        t = math.ceil(start_wall / 3600) * 3600  # 对齐到整点，每小时一个
        while t <= end_wall:
            pos = int((t - start_wall) / window * cols)
            is_major = int(round(t / 3600)) % major_h == 0
            label = datetime.fromtimestamp(t, CST_TZ).strftime("%m-%d %H:%M") if is_major else ""
            ticks.append((pos, label))
            t += 3600
        return ticks

    @staticmethod
    def _ascii_line_chart(
        values: list[float | None],
        target_lines: int,
        x_ticks: list[tuple[int, str]] | None = None,
    ) -> list[str]:
        """asciichart 式连续折线：用 ╭╮╰╯─│ 把 values 连成折线；空值断开。
        Y 轴按整齐步长(1/2/5)对齐，每一行都标出对应差价(4位小数)。可选底部按小时时间轴。"""
        present = [v for v in values if v is not None]
        if not present:
            return []
        dmn, dmx = min(present), max(present)
        step = VariationalToLighterRuntime._nice_step(dmx - dmn, max(2, target_lines))
        mn = math.floor(dmn / step) * step
        mx = math.ceil(dmx / step) * step
        if mx <= mn:
            mx = mn + step
        rows = int(round((mx - mn) / step)) + 1  # 网格线(行)数
        rng = mx - mn
        grid = [[" "] * len(values) for _ in range(rows)]

        def level(v: float) -> int:
            return min(rows - 1, max(0, int(round((v - mn) / rng * (rows - 1)))))

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

        lines: list[str] = []
        for i, row in enumerate(grid):
            val = round(mn + (rows - 1 - i) * step, 6)  # 每行对应的整齐刻度值
            lines.append(f"{val:+.4f} ┤{''.join(row)}")  # 每一小格都标
        if x_ticks:
            cols = len(values)
            markers = ["─"] * cols
            labelrow = [" "] * cols
            last_end = -1
            for pos, lab in x_ticks:
                if 0 <= pos < cols:
                    markers[pos] = "┼" if lab else "┬"  # 大刻度(带标签) / 每小时小刻度
                if not lab:
                    continue
                start = min(pos, cols - len(lab))
                if start <= last_end or start < 0:
                    continue  # 标签重叠则跳过(刻度线仍在)
                for k, ch in enumerate(lab):
                    labelrow[start + k] = ch
                last_end = start + len(lab)
            lines.append(" " * 7 + " └" + "".join(markers))
            lines.append(" " * 9 + "".join(labelrow))
        return lines

    def _render_spread_trend_panel(self, is_zh: bool) -> Panel:
        cols = max(20, min(200, self.dashboard_console.size.width - 14))
        long_vals, _lo, _hi = self._spread_trend_cache
        if len(long_vals) != cols:
            long_vals = [None] * cols
        no_data = "无数据" if is_zh else "no data"
        long_label = "做多差价·近3天(整宽=3天)" if is_zh else "Long spread · 3d (full width = 3d)"

        now_wall = time.time()
        x_ticks = self._time_ticks(cols, now_wall - SPREAD_TREND_WINDOW_SECONDS, now_wall)

        grid = Table.grid()
        grid.add_column()
        chart = self._ascii_line_chart(long_vals, 7, x_ticks=x_ticks)
        for line in (chart or [f"  ({no_data})"]):
            grid.add_row(f"[green]{line}[/]")
        return Panel(grid, title=long_label, border_style="blue")

    def _calculate_spread_stats(self, asset: str) -> dict[str, float | None]:
        cache: dict[str, float | None] = {}
        for secs, tag in ((5 * 60, "5m"), (30 * 60, "30m"), (60 * 60, "1h")):
            for long_side, side_key in ((True, "long"), (False, "short")):
                med, p90, p10 = self.spread_store.window_stats(
                    asset,
                    secs,
                    "long" if long_side else "short",
                )
                cache[f"{side_key}_median_{tag}"] = med
                cache[f"{side_key}_p90_{tag}"] = p90
                cache[f"{side_key}_p10_{tag}"] = p10
        return cache

    def _refresh_spread_stats(self) -> None:
        """Synchronously refresh stats for tests and non-async callers."""
        asset = self.variational_ticker or self.ticker
        if not asset:
            return
        self._spread_stats_cache = self._calculate_spread_stats(asset)
        self._spread_stats_asset = asset
        self._spread_stats_refreshed_at = time.monotonic()

    async def _refresh_spread_stats_if_due(self, asset: str) -> None:
        now = time.monotonic()
        if (
            self._spread_stats_asset == asset
            and
            self._spread_stats_refreshed_at > 0
            and now - self._spread_stats_refreshed_at < SPREAD_STATS_REFRESH_SECONDS
        ):
            return
        # Mark before awaiting so a slow query cannot be scheduled twice.
        self._spread_stats_refreshed_at = now
        cache = await asyncio.to_thread(self._calculate_spread_stats, asset)
        self._spread_stats_cache = cache
        self._spread_stats_asset = asset

    def _load_history_snapshot(
        self,
        asset: str,
        width: int,
    ) -> tuple[list[dict[str, Any]], tuple[list[float | None], float | None, float | None]]:
        return (
            self.spread_store.hourly_stats(asset, HOURLY_HISTORY_HOURS),
            self._spread_trend_series(0, width, time.monotonic(), asset=asset),
        )

    async def _refresh_history_cache_if_due(self, asset: str, width: int) -> None:
        now = time.monotonic()
        key = (asset, width)
        if (
            self._history_cache_key == key
            and now - self._history_cache_refreshed_at < HISTORY_CACHE_REFRESH_SECONDS
        ):
            return
        self._history_cache_key = key
        self._history_cache_refreshed_at = now
        hourly, trend = await asyncio.to_thread(self._load_history_snapshot, asset, width)
        self._hourly_rows_cache = hourly
        self._spread_trend_cache = trend

    def _hourly_bucket_cells(self, values: tuple[float | None, float | None, float | None]) -> tuple[str, str, str]:
        return tuple(self._fmt_median_pct(value) for value in values)

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

    def _session_execution_summary(self) -> tuple[Decimal, Decimal, int]:
        """Completed two-leg cashflow, quantity, and count for this process.

        The cashflow includes fills belonging to a still-open hedged position,
        so it is an execution difference rather than realized PnL.
        """
        return self._session_cashflow, self._session_qty, self._session_completed_orders

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
            if unit is None or record.lighter_filled_qty < record.qty:
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

    async def _manual_recover_missing_var_fill(self, fill_price: Decimal) -> None:
        position_qty = self._cached_position_qty
        if position_qty is None or not self._round_ledger_synced:
            self._manual_fill_recovery_status = "手动补记失败：当前仓位或本轮账本未知"
            return
        async with self._record_lock:
            delta = position_qty - self.round_exit_ledger.position_qty
            tolerance = self._balance_tolerance()
            candidates = [
                record
                for record in self.records.values()
                if record.trade_key.startswith("strategy:")
                and record.var_fill_price is None
                and record.lighter_fill_price is not None
                and record.lighter_filled_qty >= record.qty
                and abs(delta - (record.qty if record.side == "buy" else -record.qty)) <= tolerance
            ]
            if len(candidates) != 1:
                self._manual_fill_recovery_status = (
                    f"手动补记失败：匹配到 {len(candidates)} 笔候选，需保留唯一漏记订单"
                )
                return
            record = candidates[0]
            record.var_fill_ts_iso = utc_now()
            record.var_fill_price = fill_price
            record.var_fill_price_source = "manual_input"
            record.var_fill_price_estimated = True
            with contextlib.suppress(ValueError):
                self._pending_variational_strategy_order_keys.remove(record.trade_key)
            self._maybe_record_slippage_stats(record)
            payload = record.to_payload()
        await self.append_order_log("variational_fill_manual", payload)
        self._manual_fill_recovery_status = (
            f"手动补记成功：{record.side} {record.qty} @ {fill_price}；回到顶部按 Enter 启动策略"
        )
        self.logger.warning(
            "Var成交价手动补记: key=%s 方向=%s 数量=%s 价格=%s（非成交WS）",
            record.trade_key,
            record.side,
            decimal_to_str(record.qty),
            decimal_to_str(fill_price),
        )

    @staticmethod
    def _make_round_exit_signal(
        *,
        position_qty: Decimal,
        target_qty: Decimal,
        spread_pct: Decimal,
        entry_edge_pct: Decimal,
    ) -> GradientSignal:
        is_short = position_qty < 0
        return GradientSignal(
            action="open" if is_short else "close",
            section=StrategySection.OPEN if is_short else StrategySection.CLOSE,
            spread_pct=spread_pct,
            threshold_pct=entry_edge_pct,
            target_qty=target_qty,
            current_qty=position_qty,
            delta_qty=abs(target_qty - position_qty),
            source="round_exit_guard",
        )

    def _select_strategy_signal(
        self,
        long_edge_pct: Decimal | None,
        short_edge_pct: Decimal | None,
        position_qty: Decimal,
    ) -> GradientSignal | None:
        self._latest_long_edge_pct = long_edge_pct
        self._latest_short_edge_pct = short_edge_pct
        if not self.gradient_strategy.enabled or self.gradient_strategy.validation_errors():
            return None
        ledger = self.round_exit_ledger
        if self._round_ledger_synced and ledger.position_qty != position_qty:
            if self._strategy_order_in_flight:
                self._round_position_mismatch_sig = None
                self._round_position_mismatch_since = None
                return None
            mismatch_sig = (decimal_to_str(ledger.position_qty), decimal_to_str(position_qty))
            now = time.monotonic()
            if self._round_position_mismatch_sig != mismatch_sig:
                self._round_position_mismatch_sig = mismatch_sig
                self._round_position_mismatch_since = now
                return None
            if (
                self._round_position_mismatch_since is None
                or now - self._round_position_mismatch_since < ROUND_POSITION_MISMATCH_CONFIRM_SECONDS
            ):
                return None
            halt_reason = (
                f"本轮账本与真实仓位持续不一致: ledger={decimal_to_str(ledger.position_qty)} "
                f"live={decimal_to_str(position_qty)} confirm={ROUND_POSITION_MISMATCH_CONFIRM_SECONDS:.1f}s"
            )
            # Keep the first hard-stop reason. A missing fill event can first trip
            # the more precise round-accounting timeout before this position check.
            if not self._strategy_halted:
                self._strategy_halted = True
                self._halt_reason = halt_reason
                self.logger.error("策略已停止（需手动检查）：%s", self._halt_reason)
            return None
        self._round_position_mismatch_sig = None
        self._round_position_mismatch_since = None

        regular = self.gradient_strategy.evaluate(long_edge_pct, short_edge_pct, position_qty)
        if regular is not None:
            return regular

        if self._round_ledger_synced and ledger.position_qty != 0:
            executable_edge = long_edge_pct if position_qty < 0 else short_edge_pct
            exit_target = self.gradient_strategy.round_exit_target(
                long_edge_pct,
                short_edge_pct,
                position_qty,
            )
            if executable_edge is not None and exit_target is not None:
                decision = ledger.decision(
                    executable_edge,
                    live_position_qty=None,
                    order_qty=self.gradient_strategy.single_order_qty,
                    target_position_qty=exit_target,
                )
                if decision.action == "close" and ledger.entry_edge_actual is not None:
                    return self._make_round_exit_signal(
                        position_qty=position_qty,
                        target_qty=exit_target,
                        spread_pct=executable_edge,
                        entry_edge_pct=ledger.entry_edge_actual,
                    )

        return None

    def _evaluate_gradient_signal(
        self,
        open_spread_pct: Decimal | None,
        close_spread_pct: Decimal | None,
        position_qty: Decimal,
        *,
        active_quote_key: tuple[str, int] | None = None,
        now_monotonic: float | None = None,
        dispatch: bool = True,
    ) -> GradientSignal | None:
        now = time.monotonic() if now_monotonic is None else now_monotonic
        signal = self._select_strategy_signal(
            open_spread_pct,
            close_spread_pct,
            position_qty,
        )
        if signal is None:
            self._last_gradient_signal_sig = None
            self._reset_pending_signal_confirmation()
            return None
        signal_sig = signal.signature()
        if signal_sig == self._last_gradient_signal_sig or self._strategy_order_in_flight:
            return signal
        # 信号已达但被闸拦（已停止 / 两腿不平衡）→ 节流打日志，免得排查半天没线索。
        if not self._strategy_order_allowed():
            reason = "已停止" if self._strategy_halted else "两腿不平衡"
            block_sig = (
                reason,
                self._halt_reason if self._strategy_halted else (
                    decimal_to_str(self._cached_position_qty),
                    decimal_to_str(self._current_lighter_position()),
                ),
            )
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
                self._reset_pending_signal_confirmation()
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
        # 抗尖刺：同一信号必须连续成立 1 秒，并覆盖至少 4 个不同的主动报价序号。
        if signal_sig == self._pending_signal_sig:
            self._pending_signal_count += 1
        else:
            self._pending_signal_sig = signal_sig
            self._pending_signal_count = 1
            self._pending_signal_started_at = now
            self._pending_signal_quote_keys.clear()
            self._pending_signal_first_edge = signal.spread_pct
            self._pending_signal_confirmed_edge = None
        if active_quote_key is not None and active_quote_key not in self._pending_signal_quote_keys:
            self._pending_signal_quote_keys.add(active_quote_key)
            self._pending_signal_edge_samples.append(signal.spread_pct)
            if len(self._pending_signal_edge_samples) > 500:
                self._pending_signal_edge_samples = self._pending_signal_edge_samples[-250:]
                self._pending_signal_quote_keys = {active_quote_key}
        elapsed = 0.0 if self._pending_signal_started_at is None else now - self._pending_signal_started_at
        edge_range = (
            max(self._pending_signal_edge_samples) - min(self._pending_signal_edge_samples)
            if self._pending_signal_edge_samples
            else None
        )
        fast_stable = (
            elapsed >= SIGNAL_FAST_CONFIRM_SECONDS
            and len(self._pending_signal_quote_keys) >= SIGNAL_FAST_CONFIRM_MIN_QUOTES
            and edge_range is not None
            and edge_range <= SIGNAL_FAST_MAX_EDGE_RANGE_PCT
        )
        standard_confirmed = (
            elapsed >= SIGNAL_CONFIRM_SECONDS
            and len(self._pending_signal_quote_keys) >= SIGNAL_CONFIRM_MIN_QUOTES
        )
        if not fast_stable and not standard_confirmed:
            return signal
        self._pending_signal_ready = True
        self._pending_signal_confirmed_edge = signal.spread_pct
        if dispatch:
            self._dispatch_confirmed_gradient_signal(signal, elapsed)
        return signal

    def _dispatch_confirmed_gradient_signal(self, signal: GradientSignal, elapsed: float) -> None:
        self._pending_signal_ready = False
        self._stat_signal_confirm.add(elapsed * 1000)
        signal_sig = signal.signature()
        source_label = "万一" if signal.source == "round_exit_guard" else "梯度"
        section_label = "Long" if signal.section == StrategySection.OPEN else "Short"
        order_side = "买V/卖L" if signal.action == "open" else "卖V/买L"
        order_qty = self._quantize_to_lighter_lot(
            min(self.gradient_strategy.single_order_qty, signal.delta_qty)
        )
        self.logger.info(
            "信号[%s/%s] Edge=%+.4f%% 阈值=%+.4f%% | 当前仓=%s 目标仓=%s | 本单=%s %s",
            source_label,
            section_label,
            signal.spread_pct,
            signal.threshold_pct,
            f"{signal.current_qty:+}",
            f"{signal.target_qty:+}",
            order_side,
            f"{order_qty:+}",
        )
        record = self._handle_new_gradient_signal(signal)
        if record is not None:
            self._strategy_order_in_flight = True
            self._last_gradient_signal_sig = signal_sig
            self._stat_fired += 1

    def _reset_pending_signal_confirmation(self) -> None:
        self._pending_signal_sig = None
        self._pending_signal_count = 0
        self._pending_signal_started_at = None
        self._pending_signal_quote_keys.clear()
        self._pending_signal_edge_samples.clear()
        self._pending_signal_ready = False
        self._pending_signal_first_edge = None
        self._pending_signal_confirmed_edge = None

    def _current_active_quote_key(self) -> tuple[str, int] | None:
        asset = self.runtime.monitor.current_quote_asset
        quote = self.runtime.monitor.quotes.get(asset) if asset else None
        active = quote.get("active_quote") if isinstance(quote, dict) else None
        if not isinstance(active, dict) or not active.get("sessionId"):
            return None
        try:
            return str(active["sessionId"]), int(active["sequence"])
        except (KeyError, TypeError, ValueError):
            return None

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
        # 主动报价到达即重新计算价差；200ms timeout 仅作为遗漏事件时的兜底。
        self._strategy_wake.set()

    def _on_dom_quote(self, payload: dict[str, Any]) -> None:
        ts = payload.get("ts") or payload.get("capturedAtMs") or payload.get("timestamp")
        self._feed_quote_comparator("dom", payload.get("bid"), payload.get("ask"), ts, browser_wall_ms=self._epoch_ms(ts))
        self._last_dom_bid = to_decimal(payload.get("bid"))
        self._last_dom_ask = to_decimal(payload.get("ask"))
        self._last_dom_at = time.monotonic()

    def _on_dom_notice(self, payload: dict[str, Any]) -> None:
        self._schedule_background_task(self._log_dom_notice(payload))

    async def _log_dom_notice(self, payload: dict[str, Any]) -> None:
        title = re.sub(r"\s+", " ", str(payload.get("title") or "")).strip() or "页面通知"
        message = re.sub(r"\s+", " ", str(payload.get("message") or "")).strip()
        controls = payload.get("controls")
        if isinstance(controls, list):
            controls_text = ",".join(
                re.sub(r"\s+", " ", str(item)).strip()
                for item in controls
                if str(item).strip()
            ) or "-"
        else:
            controls_text = "-"
        try:
            state = await self.runtime.monitor.get_trading_state()
        except Exception:
            state = {}
        heartbeat_age = state.get("heartbeat_age")
        quote_age_ms = self.quote_comparator.freshness_ms("api", time.monotonic() * 1000.0)
        heartbeat_text = "-" if heartbeat_age is None else f"{float(heartbeat_age):.1f}s"
        quote_text = "-" if quote_age_ms is None else f"{float(quote_age_ms) / 1000.0:.1f}s"
        var_position = self._cached_position_qty
        lighter_position = (
            self._current_lighter_position() if self._current_lighter_position_known() else None
        )
        detail = f" | {message}" if message else ""
        self.logger.warning(
            "Var页面通知: %s%s | 控件=%s | 心跳Age=%s 报价Age=%s Var仓=%s Lit仓=%s",
            title,
            detail,
            controls_text,
            heartbeat_text,
            quote_text,
            decimal_to_str(var_position),
            decimal_to_str(lighter_position),
        )

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
        return cross_spread_percentages(var_bid, var_ask, sig_bid, sig_ask, PRICE_MAPPING_RATIO)

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
        self._sync_round_ledger_with_live_position(pos)
        long_edge_pct, short_edge_pct = await self._compute_signal_spreads()
        signal = self._evaluate_gradient_signal(
            long_edge_pct,
            short_edge_pct,
            pos,
            active_quote_key=self._current_active_quote_key(),
            dispatch=False,
        )
        self._latest_gradient_signal = signal
        if signal is None or not self._pending_signal_ready:
            return

        # 真正创建两腿订单前再读取一次两边报价；方向、阈值或目标任一变化都重新确认。
        fresh_long_edge, fresh_short_edge = await self._compute_signal_spreads()
        fresh_signal = self._select_strategy_signal(
            fresh_long_edge,
            fresh_short_edge,
            pos,
        )
        if fresh_signal is None or fresh_signal.signature() != signal.signature():
            self.logger.info(
                "信号发送前复核取消: confirmed=%s fresh=%s",
                signal.signature(),
                fresh_signal.signature() if fresh_signal is not None else None,
            )
            self._reset_pending_signal_confirmation()
            self._latest_gradient_signal = fresh_signal
            return
        elapsed = 0.0 if self._pending_signal_started_at is None else time.monotonic() - self._pending_signal_started_at
        self._latest_gradient_signal = fresh_signal
        self._dispatch_confirmed_gradient_signal(fresh_signal, elapsed)

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
        if prepare_sig == self._last_prepared_order_sig or prepare_sig in self._pending_prepare_sigs:
            return
        self._pending_prepare_sigs.add(prepare_sig)
        self._browser_order_queue.submit(BrowserOrderCommand(side=side, qty=qty, dry_run=True, prepare_only=True))

    def _schedule_browser_activity_click(self) -> bool:
        if (
            not self.browser_order_broker.is_connected()
            or self._strategy_order_in_flight
            or self._latest_gradient_signal is not None
            or self._pending_signal_sig is not None
            or self._pending_signal_ready
            or self._manual_fill_recovery_busy
            or self._manual_fill_price_buffer is not None
        ):
            return False
        side = self._browser_activity_next_side
        self._browser_order_queue.submit(
            BrowserOrderCommand(
                side=side,
                qty=self.gradient_strategy.single_order_qty,
                dry_run=True,
                activity_only=True,
            )
        )
        return True

    async def browser_activity_loop(self) -> None:
        while not self.stop_flag:
            await asyncio.sleep(BROWSER_ACTIVITY_INTERVAL_SECONDS)
            if self.stop_flag:
                return
            self._schedule_browser_activity_click()

    async def _send_browser_order_task(self, command: BrowserOrderCommand) -> None:
        if command.activity_only:
            await self._send_browser_activity_click(command)
            return
        if command.prepare_only:
            await self._send_prepare_browser_order(command)
            return
        await self._send_browser_order(command)

    async def _send_browser_activity_click(self, command: BrowserOrderCommand) -> None:
        if (
            self._strategy_order_in_flight
            or self._latest_gradient_signal is not None
            or self._pending_signal_sig is not None
            or self._pending_signal_ready
        ):
            self.logger.info("浏览器保活动作取消: 策略信号已出现")
            return
        try:
            result = await self.browser_order_broker.place_order(
                command,
                timeout=self._browser_order_timeout(command),
            )
        except Exception as exc:
            self.logger.info("浏览器保活动作跳过: side=%s error=%s", command.side, exc)
            return
        if not result.get("ok"):
            self.logger.info(
                "浏览器保活动作未执行: side=%s error=%s",
                command.side,
                result.get("error") or result.get("blockedReason") or "unknown",
            )
            return
        side = "sell" if command.side.strip().lower() == "sell" else "buy"
        self._browser_activity_next_side = "buy" if side == "sell" else "sell"
        self._prepared_order_side = side
        self._last_prepared_order_sig = None
        self.logger.info("浏览器保活点击: %s", side.upper())

    @staticmethod
    def _browser_order_timeout(command: BrowserOrderCommand) -> float:
        # broker 等待 = 页面内回执上限 + 余量（选方向/输入/提交轮询），保证不早于页面返回。
        return command.timeout_ms / 1000.0 + BROWSER_ORDER_BROKER_MARGIN_SECONDS

    async def _send_prepare_browser_order(self, command: BrowserOrderCommand) -> None:
        side = "sell" if command.side.strip().lower() == "sell" else "buy"
        prepare_sig = (side, format(command.qty, "f"))
        try:
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
                self._last_prepared_order_sig = prepare_sig
                self._prepared_order_side = side
        finally:
            self._pending_prepare_sigs.discard(prepare_sig)

    async def run_browser_smoke_test(self) -> None:
        self._start_session_logging()
        self.logger.info("会话目录: %s", self.run_dir.resolve())
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
            hints.append("已在 disabled 后等待恢复并重新检查")
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
            "logged_at": cst_now_iso(),
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
            await asyncio.to_thread(self.browser_smoke_file.parent.mkdir, parents=True, exist_ok=True)
            await asyncio.to_thread(self._append_line, self.browser_smoke_file, line)

    async def _append_browser_smoke_log(
        self,
        step_name: str,
        command: BrowserOrderCommand,
        result: dict[str, Any],
        elapsed_ms: float,
    ) -> None:
        row = {
            "event": "browser_smoke_step",
            "logged_at": cst_now_iso(),
            "step": step_name,
            "elapsed_ms": elapsed_ms,
            "command": command.to_payload(),
            "result": result,
            "summary": json.loads(self._browser_order_result_summary(result)),
        }
        line = json.dumps(row, ensure_ascii=False, default=str) + "\n"
        async with self._order_write_lock:
            await asyncio.to_thread(self.browser_smoke_file.parent.mkdir, parents=True, exist_ok=True)
            await asyncio.to_thread(self._append_line, self.browser_smoke_file, line)

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
                    "Var下单失败: 方向=%s 数量=%s 模拟=%s 错误=%s",
                    side,
                    decimal_to_str(command.qty),
                    command.dry_run,
                    exc,
                )
                if is_live_strategy_order:
                    self._halt_strategy(f"Variational 提交失败: {exc}")
                return
            if not result.get("ok"):
                self.logger.warning("Var下单诊断: %s", self._browser_order_result_summary(result))
                await self._record_var_order_error(command.trade_key, str(result.get("error") or result.get("blockedReason") or "browser_order_failed"))
                if is_live_strategy_order:
                    reason = str(result.get("error") or result.get("blockedReason") or "browser_order_failed")
                    self._halt_strategy(f"Variational 提交失败: {reason}")
            if result.get("ok"):
                await self._record_var_submit_result(command.trade_key, result)
                self._prepared_order_side = side
                self._last_prepared_order_sig = (side, format(command.qty, "f"))
                # 下单后确认：Var 单边仅刷新仓位；对冲模式在 10s 内确认两腿平衡，否则硬停。
                if is_live_strategy_order:
                    await self._confirm_hedge_or_halt(self._cached_position_qty)
                    if not self._strategy_halted:
                        await self._wait_for_round_accounting(command.trade_key)
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
        merged_fill_payload: dict[str, Any] | None = None
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
            if record.var_submit_order_id:
                orphan = next(
                    (
                        item
                        for item in self.records.values()
                        if item is not record and item.trade_id == record.var_submit_order_id
                    ),
                    None,
                )
                if orphan is not None and orphan.var_fill_price is not None:
                    record.trade_id = orphan.trade_id
                    record.last_variational_status = orphan.last_variational_status
                    record.var_fill_ts_iso = orphan.var_fill_ts_iso
                    record.var_fill_price = orphan.var_fill_price
                    record.var_fill_price_source = "trade_ws"
                    record.var_fill_price_estimated = False
                    self.records.pop(orphan.trade_key, None)
                    with contextlib.suppress(ValueError):
                        self.record_order.remove(orphan.trade_key)
                    with contextlib.suppress(ValueError):
                        self._pending_variational_strategy_order_keys.remove(record.trade_key)
                    self._maybe_record_slippage_stats(record)
                    merged_fill_payload = record.to_payload()
            payload = record.to_payload()
        await self.append_order_log("variational_order_submitted", payload)
        if merged_fill_payload is not None:
            await self.append_order_log("variational_fill_id_merged", merged_fill_payload)
            self.logger.info(
                "Var成交按订单ID延迟归并: id=%s key=%s",
                record.var_submit_order_id,
                record.trade_key,
            )

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

        def fmt_fill_edge(stat: RunningStat) -> str:
            if stat.n == 0:
                return "- | 总和 - / 0笔"
            return f"均 {stat.avg():+.4f}% | 总和 {stat.total:+.4f}% / {stat.n}笔"

        grid = Table.grid(expand=True)
        grid.add_column(ratio=1)
        session_cashflow, session_qty, session_count = self._session_execution_summary()
        latest_round = (
            self.round_exit_ledger.completed_rounds[-1]
            if self.round_exit_ledger.completed_rounds
            else None
        )
        if is_zh:
            grid.add_row(f"策略下单: {self._stat_fired}   两腿成交: {self._stat_both_filled}")
            grid.add_row(
                f"累计收益: {session_cashflow:+.4f}u / {session_count}笔 | "
                f"成交量 {session_qty:g} {self.ticker or ''}"
            )
            if latest_round is None:
                grid.add_row("最近完成周期收益: -")
            else:
                quote_text = (
                    f"{'毛收益' if latest_round.quote_pnl_exact else '约'} "
                    f"{latest_round.estimated_quote_pnl:+.4f}u"
                    if latest_round.estimated_quote_pnl is not None
                    else "金额不可用"
                )
                grid.add_row(
                    f"最近完成周期 第{latest_round.round_id}个: "
                    f"入 {latest_round.entry_edge_actual:+.4f}% → "
                    f"平 {latest_round.close_edge_actual:+.4f}% | "
                    f"收益 {latest_round.edge_pnl:+.4f}% ({quote_text}·未扣费)"
                )
            grid.add_row(f"Long 实际成交Edge: {fmt_fill_edge(self._stat_long_fill_edge)}")
            grid.add_row(f"Short 实际成交Edge: {fmt_fill_edge(self._stat_short_fill_edge)}")
            grid.add_row(f"信号确认时长: {fmt_ms(self._stat_signal_confirm)}")
            grid.add_row(f"Var 延迟(信号→成交): {fmt_ms(self._stat_var_latency)}")
            grid.add_row(f"  其中方向切换: {fmt_ms(self._stat_var_side_switch)}")
            grid.add_row(f"Lighter 延迟(信号→WS成交): {fmt_ms(self._stat_lighter_latency)}")
            grid.add_row(f"V 滑点·200ms主动报价(有利+): {fmt_pct(self._stat_var_slip)}")
            grid.add_row(f"V 滑点·DOM约1s报价(有利+): {fmt_pct(self._stat_var_slip_dom)}")
            grid.add_row(f"L 滑点(有利+): {fmt_pct(self._stat_lighter_slip)}")
            title = "统计（本地会话）"
        else:
            grid.add_row(f"orders: {self._stat_fired}   both-filled: {self._stat_both_filled}")
            grid.add_row(
                f"session fill cashflow: {session_cashflow:+.4f}u | "
                f"{session_qty:g} {self.ticker or ''} / {session_count} fills "
                "(includes open inventory; before fees)"
            )
            if latest_round is None:
                grid.add_row("latest completed cycle PnL: -")
            else:
                quote_text = (
                    f"{'gross' if latest_round.quote_pnl_exact else 'est.'} {latest_round.estimated_quote_pnl:+.4f}u"
                    if latest_round.estimated_quote_pnl is not None
                    else "quote unavailable"
                )
                grid.add_row(
                    f"latest completed cycle {latest_round.round_id}: "
                    f"entry {latest_round.entry_edge_actual:+.4f}% → "
                    f"close {latest_round.close_edge_actual:+.4f}% | "
                    f"PnL {latest_round.edge_pnl:+.4f}% ({quote_text}; before fees)"
                )
            grid.add_row(f"Long actual fill edge: {fmt_fill_edge(self._stat_long_fill_edge)}")
            grid.add_row(f"Short actual fill edge: {fmt_fill_edge(self._stat_short_fill_edge)}")
            grid.add_row(f"signal confirm: {fmt_ms(self._stat_signal_confirm)}")
            grid.add_row(f"Var latency(signal->fill): {fmt_ms(self._stat_var_latency)}")
            grid.add_row(f"  side switch: {fmt_ms(self._stat_var_side_switch)}")
            grid.add_row(f"Lighter latency(signal->wsfill): {fmt_ms(self._stat_lighter_latency)}")
            grid.add_row(f"V slippage/200ms active quote(+good): {fmt_pct(self._stat_var_slip)}")
            grid.add_row(f"V slippage/DOM ~1s quote(+good): {fmt_pct(self._stat_var_slip_dom)}")
            grid.add_row(f"L slippage(+good): {fmt_pct(self._stat_lighter_slip)}")
            title = "Stats (session)"
        return Panel(grid, title=title, border_style="magenta")

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
            action_text = "买入 Var" if signal.action == "open" else "卖出 Var"
            source_text = "万一Edge退出" if signal.source == "round_exit_guard" else "普通梯度"
            # 显示"这一笔实际会下的量"（单次下单量与剩余的较小值，对齐 lot），
            # 剩余总差额单列，避免与单次下单量混淆。
            order_qty = self._quantize_to_lighter_lot(
                min(self.gradient_strategy.single_order_qty, signal.delta_qty)
            )
            signal_text = (
                f"[bold yellow]{source_text} · {action_text} {order_qty:f} {self.ticker}[/] "
                f"目标 {signal.target_qty:f} | 当前 {signal.current_qty:f} | "
                f"剩余 {signal.delta_qty:f} | 触发 {signal.threshold_pct:f}%"
            )
        strategy_table.add_row(self._strategy_enabled_row_text())
        if self._strategy_halted:
            if self._manual_fill_price_buffer is not None:
                recovery_text = f"[bold yellow]手动补记 Var 成交价: {self._manual_fill_price_buffer or '_'}[/]"
            elif self._manual_fill_recovery_status:
                recovery_text = f"[yellow]{self._manual_fill_recovery_status}[/]"
            else:
                recovery_text = "[yellow]按 r 手动补记漏记的 Var 成交价[/]"
            strategy_table.add_row(recovery_text)
        validation_errors = self.gradient_strategy.validation_errors()
        for validation_error in validation_errors:
            strategy_table.add_row(f"[bold red]配置错误：{validation_error}[/]")
        strategy_table.add_row(self._strategy_single_order_qty_row_text())
        strategy_table.add_row(f"当前净仓位: {position_qty:f} {self.ticker} | 信号: {signal_text}")
        strategy_table.add_row("")
        strategy_table.add_row(
            "Long 梯度（Long Edge >= 阈值）: 做多 Var / 做空 Lighter | "
            f"Long Edge: {self._fmt_pct(open_spread_pct)}"
        )
        for index, _row in enumerate(self.gradient_strategy.open_rows):
            strategy_table.add_row(self._strategy_row_text(StrategySection.OPEN, index))
        strategy_table.add_row("")
        strategy_table.add_row(
            "Short 梯度（Short Edge <= 阈值）: 做空 Var / 做多 Lighter | "
            f"Short Edge: {self._fmt_pct(close_spread_pct)}"
        )
        for index, _row in enumerate(self.gradient_strategy.close_rows):
            strategy_table.add_row(self._strategy_row_text(StrategySection.CLOSE, index))
        strategy_table.add_row("")
        strategy_table.add_row("按键: ↑↓移动  ←→切字段  数字/.编辑  +/-增删梯度  Enter确认/停止时启动  r补记成交价  Esc取消  Tab切页  q退出")
        return Panel(strategy_table, title="触发策略", border_style="magenta")

    def _strategy_enabled_row_text(self) -> str:
        selected = self.gradient_strategy.enabled_selected()
        cursor = ">" if selected else " "
        if self._strategy_halted:
            icon = "[ ]"
            status_text = "解除停止并启动策略"
        else:
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
            PRICE_MAPPING_RATIO,
        )
        # 采集和落库由独立的 spread_sampling_loop 负责；渲染只读最新状态。
        active_asset = quote_asset or self.variational_ticker or self.ticker or "UNKNOWN"

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
            _realized_pnl, _unrealized_pnl, _pnl_pending, position_qty, _avg_open_pct = self.compute_pnl()
            session_cashflow, _session_qty, session_count = self._session_execution_summary()

        # 廉价兜底刷新缓存仓位（仅 listener，无 DOM 往返），防外部手动改仓。
        await self._refresh_position_cache(allow_dom=False)
        display_position_qty = self._cached_position_qty if self._cached_position_qty is not None else position_qty
        executable_close_edge_pct: Decimal | None = None
        position_edge_pnl_pct: Decimal | None = None
        if display_position_qty > 0:
            executable_close_edge_pct = short_var_long_lighter_pct
        elif display_position_qty < 0:
            executable_close_edge_pct = long_var_short_lighter_pct
        ledger = self.round_exit_ledger
        ledger_matches_position = (
            self._round_ledger_synced
            and display_position_qty != 0
            and ledger.position_qty != 0
            and abs(display_position_qty - ledger.position_qty) <= self._balance_tolerance()
        )
        actual_entry_edge_pct = ledger.entry_edge_actual if ledger_matches_position else None
        actual_close_edge_pct = ledger.close_edge_actual if ledger_matches_position else None
        position_edge_pnl_pct = ledger.realized_edge_pnl if ledger_matches_position else None
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

        session_color = "green" if session_cashflow >= 0 else "red"
        session_value = "0" if session_cashflow == 0 else f"{session_cashflow:+.4f}u"
        latest_round = ledger.completed_rounds[-1] if ledger.completed_rounds else None
        if latest_round is None:
            latest_round_text = "最近完成周期 -" if is_zh else "latest cycle -"
        else:
            round_color = "green" if latest_round.edge_pnl >= 0 else "red"
            quote_prefix = "" if latest_round.quote_pnl_exact else ("约" if is_zh else "est. ")
            quote_text = (
                f"{quote_prefix}{latest_round.estimated_quote_pnl:+.4f}u"
                if latest_round.estimated_quote_pnl is not None
                else "金额--"
            )
            if is_zh:
                latest_round_text = (
                    f"最近完成周期 [{round_color}]{latest_round.edge_pnl:+.4f}%({quote_text})[/]"
                )
            else:
                latest_round_text = (
                    f"latest cycle [{round_color}]{latest_round.edge_pnl:+.4f}%({quote_text})[/]"
                )
        if is_zh:
            pnl_text = (
                f"累计收益 [{session_color}]{session_value}[/]/{session_count}笔 | "
                f"{latest_round_text}"
            )
        else:
            pnl_text = (
                f"cumulative return [{session_color}]{session_value}[/]/{session_count} fills | "
                f"{latest_round_text}"
            )

        pos_label = "持仓" if is_zh else "pos"
        lighter_position_known = self._current_lighter_position_known()
        if display_position_qty == 0:
            position_summary = f"{pos_label}0{self.ticker or ''}"
            position_detail = ""
        elif actual_entry_edge_pct is None:
            close_market_text = self._fmt_pct(executable_close_edge_pct)
            position_summary = f"{pos_label}{display_position_qty:+.3f}{self.ticker}"
            position_detail = f"入场Edge -- | 实际平仓Edge -- | 可执行平仓Edge {close_market_text}"
        else:
            edge_color = "green" if position_edge_pnl_pct is not None and position_edge_pnl_pct >= 0 else "red"
            actual_close_text = "--" if actual_close_edge_pct is None else f"{actual_close_edge_pct:+.4f}%"
            edge_pnl_text = "--" if position_edge_pnl_pct is None else f"{position_edge_pnl_pct:+.4f}%"
            close_market_text = self._fmt_pct(executable_close_edge_pct)
            position_summary = f"{pos_label}{display_position_qty:+.3f}{self.ticker}"
            position_detail = (
                f"入场Edge {actual_entry_edge_pct:+.4f}% | 实际平仓Edge {actual_close_text} | "
                f"可执行平仓Edge {close_market_text} | "
                f"[{edge_color}]已平Edge收益 {edge_pnl_text}[/]"
            )
        active_signal = self._latest_gradient_signal
        if active_signal is not None and active_signal.source == "round_exit_guard":
            position_detail += f" | [bold yellow]万一Edge→{active_signal.target_qty:+.3f}[/]"
        lighter_position_summary = ""
        if self.args.auto_hedge:
            lit_label = "Lit仓" if is_zh else "Litpos"
            lit_position = f"{self._current_lighter_position():+.3f}" if lighter_position_known else "--"
            lighter_position_summary = f"  {lit_label}{lit_position}"
        halt_text = ""
        if self._strategy_halted:
            stop_label = "停止" if is_zh else "HALT"
            halt_text = f"[bold red]⛔{stop_label}: {self._halt_reason}[/] | "
        # 两腿不平衡会静默拦住策略下单，显式标出来免得找不到原因。
        imbalance_text = ""
        imbalanced = False
        if (
            self.args.auto_hedge
            and not self._strategy_halted
            and self._cached_position_qty is not None
            and lighter_position_known
        ):
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
            fully_ready = self._lighter_ready and self.lighter_order_book_ready and lighter_position_known
            if fully_ready:
                ready_label = "Lighter就绪" if is_zh else "Lighter ready"
                lit_ready_text = f" | [bold green]✅{ready_label}[/]"
            else:
                warming_label = "Lighter仓位/盘口同步中" if is_zh else "Lighter position/book syncing"
                lit_ready_text = f" | [yellow]…{warming_label}[/]"
        usdc_mid = None
        usdc_basis_pct = None
        usdc_book_bps = None
        if self.binance_usdc_bid is not None and self.binance_usdc_ask is not None:
            usdc_mid = (self.binance_usdc_bid + self.binance_usdc_ask) / Decimal("2")
            usdc_basis_pct = (usdc_mid - Decimal("1")) * Decimal("100")
            if usdc_mid > 0:
                usdc_book_bps = (self.binance_usdc_ask - self.binance_usdc_bid) / usdc_mid * Decimal("10000")
        usdc_age = None
        if self.binance_usdc_updated_at is not None:
            usdc_age = max(0.0, time.monotonic() - self.binance_usdc_updated_at)
        if usdc_mid is None:
            usdc_line = f"Binance USDC/USDT: {self.binance_usdc_status} | 仅观察，不参与下单"
        else:
            age_text = f"{usdc_age:.1f}s" if usdc_age is not None else "-"
            usdc_line = (
                f"Binance USDC/USDT  Bid {self.binance_usdc_bid:.5f}  Ask {self.binance_usdc_ask:.5f}  "
                f"Mid {usdc_mid:.5f}  基差 {usdc_basis_pct:+.4f}%  盘口 {usdc_book_bps:.2f}bp  "
                f"Age {age_text} | 仅观察"
            )
        detail_text = f" | {position_detail}" if position_detail else ""
        header = Panel(
            f"[bold cyan]{usdc_line}[/]  [bold]{position_summary}{lighter_position_summary}[/]\n"
            f"[bold]{header_title}[/bold] | [bold]{self.ticker}[/bold] | "
            f"[bold {hedge_color}]{auto_hedge_label}={hedge_text}[/] | "
            f"{halt_text}{disconnect_text}{imbalance_text}{pnl_text}{detail_text} | {now_cst_display()}{lit_ready_text}",
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
                _side_zh, side_en = self._direction_labels(row.side)
                if is_zh:
                    side_display = (
                        f"{self._compact_direction_label(row.side)} "
                        f"({self._order_time_label(row)})"
                    )
                else:
                    side_display = side_en
                var_fill_display = payload["variational_filled_price"] or "-"
                if row.var_fill_price_estimated and var_fill_display != "-":
                    source_label = "手动补记" if row.var_fill_price_source == "manual_input" else "非WS"
                    var_fill_display = f"{var_fill_display} [yellow]({source_label})[/]"
                orders_table.add_row(
                    trade_display,
                    side_display,
                    self._fmt_price(row.qty),
                    var_fill_display,
                    payload["lighter_filled_price"] or "-",
                    self._style_fill_value_by_direction(
                        self._fmt_price(fill_diff), row.side
                    ),
                    self._fmt_fill_pct_with_leg_slippage(
                        fill_diff_pct,
                        row.var_slippage_pct(),
                        row.lighter_slippage_pct(),
                        row.side,
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

        if self.current_page == 3:
            history_width = max(20, min(200, self.dashboard_console.size.width - 14))
            await self._refresh_history_cache_if_due(active_asset, history_width)
            hourly_rows = self._hourly_rows_cache[:hourly_row_limit]
        else:
            hourly_rows = []
        if not hourly_rows:
            hourly_table.add_row(no_hourly_text, "-", "-", "-", "-", "-", "-")
        else:
            for bucket in hourly_rows:
                hour_key = bucket["hour_key"]
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
            page_hint = "[1]实时 [2]策略 [3]历史 Tab切换 q退出"
            page_label = f"第{self.current_page}页"
        else:
            page_hint = "[1]Live [2]Strategy [3]History Tab q"
            page_label = f"Page {self.current_page}"
        footer = Panel(f"{page_hint}  |  {page_label}", border_style="dim")

        if self.current_page == 2:
            stats_panel = self._render_stats_panel(is_zh)
            body = Table.grid(expand=True)
            body.add_column(ratio=3)
            body.add_column(ratio=2)
            body.add_row(strategy_panel, stats_panel)
            return Group(header, body, footer)
        if self.current_page == 3:
            stats_panel = self._render_stats_panel(is_zh)
            trend_panel = self._render_spread_trend_panel(is_zh)
            return Group(header, hourly_table, stats_panel, trend_panel, footer)
        return Group(header, quote_table, spread_table, orders_table, footer)

    async def export_trade_records_csv(self) -> None:
        if self.trade_records_csv_file is None:
            return
        now = time.monotonic()
        if now - self._trade_records_exported_at < TRADE_RECORDS_EXPORT_INTERVAL_SECONDS:
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
                        "variational_fill_price_source": payload["variational_fill_price_source"],
                        "variational_fill_price_estimated": payload["variational_fill_price_estimated"],
                        "variational_filled_at": payload["variational_filled_at"],
                        "lighter_order_side": payload["lighter_order_side"],
                        "lighter_client_order_id": payload["lighter_client_order_id"],
                        "lighter_filled_price": payload["lighter_filled_price"],
                        "lighter_filled_qty": payload["lighter_filled_qty"],
                        "lighter_filled_quote": payload["lighter_filled_quote"],
                        "lighter_filled_at": payload["lighter_filled_at"],
                        "lighter_limit_price": payload["lighter_limit_price"],
                        "lighter_tx_hash": payload["lighter_tx_hash"],
                        "trigger_spread_pct": payload["trigger_spread_pct"],
                        "first_hit_spread_pct": payload["first_hit_spread_pct"],
                        "confirmed_spread_pct": payload["confirmed_spread_pct"],
                        "preflight_spread_pct": payload["preflight_spread_pct"],
                        "strategy_action": payload["strategy_action"],
                        "strategy_signal_source": payload["strategy_signal_source"],
                        "strategy_section": payload["strategy_section"],
                        "strategy_threshold_pct": payload["strategy_threshold_pct"],
                        "signal_long_edge_pct": payload["signal_long_edge_pct"],
                        "signal_short_edge_pct": payload["signal_short_edge_pct"],
                        "strategy_target_qty": payload["strategy_target_qty"],
                        "strategy_current_qty": payload["strategy_current_qty"],
                        "strategy_created_at_ms": payload["strategy_created_at_ms"],
                        "round_id": payload["round_id"],
                        "round_fill_role": payload["round_fill_role"],
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
            self._trade_records_exported_at = now
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
            "variational_fill_price_source",
            "variational_fill_price_estimated",
            "variational_filled_at",
            "lighter_order_side",
            "lighter_client_order_id",
            "lighter_filled_price",
            "lighter_filled_qty",
            "lighter_filled_quote",
            "lighter_filled_at",
            "lighter_limit_price",
            "lighter_tx_hash",
            "trigger_spread_pct",
            "first_hit_spread_pct",
            "confirmed_spread_pct",
            "preflight_spread_pct",
            "strategy_action",
            "strategy_signal_source",
            "strategy_section",
            "strategy_threshold_pct",
            "signal_long_edge_pct",
            "signal_short_edge_pct",
            "strategy_target_qty",
            "strategy_current_qty",
            "strategy_created_at_ms",
            "round_id",
            "round_fill_role",
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
            self._trade_records_exported_at = now

    @staticmethod
    def _write_csv_rows(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_name(f".{path.name}.tmp")
        with tmp_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        os.replace(tmp_path, path)

    def _dashboard_resize_in_progress(self, now: float) -> bool:
        size = self.dashboard_console.size
        current = (size.width, size.height)
        if self._dashboard_size is None:
            self._dashboard_size = current
            return False
        if current != self._dashboard_size:
            self._dashboard_size = current
            self._dashboard_resize_deadline = now + DASHBOARD_RESIZE_SETTLE_SECONDS
            return True
        return now < self._dashboard_resize_deadline

    async def _sample_current_spread(self) -> None:
        var_bid, var_ask, quote_asset = await self.get_variational_best_bid_ask(self.variational_ticker)
        lighter_bid, lighter_ask = await self.get_lighter_best_bid_ask()
        vwap_bid, vwap_ask = await self.get_lighter_depth_quote(Decimal(str(self.args.depth_notional)))
        sig_bid = vwap_bid if vwap_bid is not None else lighter_bid
        sig_ask = vwap_ask if vwap_ask is not None else lighter_ask
        long_edge, short_edge = cross_spread_percentages(
            var_bid,
            var_ask,
            sig_bid,
            sig_ask,
            PRICE_MAPPING_RATIO,
        )
        asset = quote_asset or self.variational_ticker or self.ticker or "UNKNOWN"
        await asyncio.to_thread(
            self._record_cross_spreads,
            asset,
            var_bid,
            var_ask,
            sig_bid,
            sig_ask,
            long_edge,
            short_edge,
        )
        await self._refresh_spread_stats_if_due(asset)

    async def spread_sampling_loop(self) -> None:
        while not self.stop_flag:
            started = time.monotonic()
            try:
                await self._sample_current_spread()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger.exception("价差采集异常")
            remaining = SPREAD_SAMPLE_INTERVAL_SECONDS - (time.monotonic() - started)
            await asyncio.sleep(max(0.0, remaining))

    async def dashboard_loop(self) -> None:
        refresh_interval = DASHBOARD_REFRESH_SECONDS
        initial_render = await self.render_dashboard()
        self._dashboard_resize_in_progress(time.monotonic())
        await self.export_trade_records_csv()
        with DifferentialScreen(self.dashboard_console) as screen:
            screen.update(initial_render)
            while not self.stop_flag:
                keyboard_wake = False
                try:
                    await asyncio.wait_for(self._dashboard_wake.wait(), timeout=refresh_interval)
                    keyboard_wake = True
                except asyncio.TimeoutError:
                    pass
                if keyboard_wake:
                    self._dashboard_wake.clear()
                if self._dashboard_resize_in_progress(time.monotonic()):
                    continue
                screen.update(await self.render_dashboard())
                await self.export_trade_records_csv()

    async def run(self) -> None:
        self._start_session_logging()
        self.logger.info("会话目录: %s", self.run_dir.resolve())
        self.setup_signal_handlers()
        self.setup_keyboard()
        dashboard_started = self.spread_dashboard.start()
        self.binance_usdc_task = asyncio.create_task(self.binance_usdc_book_loop())
        await self.runtime.start()
        self.browser_order_server = await run_browser_order_broker(
            FORWARDER_HOST,
            BROWSER_ORDER_BROKER_PORT,
            self.browser_order_broker,
        )
        self._browser_order_queue.start()
        self.browser_activity_task = asyncio.create_task(self.browser_activity_loop())
        self._schedule_prepare_browser_order()
        self.print_startup_next_steps()
        self.logger.info(
            "价差图表%s: http://%s:%s",
            "已启动" if dashboard_started else "已存在/端口占用",
            FORWARDER_HOST,
            self.args.dashboard_port,
        )
        self.logger.info(
            "监听: Var=ws://%s:%s,%s:%s 浏览器下单=ws://%s:%s",
            FORWARDER_HOST,
            FORWARDER_WS_PORT,
            FORWARDER_HOST,
            FORWARDER_REST_PORT,
            FORWARDER_HOST,
            BROWSER_ORDER_BROKER_PORT,
        )

        await self.wait_for_variational_ready()
        self.logger.info("Variational心跳正常")
        await self.warm_lighter()
        initial_asset = await self.wait_for_ticker_resolution()
        await self.activate_asset(initial_asset, reason="startup")

        self.trade_event_cursor = await self.runtime.monitor.get_latest_trade_event_seq()
        self.logger.info("开始跟踪Var成交: seq>%s", self.trade_event_cursor)

        self.trade_task = asyncio.create_task(self.trade_loop())
        self.spread_sampling_task = asyncio.create_task(self.spread_sampling_loop())
        self.dashboard_task = asyncio.create_task(self.dashboard_loop())
        self._strategy_task = asyncio.create_task(self.strategy_loop())
        while not self.stop_flag:
            await asyncio.sleep(0.25)

    async def close(self) -> None:
        self.stop_flag = True
        self.restore_keyboard()

        if self.binance_usdc_task and not self.binance_usdc_task.done():
            self.binance_usdc_task.cancel()
            await asyncio.gather(self.binance_usdc_task, return_exceptions=True)

        if self.browser_activity_task and not self.browser_activity_task.done():
            self.browser_activity_task.cancel()
            await asyncio.gather(self.browser_activity_task, return_exceptions=True)

        if self.dashboard_task and not self.dashboard_task.done():
            await asyncio.gather(self.dashboard_task, return_exceptions=True)

        if self.spread_sampling_task and not self.spread_sampling_task.done():
            await asyncio.gather(self.spread_sampling_task, return_exceptions=True)

        if self.trade_task and not self.trade_task.done():
            self.trade_task.cancel()
            await asyncio.gather(self.trade_task, return_exceptions=True)

        if self._strategy_task and not self._strategy_task.done():
            self._strategy_task.cancel()
            await asyncio.gather(self._strategy_task, return_exceptions=True)

        await self._browser_order_queue.stop()
        await self.lighter_gateway.stop()

        if self.browser_order_server is not None:
            self.browser_order_server.close()
            await self.browser_order_server.wait_closed()
            self.browser_order_server = None

        await self.runtime.stop()
        self.spread_dashboard.stop()
        self.spread_store.close()


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
        "--spread-db",
        type=str,
        default=None,
        help="SQLite spread history path (default: log/spread_history.sqlite3)",
    )
    parser.add_argument(
        "--dashboard-port",
        type=int,
        default=SPREAD_DASHBOARD_PORT,
        help="Local spread dashboard port (default: 8780)",
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
    memory_monitor = start_memory_monitor()
    try:
        runtime = VariationalToLighterRuntime(args)
        try:
            if args.browser_smoke_test:
                await runtime.run_browser_smoke_test()
            else:
                await runtime.run()
        finally:
            await runtime.close()
    finally:
        stop_memory_monitor(memory_monitor)


def start_memory_monitor() -> tuple[subprocess.Popen[bytes], Any] | None:
    enabled = os.environ.get(MEMORY_MONITOR_ENABLED_ENV, "1").strip().lower()
    if enabled in {"0", "false", "no", "off"} or sys.platform != "linux" or not Path("/proc/meminfo").is_file():
        return None

    repo_dir = Path(__file__).resolve().parent
    monitor_script = repo_dir / "scripts" / "monitor-memory.sh"
    if not monitor_script.is_file():
        print(f"Memory monitor was not started: missing {monitor_script}", file=sys.stderr)
        return None

    started_at = cst_now().strftime("%Y-%m-%d_%H-%M-%S_%f_UTC+8")
    output_dir = repo_dir / "log" / "memory" / started_at
    output_dir.mkdir(parents=True, exist_ok=True)
    monitor_log = (output_dir / "monitor.log").open("ab", buffering=0)
    env = os.environ.copy()
    env["MEMORY_MONITOR_OUTPUT_DIR"] = str(output_dir)
    try:
        process = subprocess.Popen(
            [str(monitor_script)],
            cwd=repo_dir,
            env=env,
            stdout=monitor_log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    except Exception as exc:
        monitor_log.close()
        print(f"Memory monitor was not started: {exc}", file=sys.stderr)
        return None
    print(f"Memory monitor: {output_dir}")
    return process, monitor_log


def stop_memory_monitor(monitor: tuple[subprocess.Popen[bytes], Any] | None) -> None:
    if monitor is None:
        return
    process, monitor_log = monitor
    try:
        if process.poll() is None:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=3)
    finally:
        monitor_log.close()


def main() -> None:
    asyncio.run(_amain())


if __name__ == "__main__":
    main()
