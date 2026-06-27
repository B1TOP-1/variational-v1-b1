import argparse
import asyncio
import contextlib
import csv
import json
import logging
import os
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
from variational.browser_order import BrowserOrderBroker, BrowserOrderCommand, run_browser_order_broker
from variational.gradient_strategy import EditableField, GradientSignal, GradientStrategyState, StrategySection

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
READY_TIMEOUT_SECONDS = 60.0
POLL_INTERVAL_SECONDS = 0.05
HEDGE_SLIPPAGE_BPS = 100.0
DASHBOARD_REFRESH_SECONDS = 1.0
DASHBOARD_ORDERS = 20
SPREAD_HISTORY_SECONDS = 3600.0
HOURLY_HISTORY_HOURS = 12
CST_TZ = timezone(timedelta(hours=8))
# Lines consumed by always-on chrome (header panel + quote/spread tables +
# the hourly/orders table frames). Remaining terminal height is split between
# the hourly-history and recent-orders rows so the dashboard fits one screen.
DASHBOARD_FIXED_OVERHEAD_LINES = 30
# Binance USDCUSDT spot price = USDT per 1 USDC; used to normalize Lighter's
# USDT-quoted prices into Variational's USDC quote so the cross-venue spread no
# longer drifts with the stablecoin basis.
BINANCE_USDCUSDT_URL = "https://api.binance.com/api/v3/ticker/price?symbol=USDCUSDT"
USDC_USDT_POLL_SECONDS = 10.0
ASSET_SWITCH_CONFIRM_TICKS = 3
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

    # USDC/USDT (USDT per 1 USDC) captured when the hedge leg fills, so the
    # directional fill spread and PnL normalize at the real fill-time rate.
    fill_usdc_usdt_rate: Decimal | None = None

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
            "fill_usdc_usdt_rate": decimal_to_str(self.fill_usdc_usdt_rate),
            "auto_hedge_enabled": self.auto_hedge_enabled,
            "hedge_error": self.hedge_error,
            "last_variational_status": self.last_variational_status,
        }


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
        self._record_lock = asyncio.Lock()
        self.cross_spread_history: deque[tuple[float, float | None, float | None]] = deque()
        self.hourly_spread_buckets: dict[int, dict[str, list[float]]] = {}
        self._asset_switch_lock = asyncio.Lock()
        self._asset_switch_candidate: str | None = None
        self._asset_switch_candidate_hits = 0

        self.trade_event_cursor = 0

        self.lighter_base_url = "https://mainnet.zklighter.elliot.ai"
        self.account_index = required_int_env("LIGHTER_ACCOUNT_INDEX")
        self.api_key_index = required_int_env("LIGHTER_API_KEY_INDEX")
        self.lighter_client: SignerClient | None = None
        self._lighter_signer_lock = asyncio.Lock()

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

        # USDT->USDC normalization (Binance USDCUSDT = USDT per 1 USDC).
        self.usdc_usdt_rate: Decimal | None = None

        # Two-page dashboard: 1 = live (quotes/signal/orders), 2 = hourly history.
        self.current_page = 1
        self._stdin_fd: int | None = None
        self._stdin_old_settings: Any = None
        self._keyboard_active = False

        self.gradient_strategy = GradientStrategyState.default()
        self._last_gradient_signal_sig: tuple[str, str, str, str, str] | None = None
        self._browser_order_task: asyncio.Task[None] | None = None

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
        elif self.current_page == 2:
            self.gradient_strategy.handle_key(key)
        elif key == "1":
            self.current_page = 1
        elif key == "2":
            self.current_page = 2

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
        """Poll Binance USDCUSDT (USDT per 1 USDC) for spread normalization."""
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
            self.lighter_client = SignerClient(
                url=self.lighter_base_url,
                account_index=self.account_index,
                api_private_keys={self.api_key_index: api_key_private_key},
            )
            err = self.lighter_client.check_client()
            if err is not None:
                raise RuntimeError(f"CheckClient error: {err}")
        return self.lighter_client

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
        self.cross_spread_history.clear()
        self.hourly_spread_buckets.clear()
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
            record.fill_usdc_usdt_rate = self.usdc_usdt_rate
            payload = record.to_payload()

        await self.append_order_log("lighter_fill", payload)

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
        except Exception as exc:
            async with self._record_lock:
                record.lighter_side = side
                record.hedge_error = str(exc)
                payload = record.to_payload()
            await self.append_order_log("lighter_error", payload)

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
        created_record: OrderLifecycle | None = None

        async with self._record_lock:
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
                )
                self.records[key] = record
                self.record_order.append(key)
                created = True
                created_record = record
            else:
                previous_status = record.last_variational_status
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
                filled_payload = record.to_payload()
            else:
                filled_payload = None

        if filled_payload is not None:
            await self.append_order_log("variational_fill", filled_payload)

        if created and created_record is not None and self.args.auto_hedge:
            await self.place_lighter_order(created_record)

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
    def _fill_diff_by_direction(
        side: str,
        var_fill_price: Decimal | None,
        lighter_fill_price: Decimal | None,
        rate: Decimal | None = None,
    ) -> tuple[Decimal | None, Decimal | None]:
        # Only the derived spread is normalized: the Lighter (USDT) leg is
        # converted to USDC for the subtraction. Displayed fill prices are
        # untouched elsewhere — these are the real venue fills.
        lighter = lighter_fill_price
        if lighter is not None and rate and rate > 0:
            lighter = lighter / rate
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
        ordered = sorted(values)
        if len(ordered) == 1:
            return float(ordered[0])
        rank = (pct / 100.0) * (len(ordered) - 1)
        low = int(rank)
        high = min(low + 1, len(ordered) - 1)
        frac = rank - low
        return float(ordered[low] + (ordered[high] - ordered[low]) * frac)

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
        """Direction-adjusted, USDC-normalized per-unit captured spread for a fully
        filled round (both legs). None if either leg is missing."""
        if record.var_fill_price is None or record.lighter_fill_price is None:
            return None
        lighter = record.lighter_fill_price
        rate = record.fill_usdc_usdt_rate or self.usdc_usdt_rate
        if rate and rate > 0:
            lighter = lighter / rate
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
            rate = record.fill_usdc_usdt_rate or self.usdc_usdt_rate
            _, pct = self._fill_diff_by_direction(
                record.side, record.var_fill_price, record.lighter_fill_price, rate
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

    def _evaluate_gradient_signal(
        self,
        open_spread_pct: Decimal | None,
        close_spread_pct: Decimal | None,
        position_qty: Decimal,
    ) -> GradientSignal | None:
        signal = self.gradient_strategy.evaluate(open_spread_pct, close_spread_pct, position_qty)
        if signal is None:
            self._last_gradient_signal_sig = None
            return None
        signal_sig = signal.signature()
        if signal_sig != self._last_gradient_signal_sig:
            self._last_gradient_signal_sig = signal_sig
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
            self._schedule_browser_order_dry_run(signal)
        return signal

    def _schedule_browser_order_dry_run(self, signal: GradientSignal) -> None:
        if self._browser_order_task is not None and not self._browser_order_task.done():
            return
        self._browser_order_task = asyncio.create_task(self._send_browser_order_dry_run(signal))

    async def _send_browser_order_dry_run(self, signal: GradientSignal) -> None:
        side = "buy" if signal.action == "open" else "sell"
        command = BrowserOrderCommand(side=side, qty=signal.delta_qty, dry_run=True)
        try:
            result = await self.browser_order_broker.place_order(command, timeout=25.0)
        except Exception as exc:
            self.logger.warning(
                "Browser order dry-run failed: action=%s side=%s qty=%s error=%s",
                signal.action,
                side,
                decimal_to_str(signal.delta_qty),
                exc,
            )
            return
        self.logger.info(
            "Browser order dry-run result: action=%s side=%s qty=%s ok=%s blocked=%s error=%s",
            signal.action,
            side,
            decimal_to_str(signal.delta_qty),
            result.get("ok"),
            result.get("blockedReason"),
            result.get("error"),
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
            signal_text = (
                f"[bold yellow]{action_text} {signal.delta_qty:f} {self.ticker}[/] "
                f"目标 {signal.target_qty:f} | 当前 {signal.current_qty:f} | "
                f"触发 {signal.threshold_pct:f}%"
            )
        strategy_table.add_row(self._strategy_enabled_row_text())
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
        row_text = f"{cursor} {index + 1}. {threshold_cell} -> {qty_cell}"
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

        # Depth-aware signal: use the VWAP to fill --depth-notional along the
        # Lighter book (not just top-of-book), then normalize that USDT VWAP into
        # Variational's USDC quote. Var leg stays top-of-book (no Var depth feed).
        depth_notional = Decimal(str(self.args.depth_notional))
        vwap_bid, vwap_ask = await self.get_lighter_depth_quote(depth_notional)
        sig_bid = vwap_bid if vwap_bid is not None else lighter_bid
        sig_ask = vwap_ask if vwap_ask is not None else lighter_ask
        rate = self.usdc_usdt_rate
        if rate and rate > 0:
            sig_bid_usdc = sig_bid / rate if sig_bid is not None else None
            sig_ask_usdc = sig_ask / rate if sig_ask is not None else None
        else:
            sig_bid_usdc, sig_ask_usdc = sig_bid, sig_ask

        long_var_short_lighter_pct = spread_percent(spread_value(var_ask, sig_bid_usdc), var_ask)
        short_var_long_lighter_pct = spread_percent(spread_value(sig_ask_usdc, var_bid), sig_ask_usdc)
        self._record_cross_spreads(
            long_var_short_lighter_pct,
            short_var_long_lighter_pct,
        )
        self._record_hourly_spread(
            long_var_short_lighter_pct,
            short_var_long_lighter_pct,
        )

        long_pct_median_5m = self._median_cross_spread(5 * 60, long_side=True)
        long_pct_median_30m = self._median_cross_spread(30 * 60, long_side=True)
        long_pct_median_1h = self._median_cross_spread(60 * 60, long_side=True)
        short_pct_median_5m = self._median_cross_spread(5 * 60, long_side=False)
        short_pct_median_30m = self._median_cross_spread(30 * 60, long_side=False)
        short_pct_median_1h = self._median_cross_spread(60 * 60, long_side=False)

        long_p90_5m = self._percentile_cross_spread(5 * 60, long_side=True, pct=90)
        long_p10_5m = self._percentile_cross_spread(5 * 60, long_side=True, pct=10)
        long_p90_30m = self._percentile_cross_spread(30 * 60, long_side=True, pct=90)
        long_p10_30m = self._percentile_cross_spread(30 * 60, long_side=True, pct=10)
        long_p90_1h = self._percentile_cross_spread(60 * 60, long_side=True, pct=90)
        long_p10_1h = self._percentile_cross_spread(60 * 60, long_side=True, pct=10)
        short_p90_5m = self._percentile_cross_spread(5 * 60, long_side=False, pct=90)
        short_p10_5m = self._percentile_cross_spread(5 * 60, long_side=False, pct=10)
        short_p90_30m = self._percentile_cross_spread(30 * 60, long_side=False, pct=90)
        short_p10_30m = self._percentile_cross_spread(30 * 60, long_side=False, pct=10)
        short_p90_1h = self._percentile_cross_spread(60 * 60, long_side=False, pct=90)
        short_p10_1h = self._percentile_cross_spread(60 * 60, long_side=False, pct=10)

        hourly_row_limit, order_row_limit = self._adaptive_row_limits()
        async with self._record_lock:
            recent_keys = list(self.record_order)[-order_row_limit:]
            rows = [self.records[key] for key in reversed(recent_keys) if key in self.records]
            realized_pnl, unrealized_pnl, _pnl_pending, position_qty, avg_open_pct = self.compute_pnl()

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
        col_fill_diff = "成交价差(归一)" if is_zh else "Fill Diff (norm)"
        col_fill_diff_pct = "成交价差%(归一)" if is_zh else "Fill Diff % (norm)"
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
        if avg_open_pct is None or position_qty == 0:
            pos_text = f"{pos_label} 0"
        else:
            pos_color = "green" if avg_open_pct >= 0 else "red"
            pos_text = f"{pos_label}{position_qty:+.3f}{self.ticker} [{pos_color}]{avg_open_pct:+.4f}%[/]"
        header = Panel(
            f"[bold]{header_title}[/bold] | [bold]{self.ticker}[/bold] | "
            f"[bold {hedge_color}]{auto_hedge_label}={hedge_text}[/] | "
            f"{pnl_text} | {pos_text} | USDC/USDT={fx_text} | {now_cst_display()}",
            border_style="cyan",
        )

        quote_table = Table(title=quote_title, show_header=True, expand=True)
        quote_table.add_column(col_exchange, style="bold")
        quote_table.add_column(col_bid, justify="right")
        quote_table.add_column(col_ask, justify="right")
        quote_table.add_column(col_book_spread, justify="right")
        quote_table.add_column(col_book_spread_pct, justify="right")
        quote_table.add_row(
            f"{variational_label} ({quote_asset or self.variational_ticker})",
            self._fmt_price(var_bid),
            self._fmt_price(var_ask),
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
                fill_diff, fill_diff_pct = self._fill_diff_by_direction(
                    row.side,
                    row.var_fill_price,
                    row.lighter_fill_price,
                    row.fill_usdc_usdt_rate or self.usdc_usdt_rate,
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
                    self._fmt_pct(fill_diff_pct),
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

        signal = self._evaluate_gradient_signal(
            long_var_short_lighter_pct,
            short_var_long_lighter_pct,
            position_qty,
        )
        strategy_panel = self._render_strategy_panel(
            long_var_short_lighter_pct,
            short_var_long_lighter_pct,
            position_qty,
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
            return Group(header, hourly_table, strategy_panel, footer)
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
                fill_diff, fill_diff_pct = self._fill_diff_by_direction(
                    record.side,
                    record.var_fill_price,
                    record.lighter_fill_price,
                    record.fill_usdc_usdt_rate or self.usdc_usdt_rate,
                )
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
                        "fill_usdc_usdt_rate": payload["fill_usdc_usdt_rate"],
                        "fill_diff_var_minus_lighter_norm": decimal_to_str(fill_diff),
                        "fill_diff_pct_vs_var_norm": decimal_to_str(fill_diff_pct),
                        "auto_hedge_enabled": payload["auto_hedge_enabled"],
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
            "fill_usdc_usdt_rate",
            "fill_diff_var_minus_lighter_norm",
            "fill_diff_pct_vs_var_norm",
            "auto_hedge_enabled",
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
        self.initialize_lighter_client()
        initial_asset = await self.wait_for_ticker_resolution()
        await self.activate_asset(initial_asset, reason="startup")

        self.trade_event_cursor = await self.runtime.monitor.get_latest_trade_event_seq()
        self.logger.info("Tracking new Variational trade events from seq>%s", self.trade_event_cursor)

        self.trade_task = asyncio.create_task(self.trade_loop())
        self.dashboard_task = asyncio.create_task(self.dashboard_loop())

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

        if self.lighter_ws_task and not self.lighter_ws_task.done():
            self.lighter_ws_task.cancel()
            await asyncio.gather(self.lighter_ws_task, return_exceptions=True)

        if self._browser_order_task and not self._browser_order_task.done():
            self._browser_order_task.cancel()
            await asyncio.gather(self._browser_order_task, return_exceptions=True)

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


def parse_args() -> argparse.Namespace:
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
    parser.set_defaults(auto_hedge=True)
    return parser.parse_args()


async def _amain() -> None:
    load_dotenv()
    args = parse_args()
    runtime = VariationalToLighterRuntime(args)
    try:
        await runtime.run()
    finally:
        await runtime.close()


def main() -> None:
    asyncio.run(_amain())


if __name__ == "__main__":
    main()
