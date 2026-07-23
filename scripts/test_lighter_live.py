#!/usr/bin/env python3
"""End-to-end diagnostic for the production Rust Lighter gateway.

Read-only mode validates authentication, account snapshot, position selection,
and public order-book updates. Live mode additionally submits a tightly capped
order, waits for execution and position WebSocket events, then restores the
starting position unless --no-restore is supplied.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from variational.lighter_rust import RustLighterGateway  # noqa: E402


LIVE_CONFIRMATION = "I_UNDERSTAND_THIS_SENDS_REAL_ORDERS"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Test the production Rust Lighter gateway end to end.")
    parser.add_argument("--env-file", default=str(ROOT / ".env"))
    parser.add_argument("--symbol", default="BTC")
    parser.add_argument("--depth-notional", type=Decimal, default=Decimal("1000"))
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument("--live", action="store_true", help="Submit a real order.")
    parser.add_argument("--confirm", default="", help=f"Required with --live: {LIVE_CONFIRMATION}")
    parser.add_argument("--side", choices=("buy", "sell"), default="buy")
    parser.add_argument("--qty", type=Decimal, default=None, help="Defaults to one minimum size step.")
    parser.add_argument("--slippage-bps", type=Decimal, default=Decimal("10"))
    parser.add_argument("--max-notional-usd", type=Decimal, default=Decimal("25"))
    parser.add_argument(
        "--restore",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Restore the starting position after a fill (default: enabled).",
    )
    return parser.parse_args()


def quantize_quantity(quantity: Decimal, size_multiplier: int) -> Decimal:
    if size_multiplier <= 0:
        raise ValueError("invalid size multiplier")
    step = Decimal(1) / Decimal(size_multiplier)
    return quantity.quantize(step, rounding=ROUND_DOWN)


def aggressive_limit(side: str, bid: Decimal, ask: Decimal, slippage_bps: Decimal) -> Decimal:
    slippage = slippage_bps / Decimal("10000")
    return ask * (Decimal(1) + slippage) if side == "buy" else bid * (Decimal(1) - slippage)


class Probe:
    def __init__(self) -> None:
        self.events: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.position_events: list[tuple[str, Decimal]] = []

    async def handle(self, event: dict[str, Any]) -> None:
        if event.get("type") == "position":
            symbol = str(event.get("symbol", "")).upper()
            quantity = Decimal(str(event["quantity"]))
            self.position_events.append((symbol, quantity))
            print(f"EVENT position symbol={symbol} quantity={quantity}", flush=True)
        elif event.get("type") == "execution":
            fields = " ".join(
                f"{name}={event.get(name)}"
                for name in ("kind", "client_order_index", "order_index", "quantity", "price", "reason")
                if event.get(name) is not None
            )
            print(f"EVENT execution {fields}", flush=True)
        elif event.get("type") == "book" and not event.get("ready"):
            print("EVENT book ready=false", flush=True)
        await self.events.put(event)

    def drain(self) -> None:
        while not self.events.empty():
            self.events.get_nowait()


async def wait_for_order(
    probe: Probe,
    gateway: RustLighterGateway,
    *,
    symbol: str,
    client_order_index: int,
    requested_quantity: Decimal,
    timeout: float,
) -> Decimal:
    filled = Decimal(0)
    order_index: int | None = None
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            event = await asyncio.wait_for(probe.events.get(), deadline - time.monotonic())
        except asyncio.TimeoutError:
            break
        if event.get("type") != "execution" or int(event.get("client_order_index", -1)) != client_order_index:
            continue
        kind = str(event.get("kind", ""))
        if event.get("order_index") is not None:
            order_index = int(event["order_index"])
        if kind == "fill":
            filled += abs(Decimal(str(event.get("quantity", "0"))))
            if filled >= requested_quantity:
                return filled
        if kind == "rejected":
            raise RuntimeError(f"order rejected: {event.get('reason')}")
        if kind == "canceled":
            return filled

    if order_index is not None:
        print(f"TIMEOUT canceling residual order_index={order_index}", flush=True)
        await gateway.command(
            "cancel_order",
            timeout=timeout,
            symbol=symbol,
            client_order_id=str(client_order_index),
            client_order_index=client_order_index,
            order_index=order_index,
        )
        if filled > 0:
            return filled
    raise TimeoutError(f"no terminal execution event within {timeout:.0f}s; filled={filled}")


async def wait_for_position_push(
    probe: Probe,
    *,
    symbol: str,
    expected: Decimal,
    start_index: int,
    tolerance: Decimal,
    timeout: float,
) -> Decimal:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for event_symbol, quantity in probe.position_events[start_index:]:
            if event_symbol == symbol and abs(quantity - expected) <= tolerance:
                return quantity
        await asyncio.sleep(0.05)
    observed = [str(quantity) for event_symbol, quantity in probe.position_events[start_index:] if event_symbol == symbol]
    raise TimeoutError(f"position push did not reach {expected}; observed={observed}")


async def submit_and_verify(
    gateway: RustLighterGateway,
    probe: Probe,
    *,
    symbol: str,
    side: str,
    quantity: Decimal,
    starting_position: Decimal,
    slippage_bps: Decimal,
    timeout: float,
    client_order_index: int,
) -> tuple[Decimal, Decimal]:
    bid, ask = gateway.best_bid, gateway.best_ask
    if bid is None or ask is None:
        raise RuntimeError("order book is not ready")
    limit_price = aggressive_limit(side, bid, ask, slippage_bps)
    signed_quantity = quantity if side == "buy" else -quantity
    position_event_index = len(probe.position_events)
    probe.drain()
    print(
        f"SUBMIT side={side} quantity={quantity} limit={limit_price} "
        f"client_order_index={client_order_index}",
        flush=True,
    )
    tx_hash = await gateway.place_order(
        symbol=symbol,
        client_order_index=client_order_index,
        signed_quantity=signed_quantity,
        limit_price=limit_price,
        reduce_only=False,
        timeout=timeout,
    )
    print(f"ACK tx_hash={tx_hash}", flush=True)
    filled = await wait_for_order(
        probe,
        gateway,
        symbol=symbol,
        client_order_index=client_order_index,
        requested_quantity=quantity,
        timeout=timeout,
    )
    if filled <= 0:
        raise RuntimeError("real order was accepted but did not fill; position push was not exercised")
    expected = starting_position + (filled if side == "buy" else -filled)
    observed = await wait_for_position_push(
        probe,
        symbol=symbol,
        expected=expected,
        start_index=position_event_index,
        tolerance=Decimal("0.5") / Decimal(gateway.size_multiplier or 1),
        timeout=timeout,
    )
    print(f"VERIFY filled={filled} expected_position={expected} pushed_position={observed}", flush=True)
    return filled, observed


async def run(args: argparse.Namespace) -> None:
    load_dotenv(dotenv_path=args.env_file, override=False)
    required = ("LIGHTER_PRIVATE_KEY", "LIGHTER_ACCOUNT_INDEX", "LIGHTER_API_KEY_INDEX")
    missing = [name for name in required if not os.getenv(name, "").strip()]
    if missing:
        raise RuntimeError(f"missing environment variables: {','.join(missing)}")
    if args.live and args.confirm != LIVE_CONFIRMATION:
        raise RuntimeError(f"--live requires --confirm {LIVE_CONFIRMATION}")

    symbol = args.symbol.strip().upper()
    probe = Probe()
    gateway = RustLighterGateway(
        execution_enabled=True,
        event_handler=probe.handle,
        log_handler=lambda message: print(f"RUST {message}", file=sys.stderr, flush=True),
    )
    try:
        print(f"START binary={gateway.binary} symbol={symbol} live={args.live}", flush=True)
        await gateway.start(timeout=args.timeout)
        print("CHECK auth_and_account_snapshot=OK", flush=True)
        market_id = await gateway.set_market(symbol, args.depth_notional, timeout=args.timeout)
        if gateway.best_bid is None or gateway.best_ask is None or symbol not in gateway.positions:
            raise RuntimeError("market did not provide both order book and authoritative position")
        starting_position = gateway.positions[symbol]
        print(
            f"CHECK market=OK market_id={market_id} bid={gateway.best_bid} ask={gateway.best_ask} "
            f"position={starting_position} min_base_amount={gateway.min_base_amount} "
            f"size_multiplier={gateway.size_multiplier}",
            flush=True,
        )
        if not args.live:
            print("PASS read_only: no order was sent", flush=True)
            return

        size_multiplier = gateway.size_multiplier or 0
        minimum = gateway.min_base_amount or (Decimal(1) / Decimal(size_multiplier))
        quantity = quantize_quantity(args.qty or minimum, size_multiplier)
        if quantity < minimum or quantity <= 0:
            raise RuntimeError(f"quantity {quantity} is below minimum {minimum}")
        reference_price = gateway.best_ask if args.side == "buy" else gateway.best_bid
        notional = quantity * reference_price
        if notional > args.max_notional_usd:
            raise RuntimeError(f"notional {notional} exceeds --max-notional-usd {args.max_notional_usd}")

        client_order_index = time.time_ns() // 1_000_000
        filled, changed_position = await submit_and_verify(
            gateway,
            probe,
            symbol=symbol,
            side=args.side,
            quantity=quantity,
            starting_position=starting_position,
            slippage_bps=args.slippage_bps,
            timeout=args.timeout,
            client_order_index=client_order_index,
        )
        if args.restore:
            restore_side = "sell" if args.side == "buy" else "buy"
            current_position = gateway.positions.get(symbol)
            if current_position != changed_position:
                raise RuntimeError(
                    f"position changed before restore: verified={changed_position} current={current_position}"
                )
            restored, final_position = await submit_and_verify(
                gateway,
                probe,
                symbol=symbol,
                side=restore_side,
                quantity=filled,
                starting_position=changed_position,
                slippage_bps=args.slippage_bps,
                timeout=args.timeout,
                client_order_index=client_order_index + 1,
            )
            if restored != filled or abs(final_position - starting_position) > Decimal("0.5") / Decimal(size_multiplier):
                raise RuntimeError(
                    f"restore mismatch: start={starting_position} final={final_position} "
                    f"opened={filled} restored={restored}"
                )
            print(f"PASS live_round_trip: final_position={final_position}", flush=True)
        else:
            print(f"PASS live_one_way: final_position={changed_position} (not restored)", flush=True)
    finally:
        await gateway.stop()


def main() -> None:
    args = parse_args()
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        raise SystemExit(130)
    except Exception as exc:
        print(f"FAIL {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
