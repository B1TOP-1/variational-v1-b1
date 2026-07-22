from __future__ import annotations

import asyncio
import contextlib
import json
import os
from collections.abc import Awaitable, Callable
from decimal import Decimal
from pathlib import Path
from typing import Any


GatewayEventHandler = Callable[[dict[str, Any]], Awaitable[None]]
GatewayLogHandler = Callable[[str], None]


class RustLighterGateway:
    def __init__(
        self,
        *,
        execution_enabled: bool,
        event_handler: GatewayEventHandler,
        log_handler: GatewayLogHandler | None = None,
        binary: Path | None = None,
    ) -> None:
        self.execution_enabled = execution_enabled
        self.event_handler = event_handler
        self.log_handler = log_handler
        self.binary = binary or self.resolve_binary()
        self.process: asyncio.subprocess.Process | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._stderr_task: asyncio.Task[None] | None = None
        self._pending: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._command_sequence = 0
        self._write_lock = asyncio.Lock()
        self.health_ready = asyncio.Event()
        self.book_ready = asyncio.Event()
        self.position_ready = asyncio.Event()
        self.symbol: str | None = None
        self.market_id: int | None = None
        self.min_base_amount: Decimal | None = None
        self.size_multiplier: int | None = None
        self.price_multiplier: int | None = None
        self.best_bid: Decimal | None = None
        self.best_ask: Decimal | None = None
        self.vwap_bid: Decimal | None = None
        self.vwap_ask: Decimal | None = None
        self.positions: dict[str, Decimal] = {}

    @staticmethod
    def resolve_binary() -> Path:
        configured = os.getenv("LIGHTER_RUST_GATEWAY_BIN", "").strip()
        if configured:
            return Path(configured).expanduser().resolve()
        candidates = [
            Path.home() / "git/bybot/bybot/lighter/target/release/variational_lighter_gateway",
            Path.home() / "git/bybot/bybot/lighter/target/debug/variational_lighter_gateway",
        ]
        return next((candidate for candidate in candidates if candidate.is_file()), candidates[0])

    async def start(self, timeout: float = 30.0) -> None:
        if self.process is not None and self.process.returncode is None:
            return
        if not self.binary.is_file():
            raise RuntimeError(
                f"Lighter Rust gateway not found: {self.binary}. "
                "Build it with `cargo build --release --bin variational_lighter_gateway`."
            )
        env = os.environ.copy()
        env["LIGHTER_EXECUTION_ENABLED"] = "1" if self.execution_enabled else "0"
        if not env.get("LIGHTER_PRIVATE_KEY") and env.get("API_KEY_PRIVATE_KEY"):
            env["LIGHTER_PRIVATE_KEY"] = env["API_KEY_PRIVATE_KEY"]
        self.process = await asyncio.create_subprocess_exec(
            str(self.binary),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )
        self._reader_task = asyncio.create_task(self._read_stdout())
        self._stderr_task = asyncio.create_task(self._read_stderr())
        health_wait = asyncio.create_task(self.health_ready.wait())
        exit_wait = asyncio.create_task(self.process.wait())
        try:
            done, _ = await asyncio.wait(
                {health_wait, exit_wait},
                timeout=timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if health_wait in done and health_wait.result():
                return
            if exit_wait in done:
                raise RuntimeError(f"Lighter Rust gateway exited during startup with code {exit_wait.result()}")
            raise TimeoutError(f"Lighter Rust gateway did not become ready within {timeout:.0f}s")
        except Exception:
            await self.stop()
            raise
        finally:
            for waiter in (health_wait, exit_wait):
                if not waiter.done():
                    waiter.cancel()
            await asyncio.gather(health_wait, exit_wait, return_exceptions=True)

    async def set_market(self, symbol: str, depth_notional: Decimal, timeout: float = 30.0) -> int:
        self.book_ready.clear()
        self.position_ready.clear()
        self.symbol = symbol.strip().upper()
        result = await self.command(
            "set_market",
            timeout=timeout,
            symbol=self.symbol,
            depth_notional=str(depth_notional),
        )
        data = result["data"]
        self.market_id = int(data["market_id"])
        self.min_base_amount = _decimal(data.get("min_base_amount"))
        self.size_multiplier = int(data["size_multiplier"])
        self.price_multiplier = int(data["price_multiplier"])
        await asyncio.wait_for(self.book_ready.wait(), timeout=timeout)
        return self.market_id

    async def place_order(
        self,
        *,
        symbol: str,
        client_order_index: int,
        signed_quantity: Decimal,
        limit_price: Decimal,
        reduce_only: bool = False,
        timeout: float = 10.0,
    ) -> str:
        result = await self.command(
            "place_order",
            timeout=timeout,
            symbol=symbol,
            client_order_id=str(client_order_index),
            client_order_index=client_order_index,
            signed_quantity=str(signed_quantity),
            limit_price=str(limit_price),
            reduce_only=reduce_only,
        )
        return str(result["data"]["tx_hash"])

    async def command(self, command_type: str, *, timeout: float = 10.0, **fields: Any) -> dict[str, Any]:
        process = self.process
        if process is None or process.returncode is not None or process.stdin is None:
            raise RuntimeError("Lighter Rust gateway is not running")
        self._command_sequence += 1
        command_id = f"py-{self._command_sequence}"
        loop = asyncio.get_running_loop()
        response: asyncio.Future[dict[str, Any]] = loop.create_future()
        self._pending[command_id] = response
        payload = json.dumps({"type": command_type, "id": command_id, **fields}, separators=(",", ":"))
        try:
            async with self._write_lock:
                process.stdin.write(payload.encode("utf-8") + b"\n")
                await process.stdin.drain()
            result = await asyncio.wait_for(response, timeout=timeout)
        finally:
            self._pending.pop(command_id, None)
        if not result.get("ok"):
            raise RuntimeError(str(result.get("error") or f"Lighter command failed: {command_type}"))
        return result

    async def stop(self) -> None:
        process = self.process
        if process is None:
            return
        if process.returncode is None:
            with contextlib.suppress(Exception):
                await self.command("shutdown", timeout=2.0)
            if process.returncode is None:
                process.terminate()
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(process.wait(), timeout=3.0)
            if process.returncode is None:
                process.kill()
                await process.wait()
        for task in (self._reader_task, self._stderr_task):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(
            *(task for task in (self._reader_task, self._stderr_task) if task is not None),
            return_exceptions=True,
        )
        self._fail_pending(RuntimeError("Lighter Rust gateway stopped"))
        self.process = None
        self.health_ready.clear()
        self.book_ready.clear()
        self.position_ready.clear()

    async def _read_stdout(self) -> None:
        assert self.process is not None and self.process.stdout is not None
        try:
            while line := await self.process.stdout.readline():
                try:
                    event = json.loads(line)
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    self._log(f"Invalid Lighter Rust gateway event: {exc}")
                    continue
                await self._handle_event(event)
        finally:
            returncode = await self.process.wait()
            self._fail_pending(RuntimeError(f"Lighter Rust gateway exited with code {returncode}"))
            self.health_ready.clear()
            self.book_ready.clear()
            self.position_ready.clear()
            self.best_bid = None
            self.best_ask = None
            self.vwap_bid = None
            self.vwap_ask = None
            await self.event_handler(
                {"type": "health", "ready": False, "error": f"gateway exited with code {returncode}"}
            )

    async def _read_stderr(self) -> None:
        assert self.process is not None and self.process.stderr is not None
        while line := await self.process.stderr.readline():
            self._log(line.decode("utf-8", errors="replace").rstrip())

    async def _handle_event(self, event: dict[str, Any]) -> None:
        event_type = event.get("type")
        if event_type == "command_result":
            pending = self._pending.get(str(event.get("id", "")))
            if pending is not None and not pending.done():
                pending.set_result(event)
            return
        if event_type == "health":
            if event.get("ready"):
                self.health_ready.set()
            else:
                self.health_ready.clear()
            return
        if event_type == "book":
            if not event.get("ready"):
                self.book_ready.clear()
                self.best_bid = None
                self.best_ask = None
                self.vwap_bid = None
                self.vwap_ask = None
                await self.event_handler(event)
                return
            self.symbol = str(event.get("symbol", "")).strip().upper()
            self.market_id = int(event["market_id"])
            self.best_bid = _decimal(event.get("bid"))
            self.best_ask = _decimal(event.get("ask"))
            self.vwap_bid = _decimal(event.get("vwap_bid"))
            self.vwap_ask = _decimal(event.get("vwap_ask"))
            if self.best_bid is not None and self.best_ask is not None:
                self.book_ready.set()
            await self.event_handler(event)
            return
        if event_type == "position":
            symbol = str(event.get("symbol", "")).strip().upper()
            quantity = _decimal(event.get("quantity"))
            if symbol and quantity is not None:
                self.positions[symbol] = quantity
                self.position_ready.set()
            await self.event_handler(event)
            return
        await self.event_handler(event)

    def _fail_pending(self, error: Exception) -> None:
        for pending in self._pending.values():
            if not pending.done():
                pending.set_exception(error)

    def _log(self, message: str) -> None:
        if message and self.log_handler is not None:
            self.log_handler(message)


def _decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None
