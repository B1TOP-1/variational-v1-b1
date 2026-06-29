from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Awaitable, Callable, Generic, TypeVar

import websockets


T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class BrowserOrderCommand:
    side: str
    qty: Decimal
    dry_run: bool = True
    prepare_only: bool = False
    trade_key: str | None = None
    submit_method: str = "js_click"
    timeout_ms: int = 20000
    wait_after_side_ms: int = 30
    wait_before_input_ms: int = 0
    wait_after_input_ms: int = 10
    wait_before_submit_ms: int = 0
    wait_after_click_ms: int = 0

    @property
    def action(self) -> str:
        return "prepare_browser_order" if self.prepare_only else "place_browser_order"

    def to_payload(self) -> dict[str, Any]:
        side = "sell" if self.side.strip().lower() == "sell" else "buy"
        return {
            "side": side,
            "qty": format(self.qty, "f"),
            "dryRun": bool(self.dry_run),
            "prepareOnly": bool(self.prepare_only),
            "simulateOnly": False,
            "timeoutMs": int(self.timeout_ms),
            "waitAfterSideMs": int(self.wait_after_side_ms),
            "waitBeforeInputMs": int(self.wait_before_input_ms),
            "waitAfterInputMs": int(self.wait_after_input_ms),
            "waitBeforeSubmitMs": int(self.wait_before_submit_ms),
            "waitAfterClickMs": int(self.wait_after_click_ms),
            "submitMethod": str(self.submit_method),
            "skipInputWhenMatched": True,
        }


class BrowserOrderBroker:
    def __init__(self) -> None:
        self._websocket: Any = None
        self._pending: dict[str, asyncio.Future[dict[str, Any]]] = {}

    def is_connected(self) -> bool:
        return self._websocket is not None

    def pending_count(self) -> int:
        return len(self._pending)

    async def on_connect(self, websocket: Any) -> None:
        self._websocket = websocket
        try:
            async for raw in websocket:
                await self.handle_raw_message(raw)
        finally:
            if self._websocket is websocket:
                self._websocket = None
                for future in self._pending.values():
                    if not future.done():
                        future.set_exception(RuntimeError("browser order broker disconnected"))
                self._pending.clear()

    async def handle_raw_message(self, raw: str | bytes) -> None:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            return
        if payload.get("event") == "hello":
            return
        message_id = str(payload.get("id", ""))
        future = self._pending.pop(message_id, None)
        if future is not None and not future.done():
            future.set_result(payload)

    async def place_order(self, command: BrowserOrderCommand, timeout: float = 25.0) -> dict[str, Any]:
        if self._websocket is None:
            raise RuntimeError("browser order broker not connected")
        message_id = str(uuid.uuid4())
        loop = asyncio.get_running_loop()
        future: asyncio.Future[dict[str, Any]] = loop.create_future()
        self._pending[message_id] = future
        try:
            await self._websocket.send(
                json.dumps(
                    {
                        "id": message_id,
                        "action": command.action,
                        "payload": command.to_payload(),
                    },
                    ensure_ascii=True,
                )
            )
            return await asyncio.wait_for(future, timeout=timeout)
        finally:
            self._pending.pop(message_id, None)


class BrowserOrderDispatchQueue(Generic[T]):
    def __init__(self, handler: Callable[[T], Awaitable[None]]) -> None:
        self._handler = handler
        self._queue: asyncio.Queue[T] = asyncio.Queue()
        self._task: asyncio.Task[None] | None = None

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run())

    def submit(self, item: T) -> None:
        self._queue.put_nowait(item)

    async def join(self) -> None:
        await self._queue.join()

    async def stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        await asyncio.gather(self._task, return_exceptions=True)
        self._task = None

    async def _run(self) -> None:
        while True:
            item = await self._queue.get()
            try:
                await self._handler(item)
            finally:
                self._queue.task_done()


async def run_browser_order_broker(host: str, port: int, broker: BrowserOrderBroker) -> Any:
    async def handler(websocket: Any) -> None:
        await broker.on_connect(websocket)

    return await websockets.serve(handler, host, port, max_size=None, ping_interval=20, ping_timeout=20)
