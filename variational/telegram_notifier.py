from __future__ import annotations

import asyncio
import logging
import urllib.parse
import urllib.request


class TelegramNotifier:
    """Best-effort Telegram delivery that never blocks the trading event loop."""

    def __init__(
        self,
        bot_token: str,
        chat_id: str,
        *,
        logger: logging.Logger,
        queue_size: int = 100,
        request_timeout: float = 5.0,
    ) -> None:
        self.bot_token = bot_token.strip()
        self.chat_id = chat_id.strip()
        self.logger = logger
        self.request_timeout = request_timeout
        self.enabled = bool(self.bot_token and self.chat_id)
        self._queue: asyncio.Queue[str] = asyncio.Queue(maxsize=queue_size)
        self._worker: asyncio.Task[None] | None = None

    def start(self) -> None:
        if self.enabled and (self._worker is None or self._worker.done()):
            self._worker = asyncio.create_task(self._run(), name="telegram-notifier")

    def enqueue(self, message: str) -> bool:
        if not self.enabled or not message.strip():
            return False
        try:
            self._queue.put_nowait(message)
            return True
        except asyncio.QueueFull:
            self.logger.warning("Telegram通知队列已满，已丢弃一条消息，交易流程不受影响")
            return False

    async def stop(self) -> None:
        worker = self._worker
        if worker is None:
            return
        try:
            await asyncio.wait_for(self._queue.join(), timeout=2.0)
        except asyncio.TimeoutError:
            self.logger.warning("Telegram通知关闭等待超时，剩余消息已放弃")
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)
        self._worker = None

    async def _run(self) -> None:
        while True:
            message = await self._queue.get()
            try:
                await self._deliver_with_retry(message)
            finally:
                self._queue.task_done()

    async def _deliver_with_retry(self, message: str) -> None:
        for attempt in range(2):
            try:
                await asyncio.wait_for(
                    asyncio.to_thread(self._send_sync, message),
                    timeout=self.request_timeout + 1.0,
                )
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if attempt == 0:
                    await asyncio.sleep(0.5)
                    continue
                status = getattr(exc, "code", None)
                detail = f" HTTP {status}" if status is not None else f" {type(exc).__name__}"
                self.logger.warning("Telegram通知发送失败:%s；交易流程不受影响", detail)

    def _send_sync(self, message: str) -> None:
        endpoint = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        body = urllib.parse.urlencode(
            {
                "chat_id": self.chat_id,
                "text": message,
                "disable_web_page_preview": "true",
            }
        ).encode("utf-8")
        request = urllib.request.Request(endpoint, data=body, method="POST")
        with urllib.request.urlopen(request, timeout=self.request_timeout) as response:
            if response.status != 200:
                raise RuntimeError(f"unexpected Telegram status {response.status}")
