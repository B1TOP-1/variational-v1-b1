import asyncio
import logging
import unittest

from variational.telegram_notifier import TelegramNotifier


class TelegramNotifierTest(unittest.IsolatedAsyncioTestCase):
    def test_enqueue_is_bounded_and_never_waits(self):
        notifier = TelegramNotifier(
            "token",
            "chat",
            logger=logging.getLogger("telegram-test"),
            queue_size=1,
        )

        self.assertTrue(notifier.enqueue("first"))
        self.assertFalse(notifier.enqueue("second"))

    async def test_worker_delivers_queued_message_off_event_loop(self):
        notifier = TelegramNotifier(
            "token",
            "chat",
            logger=logging.getLogger("telegram-test"),
        )
        delivered = []
        notifier._send_sync = delivered.append
        notifier.start()

        self.assertTrue(notifier.enqueue("hello"))
        await asyncio.wait_for(notifier._queue.join(), timeout=1.0)
        await notifier.stop()

        self.assertEqual(delivered, ["hello"])


if __name__ == "__main__":
    unittest.main()
