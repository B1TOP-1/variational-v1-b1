#!/usr/bin/env python3
"""独立诊断脚本：真实测试 Lighter 下单（BTC，走 SignerClient.create_order，与生产同路径）。

分步打印：环境变量 → 建 SignerClient → check_client → 拉市场配置 → 拉 BBO →
组装订单 → （--submit 才真正发单）。哪一步炸，一眼可见。

用法（在项目根目录，用项目 venv）:
  env/bin/python test_lighter_order_btc.py                 # dry-run：只校验+组装，不发单
  env/bin/python test_lighter_order_btc.py --submit        # 真正下单（默认挂在远离盘口处不成交）
  env/bin/python test_lighter_order_btc.py --ticker BTC --size 0.0001 --side buy --offset 0.02 --submit

环境变量（从 --env-file 载入，默认 .env）:
  LIGHTER_PRIVATE_KEY 或 API_KEY_PRIVATE_KEY, LIGHTER_ACCOUNT_INDEX, LIGHTER_API_KEY_INDEX
"""

import argparse
import asyncio
import json
import os
import time
from decimal import Decimal

import requests
import websockets
from dotenv import load_dotenv

BASE_URL = "https://mainnet.zklighter.elliot.ai"
WS_URL = "wss://mainnet.zklighter.elliot.ai/stream"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="测试 Lighter 下单")
    parser.add_argument("--env-file", default=".env", help="env 文件路径（默认 .env）")
    parser.add_argument("--ticker", default="BTC", help="标的（默认 BTC）")
    parser.add_argument("--side", choices=["buy", "sell"], default="buy", help="方向（默认 buy）")
    parser.add_argument("--size", default="0.001", help="下单量（默认 0.001）")
    parser.add_argument("--offset", type=float, default=0.02, help="挂单偏离盘口比例，默认 0.02(2%%) 远离不成交")
    parser.add_argument("--submit", action="store_true", help="真正发单（不加则仅 dry-run）")
    return parser.parse_args()


def fetch_market_config(ticker: str) -> tuple[int, int, int]:
    resp = requests.get(f"{BASE_URL}/api/v1/orderBooks", headers={"accept": "application/json"}, timeout=10)
    resp.raise_for_status()
    for market in resp.json().get("order_books", []):
        if market.get("symbol") == ticker:
            return (
                int(market["market_id"]),
                int(market["supported_size_decimals"]),
                int(market["supported_price_decimals"]),
            )
    raise RuntimeError(f"未在 orderBooks 找到 {ticker}")


def _level_price(level) -> Decimal:
    return Decimal(str(level[0] if isinstance(level, list) else level["price"]))


async def fetch_bbo(market_id: int) -> tuple[Decimal | None, Decimal | None]:
    async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20) as ws:
        await ws.send(json.dumps({"type": "subscribe", "channel": f"order_book/{market_id}"}))
        for _ in range(50):
            data = json.loads(await asyncio.wait_for(ws.recv(), timeout=10))
            if data.get("type") == "subscribed/order_book":
                book = data.get("order_book", {})
                bids = book.get("bids", [])
                asks = book.get("asks", [])
                best_bid = max((_level_price(b) for b in bids), default=None)
                best_ask = min((_level_price(a) for a in asks), default=None)
                return best_bid, best_ask
    return None, None


async def run(args: argparse.Namespace) -> None:
    print("=" * 72)
    if os.path.exists(args.env_file):
        load_dotenv(args.env_file)
        print(f"[1] 已载入 env: {args.env_file}")
    else:
        print(f"[1] 未找到 env 文件({args.env_file})，使用已导出的环境变量")

    private_key = os.getenv("API_KEY_PRIVATE_KEY", "").strip() or os.getenv("LIGHTER_PRIVATE_KEY", "").strip()
    account_index = int(os.getenv("LIGHTER_ACCOUNT_INDEX", "0"))
    api_key_index = int(os.getenv("LIGHTER_API_KEY_INDEX", "0"))
    print(f"[1] 私钥={'已设置' if private_key else '缺失!!'} account_index={account_index} api_key_index={api_key_index} url={BASE_URL}")
    if not private_key:
        print("!! 缺私钥：请设置 LIGHTER_PRIVATE_KEY 或 API_KEY_PRIVATE_KEY")
        return

    from lighter.signer_client import SignerClient

    print("[2] 建 SignerClient ...")
    client = SignerClient(url=BASE_URL, account_index=account_index, api_private_keys={api_key_index: private_key})
    try:
        print("[3] check_client ...")
        err = client.check_client()
        if err is not None:
            print(f"!! check_client 失败: {err!r}")
            return
        print("[3] check_client 通过 ✅")

        print(f"[4] 拉 {args.ticker} 市场配置 ...")
        market_id, size_decimals, price_decimals = fetch_market_config(args.ticker)
        base_mult = pow(10, size_decimals)
        price_mult = pow(10, price_decimals)
        print(f"[4] market_id={market_id} size_decimals={size_decimals} price_decimals={price_decimals}")

        print("[5] 拉 BBO ...")
        best_bid, best_ask = await fetch_bbo(market_id)
        print(f"[5] best_bid={best_bid} best_ask={best_ask}")
        if best_bid is None or best_ask is None:
            print("!! 拿不到盘口，退出")
            return

        qty = Decimal(args.size)
        offset = Decimal(str(args.offset))
        if args.side == "buy":
            is_ask = False
            limit_price = best_bid * (Decimal("1") - offset)
        else:
            is_ask = True
            limit_price = best_ask * (Decimal("1") + offset)

        base_amount = int(qty * base_mult)
        price_i = int(limit_price * price_mult)
        print("=" * 72)
        print(f"[6] 组装订单 | side={args.side} qty={qty} base_amount={base_amount} limit_price={limit_price} price_i={price_i} is_ask={is_ask}")
        if base_amount <= 0:
            print(f"!! base_amount<=0（qty {qty} 小于最小步长 {Decimal('1')/Decimal(base_mult)}）")
            return

        if not args.submit:
            print("[6] DRY-RUN：未发单。确认无误后加 --submit 真正下单。")
            return

        coid = int(time.time() * 1000)
        print(f"[7] 发单 create_order client_order_index={coid} ...")
        started = time.perf_counter()
        _tx, resp, error = await client.create_order(
            market_index=market_id,
            client_order_index=coid,
            base_amount=base_amount,
            price=price_i,
            is_ask=is_ask,
            order_type=client.ORDER_TYPE_LIMIT,
            time_in_force=client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
            reduce_only=False,
            trigger_price=0,
        )
        elapsed_ms = (time.perf_counter() - started) * 1000
        if error is not None:
            print(f"!! 下单失败: {error!r}")
        else:
            print(f"[7] 下单成功 ✅ 耗时={elapsed_ms:.1f}ms 响应={resp}")
            print("提示：这是一笔挂在远离盘口的限价单，可能不会成交；请到 Lighter 账户手动确认/撤单。")
    finally:
        with __import__("contextlib").suppress(Exception):
            await client.close()
    print("=" * 72)


def main() -> None:
    asyncio.run(run(parse_args()))


if __name__ == "__main__":
    main()
