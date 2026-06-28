# variational-v1

邀请链接：
- Variational: [https://omni.variational.io/?ref=OMNIQUANT](https://omni.variational.io/?ref=OMNIQUANT)（直升 Bronze，获得 12% 积分加成）
- Lighter: [https://app.lighter.xyz/?referral=QUANTGUY](https://app.lighter.xyz/?referral=QUANTGUY)

English version is below.

## 中文

### 概述
`variational-v1` 是一个基于 Chrome 插件转发的运行时工具，用于：
1. 跟踪 Variational 订单生命周期，
2. 在终端展示实时看板，
3. 可选地在 Lighter 自动对冲。

交易对会从 Variational 的 REST/WS 消息中自动识别，不需要手动输入 ticker。

### 核心功能
- 记录 Variational/Lighter 的订单关键信息（成交、价格、方向、价差）。
- Rich 终端看板实时展示双边盘口、价差百分比和最近订单。
- 可选Lighter自动对冲功能（默认开启，可用 `--no-hedge` 关闭）。
- 支持页面重连与交易资产自动切换（切换后自动重置对应历史窗口）。

### 项目结构
- `main.py`：主程序
- `variational/listener.py`：本地接收与监控解析
- `chrome_extension/`：CDP 转发插件

### 环境准备
#### macOS / Linux
```bash
python3 -m venv env
source env/bin/activate
pip install -r requirements.txt
```

#### Windows（PowerShell）
```powershell
py -3 -m venv env
.\env\Scripts\Activate.ps1
pip install -r requirements.txt
```

创建 `.env`并填入Lighter的信息：
```bash
LIGHTER_ACCOUNT_INDEX=...
LIGHTER_API_KEY_INDEX=...
LIGHTER_PRIVATE_KEY=...
```

如需在 Lighter WebSocket 新旧逻辑切换期间临时强制使用旧的应用层 ping/pong 逻辑，可额外设置：
```bash
LIGHTER_WS_SERVER_PINGS=true
```
不设置时默认使用新的兼容模式：客户端依赖 WebSocket protocol ping frame 保活，同时仍兼容旧服务端发出的 `ping` 消息。

### 加载 Chrome 插件
1. 打开 `chrome://extensions`
2. 在右上角开启 `Developer mode`
3. 在左上角点击 `Load unpacked`，选择：
`variational-v1/chrome_extension`

### 运行
```bash
python main.py
```

关闭自动对冲：
```bash
python main.py --no-hedge
```

Python 脚本开始运行后，打开 Variational 的交易页面，
打开 Chrome 插件列表，点击 “Variational CDP Forwarder” -> 点击 `Start`

切换看板语言为英文：
```bash
python main.py --lang en
```

### 第二页触发策略
第二页提供运行中的梯度触发策略配置，默认开仓区和清仓区各一行空参数，不会在未填完整前触发。
策略默认未启动；录入参数只会保存配置，必须把光标移动到启动行并按 `Enter` 后才会产生触发信号。

- `Tab`：切换第一页/第二页。
- `↑/↓`：在可编辑梯度行之间移动光标。
- `←/→`：切换当前行的“价差%”和“仓位”字段。
- 数字与 `.`：直接编辑当前字段。
- `Enter`：在启动行切换启动/停止；在梯度行确认当前字段。
- `Esc`：取消当前字段编辑。
- `+`：在当前区新增一条梯度。
- `-`：删除当前梯度；每个区至少保留一行，删除最后一行会清空参数。

开仓区默认信号源为“做多 Variational / 做空 Lighter”，清仓区默认信号源为“做空 Variational / 做多 Lighter”。每个梯度的仓位表示目标总仓位；触发信号会按 `min(单次下单数量, 距离目标仓位差额)` 执行。

Chrome 插件启动后会同时连接浏览器下单 broker，默认地址为 `ws://127.0.0.1:8768`。策略信号触发后会创建一条本地策略订单记录，并同时提交 Variational 浏览器订单和 Lighter 对冲单；Lighter 不等待 Variational 成交确认。后续两边成交事件会分别回填到同一条策略记录，用于成交价差和滑点统计。`--no-hedge` 只关闭 Lighter 自动对冲，不会关闭 Variational 浏览器下单。

### 输出日志
默认目录：`./log`
- `runtime.log`（程序运行日志）
- `order_metrics.jsonl`
- `trade_records.csv`（当前交易记录快照，dashboard 刷新时按最新状态覆盖写）

说明：终端仅用于显示 dashboard。程序不会落盘原始 REST/WS 消息，只会写运行日志、订单指标日志和交易记录 CSV 快照。

---

## English

Referral Links:
- Variational: [https://omni.variational.io/?ref=OMNIQUANT](https://omni.variational.io/?ref=OMNIQUANT) (instant Bronze tier + 12% points bonus)
- Lighter: [https://app.lighter.xyz/?referral=QUANTGUY](https://app.lighter.xyz/?referral=QUANTGUY)

### Overview
`variational-v1` is a Chrome-extension-assisted runtime for:
1. tracking Variational order lifecycle,
2. showing a terminal dashboard,
3. optionally auto-hedging on Lighter.

Ticker is auto-derived from incoming Variational REST/WS messages.

### Core Features
- Tracks key Variational/Lighter order data (fills, prices, direction, spread).
- Rich terminal dashboard for live two-venue quotes, spread percentages, and recent orders.
- Optional auto-hedge (enabled by default, disable with `--no-hedge`).
- Handles page reconnects and automatic asset switching (with related history reset on switch).

### Repository Layout
- `main.py`: main runtime
- `variational/listener.py`: local receiver + monitor parsing
- `chrome_extension/`: CDP forwarder extension

### Setup
#### macOS / Linux
```bash
python3 -m venv env
source env/bin/activate
pip install -r requirements.txt
```

#### Windows (PowerShell)
```powershell
py -3 -m venv env
.\env\Scripts\Activate.ps1
pip install -r requirements.txt
```

Create `.env`:
```bash
LIGHTER_ACCOUNT_INDEX=...
LIGHTER_API_KEY_INDEX=...
LIGHTER_PRIVATE_KEY=...
```

If you need to temporarily force Lighter's legacy application-level ping/pong behavior during the rollout window, you can also set:
```bash
LIGHTER_WS_SERVER_PINGS=true
```
When unset, the runtime uses the forward-compatible path: it keeps the socket alive with WebSocket protocol ping frames and still responds to legacy server `ping` messages.

### Load Chrome Extension
1. Open `chrome://extensions`
2. Enable `Developer mode` (top-right)
3. Click `Load unpacked` (top-left), then choose:
`variational-v1/chrome_extension`

### Run
```bash
python main.py
```

Disable hedge:
```bash
python main.py --no-hedge
```

After the Python script starts, open the Variational trading page,
open the Chrome extensions list, click `Variational CDP Forwarder`, then click `Start`.

Switch dashboard language to Chinese:
```bash
python main.py --lang zh
```

### Page 2 Trigger Strategy
Page 2 provides live gradient trigger configuration. The open and close sections each start with one empty row and do not trigger until a row is complete.
The strategy is disabled by default. Entering values only saves configuration; move the cursor to the start row and press `Enter` to enable trigger signals.

- `Tab`: switch between page 1 and page 2.
- `↑/↓`: move the cursor across editable gradient rows.
- `←/→`: switch between the spread % and position fields on the selected row.
- Digits and `.`: edit the selected field.
- `Enter`: start/stop on the start row; commit the selected field on gradient rows.
- `Esc`: cancel the selected field edit.
- `+`: add one gradient row in the current section.
- `-`: delete the selected gradient row; the last row in a section is cleared instead of removed.

The open section uses `Long Variational / Short Lighter`; the close section uses `Short Variational / Long Lighter`. Each row's position is the target total position. Trigger signals execute `min(single order qty, distance to target position)`.

After the Chrome extension starts, it also connects to the browser order broker at `ws://127.0.0.1:8768` by default. A strategy signal creates one local strategy order record, submits the Variational browser order, and submits the Lighter hedge immediately; Lighter does not wait for Variational fill confirmation. Later fills from both venues update the same strategy record for fill spread and slippage statistics. `--no-hedge` disables only Lighter auto-hedging, not the Variational browser order.

### Output Logs
Default path: `./log`
- `runtime.log` (runtime log messages)
- `order_metrics.jsonl`
- `trade_records.csv` (current trade-record snapshot, overwritten on dashboard refresh with latest state)

Note: the terminal is reserved for the dashboard. Raw REST/WS payloads are not persisted; only runtime logs, order-metrics logs, and trade-record CSV snapshots are written.
