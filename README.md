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

### 主动报价采集
插件 `1.2.0` 默认启用主动 indicative quote 采集，间隔为 `200ms`，不需要打开 DevTools Console。

插件不会读取或复制 Cookie。启动后先通过 CDP 捕获 Omni 网页原生的
`POST /api/quotes/indicative` 请求体，再把该请求体作为模板注入网页 MAIN world，
使用网页已经通过 Cloudflare 验证的同源会话主动获取报价。响应仍通过原有 CDP 和
`ws://127.0.0.1:8767` 转发给 Python。

在 Omni 网页切换 BTC、CL 或其他标的后，网页原生请求体会随标的变化；插件检测到新模板后会自动停止旧采集器并启动新采集器，不需要手动修改 instrument。

插件弹窗可配置：
- 是否启用主动报价；
- 请求间隔，默认 `200ms`；
- Var 报价深度，默认 `$500` 等值数量并取两位有效数字；
- 最大并发，默认 `4`；
- 单请求超时，默认 `1500ms`。

长期运行保护：
- 每个页面采集会话使用独立 session id 和递增 sequence；晚到的旧响应不会进入策略；
- 达到最大并发时跳过本轮，不无限堆积请求；
- `403` 停止采集并等待重新验证；`429`、`5xx` 和网络错误指数退避；
- 页面刷新、插件 Service Worker 重启后恢复采集；关闭目标标签时清理运行状态；
- 页面定时器与扩展 Service Worker 监督器共同触发同一个去重 `kick`，降低后台标签定时器被 Chrome 降频的影响；
- 本地转发 WebSocket 每 20 秒发送扩展保活消息。

跨平台 Edge 使用对称百分比差：Long Edge 为 `200 × (Lighter VWAP bid × 映射比例 - Var ask) / (Lighter VWAP bid × 映射比例 + Var ask)`，Short Edge 为 `200 × (Lighter VWAP ask × 映射比例 - Var bid) / (Lighter VWAP ask × 映射比例 + Var bid)`。Long 越高越有利，Short 越低越有利。Lighter 默认使用 `$2000` 深度 VWAP；Var 的 indicative bid/ask 使用插件请求的约 `$500` 等值数量。持仓 Edge 收益统一为 `Entry Edge - Current Edge`，并使用持仓方向对应的同一条 Edge。

持仓 Edge 收益只用于收益展示，不作为梯度输入。Long 仓使用 `入场 Long Edge - 当前 Short Edge`，Short 仓使用 `当前 Long Edge - 入场 Short Edge`，即始终采用反方向可执行平仓 Edge。Long 梯度直接读取实时 Long Edge，Short 梯度直接读取实时 Short Edge，二者共同解析出唯一的带符号目标仓位。

成交 Edge 使用两边实际成交价格按同一对称公式计算。Long 滑点为 `Fill Long - Trigger Long`；Short 滑点为 `Trigger Short - Fill Short`，正数统一表示成交更有利。Var 实际成交价由插件转发的成交事件回填，Lighter 实际成交价由成交数量和成交金额计算。

### 长期价差记录与本地浏览器看板

`main.py` 每秒把当前资产的 Var bid/ask、Lighter 深度 VWAP bid/ask、Long Edge 和 Short Edge 追加到 `log/spread_history.sqlite3`。数据库使用 SQLite WAL 模式，程序重启和切换币种不会删除历史；不同资产按 symbol 隔离。

5分钟、30分钟、1小时窗口、12小时分时统计和三天终端走势图都以 SQLite 为数据源，不再依赖重启即丢失的原始内存队列。可通过 `--spread-db /path/to/file.sqlite3` 修改数据库位置。

`main` 启动时同时提供本地看板 [http://127.0.0.1:8780](http://127.0.0.1:8780)。页面支持资产切换、Long/Short Edge 双线、1小时/6小时/24小时/7天范围、悬停读数、最新两边价格和5分钟中位数，每5秒自动刷新。可使用 `--dashboard-port` 修改端口。数据库保留原始每秒记录，API 下采样只影响图表显示点数，不会删除原始数据。Chrome 插件继续只负责报价采集与运行状态。

运行数天时，请在 Chrome 性能设置中将 `omni.variational.io` 加入“始终保持这些网站处于活动状态”，并确保系统不会自动睡眠。网页切换标的后，应在插件状态中确认 `Active quote` 显示新的 asset。

### 运行
```bash
python main.py
```

关闭自动对冲：
```bash
python main.py --no-hedge
```

`--no-hedge` 模式只连接 Lighter 只读 WebSocket：`wss://mainnet.zklighter.elliot.ai/stream?readonly=true`，不会调用 Lighter REST，也不会初始化 Lighter 签名客户端；适合仅读取盘口和运行 Var 浏览器下单。

Python 脚本开始运行后，打开 Variational 的交易页面，
打开 Chrome 插件列表，点击 “Variational CDP Forwarder” -> 点击 `Start`

切换看板语言为英文：
```bash
python main.py --lang en
```

### 第二页触发策略
第二页提供运行中的梯度目标仓位配置，默认 Long 梯度和 Short 梯度各一行空参数，不会在未填完整前触发。
策略默认未启动；录入参数只会保存配置，必须把光标移动到启动行并按 `Enter` 后才会产生触发信号。

- `Tab`：切换第一页/第二页。
- `↑/↓`：在可编辑梯度行之间移动光标。
- `←/→`：切换当前行的“价差%”和“仓位”字段。
- 数字与 `.`：直接编辑当前字段。
- `Enter`：在启动行切换启动/停止；在梯度行确认当前字段。
- `Esc`：取消当前字段编辑。
- `+`：在当前区新增一条梯度。
- `-`：删除当前梯度；每个区至少保留一行，删除最后一行会清空参数。

Long Edge 只查询 Long 梯度，并在 `Long Edge >= 阈值` 时命中；Short Edge 只查询 Short 梯度，并在 `Short Edge <= 阈值` 时命中。每行仓位都是带符号的目标净仓位：正数代表做多 Var / 做空 Lighter，`0` 代表清仓，负数代表做空 Var / 做多 Lighter。若两侧同时命中，选择数值更高的 Edge；Edge 相同则选择离当前仓位更近的目标。最终按 `min(单次下单数量, 目标仓位与当前仓位的差额)` 分批执行。

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

`--no-hedge` mode only connects to the read-only Lighter WebSocket: `wss://mainnet.zklighter.elliot.ai/stream?readonly=true`. It does not call Lighter REST and does not initialize the Lighter signer client, so it is suitable for read-only books plus Var browser orders.

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
