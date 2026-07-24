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

可选配置 Telegram 成交推送；两项都设置时启用：

```bash
TELEGRAM_BOT_TOKEN=123456789:your_bot_token
TELEGRAM_CHAT_ID=123456789
```

每笔两腿完整成交推送一次，持仓周期归零时再单独推送周期结果。通知使用有界内存队列和
独立后台发送任务；Telegram 网络超时、失败或队列满不会阻塞行情、信号和下单流程。

可以同时在 `.env` 预制第二页的梯度。每档格式为 `阈值%:目标净仓位`，档数不限，
用逗号分隔后界面会按实际档数显示：

```bash
GRADIENT_SINGLE_ORDER_QTY=0.001
GRADIENT_LONG=0.060:0.005,0.065:0.010,0.070:0.015
GRADIENT_SHORT=0.040:-0.005,0.035:-0.010,0.030:-0.015
```

程序每次启动时读取这些值，但不会自动启动策略；进入第二页在启动行按一次 `Enter`
才会开始下单。运行中仍可自由增删或修改梯度，修改只在本次进程内生效，不会回写
`.env`。环境变量格式错误会阻止程序启动；梯度顺序、重复值等策略错误会显示在第二页并
保持未启动。

### Lighter Rust 全流程

Lighter 的公共订单簿、指定美元深度 VWAP、市场精度、nonce、原生签名、HTTP
下单/撤单、私有 WebSocket、成交与仓位均由 `bybot/lighter` 的常驻 Rust gateway
持有。Python 不再依赖 `lighter-sdk`，也不再维护第二份 Lighter 订单簿或账户流。

首次部署需要同时克隆 bybot 并构建 release binary：

```bash
mkdir -p ~/git/bybot
git clone git@github.com:B1TOP-1/bybot.git ~/git/bybot/bybot

cd ~/git/var/variational-v1
./scripts/build-lighter-rust.sh
```

默认 binary 路径是
`~/git/bybot/bybot/lighter/target/release/variational_lighter_gateway`。自定义位置可在
`.env` 设置 `LIGHTER_RUST_GATEWAY_BIN=/absolute/path/variational_lighter_gateway`。
日常启动命令仍是 `python main.py`，主程序会启动和回收 gateway。`--no-hedge`
会让 gateway 进入只读行情模式，不读取私钥、不启动私有账户流。

完整执行模式必须部署在能访问 Lighter 标准 `wss://.../stream` 的 VPS。受限地区只能
连接 `?readonly=true`，可以看行情但不能取得认证私有账户快照，因此程序会保持未就绪，
不能用于自动对冲。Rust gateway 运行中退出或订单簿失效时，Python 会清空旧报价并停止策略。

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

Ubuntu/VPS 长期运行建议使用仓库脚本启动 Chrome，以隐藏 Chrome 强制显示的
Debugger 提示条，同时保留报价、成交事件和可信点击所需的 CDP 连接：

```bash
./scripts/start-variational-chrome.sh
```

脚本使用独立的 `~/.config/variational-chrome` profile，只检测并拒绝重复启动自己的
Variational 实例，不影响系统中其他项目的 Chrome。它会自动加载本仓库扩展并打开 BTC 页面。
不同项目必须使用不同的 `--user-data-dir`，不能共享该 profile。

默认启用适合交易 VPS 的参数：`--process-per-site`、关闭 Chrome 后台服务、关闭同步和
组件更新，并禁止后台标签定时器降频，保证窗口失焦后 200ms 报价仍能工作。VPS 常见的
小容量 `/dev/shm` 默认使用 `--disable-dev-shm-usage`；若主机 `/dev/shm` 足够大可关闭：

```bash
VARIATIONAL_DISABLE_DEV_SHM=0 ./scripts/start-variational-chrome.sh
```

`--disable-gpu` 默认不启用，因为可能增加 CPU 和软件渲染压力；确认 VPS GPU 进程无用时可选：

```bash
VARIATIONAL_DISABLE_GPU=1 ./scripts/start-variational-chrome.sh
```

也可以覆盖专用 profile 和初始标的：

```bash
VARIATIONAL_CHROME_PROFILE="$HOME/.config/var-btc" \
VARIATIONAL_CHROME_URL="https://omni.variational.io/perpetual/BTC" \
./scripts/start-variational-chrome.sh
```

启动后可在 `chrome://version` 的“命令行”中确认存在
`--silent-debugger-extension-api`。该模式仅建议用于专用交易 VPS/Profile。

继续使用原来的 Chrome 时，启动命令保持不变：

```bash
cd ~/git/var/variational-v1
source venv/bin/activate
python main.py
```

Linux/VPS 上 `main.py` 会自动在后台运行内存监督；主程序退出时监督进程也会停止。
它不会启动 Chrome，浏览器仍按原来的方式手动打开。若临时不需要监督，可使用
`VARIATIONAL_MEMORY_MONITOR=0 python main.py`。以后需要专用 Chrome 时，再单独运行
`./scripts/start-variational-chrome.sh`，它不是启动主程序的必要条件。

内存监督默认每 10 秒把系统内存和 RSS 最大的 40 个进程直接追加到
`log/memory/<启动时间>/system.csv` 与 `processes.csv`，不会在内存中累计历史。
它会区分 Chrome browser、renderer、GPU、network、storage、extension 以及
`python main.py`。可用内存低于 250MB 或单进程超过 500MB 时，完整 `ps` 快照写入
同目录的 `snapshots/`，触发时间与原因写入 `alerts.log`。

```bash
# 实时观察系统内存和告警
latest="$(ls -dt log/memory/* | head -1)"
tail -f "$latest/system.csv" "$latest/alerts.log"

# 示例：改为每 5 秒采样，单进程超过 350MB 时保存快照
MEMORY_MONITOR_INTERVAL_SECONDS=5 \
MEMORY_MONITOR_PROCESS_ALERT_MB=350 \
python main.py
```

插件会记住最后一次成功预填的下单方向和数量。Start/重连引起页面自动刷新后，
插件会等待交易表单重新挂载并恢复数量，避免输入框为空导致策略无法触发下单。

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

策略信号采用两级防尖刺确认：Edge 在最近不同主动报价间的极差不超过 `0.03` 个
百分点时，连续 `400ms` 且至少 `3` 个不同报价即可走快速下单；波动较大的信号仍需
连续 `1s` 且至少 `4` 个不同报价。下单前仍会重新读取两边价格并复核方向、梯度和目标仓位。

跨平台 Edge 使用对称百分比差：Long Edge 为 `200 × (Lighter VWAP bid × 映射比例 - Var ask) / (Lighter VWAP bid × 映射比例 + Var ask)`，Short Edge 为 `200 × (Lighter VWAP ask × 映射比例 - Var bid) / (Lighter VWAP ask × 映射比例 + Var bid)`。Long 越高越有利，Short 越低越有利。Lighter 默认使用 `$2000` 深度 VWAP；Var 的 indicative bid/ask 使用插件请求的约 `$500` 等值数量。持仓 Edge 收益统一为 `Entry Edge - Current Edge`，并使用持仓方向对应的同一条 Edge。

持仓 Edge 收益只用于收益展示，不作为梯度输入。Long 仓使用 `入场 Long Edge - 当前 Short Edge`，Short 仓使用 `当前 Long Edge - 入场 Short Edge`，即始终采用反方向可执行平仓 Edge。Long 梯度直接读取实时 Long Edge，Short 梯度直接读取实时 Short Edge，二者共同解析出唯一的带符号目标仓位。

成交 Edge 使用两边实际成交价格按同一对称公式计算。Long 滑点为 `Fill Long - Trigger Long`；Short 滑点为 `Trigger Short - Fill Short`，正数统一表示成交更有利。Var 实际成交价由插件转发的成交事件回填，Lighter 实际成交价由成交数量和成交金额计算。

### 长期价差记录与本地浏览器看板

`main.py` 约每 `200ms` 把当前资产的 Var bid/ask、Lighter 深度 VWAP bid/ask、Long Edge 和 Short Edge 追加到 `log/spread_history.sqlite3`。数据库使用 SQLite WAL 模式，程序重启和切换币种不会删除历史；不同资产按 symbol 隔离。

5分钟、30分钟、1小时窗口、12小时分时统计和三天终端走势图都以 SQLite 为数据源，不再依赖重启即丢失的原始内存队列。每条约 `200ms` 的价差样本同时保存 Binance `USDC/USDT` bid/ask 及其最近接收时间，供后续分析稳定币基差和识别陈旧数据，但不参与当前交易 Edge。旧数据库启动时会自动增加字段。可通过 `--spread-db /path/to/file.sqlite3` 修改数据库位置。

`main` 启动时同时提供本地看板 [http://127.0.0.1:8780](http://127.0.0.1:8780)。页面支持资产切换、Long/Short Edge 双线、1小时/6小时/24小时/7天范围、悬停读数、最新两边价格和5分钟中位数，每5秒自动刷新。可使用 `--dashboard-port` 修改端口。数据库保留约 `200ms` 的原始记录，API 下采样只影响图表显示点数，不会删除原始数据。Chrome 插件继续只负责报价采集与运行状态。

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
第二页提供运行中的梯度目标仓位配置。未设置 `.env` 预制梯度时，Long 梯度和 Short 梯度各有一行空参数，不会在未填完整前触发。
策略默认未启动；录入参数只会保存配置，必须把光标移动到启动行并按 `Enter` 后才会产生触发信号。

- `Tab`：切换第一页/第二页。
- `↑/↓`：在可编辑梯度行之间移动光标。
- `←/→`：切换当前行的“价差%”和“仓位”字段。
- 数字与 `.`：直接编辑当前字段。
- `Enter`：在启动行切换启动/停止；在梯度行确认当前字段。
- `Esc`：取消当前字段编辑。
- `+`：在当前区新增一条梯度。
- `-`：删除当前梯度；每个区至少保留一行，删除最后一行会清空参数。

Long Edge 只查询 Long 梯度，并在 `Long Edge >= 阈值` 时命中；它只能产生买单，将目标净仓位向更大的方向推进，例如从 `-0.2` 买到 `0`，再买到 `+0.2`。Short Edge 只查询 Short 梯度，并在 `Short Edge <= 阈值` 时命中；它只能产生卖单，将目标净仓位向更小的方向推进，例如从 `+0.2` 卖到 `0`，再卖到 `-0.2`。每行仓位都是带符号的目标净仓位：正数代表做多 Var / 做空 Lighter，`0` 代表清仓，负数代表做空 Var / 做多 Lighter。若 Long 与 Short 同时要求向相反方向下单，策略不会择优猜测，而是停止本次触发。最终按 `min(单次下单数量, 目标仓位与当前仓位的差额)` 分批执行。

启动策略前会校验配置：禁止半填写行、重复阈值、重复目标仓位和非正数单次下单量。Long 阈值升高时目标仓位必须严格增大；Short 阈值降低时目标仓位必须严格减小。非法配置会在策略面板显示具体行和值，并保持未启动状态。

“万一 Edge 退出”把梯度和成交成本分开处理：普通 Long/Short 梯度照常决定开仓方向；持仓后以真实入场成交的数量加权 Edge 为成本。Short 持仓仅当当前可执行 Long Edge 达到入场均价 `+0.0100%`，Long 持仓仅当当前可执行 Short Edge 达到入场均价 `-0.0100%`，才允许向当前同方向梯度档位的最大仓位减仓。每次发单只判断当前可执行 Edge，实际平仓均价只记录复盘，不阻止下一笔。完整规则见 [`docs/round_break_even_exit_strategy.md`](docs/round_break_even_exit_strategy.md)，测试报告见 [`docs/round_exit_strategy_test_report.md`](docs/round_exit_strategy_test_report.md)。

Chrome 插件启动后会同时连接浏览器下单 broker，默认地址为 `ws://127.0.0.1:8768`。策略信号触发后会创建一条本地策略订单记录，并同时提交 Variational 浏览器订单和 Lighter 对冲单；Lighter 不等待 Variational 成交确认。后续两边成交事件会分别回填到同一条策略记录，用于成交价差和滑点统计。`--no-hedge` 只关闭 Lighter 自动对冲，不会关闭 Variational 浏览器下单。

### 输出日志
长期价差数据库固定为 `./log/spread_history.sqlite3`。每次启动创建独立的 UTC+8 时间目录：
- `./log/runs/YYYY-MM-DD_HH-MM-SS_ffffff_UTC+8/runtime.log`
- `./log/runs/YYYY-MM-DD_HH-MM-SS_ffffff_UTC+8/order_metrics.jsonl`
- `./log/runs/YYYY-MM-DD_HH-MM-SS_ffffff_UTC+8/trade_records.csv`
- smoke test 另有同目录下的 `browser_smoke_test.jsonl`

`runtime.log` 和订单事件的 `logged_at` 使用 UTC+8。JSONL 保持逐行追加，程序异常时仍能保留已经写入的记录。

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

Optional Page 2 gradient presets can be loaded from `.env`. Any number of levels
can be configured as `threshold_percent:target_net_position`, separated by commas;
the UI renders the configured row count:

```bash
GRADIENT_SINGLE_ORDER_QTY=0.001
GRADIENT_LONG=0.060:0.005,0.065:0.010,0.070:0.015
GRADIENT_SHORT=0.040:-0.005,0.035:-0.010,0.030:-0.015
```

Presets are loaded on each process start, but the strategy remains disabled until
you press `Enter` on the Page 2 start row. Runtime edits never write back to `.env`.

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
Long-term spread history remains at `./log/spread_history.sqlite3`. Every process start creates a separate UTC+8 run directory:
- `./log/runs/YYYY-MM-DD_HH-MM-SS_ffffff_UTC+8/runtime.log`
- `./log/runs/YYYY-MM-DD_HH-MM-SS_ffffff_UTC+8/order_metrics.jsonl`
- `./log/runs/YYYY-MM-DD_HH-MM-SS_ffffff_UTC+8/trade_records.csv`
- browser smoke tests also write `browser_smoke_test.jsonl` in that run directory

Runtime timestamps and order-event `logged_at` values use UTC+8. JSONL remains append-only so completed records survive an abnormal shutdown.

Note: the terminal is reserved for the dashboard. Raw REST/WS payloads are not persisted; only runtime logs, order-metrics logs, and trade-record CSV snapshots are written.
