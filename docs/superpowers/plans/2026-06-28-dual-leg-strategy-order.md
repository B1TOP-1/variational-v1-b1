# Dual-Leg Strategy Order Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Execute Variational and Lighter immediately from one gradient signal and reconcile both fills into one strategy order record.

**Architecture:** Reuse `OrderLifecycle` as the strategy order record, adding strategy metadata and error fields. `main.py` creates a local strategy record before order submission, queues it for Variational event binding by side/qty/asset, maps Lighter fills by `client_order_id`, and removes the old Var-created hedge trigger.

**Tech Stack:** Python `asyncio`, `decimal.Decimal`, existing `BrowserOrderCommand`, Lighter SDK signer client, `unittest`.

---

### Task 1: Tests for Strategy Record Creation and Dispatch

**Files:**
- Modify: `tests/test_spread_math.py`
- Modify: `main.py`

- [ ] **Step 1: Write failing tests**

Add tests that instantiate `VariationalToLighterRuntime` with `object.__new__`, inject fake queues/locks, call a new strategy dispatch helper, and assert:

```python
runtime._handle_new_gradient_signal(signal)
```

creates one `OrderLifecycle` with a `strategy:` key, `side == "buy"` for open, `lighter_side == "SELL"`, `qty == min(single_order_qty, signal.delta_qty)`, and submits a live browser command (`dry_run is False`).

- [ ] **Step 2: Verify RED**

Run:

```bash
python -m unittest tests.test_spread_math.SpreadMathTest
```

Expected: fail because `_handle_new_gradient_signal` and strategy metadata do not exist.

- [ ] **Step 3: Minimal implementation**

Extend `OrderLifecycle` and add helper methods in `main.py`:

- `_create_strategy_order_from_signal(signal, side, qty)`
- `_queue_pending_variational_strategy_order(record)`
- `_handle_new_gradient_signal(signal)`

The helper should submit a `BrowserOrderCommand(side=side, qty=qty, dry_run=False)`.

- [ ] **Step 4: Verify GREEN**

Run:

```bash
python -m unittest tests.test_spread_math.SpreadMathTest
```

Expected: all spread math tests pass.

### Task 2: Tests for Immediate Lighter Hedge and Old Chain Removal

**Files:**
- Modify: `tests/test_spread_math.py`
- Modify: `main.py`

- [ ] **Step 1: Write failing tests**

Add async tests with `unittest.IsolatedAsyncioTestCase` or existing test style that assert:

- `_submit_lighter_hedge_for_strategy_order(record)` sets `lighter_side` based on the signal-derived Var side.
- `process_variational_trade_event` does not call `place_lighter_order` when it creates a non-strategy record from a Var event.

- [ ] **Step 2: Verify RED**

Run:

```bash
python -m unittest tests.test_spread_math
```

Expected: fail because the new helper is missing and the old Var-created hedge chain still exists.

- [ ] **Step 3: Minimal implementation**

Refactor `place_lighter_order(record)` into a general helper that can be called during strategy signal handling. Remove the call from `process_variational_trade_event`:

```python
if created and created_record is not None and self.args.auto_hedge:
    await self.place_lighter_order(created_record)
```

The signal path should call the Lighter helper immediately after creating the record, independently of Var fill.

- [ ] **Step 4: Verify GREEN**

Run:

```bash
python -m unittest tests.test_spread_math
```

Expected: all tests pass.

### Task 3: Tests for Variational Fill Binding

**Files:**
- Modify: `tests/test_spread_math.py`
- Modify: `main.py`

- [ ] **Step 1: Write failing test**

Create a strategy record, queue it as pending, then call `process_variational_trade_event` with matching asset, side, qty, status `confirmed`, price `100`. Assert the existing strategy record receives `trade_id`, `last_variational_status == "filled"`, `var_fill_price == Decimal("100")`, and no duplicate record is appended.

- [ ] **Step 2: Verify RED**

Run:

```bash
python -m unittest tests.test_spread_math
```

Expected: fail because Var events still key directly by Var trade key.

- [ ] **Step 3: Minimal implementation**

Add a pending strategy queue and matching helper:

- `self.pending_variational_strategy_order_keys: deque[str]`
- `_match_pending_variational_strategy_order(side, qty, asset)`

In `process_variational_trade_event`, try the pending match before creating a record from the Var trade key. If matched, update that record and keep its strategy key.

- [ ] **Step 4: Verify GREEN**

Run:

```bash
python -m unittest tests.test_spread_math
```

Expected: all tests pass.

### Task 4: Runtime Integration and Docs

**Files:**
- Modify: `main.py`
- Modify: `README.md`

- [ ] **Step 1: Wire signal handling**

Change `_evaluate_gradient_signal` so a new signal signature calls `_handle_new_gradient_signal(signal)` instead of dry-run spread logging and dry-run browser submission.

- [ ] **Step 2: Update dispatch queue typing**

Change `_browser_order_queue` to carry only `BrowserOrderCommand`, and remove dry-run signal dispatch helpers that are no longer used.

- [ ] **Step 3: Update README**

Replace the dry-run strategy text with live dual-leg execution behavior and clarify that `--no-hedge` only disables Lighter auto-hedge.

- [ ] **Step 4: Full verification**

Run:

```bash
python -m unittest
```

Expected: all tests pass.
