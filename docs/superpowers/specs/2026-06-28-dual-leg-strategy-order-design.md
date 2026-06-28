# Dual-Leg Strategy Order Design

## Goal

Gradient strategy signals execute both venues immediately:

- Open: buy Variational, sell Lighter.
- Close: sell Variational, buy Lighter.
- Quantity: `min(single_order_qty, signal.delta_qty)`.

Lighter hedging must not wait for Variational fill confirmation. Both venue fills later update the same strategy order record so spread, slippage, position, and PnL use one coherent round.

## Current Behavior

Gradient signals only submit dry-run browser orders. Live order records are created from Variational trade events, and new Variational records call `place_lighter_order(created_record)`. That makes Lighter depend on Variational fill discovery.

## Target Behavior

When a new gradient signal signature appears:

1. Create one local strategy order record with a `strategy:<time>` key.
2. Store action, side, qty, target qty, current qty, trigger spread, and asset.
3. Add the record to a pending Variational binding queue keyed by side, qty, and asset.
4. Submit a real Variational browser order.
5. Submit a Lighter hedge order immediately if auto-hedge is enabled.
6. Later Variational trade events bind to the pending strategy record and fill Var price/status fields.
7. Later Lighter account-order updates bind through `client_order_id` and fill Lighter price fields.

The old `process_variational_trade_event -> created_record -> place_lighter_order` chain is disabled.

## Matching Rules

Lighter matching remains exact through `client_order_id`.

Variational matching uses the best available local correlation because the browser order broker does not return a Variational order ID. The runtime binds incoming Variational trade events to the oldest pending strategy order with matching asset, side, and quantity. If no pending strategy order matches, it falls back to creating a normal record from the Variational event.

## Failure Handling

If the Var browser order fails, the strategy record stores a `var_order_error` and remains visible. Lighter may already have been submitted because execution is concurrent.

If the Lighter order fails, the strategy record stores `hedge_error` as it does today.

If either venue fills later, that venue updates the same strategy record independently.

## Tests

Add focused tests for:

- Strategy signal creates one record and enqueues both Var and Lighter work.
- Browser order command is live, not dry-run.
- Lighter hedge side follows the signal action.
- Variational fill binds to the pending strategy record.
- New Variational event creation no longer triggers Lighter hedging.
