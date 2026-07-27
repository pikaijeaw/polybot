#!/usr/bin/env python3
"""
Order executor for the Polymarket CLOB.

Thin, safety-first wrapper around polymarket-client (the official SDK) for
placing/cancelling orders and checking balances. Everything defaults to a
dry run that prints exactly what would be submitted without sending it —
pass --live to actually place an order, and you'll still be asked to type
"yes" unless you also pass --yes (fine for scripted/automated use once you
trust the caller).

Migration note: this used to wrap py_clob_client, which Polymarket has
since archived ("no longer functional" per its own README) in favor of the
unified polymarket-client SDK. The account model changed alongside the
library: trading collateral is now pUSD (wrapped from USDC/USDC.e via
Polymarket's "Collateral Onramp"), held in a "Deposit Wallet" — a
smart-contract wallet deterministically derived from your EOA's private
key, not the raw EOA address itself. polymarket-client's SecureClient
handles that derivation automatically; you don't need to know or compute
the Deposit Wallet address yourself (see --wallet below for the rare case
you'd want to override it).

Credentials:
    Never pass a private key on the command line (it lands in shell history
    and `ps`). Supply it one of two ways:

      --private-key-env VAR   Read the key from env var VAR (default:
                               POLYMARKET_PRIVATE_KEY). e.g.:
                                 export POLYMARKET_PRIVATE_KEY=0x...
                                 python order_executor.py balance

      --keystore PATH          An encrypted keystore JSON, e.g. one produced
                               by createwallet/create_polygon_wallet.py
                               --save-keystore. You'll be prompted for the
                               password.

    A .env file next to this script (see .env.example) is loaded
    automatically — POLYMARKET_PRIVATE_KEY set there works the same as
    exporting it, without polluting your shell's env var history. Real
    environment variables still take precedence over .env.

    API credentials are derived from the private key fresh on every run by
    SecureClient.create() — nothing is cached to disk.

    --wallet overrides the address SecureClient acts for. Leave it unset —
    it defaults to your signer's Deposit Wallet, which is what you want in
    the overwhelming majority of cases. This replaces the old
    --signature-type/--funder pair entirely; the new SDK auto-detects
    whether an address is a plain EOA or a Deposit Wallet, so there's
    nothing to tell it manually anymore.

Usage:
    python order_executor.py balance --token-id <id>
    python order_executor.py limit  --token-id <id> --side BUY --price 0.52 --size 20
    python order_executor.py limit  --token-id <id> --side BUY --price 0.52 --size 20 --live
    python order_executor.py market --token-id <id> --side BUY --amount 10 --live --yes
    python order_executor.py orders
    python order_executor.py cancel --order-id <id> --live
    python order_executor.py cancel --all --live

Note: limit orders no longer take an --order-type (GTC/GTD/FOK/FAK) choice —
the new SDK only exposes FOK/FAK for market orders. A limit order is GTC by
default; pass --expiration <unix-ts> for GTD instead. This is a genuine API
shape change from py_clob_client, not an oversight.

This only talks to Polymarket's CLOB (clob.polymarket.com); note that the
CLOB itself geoblocks some regions/VPNs regardless of what this script does
— a 403 "Trading restricted in your region" is Polymarket's server, not a
bug here.
"""

import argparse
import getpass
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv
from polymarket import PolymarketError, SecureClient

# Real env vars always win over .env — load_dotenv() defaults to not overriding.
load_dotenv(Path(__file__).resolve().parent / ".env")

BUY = "BUY"
SELL = "SELL"
VALID_SIDES = (BUY, SELL)
VALID_MARKET_ORDER_TYPES = ("FOK", "FAK")


# ---------------------------------------------------------------------------
# Credentials / client setup
# ---------------------------------------------------------------------------


def load_private_key(args) -> str:
    if args.keystore:
        from eth_account import Account

        with open(args.keystore) as f:
            keystore = json.load(f)
        password = getpass.getpass(f"Password for keystore {args.keystore}: ")
        account = Account.decrypt(keystore, password)
        return "0x" + account.hex()

    key = os.environ.get(args.private_key_env)
    if not key:
        print(
            f"No private key found. Set ${args.private_key_env} or pass --keystore PATH.\n"
            f"(createwallet/create_polygon_wallet.py can generate a wallet/keystore.)",
            file=sys.stderr,
        )
        sys.exit(1)
    return key


def build_client(args) -> SecureClient:
    private_key = load_private_key(args)
    client = SecureClient.create(private_key=private_key, wallet=args.wallet)
    print(f"Authenticated as {client.wallet} (wallet_type={client.wallet_type})", file=sys.stderr)
    return client


# ---------------------------------------------------------------------------
# Risk controls
# ---------------------------------------------------------------------------


@dataclass
class RiskLimits:
    max_order_usd: float = 20.0
    min_price: float = 0.01
    max_price: float = 0.99


class RiskCheckFailed(Exception):
    pass


def check_limit_order(limits: RiskLimits, price: float, size: float) -> None:
    if not (limits.min_price <= price <= limits.max_price):
        raise RiskCheckFailed(f"price {price} outside allowed range [{limits.min_price}, {limits.max_price}]")
    notional = price * size
    if notional > limits.max_order_usd:
        raise RiskCheckFailed(f"order notional ${notional:.2f} exceeds --max-order-usd ${limits.max_order_usd:.2f}")


def check_market_order(limits: RiskLimits, side: str, amount: float, reference_price: float | None) -> None:
    if side == "BUY":
        notional = amount  # amount is already $$$ for a BUY market order
    else:
        notional = amount * reference_price if reference_price else amount  # best-effort estimate for a SELL
    if notional > limits.max_order_usd:
        raise RiskCheckFailed(f"order notional ~${notional:.2f} exceeds --max-order-usd ${limits.max_order_usd:.2f}")


# ---------------------------------------------------------------------------
# Confirmation
# ---------------------------------------------------------------------------


def confirm_or_abort(summary: str, auto_yes: bool) -> None:
    print(summary)
    if auto_yes:
        return
    try:
        answer = input("Type 'yes' to submit this order: ")
    except EOFError:
        answer = ""
    if answer.strip().lower() != "yes":
        print("Not confirmed; aborting without submitting.", file=sys.stderr)
        sys.exit(1)


# ---------------------------------------------------------------------------
# Order operations
# ---------------------------------------------------------------------------


def place_limit_order(
    client: SecureClient,
    limits: RiskLimits,
    token_id: str,
    side: str,
    price: float,
    size: float,
    expiration: int | None,
    live: bool,
    auto_yes: bool,
):
    check_limit_order(limits, price, size)

    summary = (
        f"LIMIT order  side={side}  token_id={token_id}  price={price}  size={size}  "
        f"expiration={expiration or 'GTC (none)'}  notional=${price * size:.2f}  mode={'LIVE' if live else 'DRY RUN'}"
    )

    if not live:
        print(summary)
        print("(dry run — pass --live to submit)")
        return None

    confirm_or_abort(summary, auto_yes)

    result = client.place_limit_order(token_id=token_id, price=price, size=size, side=side, expiration=expiration)
    print(result.model_dump_json(indent=2))
    return result


def place_market_order(
    client: SecureClient,
    limits: RiskLimits,
    token_id: str,
    side: str,
    amount: float,
    order_type: str,
    live: bool,
    auto_yes: bool,
):
    reference_price = None
    try:
        reference_price = float(client.get_price(token_id=token_id, side=side))
    except Exception:
        pass
    check_market_order(limits, side, amount, reference_price)

    unit = "USD to spend" if side == "BUY" else "shares to sell"
    summary = (
        f"MARKET order  side={side}  token_id={token_id}  amount={amount} ({unit})  "
        f"type={order_type}  reference_price={reference_price}  mode={'LIVE' if live else 'DRY RUN'}"
    )

    if not live:
        print(summary)
        print("(dry run — pass --live to submit)")
        return None

    confirm_or_abort(summary, auto_yes)

    kwarg = {"amount": amount} if side == "BUY" else {"shares": amount}
    result = client.place_market_order(token_id=token_id, side=side, order_type=order_type, **kwarg)
    print(result.model_dump_json(indent=2))
    return result


def cancel_order(client: SecureClient, order_id: str | None, cancel_all: bool, live: bool, auto_yes: bool):
    if cancel_all:
        summary = f"CANCEL ALL open orders  mode={'LIVE' if live else 'DRY RUN'}"
    else:
        summary = f"CANCEL order_id={order_id}  mode={'LIVE' if live else 'DRY RUN'}"

    if not live:
        print(summary)
        print("(dry run — pass --live to submit)")
        return None

    confirm_or_abort(summary, auto_yes)

    result = client.cancel_all() if cancel_all else client.cancel_order(order_id=order_id)
    print(result.model_dump_json(indent=2))
    return result


def show_balance(client: SecureClient, token_id: str | None):
    collateral = client.get_balance_allowance(asset_type="COLLATERAL")
    print("Collateral (pUSD):")
    print(collateral.model_dump_json(indent=2))

    if token_id:
        conditional = client.get_balance_allowance(asset_type="CONDITIONAL", token_id=token_id)
        print(f"\nConditional token {token_id}:")
        print(conditional.model_dump_json(indent=2))


def show_orders(client: SecureClient, market: str | None, token_id: str | None):
    orders = list(client.list_open_orders(market=market, token_id=token_id).iter_items())
    print(json.dumps([o.model_dump(mode="json") for o in orders], indent=2))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def add_common_args(parser: argparse.ArgumentParser):
    parser.add_argument(
        "--private-key-env",
        default="POLYMARKET_PRIVATE_KEY",
        help="Env var holding the private key (default: POLYMARKET_PRIVATE_KEY)",
    )
    parser.add_argument("--keystore", default=None, help="Path to an encrypted keystore JSON instead of an env var")
    parser.add_argument(
        "--wallet",
        default=os.environ.get("POLYMARKET_WALLET") or None,
        help="Address to act for (default: your signer's Deposit Wallet, auto-derived — leave unset unless you have a specific reason to override it)",
    )
    parser.add_argument(
        "--max-order-usd", type=float, default=20.0, help="Hard cap on order notional in USD (default: 20.0)"
    )
    parser.add_argument(
        "--live", action="store_true", help="Actually submit to the CLOB (default: dry run / print only)"
    )
    parser.add_argument("--yes", action="store_true", help="Skip the interactive confirmation prompt for --live orders")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    p_balance = sub.add_parser("balance", help="Show pUSD and (optionally) a conditional token balance")
    p_balance.add_argument("--token-id", default=None)
    add_common_args(p_balance)

    p_orders = sub.add_parser("orders", help="List open orders")
    p_orders.add_argument("--market", default=None, help="Filter by condition id")
    p_orders.add_argument("--token-id", default=None, help="Filter by token id")
    add_common_args(p_orders)

    p_limit = sub.add_parser("limit", help="Place a limit order")
    p_limit.add_argument("--token-id", required=True)
    p_limit.add_argument("--side", required=True, choices=VALID_SIDES)
    p_limit.add_argument("--price", type=float, required=True)
    p_limit.add_argument("--size", type=float, required=True, help="Size in shares of the conditional token")
    p_limit.add_argument("--expiration", type=int, default=None, help="Unix timestamp for GTD; omit for GTC (default)")
    add_common_args(p_limit)

    p_market = sub.add_parser("market", help="Place a market order")
    p_market.add_argument("--token-id", required=True)
    p_market.add_argument("--side", required=True, choices=VALID_SIDES)
    p_market.add_argument("--amount", type=float, required=True, help="BUY: USD to spend. SELL: shares to sell.")
    p_market.add_argument("--order-type", default="FOK", choices=VALID_MARKET_ORDER_TYPES)
    add_common_args(p_market)

    p_cancel = sub.add_parser("cancel", help="Cancel an order (or all orders)")
    p_cancel.add_argument("--order-id", default=None)
    p_cancel.add_argument("--all", dest="cancel_all", action="store_true")
    add_common_args(p_cancel)

    args = parser.parse_args()

    if args.command == "cancel" and not args.cancel_all and not args.order_id:
        parser.error("cancel requires --order-id or --all")

    client = build_client(args)
    limits = RiskLimits(max_order_usd=args.max_order_usd)

    try:
        if args.command == "balance":
            show_balance(client, args.token_id)
        elif args.command == "orders":
            show_orders(client, args.market, args.token_id)
        elif args.command == "limit":
            place_limit_order(
                client,
                limits,
                args.token_id,
                args.side,
                args.price,
                args.size,
                args.expiration,
                args.live,
                args.yes,
            )
        elif args.command == "market":
            place_market_order(
                client,
                limits,
                args.token_id,
                args.side,
                args.amount,
                args.order_type,
                args.live,
                args.yes,
            )
        elif args.command == "cancel":
            cancel_order(client, args.order_id, args.cancel_all, args.live, args.yes)
    except RiskCheckFailed as e:
        print(f"Risk check failed: {e}", file=sys.stderr)
        sys.exit(1)
    except PolymarketError as e:
        print(f"CLOB API error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        client.close()


if __name__ == "__main__":
    main()
