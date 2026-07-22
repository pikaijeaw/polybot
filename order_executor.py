#!/usr/bin/env python3
"""
Order executor for the Polymarket CLOB.

Thin, safety-first wrapper around py_clob_client for placing/cancelling
orders and checking balances. Everything defaults to a dry run that prints
exactly what would be submitted without sending it — pass --live to actually
place an order, and you'll still be asked to type "yes" unless you also pass
--yes (fine for scripted/automated use once you trust the caller).

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

    API credentials (key/secret/passphrase) are derived from the private key
    fresh on every run via create_or_derive_api_creds() — nothing is cached
    to disk.

    --signature-type defaults to 0 (a plain EOA wallet trading directly, no
    Polymarket proxy). If you're trading through Polymarket's email/magic or
    browser-wallet proxy instead, pass --signature-type 1 or 2 and --funder
    <proxy address>.

Usage:
    python order_executor.py balance --token-id <id>
    python order_executor.py limit  --token-id <id> --side BUY --price 0.52 --size 20
    python order_executor.py limit  --token-id <id> --side BUY --price 0.52 --size 20 --live
    python order_executor.py market --token-id <id> --side BUY --amount 10 --live --yes
    python order_executor.py orders
    python order_executor.py cancel --order-id <id> --live
    python order_executor.py cancel --all --live

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
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import (
    AssetType,
    BalanceAllowanceParams,
    MarketOrderArgs,
    OpenOrderParams,
    OrderArgs,
    OrderType,
)
from py_clob_client.exceptions import PolyException
from py_clob_client.order_builder.constants import BUY, SELL

# Real env vars always win over .env — load_dotenv() defaults to not overriding.
load_dotenv(Path(__file__).resolve().parent / ".env")

DEFAULT_HOST = "https://clob.polymarket.com"
DEFAULT_CHAIN_ID = 137  # Polygon mainnet
VALID_SIDES = (BUY, SELL)
VALID_ORDER_TYPES = {t: getattr(OrderType, t) for t in ("GTC", "GTD", "FOK", "FAK")}


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


def build_client(args) -> ClobClient:
    private_key = load_private_key(args)
    client = ClobClient(
        host=args.host,
        chain_id=args.chain_id,
        key=private_key,
        signature_type=args.signature_type,
        funder=args.funder,
    )
    creds = client.create_or_derive_api_creds()
    if creds is None:
        print("Failed to create/derive CLOB API credentials.", file=sys.stderr)
        sys.exit(1)
    client.set_api_creds(creds)
    print(f"Authenticated as {client.get_address()} (signature_type={args.signature_type})", file=sys.stderr)
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
    if side == BUY:
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

def place_limit_order(client: ClobClient, limits: RiskLimits, token_id: str, side: str,
                       price: float, size: float, order_type: OrderType,
                       live: bool, auto_yes: bool):
    check_limit_order(limits, price, size)

    summary = (
        f"LIMIT order  side={side}  token_id={token_id}  price={price}  size={size}  "
        f"type={order_type}  notional=${price * size:.2f}  mode={'LIVE' if live else 'DRY RUN'}"
    )

    if not live:
        print(summary)
        print("(dry run — pass --live to submit)")
        return None

    confirm_or_abort(summary, auto_yes)

    order_args = OrderArgs(token_id=token_id, price=price, size=size, side=side)
    signed_order = client.create_order(order_args)
    result = client.post_order(signed_order, orderType=order_type)
    print(json.dumps(result, indent=2))
    return result


def place_market_order(client: ClobClient, limits: RiskLimits, token_id: str, side: str,
                        amount: float, order_type: OrderType, live: bool, auto_yes: bool):
    reference_price = None
    try:
        reference_price = float(client.get_price(token_id, side)["price"])
    except Exception:
        pass
    check_market_order(limits, side, amount, reference_price)

    unit = "USD to spend" if side == BUY else "shares to sell"
    summary = (
        f"MARKET order  side={side}  token_id={token_id}  amount={amount} ({unit})  "
        f"type={order_type}  reference_price={reference_price}  mode={'LIVE' if live else 'DRY RUN'}"
    )

    if not live:
        print(summary)
        print("(dry run — pass --live to submit)")
        return None

    confirm_or_abort(summary, auto_yes)

    order_args = MarketOrderArgs(token_id=token_id, amount=amount, side=side, order_type=order_type)
    signed_order = client.create_market_order(order_args)
    result = client.post_order(signed_order, orderType=order_type)
    print(json.dumps(result, indent=2))
    return result


def cancel_order(client: ClobClient, order_id: str | None, cancel_all: bool, live: bool, auto_yes: bool):
    if cancel_all:
        summary = f"CANCEL ALL open orders  mode={'LIVE' if live else 'DRY RUN'}"
    else:
        summary = f"CANCEL order_id={order_id}  mode={'LIVE' if live else 'DRY RUN'}"

    if not live:
        print(summary)
        print("(dry run — pass --live to submit)")
        return None

    confirm_or_abort(summary, auto_yes)

    result = client.cancel_all() if cancel_all else client.cancel(order_id)
    print(json.dumps(result, indent=2))
    return result


def show_balance(client: ClobClient, token_id: str | None):
    usdc = client.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
    print("USDC (collateral):")
    print(json.dumps(usdc, indent=2))

    if token_id:
        conditional = client.get_balance_allowance(
            BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token_id)
        )
        print(f"\nConditional token {token_id}:")
        print(json.dumps(conditional, indent=2))


def show_orders(client: ClobClient, market: str | None, asset_id: str | None):
    params = OpenOrderParams(market=market, asset_id=asset_id) if (market or asset_id) else None
    orders = client.get_orders(params)
    print(json.dumps(orders, indent=2))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def add_common_args(parser: argparse.ArgumentParser):
    parser.add_argument("--host", default=DEFAULT_HOST, help=f"CLOB host (default: {DEFAULT_HOST})")
    parser.add_argument("--chain-id", type=int, default=DEFAULT_CHAIN_ID, help=f"Chain id (default: {DEFAULT_CHAIN_ID}, Polygon)")
    parser.add_argument("--private-key-env", default="POLYMARKET_PRIVATE_KEY", help="Env var holding the private key (default: POLYMARKET_PRIVATE_KEY)")
    parser.add_argument("--keystore", default=None, help="Path to an encrypted keystore JSON instead of an env var")
    parser.add_argument("--signature-type", type=int, default=0, choices=[0, 1, 2], help="0=EOA (default), 1=email/magic proxy, 2=browser-wallet proxy")
    parser.add_argument("--funder", default=os.environ.get("POLYMARKET_FUNDER") or None, help="Proxy wallet address, required if --signature-type is 1 or 2 (or set POLYMARKET_FUNDER)")
    parser.add_argument("--max-order-usd", type=float, default=20.0, help="Hard cap on order notional in USD (default: 20.0)")
    parser.add_argument("--live", action="store_true", help="Actually submit to the CLOB (default: dry run / print only)")
    parser.add_argument("--yes", action="store_true", help="Skip the interactive confirmation prompt for --live orders")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    p_balance = sub.add_parser("balance", help="Show USDC and (optionally) a conditional token balance")
    p_balance.add_argument("--token-id", default=None)
    add_common_args(p_balance)

    p_orders = sub.add_parser("orders", help="List open orders")
    p_orders.add_argument("--market", default=None, help="Filter by condition id")
    p_orders.add_argument("--token-id", dest="asset_id", default=None, help="Filter by token id")
    add_common_args(p_orders)

    p_limit = sub.add_parser("limit", help="Place a limit order")
    p_limit.add_argument("--token-id", required=True)
    p_limit.add_argument("--side", required=True, choices=VALID_SIDES)
    p_limit.add_argument("--price", type=float, required=True)
    p_limit.add_argument("--size", type=float, required=True, help="Size in shares of the conditional token")
    p_limit.add_argument("--order-type", default="GTC", choices=list(VALID_ORDER_TYPES))
    add_common_args(p_limit)

    p_market = sub.add_parser("market", help="Place a market order")
    p_market.add_argument("--token-id", required=True)
    p_market.add_argument("--side", required=True, choices=VALID_SIDES)
    p_market.add_argument("--amount", type=float, required=True, help="BUY: USD to spend. SELL: shares to sell.")
    p_market.add_argument("--order-type", default="FOK", choices=["FOK", "FAK"])
    add_common_args(p_market)

    p_cancel = sub.add_parser("cancel", help="Cancel an order (or all orders)")
    p_cancel.add_argument("--order-id", default=None)
    p_cancel.add_argument("--all", dest="cancel_all", action="store_true")
    add_common_args(p_cancel)

    args = parser.parse_args()

    if args.command == "cancel" and not args.cancel_all and not args.order_id:
        parser.error("cancel requires --order-id or --all")
    if args.signature_type in (1, 2) and not args.funder:
        parser.error("--funder is required when --signature-type is 1 or 2")

    client = build_client(args)
    limits = RiskLimits(max_order_usd=args.max_order_usd)

    try:
        if args.command == "balance":
            show_balance(client, args.token_id)
        elif args.command == "orders":
            show_orders(client, args.market, args.asset_id)
        elif args.command == "limit":
            place_limit_order(
                client, limits, args.token_id, args.side, args.price, args.size,
                VALID_ORDER_TYPES[args.order_type], args.live, args.yes,
            )
        elif args.command == "market":
            place_market_order(
                client, limits, args.token_id, args.side, args.amount,
                VALID_ORDER_TYPES[args.order_type], args.live, args.yes,
            )
        elif args.command == "cancel":
            cancel_order(client, args.order_id, args.cancel_all, args.live, args.yes)
    except RiskCheckFailed as e:
        print(f"Risk check failed: {e}", file=sys.stderr)
        sys.exit(1)
    except PolyException as e:
        print(f"CLOB API error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
