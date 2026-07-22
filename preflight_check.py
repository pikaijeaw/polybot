#!/usr/bin/env python3
"""
Preflight check for Polymarket trading: wallet, CLOB auth, and approved
wallet (allowance) checks — read-only, no orders placed and no transactions
sent. Run this before pointing order_executor.py at real funds.

Three sections:

    WALLET   On-chain (via public Polygon RPC): does the address derived
             from your key/keystore actually hold POL (gas) and USDC.e
             (the collateral Polymarket trades against)?

    CLOB     Can we reach clob.polymarket.com, and does auth work all the
             way up the stack: Level 0 (public), Level 1 (private-key
             signature), Level 2 (derived API key/secret/passphrase)? Also
             checks local/server clock skew (large skew breaks L2 auth
             signatures) and whether the account is flagged closed-only
             (Polymarket restricting you to closing positions, not opening
             new ones).

    APPROVED WALLET   Polymarket's exchange contracts need an ERC20
             allowance on your USDC (to buy) and, if you intend to sell
             conditional tokens, an ERC1155 setApprovalForAll on those
             (to sell). This reads the allowances Polymarket's own backend
             currently sees for your address via get_balance_allowance —
             more authoritative than guessing spender addresses locally,
             since that can go stale. Usually set once by trading through
             polymarket.com's UI, which prompts a MetaMask approval.

This script only reports; it does not submit approvals or trades. Credential
loading is shared with order_executor.py (same env var / keystore flags).

Usage:
    python preflight_check.py
    python preflight_check.py --token-id <clob_token_id>   # also check that token's approval/holdings
    python preflight_check.py --json
"""

import argparse
import json
import sys
import time
from dataclasses import dataclass

from py_clob_client.clob_types import AssetType, BalanceAllowanceParams
from py_clob_client.exceptions import PolyException
from web3 import Web3

from order_executor import (
    DEFAULT_CHAIN_ID,
    DEFAULT_HOST,
    load_private_key,
)

DEFAULT_RPC_URL = "https://polygon-bor-rpc.publicnode.com"
USDC_DECIMALS = 6

ERC20_ABI = [
    {
        "constant": True,
        "inputs": [{"name": "_owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "balance", "type": "uint256"}],
        "type": "function",
    },
]


@dataclass
class CheckResult:
    section: str
    name: str
    status: str  # "PASS" | "WARN" | "FAIL"
    detail: str


def human_usdc(raw) -> str:
    try:
        return f"{int(raw) / 10**USDC_DECIMALS:,.4f}"
    except (TypeError, ValueError):
        return str(raw)


# ---------------------------------------------------------------------------
# Section 1: wallet (on-chain, via public RPC — no CLOB auth needed)
# ---------------------------------------------------------------------------


def wallet_checks(address: str, w3: Web3, collateral_address: str, min_gas_pol: float) -> list:
    results = []

    try:
        native_wei = w3.eth.get_balance(Web3.to_checksum_address(address))
        native_pol = native_wei / 10**18
        if native_pol >= min_gas_pol:
            status, detail = "PASS", f"{native_pol:.4f} POL"
        elif native_pol > 0:
            status, detail = (
                "WARN",
                f"{native_pol:.6f} POL — below --min-gas-pol {min_gas_pol}; fine if you never submit your own on-chain txs (order placement itself is gasless), but you'll need some to submit an approval tx yourself",
            )
        else:
            status, detail = "WARN", "0 POL — can't submit any on-chain tx yourself (e.g. approvals) from this wallet"
        results.append(CheckResult("WALLET", "POL (gas) balance", status, detail))
    except Exception as e:
        results.append(CheckResult("WALLET", "POL (gas) balance", "FAIL", f"RPC error: {e}"))

    try:
        usdc = w3.eth.contract(address=Web3.to_checksum_address(collateral_address), abi=ERC20_ABI)
        raw = usdc.functions.balanceOf(Web3.to_checksum_address(address)).call()
        if raw > 0:
            results.append(CheckResult("WALLET", "USDC.e balance", "PASS", f"{human_usdc(raw)} USDC"))
        else:
            results.append(
                CheckResult(
                    "WALLET",
                    "USDC.e balance",
                    "FAIL",
                    "0 USDC — nothing to trade with. Deposit USDC.e on Polygon to this address.",
                )
            )
    except Exception as e:
        results.append(CheckResult("WALLET", "USDC.e balance", "FAIL", f"RPC error: {e}"))

    return results


# ---------------------------------------------------------------------------
# Section 2: CLOB connectivity + auth ladder
# ---------------------------------------------------------------------------


def clob_connectivity_checks(client) -> list:
    results = []

    try:
        ok = client.get_ok()
        results.append(CheckResult("CLOB", "Level 0: reachable", "PASS", str(ok)))
    except Exception as e:
        results.append(CheckResult("CLOB", "Level 0: reachable", "FAIL", str(e)))
        return results  # nothing downstream will work either

    try:
        server_time = int(client.get_server_time())
        skew = abs(server_time - int(time.time()))
        if skew <= 10:
            results.append(CheckResult("CLOB", "Clock skew", "PASS", f"{skew}s"))
        elif skew <= 60:
            results.append(
                CheckResult(
                    "CLOB",
                    "Clock skew",
                    "WARN",
                    f"{skew}s — L2 auth signatures are timestamp-sensitive, keep this small",
                )
            )
        else:
            results.append(
                CheckResult("CLOB", "Clock skew", "FAIL", f"{skew}s — likely to break L2 auth; fix your system clock")
            )
    except Exception as e:
        results.append(CheckResult("CLOB", "Clock skew", "WARN", f"couldn't check: {e}"))

    return results


def clob_auth_checks(client) -> list:
    results = []

    try:
        creds = client.create_or_derive_api_creds()
        if creds is None:
            raise Exception("no creds returned")
        client.set_api_creds(creds)
        results.append(CheckResult("CLOB", "Level 1: private-key auth", "PASS", f"api_key={creds.api_key}"))
    except Exception as e:
        results.append(CheckResult("CLOB", "Level 1: private-key auth", "FAIL", str(e)))
        return results  # Level 2 needs Level 1 to have succeeded

    try:
        orders = client.get_orders()
        results.append(CheckResult("CLOB", "Level 2: API key auth", "PASS", f"{len(orders)} open order(s) visible"))
    except PolyException as e:
        results.append(CheckResult("CLOB", "Level 2: API key auth", "FAIL", str(e)))
    except Exception as e:
        results.append(CheckResult("CLOB", "Level 2: API key auth", "FAIL", str(e)))

    try:
        closed_only = client.get_closed_only_mode()
        is_closed_only = (
            bool(closed_only) if isinstance(closed_only, bool) else bool(closed_only.get("closed_only", closed_only))
        )
        if is_closed_only:
            results.append(
                CheckResult(
                    "CLOB",
                    "Account restriction",
                    "FAIL",
                    f"account is CLOSED-ONLY (can't open new positions): {closed_only}",
                )
            )
        else:
            results.append(CheckResult("CLOB", "Account restriction", "PASS", "not restricted to closed-only"))
    except Exception as e:
        results.append(CheckResult("CLOB", "Account restriction", "WARN", f"couldn't check: {e}"))

    return results


# ---------------------------------------------------------------------------
# Section 3: approved wallet (allowances) check
# ---------------------------------------------------------------------------


def approval_checks(client, token_id: str | None) -> list:
    results = []

    try:
        collateral = client.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
        balance = collateral.get("balance")
        allowances = collateral.get("allowances", {})

        results.append(CheckResult("APPROVED WALLET", "USDC balance (per CLOB)", "PASS", f"{human_usdc(balance)} USDC"))

        if not allowances:
            results.append(CheckResult("APPROVED WALLET", "USDC allowance", "WARN", "no spender allowances reported"))
        else:
            unapproved = [addr for addr, amt in allowances.items() if int(amt or 0) == 0]
            if not unapproved:
                results.append(
                    CheckResult(
                        "APPROVED WALLET",
                        "USDC allowance",
                        "PASS",
                        f"approved for all {len(allowances)} exchange spender(s)",
                    )
                )
            else:
                results.append(
                    CheckResult(
                        "APPROVED WALLET",
                        "USDC allowance",
                        "FAIL",
                        f"{len(unapproved)}/{len(allowances)} spender(s) NOT approved: {unapproved}. "
                        f"Buy orders routed through an unapproved spender will fail. "
                        f"Fix by trading once through polymarket.com's UI (triggers the MetaMask approval), "
                        f"or submit the ERC20 approve() tx yourself.",
                    )
                )
            for addr, amt in allowances.items():
                status = "PASS" if int(amt or 0) > 0 else "FAIL"
                results.append(CheckResult("APPROVED WALLET", f"  spender {addr}", status, human_usdc(amt)))
    except Exception as e:
        results.append(CheckResult("APPROVED WALLET", "USDC allowance", "FAIL", f"couldn't fetch: {e}"))

    if token_id:
        try:
            conditional = client.get_balance_allowance(
                BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token_id)
            )
            balance = conditional.get("balance")
            allowances = conditional.get("allowances", {})
            results.append(
                CheckResult(
                    "APPROVED WALLET",
                    f"conditional token {token_id[:12]}... balance",
                    "PASS",
                    f"{human_usdc(balance)} shares",
                )
            )
            unapproved = [addr for addr, amt in allowances.items() if int(amt or 0) == 0]
            if unapproved:
                results.append(
                    CheckResult(
                        "APPROVED WALLET",
                        "conditional token allowance",
                        "WARN",
                        f"{len(unapproved)}/{len(allowances)} spender(s) not approved — SELL orders for this token will fail until approved",
                    )
                )
            else:
                results.append(
                    CheckResult(
                        "APPROVED WALLET",
                        "conditional token allowance",
                        "PASS",
                        f"approved for all {len(allowances)} spender(s) (needed to sell)",
                    )
                )
        except Exception as e:
            results.append(
                CheckResult("APPROVED WALLET", "conditional token allowance", "WARN", f"couldn't fetch: {e}")
            )

    return results


def run_all_checks(
    address: str, w3: Web3, collateral_address: str, client, min_gas_pol: float = 0.05, token_id: str | None = None
) -> list:
    """Runs every section (wallet/CLOB/approved-wallet) in the same order as
    main() below, and is the single reusable entry point for callers that
    need a pass/fail verdict rather than a printed report — e.g.
    live_trader.py's mandatory preflight gate and web_dashboard.py's
    Live-mode status display both call this instead of re-deriving the
    check sequence themselves."""
    results = wallet_checks(address, w3, collateral_address, min_gas_pol)

    conn_results = clob_connectivity_checks(client)
    results += conn_results
    if not any(r.status == "FAIL" for r in conn_results):
        auth_results = clob_auth_checks(client)
        results += auth_results
        if client.creds is not None:
            results += approval_checks(client, token_id)

    return results


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

STATUS_ICON = {"PASS": "[PASS]", "WARN": "[WARN]", "FAIL": "[FAIL]"}


def print_report(results: list, address: str):
    print(f"Preflight check for {address}\n")
    current_section = None
    for r in results:
        if r.section != current_section:
            current_section = r.section
            print(f"\n-- {current_section} --")
        print(f"{STATUS_ICON[r.status]} {r.name}: {r.detail}")

    n_fail = sum(1 for r in results if r.status == "FAIL")
    n_warn = sum(1 for r in results if r.status == "WARN")
    print()
    if n_fail:
        print(f"NOT READY — {n_fail} failing check(s), {n_warn} warning(s)")
    elif n_warn:
        print(f"READY WITH WARNINGS — {n_warn} warning(s), no failures")
    else:
        print("READY — all checks passed")
    return n_fail == 0


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--chain-id", type=int, default=DEFAULT_CHAIN_ID)
    parser.add_argument(
        "--rpc-url",
        default=DEFAULT_RPC_URL,
        help=f"Public Polygon RPC for on-chain checks (default: {DEFAULT_RPC_URL})",
    )
    parser.add_argument("--private-key-env", default="POLYMARKET_PRIVATE_KEY")
    parser.add_argument("--keystore", default=None)
    parser.add_argument("--signature-type", type=int, default=0, choices=[0, 1, 2])
    parser.add_argument("--funder", default=None)
    parser.add_argument(
        "--min-gas-pol", type=float, default=0.05, help="POL balance below which gas is flagged as low (default: 0.05)"
    )
    parser.add_argument(
        "--token-id", default=None, help="Also check conditional-token balance/allowance for this CLOB token id"
    )
    parser.add_argument("--json", action="store_true", help="Print results as JSON instead of a text report")
    args = parser.parse_args()

    if args.signature_type in (1, 2) and not args.funder:
        parser.error("--funder is required when --signature-type is 1 or 2")

    # Loaded exactly once — with --keystore this prompts for a password, and
    # we don't want to ask twice.
    private_key = load_private_key(args)
    from eth_account import Account

    address = Account.from_key(private_key).address

    w3 = Web3(Web3.HTTPProvider(args.rpc_url, request_kwargs={"timeout": 10}))
    if not w3.is_connected():
        print(f"Could not connect to RPC {args.rpc_url}", file=sys.stderr)
        sys.exit(1)

    from py_clob_client.config import get_contract_config

    collateral_address = get_contract_config(args.chain_id).collateral

    from py_clob_client.client import ClobClient

    client = ClobClient(
        host=args.host,
        chain_id=args.chain_id,
        key=private_key,
        signature_type=args.signature_type,
        funder=args.funder,
    )

    results = run_all_checks(address, w3, collateral_address, client, args.min_gas_pol, args.token_id)

    if args.json:
        print(json.dumps([r.__dict__ for r in results], indent=2))
        ready = not any(r.status == "FAIL" for r in results)
    else:
        ready = print_report(results, address)

    sys.exit(0 if ready else 1)


if __name__ == "__main__":
    main()
