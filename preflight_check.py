#!/usr/bin/env python3
"""
Preflight check for Polymarket trading: wallet, CLOB auth, and approved
wallet (allowance) checks — read-only, no orders placed and no transactions
sent. Run this before pointing order_executor.py at real funds.

Migration note: this used to read a raw EOA's on-chain USDC.e balance and
authenticate a py_clob_client ClobClient directly. Both changed together —
py_clob_client is archived ("no longer functional" per its own README), and
Polymarket's account model has moved to pUSD (wrapped from USDC/USDC.e via
the "Collateral Onramp") held in a Deposit Wallet — a smart-contract wallet
deterministically derived from your EOA, not the EOA itself. This script
now authenticates via polymarket-client's SecureClient (which derives that
Deposit Wallet address automatically) and reports on TWO addresses: your
EOA (client.signer — still relevant for POL/gas and any on-chain tx you'd
submit yourself) and your Deposit Wallet (client.wallet — where pUSD
collateral actually lives and what the CLOB trades against).

Three sections:

    WALLET   On-chain (via public Polygon RPC): does your EOA hold POL
             (gas), and does your Deposit Wallet hold pUSD (the collateral
             Polymarket trades against)? The pUSD check here is an
             independent on-chain cross-check of the CLOB-reported balance
             in APPROVED WALLET below, same "don't trust one source"
             philosophy as before.

    CLOB     Can we reach clob.polymarket.com, and does auth work all the
             way up the stack: Level 0 (public), Level 1 (private-key
             signature — proven by SecureClient.create() succeeding at
             all), Level 2 (authenticated reads working). Also checks
             whether the account is flagged closed-only (Polymarket
             restricting you to closing positions, not opening new ones)
             and whether the Deposit Wallet is gasless-ready (order
             placement won't require POL if so). There is no clock-skew
             check anymore — the old py_clob_client exposed a raw
             get_server_time() call for this because it built L2 auth
             headers manually; polymarket-client doesn't expose an
             equivalent, and its own request signing isn't something this
             script has visibility into to second-guess.

    APPROVED WALLET   Polymarket's exchange contracts need an ERC20
             allowance on your pUSD (to buy) and, if you intend to sell
             conditional tokens, an ERC1155 setApprovalForAll on those (to
             sell). This reads the allowances Polymarket's own backend
             currently sees for your Deposit Wallet via
             get_balance_allowance — more authoritative than guessing
             spender addresses locally. For a Deposit Wallet these are
             normally already set (gaslessly) by Polymarket's own
             Collateral Onramp when you convert into pUSD — nothing to run
             yourself in the common case.

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
from dataclasses import dataclass

from polymarket import PublicClient, SecureClient
from web3 import Web3

from order_executor import load_private_key

DEFAULT_RPC_URL = "https://polygon-bor-rpc.publicnode.com"
COLLATERAL_DECIMALS = 6

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


def human_pusd(raw) -> str:
    try:
        return f"{int(raw) / 10**COLLATERAL_DECIMALS:,.4f}"
    except (TypeError, ValueError):
        return str(raw)


# ---------------------------------------------------------------------------
# Section 1: wallet (on-chain, via public RPC — no CLOB auth needed)
# ---------------------------------------------------------------------------


def wallet_checks(
    eoa_address: str, deposit_wallet_address: str, w3: Web3, collateral_address: str, min_gas_pol: float
) -> list:
    results = []

    try:
        native_wei = w3.eth.get_balance(Web3.to_checksum_address(eoa_address))
        native_pol = native_wei / 10**18
        if native_pol >= min_gas_pol:
            status, detail = "PASS", f"{native_pol:.4f} POL"
        elif native_pol > 0:
            status, detail = (
                "WARN",
                f"{native_pol:.6f} POL — below --min-gas-pol {min_gas_pol}; fine if you never submit your own on-chain txs (order placement itself is gasless once the Deposit Wallet is set up), but you'll need some to submit a transaction yourself",
            )
        else:
            status, detail = (
                "WARN",
                "0 POL — can't submit any on-chain tx yourself from this EOA (order placement itself is still gasless)",
            )
        results.append(CheckResult("WALLET", "POL (gas) balance (EOA)", status, detail))
    except Exception as e:
        results.append(CheckResult("WALLET", "POL (gas) balance (EOA)", "FAIL", f"RPC error: {e}"))

    try:
        pusd = w3.eth.contract(address=Web3.to_checksum_address(collateral_address), abi=ERC20_ABI)
        raw = pusd.functions.balanceOf(Web3.to_checksum_address(deposit_wallet_address)).call()
        if raw > 0:
            results.append(
                CheckResult("WALLET", "pUSD balance (Deposit Wallet, on-chain)", "PASS", f"{human_pusd(raw)} pUSD")
            )
        else:
            results.append(
                CheckResult(
                    "WALLET",
                    "pUSD balance (Deposit Wallet, on-chain)",
                    "FAIL",
                    "0 pUSD — nothing to trade with. Convert USDC/USDC.e into pUSD via Polymarket's deposit flow.",
                )
            )
    except Exception as e:
        results.append(CheckResult("WALLET", "pUSD balance (Deposit Wallet, on-chain)", "FAIL", f"RPC error: {e}"))

    return results


# ---------------------------------------------------------------------------
# Section 2: CLOB connectivity + auth ladder
# ---------------------------------------------------------------------------


def clob_reachability_check(environment) -> list:
    try:
        with PublicClient(environment=environment) as public_client:
            public_client.list_markets(page_size=1).first_page()
        return [CheckResult("CLOB", "Level 0: reachable", "PASS", "OK")]
    except Exception as e:
        return [CheckResult("CLOB", "Level 0: reachable", "FAIL", str(e))]


def clob_auth_checks(client: SecureClient) -> list:
    """Level 1 (private-key auth) is proven implicitly by having a
    constructed, credentialed SecureClient at all — SecureClient.create()
    raises if credential derivation/validation fails, so by the time this
    is called that step has already succeeded. Level 2 (API key actually
    authorizing requests) still needs its own explicit check, since a
    derived-but-invalid key could still construct successfully."""
    results = [CheckResult("CLOB", "Level 1: private-key auth", "PASS", f"api_key={client.credentials.key}")]

    try:
        orders = list(client.list_open_orders().iter_items())
        results.append(CheckResult("CLOB", "Level 2: API key auth", "PASS", f"{len(orders)} open order(s) visible"))
    except Exception as e:
        results.append(CheckResult("CLOB", "Level 2: API key auth", "FAIL", str(e)))

    try:
        is_closed_only = client.get_closed_only_mode()
        if is_closed_only:
            results.append(
                CheckResult("CLOB", "Account restriction", "FAIL", "account is CLOSED-ONLY (can't open new positions)")
            )
        else:
            results.append(CheckResult("CLOB", "Account restriction", "PASS", "not restricted to closed-only"))
    except Exception as e:
        results.append(CheckResult("CLOB", "Account restriction", "WARN", f"couldn't check: {e}"))

    try:
        gasless_ready = client.is_gasless_ready()
        if gasless_ready:
            results.append(
                CheckResult("CLOB", "Gasless Deposit Wallet", "PASS", "ready — order placement won't need POL")
            )
        else:
            results.append(
                CheckResult(
                    "CLOB",
                    "Gasless Deposit Wallet",
                    "WARN",
                    "not ready yet — call client.setup_gasless_wallet() once, or place an order through polymarket.com's UI first",
                )
            )
    except Exception as e:
        results.append(CheckResult("CLOB", "Gasless Deposit Wallet", "WARN", f"couldn't check: {e}"))

    return results


# ---------------------------------------------------------------------------
# Section 3: approved wallet (allowances) check
# ---------------------------------------------------------------------------


def approval_checks(client: SecureClient, token_id: str | None) -> list:
    results = []

    try:
        collateral = client.get_balance_allowance(asset_type="COLLATERAL")
        allowances = collateral.allowances or {}

        results.append(
            CheckResult("APPROVED WALLET", "pUSD balance (per CLOB)", "PASS", f"{human_pusd(collateral.balance)} pUSD")
        )

        if not allowances:
            results.append(CheckResult("APPROVED WALLET", "pUSD allowance", "WARN", "no spender allowances reported"))
        else:
            unapproved = [addr for addr, amt in allowances.items() if int(amt or 0) == 0]
            if not unapproved:
                results.append(
                    CheckResult(
                        "APPROVED WALLET",
                        "pUSD allowance",
                        "PASS",
                        f"approved for all {len(allowances)} exchange spender(s)",
                    )
                )
            else:
                results.append(
                    CheckResult(
                        "APPROVED WALLET",
                        "pUSD allowance",
                        "FAIL",
                        f"{len(unapproved)}/{len(allowances)} spender(s) NOT approved: {unapproved}. "
                        f"Buy orders routed through an unapproved spender will fail. For a Deposit Wallet this is "
                        f"normally set automatically by Polymarket's Collateral Onramp — call "
                        f"client.setup_trading_approvals() if it wasn't, or trade once through polymarket.com's UI.",
                    )
                )
            for addr, amt in allowances.items():
                status = "PASS" if int(amt or 0) > 0 else "FAIL"
                results.append(CheckResult("APPROVED WALLET", f"  spender {addr}", status, human_pusd(amt)))
    except Exception as e:
        results.append(CheckResult("APPROVED WALLET", "pUSD allowance", "FAIL", f"couldn't fetch: {e}"))

    if token_id:
        try:
            conditional = client.get_balance_allowance(asset_type="CONDITIONAL", token_id=token_id)
            allowances = conditional.allowances or {}
            results.append(
                CheckResult(
                    "APPROVED WALLET",
                    f"conditional token {token_id[:12]}... balance",
                    "PASS",
                    f"{human_pusd(conditional.balance)} shares",
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


def run_all_checks(client: SecureClient, w3: Web3, min_gas_pol: float = 0.05, token_id: str | None = None) -> list:
    """Runs every section (wallet/CLOB/approved-wallet) in the same order as
    main() below, and is the single reusable entry point for callers that
    need a pass/fail verdict rather than a printed report — e.g.
    live_trader.py's mandatory preflight gate and web_dashboard.py's
    Live-mode status display both call this instead of re-deriving the
    check sequence themselves. Takes an already-constructed, authenticated
    SecureClient (Level 1 auth already proven by that point) rather than a
    raw address/key, since client construction itself IS the Level 1 check
    now — see clob_auth_checks' docstring."""
    results = wallet_checks(client.signer, client.wallet, w3, client.environment.collateral_token, min_gas_pol)
    results += clob_reachability_check(client.environment)
    results += clob_auth_checks(client)
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
    parser.add_argument(
        "--rpc-url",
        default=DEFAULT_RPC_URL,
        help=f"Public Polygon RPC for on-chain checks (default: {DEFAULT_RPC_URL})",
    )
    parser.add_argument("--private-key-env", default="POLYMARKET_PRIVATE_KEY")
    parser.add_argument("--keystore", default=None)
    parser.add_argument(
        "--wallet", default=None, help="Address to act for (default: your signer's Deposit Wallet, auto-derived)"
    )
    parser.add_argument(
        "--min-gas-pol", type=float, default=0.05, help="POL balance below which gas is flagged as low (default: 0.05)"
    )
    parser.add_argument(
        "--token-id", default=None, help="Also check conditional-token balance/allowance for this CLOB token id"
    )
    parser.add_argument("--json", action="store_true", help="Print results as JSON instead of a text report")
    args = parser.parse_args()

    # Loaded exactly once — with --keystore this prompts for a password, and
    # we don't want to ask twice.
    private_key = load_private_key(args)

    w3 = Web3(Web3.HTTPProvider(args.rpc_url, request_kwargs={"timeout": 10}))
    if not w3.is_connected():
        print(f"Could not connect to RPC {args.rpc_url}", file=sys.stderr)
        sys.exit(1)

    try:
        client = SecureClient.create(private_key=private_key, wallet=args.wallet)
    except Exception as e:
        print(f"Level 1 auth failed — could not construct an authenticated client: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        results = run_all_checks(client, w3, args.min_gas_pol, args.token_id)
    finally:
        client.close()

    if args.json:
        print(json.dumps([r.__dict__ for r in results], indent=2))
        ready = not any(r.status == "FAIL" for r in results)
    else:
        ready = print_report(results, client.wallet)

    sys.exit(0 if ready else 1)


if __name__ == "__main__":
    main()
