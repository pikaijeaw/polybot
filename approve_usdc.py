#!/usr/bin/env python3
"""
Approve USDC allowance for Polymarket's exchange contracts.

This is what fixes preflight_check.py's "USDC allowance: 3/3 spender(s) NOT
approved" failure. Polymarket's exchange contracts (CTF Exchange, Neg-Risk
CTF Exchange, Neg-Risk Adapter) need an ERC20 allowance on your USDC.e before
they can pull funds to settle a trade you placed — this submits one approve()
transaction per spender that isn't already approved.

Spender addresses are NOT hardcoded here — they're read live from
Polymarket's own get_balance_allowance API (same technique preflight_check.py
uses), because the addresses returned there don't match what's bundled in
py_clob_client's local config, and the live API is the authoritative source
for what your CLOB orders will actually be checked against.

This sends REAL on-chain transactions and costs REAL POL for gas — unlike
order_executor.py (which signs off-chain orders relayed by Polymarket), an
ERC20 approve() is your wallet directly calling the contract. Defaults to a
dry run; --live is required to actually broadcast, and you'll be asked to
type "yes" for each transaction unless you also pass --yes.

By default approves for the max uint256 amount ("infinite approval", the
same thing Polymarket's own website does on first connect) so you only need
to run this once. Use --amount to approve a smaller/finite amount instead.

Usage:
    python approve_usdc.py                    # dry run — shows what would be sent
    python approve_usdc.py --live              # actually submit, confirm each tx
    python approve_usdc.py --live --yes        # no per-tx confirmation prompt
    python approve_usdc.py --live --amount 1000  # finite approval instead of unlimited
    python approve_usdc.py --force --live      # re-approve even already-approved spenders
"""

import argparse
import sys

from eth_account import Account
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import AssetType, BalanceAllowanceParams
from py_clob_client.config import get_contract_config
from web3 import Web3

from order_executor import DEFAULT_CHAIN_ID, load_private_key

DEFAULT_RPC_URL = "https://polygon-bor-rpc.publicnode.com"
MAX_UINT256 = 2**256 - 1
USDC_DECIMALS = 6

ERC20_ABI = [
    {"constant": True, "inputs": [{"name": "_owner", "type": "address"}, {"name": "_spender", "type": "address"}],
     "name": "allowance", "outputs": [{"name": "", "type": "uint256"}], "type": "function"},
    {"constant": False, "inputs": [{"name": "_spender", "type": "address"}, {"name": "_value", "type": "uint256"}],
     "name": "approve", "outputs": [{"name": "", "type": "bool"}], "type": "function"},
]


def human_usdc(raw: int) -> str:
    if raw >= MAX_UINT256 // 2:
        return "unlimited"
    return f"{raw / 10 ** USDC_DECIMALS:,.2f}"


def get_live_spenders(clob_client) -> dict:
    """Reads current USDC allowances per spender straight from Polymarket's
    own API — the authoritative list of who needs approving."""
    result = clob_client.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
    return {addr: int(amt or 0) for addr, amt in (result.get("allowances") or {}).items()}


def confirm_or_abort(summary: str, auto_yes: bool) -> bool:
    print(summary)
    if auto_yes:
        return True
    try:
        answer = input("Type 'yes' to submit this transaction: ")
    except EOFError:
        answer = ""
    if answer.strip().lower() != "yes":
        print("Not confirmed; skipping.", file=sys.stderr)
        return False
    return True


def approve_spender(w3: Web3, usdc_contract, account, spender: str, amount: int,
                     gas_price: int, live: bool, auto_yes: bool) -> bool:
    spender_cs = Web3.to_checksum_address(spender)

    try:
        gas_estimate = usdc_contract.functions.approve(spender_cs, amount).estimate_gas({"from": account.address})
    except Exception as e:
        print(f"  gas estimation failed for {spender}: {e}", file=sys.stderr)
        gas_estimate = 60000  # typical ERC20 approve cost; used as a fallback only

    gas_limit = int(gas_estimate * 1.2)
    est_cost_pol = (gas_limit * gas_price) / 10**18

    summary = (
        f"APPROVE spender={spender_cs}  amount={human_usdc(amount)} USDC  "
        f"est_gas={gas_limit}  est_cost={est_cost_pol:.6f} POL  mode={'LIVE' if live else 'DRY RUN'}"
    )

    if not live:
        print(summary)
        print("  (dry run — pass --live to submit)")
        return True

    if not confirm_or_abort(summary, auto_yes):
        return False

    tx = usdc_contract.functions.approve(spender_cs, amount).build_transaction({
        "from": account.address,
        "nonce": w3.eth.get_transaction_count(account.address),
        "gas": gas_limit,
        "gasPrice": gas_price,
        "chainId": DEFAULT_CHAIN_ID,
    })
    signed = account.sign_transaction(tx)
    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
    print(f"  submitted {tx_hash.hex()} — waiting for confirmation...")

    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
    if receipt.status == 1:
        print(f"  confirmed in block {receipt.blockNumber}, gas used {receipt.gasUsed}")
        return True
    else:
        print(f"  TRANSACTION REVERTED (block {receipt.blockNumber})", file=sys.stderr)
        return False


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--host", default="https://clob.polymarket.com")
    parser.add_argument("--chain-id", type=int, default=DEFAULT_CHAIN_ID)
    parser.add_argument("--rpc-url", default=DEFAULT_RPC_URL)
    parser.add_argument("--private-key-env", default="POLYMARKET_PRIVATE_KEY")
    parser.add_argument("--keystore", default=None)
    parser.add_argument("--signature-type", type=int, default=0, choices=[0, 1, 2])
    parser.add_argument("--funder", default=None)
    parser.add_argument("--amount", type=float, default=None, help="USDC amount to approve (default: unlimited)")
    parser.add_argument("--force", action="store_true", help="Re-approve even spenders that already have a non-zero allowance")
    parser.add_argument("--gas-price-gwei", type=float, default=None, help="Override gas price (default: current network gas price)")
    parser.add_argument("--live", action="store_true", help="Actually submit transactions (default: dry run)")
    parser.add_argument("--yes", action="store_true", help="Skip the per-transaction confirmation prompt")
    args = parser.parse_args()

    if args.signature_type in (1, 2) and not args.funder:
        parser.error("--funder is required when --signature-type is 1 or 2")

    # Loaded exactly once — with --keystore this prompts for a password, and
    # we don't want to ask twice.
    private_key = load_private_key(args)
    account = Account.from_key(private_key)

    w3 = Web3(Web3.HTTPProvider(args.rpc_url, request_kwargs={"timeout": 10}))
    if not w3.is_connected():
        print(f"Could not connect to RPC {args.rpc_url}", file=sys.stderr)
        sys.exit(1)

    collateral_address = get_contract_config(args.chain_id).collateral
    usdc_contract = w3.eth.contract(address=Web3.to_checksum_address(collateral_address), abi=ERC20_ABI)

    clob_client = ClobClient(
        host=args.host, chain_id=args.chain_id, key=private_key,
        signature_type=args.signature_type, funder=args.funder,
    )
    creds = clob_client.create_or_derive_api_creds()
    if creds is None:
        print("Failed to create/derive CLOB API credentials.", file=sys.stderr)
        sys.exit(1)
    clob_client.set_api_creds(creds)

    spenders = get_live_spenders(clob_client)
    if not spenders:
        print("No spender list returned by the CLOB API — nothing to approve.", file=sys.stderr)
        sys.exit(1)

    to_approve = spenders if args.force else {a: v for a, v in spenders.items() if v == 0}
    already_ok = {a: v for a, v in spenders.items() if a not in to_approve}

    print(f"Wallet: {account.address}")
    print(f"USDC.e contract: {collateral_address}")
    print(f"Spenders reported by Polymarket: {len(spenders)}")
    for a, v in already_ok.items():
        print(f"  already approved: {a} ({human_usdc(v)} USDC)")
    if not to_approve:
        print("\nAll spenders already approved — nothing to do.")
        sys.exit(0)
    print(f"To approve: {len(to_approve)}\n")

    amount = MAX_UINT256 if args.amount is None else int(args.amount * 10**USDC_DECIMALS)
    gas_price = int(args.gas_price_gwei * 10**9) if args.gas_price_gwei is not None else w3.eth.gas_price

    if args.live:
        pol_balance = w3.eth.get_balance(account.address) / 10**18
        print(f"POL balance: {pol_balance:.6f}  (gas price: {gas_price / 10**9:.2f} gwei)\n")
        if pol_balance == 0:
            print("0 POL — can't pay gas for any transaction. Fund this wallet with a small amount of POL first.", file=sys.stderr)
            sys.exit(1)

    results = {}
    for spender in to_approve:
        ok = approve_spender(w3, usdc_contract, account, spender, amount, gas_price, args.live, args.yes)
        results[spender] = ok
        print()

    approved_now = sum(1 for ok in results.values() if ok)
    total_spenders = len(spenders)
    already_count = len(already_ok)
    if args.live:
        print(f"Done: {already_count + approved_now}/{total_spenders} spenders approved "
              f"({approved_now} just now, {already_count} already were).")
        print("Re-run preflight_check.py to confirm.")
    else:
        print(f"Dry run complete — {len(to_approve)} approval(s) would be submitted with --live.")

    if args.live and approved_now < len(to_approve):
        sys.exit(1)


if __name__ == "__main__":
    main()
