#!/usr/bin/env python3
"""
Set up trading approvals for Polymarket's exchange contracts.

This is what fixes preflight_check.py's "pUSD allowance: N/3 spender(s) NOT
approved" failure. Polymarket's exchange contracts need an ERC20 allowance
on your pUSD before they can pull funds to settle a trade you placed.

Migration note: this used to send its own raw ERC20 approve() transactions
against USDC.e, hand-rolled with web3.py. That's gone now — py_clob_client
is archived, the collateral token is pUSD (not USDC.e; see
oracle CLAUDE.md's pUSD/Deposit Wallet gotcha), and polymarket-client's
SecureClient.setup_trading_approvals() is the correct, officially supported
way to do this: it reads which of the standard spenders still need
approving (skipping ones that already do), and submits exactly those —
gaslessly relayed if you're on a Deposit Wallet (the common case), or a
direct on-chain tx if you're a plain EOA. In practice this script is rarely
needed at all for a Deposit Wallet: Polymarket's own Collateral Onramp sets
these approvals automatically when you convert into pUSD. Run this only if
preflight_check.py reports an allowance FAIL despite that.

Unlike the old per-spender flow, setup_trading_approvals() is one atomic
call covering every missing spender — there's no --amount (always the
standard/unlimited approval) or --force (always re-approve) knob anymore;
the SDK's own missing-approval detection replaces both. If you need a
finite or single-spender approval for some other reason, use
client.approve_erc20(token_address=..., spender_address=..., amount=...)
directly in a Python shell — that lower-level method still exists, this
script just doesn't wrap it.

Defaults to a dry run; --live is required to actually submit, and you'll be
asked to type "yes" unless you also pass --yes.

Usage:
    python approve_usdc.py                    # dry run — shows what would be approved
    python approve_usdc.py --live              # actually submit, with confirmation
    python approve_usdc.py --live --yes        # no confirmation prompt
"""

import argparse
import sys
from pathlib import Path

from dotenv import load_dotenv
from polymarket import SecureClient

from order_executor import load_private_key

load_dotenv(Path(__file__).resolve().parent / ".env")


def human_pusd(raw) -> str:
    try:
        amt = int(raw)
    except (TypeError, ValueError):
        return str(raw)
    if amt >= 2**255:
        return "unlimited"
    return f"{amt / 10**6:,.2f}"


def confirm_or_abort(summary: str, auto_yes: bool) -> bool:
    print(summary)
    if auto_yes:
        return True
    try:
        answer = input("Type 'yes' to submit: ")
    except EOFError:
        answer = ""
    if answer.strip().lower() != "yes":
        print("Not confirmed; aborting.", file=sys.stderr)
        return False
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--private-key-env", default="POLYMARKET_PRIVATE_KEY")
    parser.add_argument("--keystore", default=None)
    parser.add_argument(
        "--wallet", default=None, help="Address to act for (default: your signer's Deposit Wallet, auto-derived)"
    )
    parser.add_argument("--live", action="store_true", help="Actually submit (default: dry run)")
    parser.add_argument("--yes", action="store_true", help="Skip the confirmation prompt")
    args = parser.parse_args()

    # Loaded exactly once — with --keystore this prompts for a password, and
    # we don't want to ask twice.
    private_key = load_private_key(args)
    client = SecureClient.create(private_key=private_key, wallet=args.wallet)

    print(f"Wallet: {client.wallet} ({client.wallet_type})")
    print(f"Collateral token: {client.environment.collateral_token} (pUSD)")

    try:
        collateral = client.get_balance_allowance(asset_type="COLLATERAL")
        allowances = collateral.allowances or {}
        unapproved = [addr for addr, amt in allowances.items() if int(amt or 0) == 0]
        already_ok = [addr for addr in allowances if addr not in unapproved]

        for addr in already_ok:
            print(f"  already approved: {addr} ({human_pusd(allowances[addr])} pUSD)")

        if not unapproved:
            print("\nAll spenders already approved — nothing to do.")
            return

        print(f"\nTo approve: {len(unapproved)} spender(s): {unapproved}")
        summary = (
            f"SETUP TRADING APPROVALS  wallet={client.wallet}  spenders={unapproved}  "
            f"mode={'LIVE' if args.live else 'DRY RUN'}"
        )

        if not args.live:
            print(summary)
            print("(dry run — pass --live to submit)")
            return

        if not confirm_or_abort(summary, args.yes):
            sys.exit(1)

        client.setup_trading_approvals()
        print("Submitted and confirmed. Re-run preflight_check.py to confirm.")
    finally:
        client.close()


if __name__ == "__main__":
    main()
