#!/usr/bin/env python3
"""
Create a new Polygon (PoS) wallet.

Polygon is EVM-compatible, so a wallet here is just a standard secp256k1
keypair / address — the same format used on Ethereum and other EVM chains.
This script can generate a fresh wallet from a BIP-39 mnemonic and optionally
save it to an encrypted keystore file (never saves the raw private key to disk
unless you explicitly ask for it).

Requirements:
    pip install eth-account

Usage:
    python create_polygon_wallet.py
    python create_polygon_wallet.py --save-keystore wallet.json
    python create_polygon_wallet.py --save-plaintext wallet_plain.json   # NOT recommended
"""

import argparse
import getpass
import json
import sys

from eth_account import Account


def generate_wallet():
    """Generate a new wallet with a BIP-39 mnemonic seed phrase."""
    Account.enable_unaudited_hdwallet_features()
    account, mnemonic = Account.create_with_mnemonic()
    return account, mnemonic


def save_keystore(account: Account, path: str):
    password = getpass.getpass("Set a password to encrypt the keystore: ")
    confirm = getpass.getpass("Confirm password: ")
    if password != confirm:
        print("Passwords do not match. Aborting keystore save.", file=sys.stderr)
        sys.exit(1)

    keystore = Account.encrypt(account.key, password)
    with open(path, "w") as f:
        json.dump(keystore, f, indent=2)
    print(f"Encrypted keystore saved to: {path}")
    print("You will need the password you just set to decrypt/use this file.")


def save_plaintext(account: Account, private_key_hex: str, mnemonic: str, path: str):
    data = {
        "address": account.address,
        "private_key": private_key_hex,
        "mnemonic": mnemonic,
    }
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"WARNING: Plaintext wallet data saved to: {path}")
    print("This file contains your private key in cleartext. Protect it accordingly.")


def main():
    parser = argparse.ArgumentParser(description="Create a new Polygon wallet.")
    parser.add_argument("--save-keystore", metavar="PATH", help="Save an encrypted (password-protected) keystore JSON file")
    parser.add_argument("--save-plaintext", metavar="PATH", help="Save address/private key/mnemonic as plaintext JSON (not recommended)")
    args = parser.parse_args()

    account, mnemonic = generate_wallet()
    private_key_hex = account.key.hex()
    if not private_key_hex.startswith("0x"):
        private_key_hex = "0x" + private_key_hex

    print("=" * 60)
    print("New Polygon Wallet Created")
    print("=" * 60)
    print(f"Address:      {account.address}")
    print(f"Private Key:  {private_key_hex}")
    print(f"Mnemonic:     {mnemonic}")
    print("=" * 60)
    print("SECURITY WARNING:")
    print("  - Anyone with your private key or mnemonic has FULL control")
    print("    of this wallet and any funds sent to it.")
    print("  - Never share them, commit them to git, or paste them into")
    print("    chat tools, websites, or scripts you don't trust.")
    print("  - Store them offline (e.g. a password manager or hardware wallet).")
    print("=" * 60)

    if args.save_keystore:
        save_keystore(account, args.save_keystore)

    if args.save_plaintext:
        save_plaintext(account, private_key_hex, mnemonic, args.save_plaintext)

    if not args.save_keystore and not args.save_plaintext:
        print("\nNo file saved. Copy the details above and store them securely now —")
        print("they will not be shown again.")


if __name__ == "__main__":
    main()
