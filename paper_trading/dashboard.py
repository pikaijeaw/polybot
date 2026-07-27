#!/usr/bin/env python3
"""
Live terminal dashboard for the oracle-lag scalper.

Read-only monitor — it never signs anything, places orders, or touches
paper_trader.py's state, it just watches:

    PRICE      Live BTC price (own lightweight Binance REST poll, so this
               works even if paper_trader.py isn't running) plus the current
               Polymarket window's anchor/quotes/model probability, read
               from paper_trader.py's state file if present.

    ORDERS     Open and recently-closed paper positions, win rate, realized
               P&L — from paper_trader.py's state file.

    WALLET     Paper bankroll/equity always; PLUS real on-chain POL (at your
               EOA) and pUSD (at your Deposit Wallet — a smart-contract
               wallet derived from your EOA, not the EOA itself; see
               CLAUDE.md's "Collateral and the CLOB SDK" section) for
               POLYMARKET_PRIVATE_KEY's address if one is configured (.env),
               refreshed on a slower cadence. Only ever reads public chain
               state — no order placement.

Run it alongside paper_trader.py in a separate terminal:

    python paper_trading/paper_trader.py            # terminal 1: the bot
    python paper_trading/dashboard.py                # terminal 2: the monitor

It degrades gracefully with no paper_trader.py running (shows "no data yet")
and with no wallet configured (shows "not configured").

Usage (run from anywhere — paths resolve relative to this script):
    python paper_trading/dashboard.py
    python paper_trading/dashboard.py --refresh-interval 1
    python paper_trading/dashboard.py --state-file /path/to/paper_state.json
"""

import argparse
import json
import os
import time
from pathlib import Path

import requests
from dotenv import load_dotenv
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from web3 import Web3

# This script lives in paper_trading/ but .env lives in the project root.
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
load_dotenv(PROJECT_ROOT / ".env")

DEFAULT_STATE_PATH = SCRIPT_DIR / "paper_state.json"
DEFAULT_RPC_URL = "https://polygon-bor-rpc.publicnode.com"
COLLATERAL_DECIMALS = 6
WALLET_REFRESH_SECONDS = 30.0
PRICE_REFRESH_SECONDS = 2.0

ERC20_ABI = [
    {
        "constant": True,
        "inputs": [{"name": "_owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "balance", "type": "uint256"}],
        "type": "function",
    },
]


# ---------------------------------------------------------------------------
# Data sources
# ---------------------------------------------------------------------------


class LivePrice:
    """Polls Binance's REST last-price endpoint on its own slow cadence —
    a dashboard has no need for the full websocket tick stream."""

    def __init__(self, symbol: str = "BTCUSDT"):
        self.symbol = symbol
        self.price: float | None = None
        self.error: str | None = None
        self._next_poll = 0.0

    def maybe_refresh(self):
        if time.time() < self._next_poll:
            return
        try:
            resp = requests.get(
                "https://api.binance.com/api/v3/ticker/price", params={"symbol": self.symbol}, timeout=5
            )
            resp.raise_for_status()
            self.price = float(resp.json()["price"])
            self.error = None
        except Exception as e:
            self.error = str(e)
        self._next_poll = time.time() + PRICE_REFRESH_SECONDS


class WalletWatcher:
    """Read-only on-chain balance check. Never signs an order, never submits
    a transaction. Two addresses matter, not one: `eoa_address` is derived
    locally from the private key; `address` is the Deposit Wallet -- a
    smart-contract wallet derived from that EOA, not the EOA itself, which
    is what actually holds pUSD and what the CLOB trades against (see
    CLAUDE.md's "Collateral and the CLOB SDK" section). Deriving it needs a
    real (read-only) SecureClient construction, done ONCE and cached in
    _ensure_deposit_wallet() rather than on every refresh, since it's
    deterministic for a given EOA and can never change."""

    def __init__(self, rpc_url: str):
        self.eoa_address: str | None = None
        self.address: str | None = None
        self.error: str | None = None
        self.pol_balance: float | None = None
        self.pusd_balance: float | None = None
        self._next_poll = 0.0
        self._private_key = os.environ.get("POLYMARKET_PRIVATE_KEY")
        self._pusd_contract = None

        if self._private_key:
            try:
                from eth_account import Account

                self.eoa_address = Account.from_key(self._private_key).address
            except Exception as e:
                self.error = f"bad POLYMARKET_PRIVATE_KEY: {e}"

        self.w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 10}))

    def _ensure_deposit_wallet(self):
        if self.address is not None or self._private_key is None:
            return
        from polymarket import SecureClient

        client = SecureClient.create(private_key=self._private_key)
        try:
            self.address = client.wallet
            self._pusd_contract = self.w3.eth.contract(
                address=Web3.to_checksum_address(client.environment.collateral_token), abi=ERC20_ABI
            )
        finally:
            client.close()

    def maybe_refresh(self):
        if self.eoa_address is None or time.time() < self._next_poll:
            return
        try:
            self._ensure_deposit_wallet()
            self.pol_balance = self.w3.eth.get_balance(Web3.to_checksum_address(self.eoa_address)) / 10**18
            self.pusd_balance = (
                self._pusd_contract.functions.balanceOf(Web3.to_checksum_address(self.address)).call()
                / 10**COLLATERAL_DECIMALS
            )
            self.error = None
        except Exception as e:
            self.error = str(e)
        self._next_poll = time.time() + WALLET_REFRESH_SECONDS


def load_paper_state(state_path: Path) -> dict | None:
    try:
        return json.loads(state_path.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return None


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def fmt_money(x, signed=False) -> str:
    if x is None:
        return "-"
    sign = "+" if signed and x > 0 else ""
    return f"{sign}${x:,.2f}"


def fmt_pct(x, signed=True) -> str:
    if x is None:
        return "-"
    sign = "+" if signed and x > 0 else ""
    return f"{sign}{x * 100:.1f}%"


def age_str(ts: float | None) -> str:
    if ts is None:
        return "-"
    dt = time.time() - ts
    if dt < 0:
        return "just now"
    if dt < 60:
        return f"{dt:.0f}s ago"
    return f"{dt / 60:.1f}m ago"


def render_price_panel(live_price: LivePrice, state: dict | None) -> Panel:
    lines = Text()
    if live_price.price is not None:
        lines.append("BTC/USDT (Binance): ", style="bold")
        lines.append(f"${live_price.price:,.2f}\n", style="bold cyan")
    else:
        lines.append(f"BTC/USDT: unavailable ({live_price.error})\n", style="red")

    engine = (state or {}).get("engine")
    if not engine:
        lines.append("\nNo active Polymarket window tracked yet.", style="dim")
        return Panel(lines, title="PRICE", border_style="cyan")

    approx = "~" if engine.get("anchor_is_approximate") else ""
    lines.append(f"\nWindow: {engine.get('market_slug', '-')}\n")
    lines.append(
        f"Anchor: {approx}{engine.get('anchor_price', 0):.2f}   "
        f"Last: {engine.get('last_price', 0):.2f}   "
        f"T-{engine.get('seconds_remaining', 0):.0f}s\n"
    )
    p_up = engine.get("p_up")
    if p_up is not None:
        lines.append(f"Model P(Up): {p_up * 100:.1f}%   sigma/sqrt(s): {engine.get('sigma_per_sqrt_sec', 0):.6f}\n")
    up_edge = engine.get("up_edge")
    down_edge = engine.get("down_edge")
    if up_edge is not None:
        up_style = "green" if up_edge > 0 else "dim"
        down_style = "green" if (down_edge or 0) > 0 else "dim"
        lines.append(f"Up   ask={engine.get('up_ask', 0):.3f}  edge=", style=up_style)
        lines.append(f"{fmt_pct(up_edge)}\n", style=up_style)
        lines.append(f"Down ask={engine.get('down_ask', 0):.3f}  edge=", style=down_style)
        lines.append(f"{fmt_pct(down_edge)}\n", style=down_style)

    return Panel(lines, title="PRICE", border_style="cyan")


def render_orders_panel(state: dict | None) -> Panel:
    if not state:
        return Panel(
            Text("No paper_trader.py data yet — is it running?", style="dim"), title="ORDERS", border_style="yellow"
        )

    open_positions = state.get("open_positions", [])
    recent_closed = state.get("recent_closed", [])[::-1]  # most recent first

    table = Table(expand=True, show_lines=False)
    table.add_column("Status")
    table.add_column("Window")
    table.add_column("Side")
    table.add_column("Entry")
    table.add_column("Stake")
    table.add_column("Edge")
    table.add_column("P&L / T-remaining")

    for p in open_positions:
        remaining = p["end_epoch"] - time.time()
        table.add_row(
            "[yellow]OPEN[/yellow]",
            p["market_slug"],
            p["side"],
            f"{p['entry_price']:.3f}",
            fmt_money(p["stake_usd"]),
            fmt_pct(p["edge_at_entry"]),
            f"T{remaining:+.0f}s",
        )

    for p in recent_closed[:8]:
        status_style = "green" if p["status"] == "WON" else "red"
        table.add_row(
            f"[{status_style}]{p['status']}[/{status_style}]",
            p["market_slug"],
            p["side"],
            f"{p['entry_price']:.3f}",
            fmt_money(p["stake_usd"]),
            fmt_pct(p["edge_at_entry"]),
            fmt_money(p["pnl_usd"], signed=True),
        )

    if not open_positions and not recent_closed:
        return Panel(Text("No trades yet.", style="dim"), title="ORDERS", border_style="yellow")

    return Panel(table, title=f"ORDERS  ({len(open_positions)} open)", border_style="yellow")


def render_wallet_panel(state: dict | None, wallet: WalletWatcher) -> Panel:
    lines = Text()
    lines.append("Paper wallet\n", style="bold")
    if state:
        equity = state.get("equity")
        starting = state.get("starting_bankroll")
        ret = (equity - starting) / starting if starting else None
        ret_style = "green" if (ret or 0) >= 0 else "red"
        lines.append(f"  Cash:   {fmt_money(state.get('cash'))}\n")
        lines.append(f"  Equity: {fmt_money(equity)}  (", style="")
        lines.append(f"{fmt_pct(ret)}", style=ret_style)
        lines.append(")\n")
        stats = state.get("stats", {})
        lines.append(
            f"  Trades: {stats.get('trades', 0)}   "
            f"Win rate: {fmt_pct(stats.get('win_rate'), signed=False)}   "
            f"Realized P&L: "
        )
        pnl_style = "green" if (stats.get("realized_pnl") or 0) >= 0 else "red"
        lines.append(f"{fmt_money(stats.get('realized_pnl'), signed=True)}\n", style=pnl_style)
        lines.append(f"  Updated: {age_str(state.get('updated_at'))}\n", style="dim")
    else:
        lines.append("  no data yet — is paper_trader.py running?\n", style="dim")

    lines.append("\nReal wallet (read-only)\n", style="bold")
    if wallet.eoa_address is None:
        reason = wallet.error or "POLYMARKET_PRIVATE_KEY not set in .env"
        lines.append(f"  not configured ({reason})\n", style="dim")
    else:
        lines.append(f"  EOA: {wallet.eoa_address}\n", style="dim")
        if wallet.address:
            lines.append(f"  Deposit Wallet: {wallet.address}\n", style="dim")
        if wallet.error:
            lines.append(f"  error: {wallet.error}\n", style="red")
        else:
            pol_style = "green" if (wallet.pol_balance or 0) > 0.01 else "yellow"
            pusd_style = "green" if (wallet.pusd_balance or 0) > 0 else "yellow"
            lines.append("  POL: ", style="")
            lines.append(f"{wallet.pol_balance:.4f}" if wallet.pol_balance is not None else "-", style=pol_style)
            lines.append("   pUSD: ", style="")
            lines.append(f"{wallet.pusd_balance:.4f}\n" if wallet.pusd_balance is not None else "-\n", style=pusd_style)

    return Panel(lines, title="WALLET", border_style="magenta")


def render(state: dict | None, live_price: LivePrice, wallet: WalletWatcher) -> Layout:
    layout = Layout()
    layout.split_column(
        Layout(name="top", size=10),
        Layout(name="middle"),
        Layout(name="bottom", size=11),
    )
    layout["top"].update(render_price_panel(live_price, state))
    layout["middle"].update(render_orders_panel(state))
    layout["bottom"].update(render_wallet_panel(state, wallet))
    return layout


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--state-file", default=str(DEFAULT_STATE_PATH))
    parser.add_argument("--rpc-url", default=DEFAULT_RPC_URL)
    parser.add_argument(
        "--refresh-interval", type=float, default=1.0, help="Screen redraw interval in seconds (default: 1.0)"
    )
    args = parser.parse_args()

    state_path = Path(args.state_file)
    live_price = LivePrice()
    wallet = WalletWatcher(args.rpc_url)

    with Live(refresh_per_second=4, screen=True) as live:
        while True:
            live_price.maybe_refresh()
            wallet.maybe_refresh()
            state = load_paper_state(state_path)
            live.update(render(state, live_price, wallet))
            time.sleep(args.refresh_interval)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
