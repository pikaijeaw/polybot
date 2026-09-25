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

    WALLET     Paper bankroll/equity.

Run it alongside paper_trader.py in a separate terminal:

    python paper_trading/paper_trader.py            # terminal 1: the bot
    python paper_trading/dashboard.py                # terminal 2: the monitor

It degrades gracefully with no paper_trader.py running (shows "no data yet").

Usage (run from anywhere — paths resolve relative to this script):
    python paper_trading/dashboard.py
    python paper_trading/dashboard.py --refresh-interval 1
    python paper_trading/dashboard.py --state-file /path/to/paper_state.json
"""

import argparse
import json
import time
from pathlib import Path

import requests
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

SCRIPT_DIR = Path(__file__).resolve().parent

DEFAULT_STATE_PATH = SCRIPT_DIR / "paper_state.json"
PRICE_REFRESH_SECONDS = 2.0


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


def render_wallet_panel(state: dict | None) -> Panel:
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

    return Panel(lines, title="WALLET", border_style="magenta")


def render(state: dict | None, live_price: LivePrice) -> Layout:
    layout = Layout()
    layout.split_column(
        Layout(name="top", size=10),
        Layout(name="middle"),
        Layout(name="bottom", size=8),
    )
    layout["top"].update(render_price_panel(live_price, state))
    layout["middle"].update(render_orders_panel(state))
    layout["bottom"].update(render_wallet_panel(state))
    return layout


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--state-file", default=str(DEFAULT_STATE_PATH))
    parser.add_argument(
        "--refresh-interval", type=float, default=1.0, help="Screen redraw interval in seconds (default: 1.0)"
    )
    args = parser.parse_args()

    state_path = Path(args.state_file)
    live_price = LivePrice()

    with Live(refresh_per_second=4, screen=True) as live:
        while True:
            live_price.maybe_refresh()
            state = load_paper_state(state_path)
            live.update(render(state, live_price))
            time.sleep(args.refresh_interval)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
