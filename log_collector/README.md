# log_collector

Every bot writes all of its logs here, one sub-folder per bot. The folder is
created the first time a bot starts.

```
log_collector/
├── arbitrage_pair_cost/   Arbitrage (Pair Cost)    (arb)
├── arbitrage_pair_cost_v2/ Arbitrage (Pair Cost) v2 (arb2)
├── copy_trading/          Copy Trading             (copy)
├── archive/               finished runs: <bot folder>_<UTC date>_<time>/
└── dashboard.log          only if you start the dashboard with the README command
```

Each bot folder holds the same files:

| File | What it holds | Read it for |
|---|---|---|
| `runs.jsonl` | One line per start: time and the exact settings. | Which settings produced these results. Read first. |
| `trades.csv` | One row per settled trade. Same columns for every bot. | Win rate, P&L, comparing bots. |
| `events.jsonl` | One line per event, each with `ts_iso` and `event`. | What the bot did and when (see below). |
| `signals.csv` | Arbitrage: one row every ~2 seconds with both asks. Copy Trading leaves it empty. | What prices were available. Large. |
| `bot.log` | Raw program output. | Errors and crashes. |

Events in `events.jsonl`:

| Bot | Events |
|---|---|
| Arbitrage (Pair Cost) | `BUY` (`leg` is `open` or `hedge`, `want_shares`, `limit_price`, filled `shares`/`price`, both asks, `pair_cost`), `MISS` (order found nothing to fill; `reason`), `CLOSE` |
| Arbitrage (Pair Cost) v2 | everything v1 logs, plus `PAIR` (both legs sent at once: `want_shares`, `planned_profit_usd`, per-leg `up`/`down` fills, `risk_free` = both legs filled in full) |
| Copy Trading | `BUY`, `SELL`, `SKIP` (`reason`, `leader_price`, `our_price`, `lag_s`), `CLOSE` |

Notes:

- Times: `ts` is Unix seconds, `ts_iso` is the same moment in UTC.
- `entry_price` and `stake_usd` in `trades.csv` include fees. For Arbitrage, `side` is `PAIR`
  and `entry_price` is the pair cost (or the single leg, if it was never hedged).
- Reset and Kill Switch on the dashboard move a bot's whole folder, plus its
  state file, into `archive/`. Nothing is deleted.
- Quick summary of one bot: `.venv/bin/python performance_tracker.py --report --log-dir log_collector/arbitrage_pair_cost`
- Data from before 2026-10-12 (in `paper_trading/archive/`) was measured on the
  wrong market window. Do not mix it with data here.
