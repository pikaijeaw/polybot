# log_collector

Every bot writes all of its logs here, one sub-folder per bot. The folder is
created the first time a bot starts.

```
log_collector/
├── oracle_v1/             Oracle-Lag v1            (dashboard key: paper)
├── oracle_v2/             Oracle-Lag v2            (oracle_v2)
├── oracle_v3/             Oracle-Lag v3            (oracle_v3)
├── early_move/            Early-Move               (early)
├── arbitrage_pair_cost/   Arbitrage (Pair Cost)    (arb)
├── copy_trading/          Copy Trading             (copy)
├── archive/               finished runs: <bot folder>_<UTC date>_<time>/
├── watchdog.log
└── dashboard.log          only if you start the dashboard with the README command
```

Each bot folder holds the same files:

| File | What it holds | Read it for |
|---|---|---|
| `runs.jsonl` | One line per start: time and the exact settings. | Which settings produced these results. Read first. |
| `trades.csv` | One row per settled trade. Same columns for every bot. | Win rate, P&L, comparing bots. |
| `events.jsonl` | One line per event, each with `ts_iso` and `event`. | What the bot did and when (see below). |
| `signals.csv` | One row every ~2 seconds: prices, and the model's numbers. | Why a trade was or was not taken. Large. |
| `executions.csv` | API call speeds (Oracle-Lag and Early-Move only). | Latency problems. |
| `bot.log` | Raw program output. | Errors and crashes. |

Events in `events.jsonl`:

| Bot | Events |
|---|---|
| Oracle-Lag v1/v2/v3, Early-Move | `OPEN`, `CLOSE` |
| Arbitrage (Pair Cost) | `BUY` (`leg` is `open` or `hedge`, both asks, `pair_cost`), `CLOSE` |
| Copy Trading | `BUY`, `SELL`, `SKIP` (`reason`, `leader_price`, `our_price`, `lag_s`), `CLOSE` |

Notes:

- Times: `ts` is Unix seconds, `ts_iso` is the same moment in UTC.
- Blank columns in `trades.csv` / `signals.csv` mean "does not apply to this bot"
  (Arbitrage and Copy Trading have no probability model).
- Reset and Kill Switch on the dashboard move a bot's whole folder, plus its
  state file, into `archive/`. Nothing is deleted.
- Quick summary of one bot: `.venv/bin/python performance_tracker.py --report --log-dir log_collector/oracle_v1`
- Data from before 2026-10-12 (in `paper_trading/archive/`) was measured on the
  wrong market window. Do not mix it with data here.
