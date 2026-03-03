# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Python client library for the Korea Investment & Securities (KIS) Open API. Supports REST and WebSocket protocols for stock trading, with both production and paper trading (모의투자) environments.

- **Python 3.13+** required
- **Package manager:** `uv`
- **Package name:** `blsh` (buy low sell high)

## Commands

```bash
# Install dependencies
uv sync

# Run market data parsers (download, parse, save to PostgreSQL)
uv run python -m blsh.market_data.kis_kospi
uv run python -m blsh.market_data.kis_kosdaq
uv run python -m blsh.market_data.kis_sector

# Run tests
uv run python tests/test_connection.py     # KIS API connection test (paper trading)
uv run python tests/db_connection_test.py   # PostgreSQL connection test

# Start PostgreSQL via Docker
docker compose up -d postgres
```

No formal test framework, linter, or formatter is configured.

## Architecture

### Package Structure (src layout)

```
src/blsh/
├── __init__.py
├── kis_auth.py              # core KIS API module (~800 lines)
├── common/
│   ├── _constants.py        # CONFIG_ROOT (~/.blsh/config)
│   └── _common.py           # shared utils: DB engine, download/extract, save_to_db
└── market_data/
    ├── kis_kospi.py          # KOSPI stock code parser
    ├── kis_kosdaq.py         # KOSDAQ stock code parser
    └── kis_sector.py         # sector/industry classification parser
```

### kis_auth.py — Core API Module

Handles all KIS API communication. Key components:

- **Global state:** `_TRENV` (named tuple for current environment), `_cfg` (YAML config loaded at import time from `~/.blsh/config/kis_devlp.yaml` with `$ENV_VAR` expansion), `_base_headers` (shared HTTP headers). These are module-level singletons.
- **`auth(svr=)`** — Acquires REST access token. `svr="prod"` for real, `svr="vps"` for paper trading. Tokens cached to `~/.blsh/config/KIS{YYYYMMDD}`.
- **`auth_ws()`** — Acquires WebSocket approval key.
- **`_url_fetch()`** — Common HTTP request wrapper. Returns `APIResp` on success, `APIRespError` on failure.
- **`APIResp` / `APIRespError`** — Response wrapper classes parsing header/body into named tuples.
- **`KISWebSocket`** — Async WebSocket manager supporting up to 40 subscriptions, with AES-CBC decryption and PINGPONG heartbeat. Uses `open_map` / `data_map` module-level dicts for subscription state.
- **TR ID auto-conversion:** Production TR IDs starting with `T/J/C` get prefix-swapped to `V` for paper trading via `_url_fetch()`.
- **Rate limiting:** `smart_sleep()` enforces 0.05s (prod) or 0.5s (paper) delay.

**Important:** Importing `kis_auth` has side effects — it reads the YAML config, creates a token file, and prints the app key. The `__init__.py` re-exports are currently commented out.

### common/_common.py — Shared Utilities

- `_get_engine()` — Creates SQLAlchemy PostgreSQL engine from `.env` vars (`DB_USER`, `DB_PASSWORD`, `DB_NAME`, `DB_HOST`, `DB_PORT`)
- `download_and_extract()` — Downloads zip from URL, extracts, removes zip
- `save_to_db()` — Writes a DataFrame to PostgreSQL via SQLAlchemy

### Market Data Parsers

Scripts that download fixed-width binary `.mst` master files from KRX, parse them into DataFrames, and save to PostgreSQL. KOSPI/KOSDAQ parsers split each line into a CSV portion (code, standard code, name) and a fixed-width portion (60+ financial fields), then merge.

### Configuration

- **KIS credentials:** `~/.blsh/config/kis_devlp.yaml` — app key, secret, account numbers, URLs. Supports `$ENV_VAR` substitution via `os.path.expandvars()`.
- **Environment vars:** `~/.blsh/config/.env` — loaded via `python-dotenv`. Contains DB credentials and Docker config.
- **Token storage:** `~/.blsh/config/KIS{YYYYMMDD}` — auto-created daily
- **Docker:** `docker-compose.yml` runs PostgreSQL 16 and a KIS trade MCP service

### Environment Modes

| Mode | `svr` param | Rate limit |
|------|-------------|------------|
| Production | `"prod"` | 0.05s |
| Paper trading | `"vps"` | 0.5s |

Product codes: `"01"` (stocks), `"03"` (derivatives), `"08"` (foreign futures), `"22"` (pension), `"29"` (retirement).

## Key Dependencies

pandas, requests, websockets, pycryptodome (AES decryption), PyYAML, python-dotenv, SQLAlchemy (PostgreSQL), PyQt6/PySide6, openpyxl
