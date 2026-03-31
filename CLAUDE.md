# CLAUDE.md

This file provides guidance to Claude Code when working with code in this repository.

## 보안 금지 사항 (SECURITY — NEVER VIOLATE)

다음 파일/폴더는 **절대** 읽기·수정·삭제·git 커밋·푸시 금지:
- 모든 경로의 dotenv 파일 (환경변수 파일)
- `~/.blsh/config/` 폴더 및 그 하위 파일 전체 (API 키, 토큰, 계좌번호 포함)

이 제한은 PreToolUse 훅으로도 강제됩니다. 어떤 이유로도 예외 없음.

## Project Overview

**blsh** (buy low sell high) — 한국 주식(KOSPI/KOSDAQ) 자동매매 봇.
KIS(한국투자증권) Open API로 주문 실행, KRX 데이터로 신호 생성, PostgreSQL로 OHLCV·체결 이력 저장.

- **Python 3.13+** required
- **Package manager:** `uv`
- **Package name:** `wye.blsh` (src layout: `src/wye/blsh/`)

## Commands

```bash
# 의존성 설치
uv sync

# 자동매매 실행 (기본: KIS_ENV=demo 모의투자)
uv run python -m wye.blsh

# 실전투자 🚨
KIS_ENV=real uv run python -m wye.blsh

# WebSocket 실시간 체결가 사용
USE_WEBSOCKET=1 uv run python -m wye.blsh

# 매수 후보 스캔 + PO 파일 생성
uv run python -m wye.blsh po

# 그리드 서치 최적화
uv run python -m wye.blsh.domestic.optimize.grid_search
uv run python -m wye.blsh.domestic.optimize.grid_search --rebuild  # 캐시 재생성
uv run python -m wye.blsh.domestic.optimize.grid_search --years 2

# 수급 캡 비교 백테스트
uv run python -m wye.blsh.domestic.optimize.supply_cap_test

# bin/ 래퍼 스크립트
bin/start_trade.sh    # 트레이더 실행
bin/make_po.sh        # PO 생성
bin/optimize.sh       # 그리드 서치
bin/watchdog.sh       # 크래시 감지 + 재시작
bin/setup_cron.sh     # cron 설치
```

No formal test framework, linter, or formatter is configured.

## Architecture

### Package Structure

```
src/wye/blsh/
├── __init__.py             # 로깅 부트스트랩
├── __main__.py             # CLI: 인수 없음 → trader.run() / "po" → collect+scan
├── common/
│   ├── env.py              # 환경변수 로드 + 경로 상수 (BLSH_HOME=~/.blsh)
│   ├── dtutils.py          # 날짜/시간 유틸
│   ├── fileutils.py        # JSON/Excel I/O
│   └── messageutils.py     # 텔레그램 알림 (전용 이벤트 루프, asyncio 충돌 방지)
├── database/
│   ├── __init__.py         # SQLAlchemy 엔진 + CRUD
│   ├── models.py           # ORM 모델 (OHLCV, 수급, 종목정보, 거래이력)
│   └── query.py            # 도메인 쿼리 (OHLCV, 영업일, 체결이력)
├── domestic/
│   ├── __init__.py         # PO, PO_TYPE_*, Tick, Milestone
│   ├── factor.py           # 최적 파라미터 (UPPERCASE 상수, 그리드 서치 결과)
│   ├── scanner.py          # 4단계 신호 생성 + PO 파일 생성
│   ├── trader.py           # 실시간 주문 실행 + SL/TP 관리 (09:00–20:00)
│   ├── collector.py        # OHLCV + 수급 데이터 수집 (KRX + KIS API)
│   ├── simulator.py        # 백테스트 시뮬레이터 (ATR 기반 SL/TP 재현)
│   ├── kis_client.py       # KIS API 래퍼 (현재가·주문·잔고·체결조회)
│   ├── ws_monitor.py       # WebSocket 실시간 체결가 (KIS H0UNCNT0)
│   ├── reporter.py         # 스캔 결과 출력/로그
│   ├── sector.py           # 업종 코드 ↔ 이름 매핑
│   ├── sector_check.py     # 업종 건강도 평가
│   ├── log_analyzer.py     # 체결 로그 분석
│   └── optimize/
│       ├── grid_search.py       # 그리드 서치 (12차원, 멀티프로세싱)
│       ├── _cache.py            # 백테스트 캐시 빌더
│       ├── signal_analysis.py   # 플래그별 성과 분석
│       ├── supply_cap_test.py   # 수급 점수 상한 비교 백테스트
│       └── diag*.py             # 각종 진단 스크립트
├── kis/
│   ├── kis_auth.py                         # OAuth2 토큰 관리 + API 공통 헤더
│   └── domestic_stock/
│       ├── domestic_stock_functions.py     # REST: 현재가·주문·잔고·투자자동향
│       ├── domestic_stock_functions_ws.py  # WebSocket: 실시간 호가
│       └── domestic_stock_info.py          # 종목 메타 조회
└── krx/
    ├── krx_auth.py       # KRX 사이트 로그인
    └── krx_data/
        ├── idx.py        # 지수 OHLCV (KOSPI/KOSDAQ/테마)
        ├── isu.py        # 종목 OHLCV + 투자자별 매매동향
        └── etx.py        # ETF OHLCV
```

### Environment Variables

`~/.blsh/config/.env` 에서 로드:

| 변수 | 설명 |
|------|------|
| `KIS_APP_KEY`, `KIS_APP_SECRET` | KIS API 인증 |
| `KIS_ENV` | `"demo"` (모의투자, 기본값) \| `"real"` (실전 🚨) |
| `USE_WEBSOCKET` | `"1"` → WebSocket 체결가 / 그 외 → REST 폴링 |
| `DB_USER/PASSWORD/NAME/HOST/PORT` | PostgreSQL |
| `KRX_LOGIN_ID`, `KRX_LOGIN_PW` | KRX 사이트 로그인 |
| `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` | 알림 |

런타임 경로 (`BLSH_HOME = ~/.blsh`): `DATA_DIR`, `LOG_DIR`, `BACKUP_DIR`, `TEMP_DIR`

### Configuration

- **KIS 인증:** `~/.blsh/config/kis_devlp.yaml` — appkey, appsecret, 계좌번호, URL. `$ENV_VAR` 치환 지원.
- **토큰 캐시:** `~/.blsh/config/KIS{YYYYMMDD}` — 매일 자동 갱신
- **Docker:** `docker-compose.yml` — PostgreSQL 16 + KIS trade MCP 서비스

### factor.py — 최적 파라미터

그리드 서치 결과를 반영한 flat UPPERCASE 상수. `Factor` 클래스 없음. DAY/SWING 분리 없음.

```python
INVEST_MIN_SCORE = 9       # 투자 진입 최소 점수
ATR_SL_MULT = 2.5          # 손절: buy - ATR × 2.5
ATR_TP_MULT = 3.0          # 2차 익절: buy + ATR × 3.0
TP1_MULT = 1.5             # 1차 익절: buy + ATR × 1.5
TP1_RATIO = 0.3            # 1차 익절 매도 비율 (30%)
GAP_DOWN_LIMIT = 0.05      # 갭 하락 5% 초과 시 진입 스킵
MAX_HOLD_DAYS = 7          # REV 최대 보유일
MAX_HOLD_DAYS_MIX = 2      # MIX 최대 보유일
MAX_HOLD_DAYS_MOM = 3      # MOM 최대 보유일
SECTOR_PENALTY_THRESHOLD = -0.03
SECTOR_PENALTY_PTS = -2
SECTOR_BONUS_PTS = 1
```

`grid_search._update_factor_file()`이 이 파일을 자동 갱신함. 포맷 변경 시 양쪽 동기화 필요.

### scanner.py — 신호 생성 4단계

**1단계 기술 점수** (OHLCV): MACD·RSI·볼린저·이동평균·거래량 등 14개 플래그.
신호 타입: `MOM`(추세추종) · `REV`(반전) · `MIX`(혼합) · `WEAK`

**2단계 수급 보정**: 기관·외국인 순매수 전환 (+3), 연속 매수 (+2/+1), 동반 (+1).
수급 가산 상한 = **+3** (`_SUPPLY_CAP = 3`, 백테스트 검증).

**3단계 업종 환경**: 업종 지수 MA20 대비 강도로 패널티/보너스.

**4단계 PO 파일 생성**: 최종 점수 ≥ `INVEST_MIN_SCORE` 종목을 JSON으로 저장.
- `po-{date}-pre.json` — 전일 스캔 (30%, NXT 08:00 매수)
- `po-{date}-ini.json` — 오전 스캔 (15%, KRX ~10:10 매수)
- `po-{date}-fin.json` — 청산 후 스캔 (55%, NXT 15:30 매수)

### trader.py — 실시간 매매

단일 스레드, 차등 주기 루프 (10초 틱 / 30초 슬로우):

| 시각 | 동작 |
|------|------|
| 08:00 | PO① NXT 지정가 매수 (30%) |
| 09:00 | KRX 개장, SL/TP 모니터링 시작 |
| ~10:10 | PO② KRX 지정가 매수 (15%), 10분 후 미체결 취소 |
| 15:15 | 만기 종목 전량 시장가 청산 |
| 15:15–15:30 | PO③ NXT 지정가 매수 (55%) |
| 15:30–20:00 | NXT 에프터마켓 SL/TP 지속 (NXT는 지정가만) |

**SL/TP 로직:**
- 손절: `현재가 ≤ SL` → 전량 시장가 매도
- 1차 익절: `현재가 ≥ TP1` → TP1_RATIO 매도, SL → 매수가(본전)
- 트레일링 SL: 전일 고가 기준 `(prev_high - ATR × ATR_SL_MULT)`로 갱신 (보수적)
- 2차 익절: `현재가 ≥ TP2` → 잔량 전량 매도

**KRX 시장가 매도** (`_sell_market`): 매도 후 2초 대기 → `inquire_daily_ccld`로 실제 체결가 조회 → DB/텔레그램 기록.

**Position 영속화**: `~/.blsh/data/positions.json` (백업: `~/.blsh/backup/positions.json.bak`)

### kis_client.py — KIS API 래퍼

```python
KISClient(env_dv="demo")          # "real" 시 경고

get_price(ticker)                  # 단일 현재가
fetch_prices(tickers)              # 병렬 조회 (ThreadPoolExecutor)
get_balance()                      # → (holdings, avg_prices, cash)
buy(ticker, qty, price)            # 지정가 매수 (SOR: KRX/NXT), odno 반환
buy_market(ticker, qty)            # 시장가 매수 (KRX 고정)
sell(ticker, qty)                  # 시장가 매도 (KRX), odno 반환
sell_nxt(ticker, qty, price)       # 지정가 매도 (NXT)
cancel_order(ticker, odno, qty)    # 주문 취소
get_filled_price(ticker, odno, today)  # 체결가 조회
```

Rate limit: 모의 2 calls/s / 실전 4 calls/s. 동시 호출 수 = `_API_CONCURRENCY = 2`.

### KIS 인증 흐름

1. `~/.blsh/config/kis_devlp.yaml` 파싱 (환경변수 치환)
2. `~/.blsh/config/KIS{YYYYMMDD}` 토큰 캐시 확인
3. 만료/없으면 `/oauth2/tokenP` POST → JWT 발급·저장
4. `auth("prod")` = 실전 / `auth("vps")` = 모의투자
5. 실전 TR ID(`T/J/C` prefix) → 모의 시 자동으로 `V` prefix 변환

### optimize/ — 파라미터 최적화

- **`grid_search.py`**: 단일 `GRID` dict 기반 전수 탐색, 최적 파라미터를 `factor.py`에 자동 기록
- **`_cache.py`**: 백테스트용 신호·OHLCV 캐시 (수급 점수 상한 포함)
- **`signal_analysis.py`**: 플래그별 승률·손절률 (Step1) + ON/OFF 그리드 (Step2)
- **`supply_cap_test.py`**: 수급 상한 비교 (캡 없음 / +3 / +2)

### simulator.py — 백테스트 시뮬레이터

`trader.py`의 SL/TP 로직을 일봉으로 재현. 보수적 처리:
- 트레일링 SL: **전일** 고가 기준으로만 갱신 (당일 고→저 순서 불명)
- TP1 + 본전 SL 동일 봉: TP1 체결 후 잔량 본전 청산 (가장 불리한 시나리오)

`simulator.py`와 `grid_search._simulate_one()`은 동일 로직을 병행 유지해야 함.

## Key Dependencies

pandas, requests, websockets, pycryptodome, PyYAML, python-dotenv, SQLAlchemy, psycopg2-binary, python-telegram-bot, httpx, pykrx, pyqt6, pyside6, openpyxl

## Important Notes

- `domestic/codex/` 하위는 실험적 모듈 — 모든 작업에서 무시할 것
- `factor.py`는 flat 상수만 정의. `Factor` 클래스, `TRADE_FLAG`, DAY/SWING 분리 없음
- `grid_search._update_factor_file()`이 `factor.py`를 직접 생성하므로 포맷 변경 시 양쪽 동기화 필요
- 수급 점수 상한(`_SUPPLY_CAP = 3`)은 `scanner.py`와 `_cache.py` 양쪽에서 적용
