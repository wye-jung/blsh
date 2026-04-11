# Data Collection — 데이터 수집 + KIS 인증

## Collector (collector.py)

`collector.collect()` 흐름:

1. KRX 최근 영업일 확인 -> DB 최신 일자와 비교
2. 미수집 기간 있으면 일자별로 순차 수집:
   - `_collect_idx_data()`: 지수 OHLCV (KOSPI/KOSDAQ/KRX/테마)
   - `_collect_isu_data()`: 종목 OHLCV + 투자자별 매매동향 (수급)
   - `_collect_etx_data()`: ETF OHLCV
   - `_collect_base_info()`: 종목/ETF 기본정보 (전체 delete + insert)
3. 장중 재실행 시 당일 데이터 갱신
   - 수급은 KRX가 장중 미업데이트 가능 -> scanner에서 KIS API fallback

### 저장 방식

`_recreate()` -- 해당 일자 delete + insert:
- 트랜잭션 분리 (delete/create 별도 커밋) -- 묶으면 커넥션 경합 발생
- base_info는 필터 없이 전체 delete + insert (스냅샷 교체)

## 환경변수

`~/.blsh/config/.env`:

| 변수 | 설명 |
|------|------|
| `KIS_APP_KEY`, `KIS_APP_SECRET` | KIS API 인증 |
| `KIS_ENV` | `demo` (모의투자, 기본) / `real` (실전) |
| `USE_WEBSOCKET` | `1` -> WebSocket 체결가 / 그 외 -> REST 폴링 |
| `DB_USER/PASSWORD/NAME/HOST/PORT` | PostgreSQL |
| `KRX_LOGIN_ID`, `KRX_LOGIN_PW` | KRX 사이트 로그인 |
| `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` | 알림 |

런타임 경로 (`BLSH_HOME = ~/.blsh`):
- 환경별: `DATA_DIR`, `LOG_DIR`, `BACKUP_DIR` -> `~/.blsh/{KIS_ENV}/{data,logs,backup}`
- 공통: `CACHE_DIR` -> `~/.blsh/cache`, `TEMP_DIR` -> `~/.blsh/temp`

## 실시간 수급 추정 (scanner.py)

`fetch_investor_estimate()` -- 실전투자 장중 전용:
- KIS `investor_trend_estimate` API (TR_ID: HHPTJ04160200) 호출
- 외국인 09:30, 기관 11:20부터 당일 가집계 데이터 제공
- DB에 당일 수급 데이터가 없는 종목에 대해 보강
- 모의투자에서는 미지원 (실전투자 전용 API)

## KIS Client (kis_client.py)

```python
KISClient(env_dv="demo")          # "real" 시 경고

get_price(ticker)                  # 단일 현재가
fetch_prices(tickers)              # 병렬 조회 (ThreadPoolExecutor)
get_balance()                      # -> (holdings, avg_prices, cash)
buy(ticker, qty, price)            # 지정가 매수 (SOR: KRX/NXT), odno 반환
buy_market(ticker, qty)            # 시장가 매수 (KRX 고정)
sell(ticker, qty)                  # 시장가 매도 (KRX), odno 반환
sell_nxt(ticker, qty, price)       # 지정가 매도 (NXT)
cancel_order(ticker, odno, qty)    # 주문 취소
get_filled_price(ticker, odno, today)  # 체결가 조회
```

Rate limit: 모의 2 calls/s / 실전 4 calls/s. 동시 호출 수 = `_API_CONCURRENCY = 2`.

## KIS 인증 흐름

1. `~/.blsh/config/kis_devlp.yaml` 파싱 (환경변수 치환)
2. `~/.blsh/config/KIS{YYYYMMDD}` 토큰 캐시 확인
3. 만료/없으면 `/oauth2/tokenP` POST -> JWT 발급/저장
4. `auth("prod")` = 실전 / `auth("vps")` = 모의투자
5. 실전 TR ID(`T/J/C` prefix) -> 모의 시 자동으로 `V` prefix 변환

## Configuration

- **KIS 인증:** `~/.blsh/config/kis_devlp.yaml` -- appkey, appsecret, 계좌번호, URL
- **토큰 캐시:** `~/.blsh/config/KIS{YYYYMMDD}` -- 매일 자동 갱신
- **Docker:** `docker-compose.yml` -- PostgreSQL 16 + KIS trade MCP 서비스

## Key Dependencies

pandas, requests, websockets, pycryptodome, PyYAML, python-dotenv,
SQLAlchemy, psycopg2-binary, python-telegram-bot, httpx, pykrx, numba, openpyxl
