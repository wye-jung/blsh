# blsh (Buy Low Sell High)

한국 주식(KOSPI/KOSDAQ) 자동매매 봇.

KIS(한국투자증권) Open API로 주문 실행, KRX 데이터로 매수 신호 생성, PostgreSQL로 OHLCV 및 체결 이력 저장.

## 주요 기능

- **4단계 신호 생성**: 기술 지표(15개 플래그) → 수급 보정 → 업종 환경 → PO 발행
- **실시간 매매**: 08:00 NXT 프리마켓 ~ 20:00 NXT 에프터마켓, SL/TP 자동 관리
- **하루 3회 PO**: PRE(전일 스캔 30%) + INI(장초반 15%) + FIN(청산 후 55%)
- **자동 파라미터 최적화**: 매주 토요일 grid search (2단계, numba JIT 백테스트)
- **크론 파이프라인**: 데이터 수집 → 스캔 → 매매 → 분석 → 텔레그램 리포트

## 요구사항

- Python 3.12+ (pykrx 의존성으로 3.12 권장)
- PostgreSQL 16+
- KIS 증권 API 키
- KRX 로그인 계정

## 빠른 시작

```bash
# 1. 의존성 설치
uv sync

# 2. 환경변수 설정
# ~/.blsh/config/.env 에 KIS_APP_KEY, DB_USER 등 설정

# 3. 모의투자 실행
uv run python -m wye.blsh

# 4. 크론탭 등록 (전체 파이프라인 자동화)
bin/setup_cron.sh install
```

## CLI

```bash
uv run python -m wye.blsh                  # 트레이더 실행
uv run python -m wye.blsh stop             # 트레이더 종료
uv run python -m wye.blsh status           # 상태 확인
uv run python -m wye.blsh po              # 데이터 수집 + PO 생성
uv run python -m wye.blsh holiday         # 휴장일 수집
uv run python -m wye.blsh sector          # 업종 매핑 확인
uv run python -m wye.blsh analyze         # 일일 분석 리포트

bin/watchdog.sh                            # 트레이더 + 모니터링
bin/watchdog.sh stop                       # 전체 종료
bin/setup_cron.sh install                  # 크론탭 등록
```

## 문서

| 문서 | 내용 |
|------|------|
| [docs/scanner.md](docs/scanner.md) | 신호 생성 4단계 (플래그, 수급, 업종, PO) |
| [docs/trader.md](docs/trader.md) | 실시간 매매 (타임라인, SL/TP, 자금 배분) |
| [docs/cron-pipeline.md](docs/cron-pipeline.md) | 크론 파이프라인 + watchdog |
| [docs/optimize.md](docs/optimize.md) | 파라미터 최적화, 백테스트 |
| [docs/data-collection.md](docs/data-collection.md) | 데이터 수집 + KIS 인증 |

## 환경변수

`~/.blsh/config/.env`:

| 변수 | 설명 |
|------|------|
| `KIS_APP_KEY`, `KIS_APP_SECRET` | KIS API 인증 |
| `KIS_ENV` | `demo` (기본) / `real` (실전 투자) |
| `DB_USER/PASSWORD/NAME/HOST/PORT` | PostgreSQL |
| `KRX_LOGIN_ID`, `KRX_LOGIN_PW` | KRX 로그인 |
| `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` | 알림 |
| `USE_WEBSOCKET` | `1` = WebSocket 체결가 |
