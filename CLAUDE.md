# CLAUDE.md

Claude Code 작업 지침. 상세 문서는 `docs/` 참조.

## 보안 금지 사항 (SECURITY — NEVER VIOLATE)

다음 파일/폴더는 **절대** 읽기·수정·삭제·git 커밋·푸시 금지:
- 모든 경로의 dotenv 파일 (환경변수 파일)
- `~/.blsh/config/` 폴더 및 그 하위 파일 전체 (API 키, 토큰, 계좌번호 포함)

이 제한은 PreToolUse 훅으로도 강제됩니다. 어떤 이유로도 예외 없음.

## Project Overview

**blsh** (buy low sell high) — 한국 주식(KOSPI/KOSDAQ) 자동매매 봇.
KIS Open API로 주문 실행, KRX 데이터로 신호 생성, PostgreSQL로 OHLCV·체결 이력 저장.

- **Python 3.12+** (pykrx 의존성으로 3.12 권장), **uv**, `wye.blsh` (src layout: `src/wye/blsh/`)

## Commands

```bash
uv sync                                    # 의존성 설치

# CLI 서브커맨드
uv run python -m wye.blsh                  # 트레이더 실행 (= start)
uv run python -m wye.blsh stop             # 트레이더 종료
uv run python -m wye.blsh status [positions|pendings|holdings|cash]
uv run python -m wye.blsh po              # 데이터 수집 + PO 생성
uv run python -m wye.blsh holiday         # 휴장일 수집
uv run python -m wye.blsh sector          # 업종지수 매핑 확인
uv run python -m wye.blsh analyze [YYYYMMDD]  # 일일 분석 리포트

# 실전투자 🚨
KIS_ENV=real uv run python -m wye.blsh

# 최적화
uv run python -m wye.blsh.domestic.optimize.grid_search [--rebuild|--years N|--alternating]
uv run python -m wye.blsh.domestic.optimize.grid_search --walkforward [--train-months 18 --val-months 6]
uv run python -m wye.blsh.domestic.optimize.signal_analysis
uv run python -m wye.blsh.domestic.optimize.supply_cap_test

# bin/ 스크립트
bin/watchdog.sh [monitor|stop|status]     # 트레이더 + 모니터링
bin/setup_cron.sh [install|remove|status] # 크론탭 관리
```

No formal test framework, linter, or formatter is configured.

## Package Structure

```
src/wye/blsh/
├── __main__.py          # CLI (start/stop/status/po/holiday/sector/analyze)
├── common/              # env, dtutils, fileutils, messageutils (텔레그램)
├── database/            # SQLAlchemy 엔진, ORM 모델, 도메인 쿼리
├── domestic/
│   ├── config.py        # Optimized 클래스 + 스캔/매매 상수 (grid_search 자동 갱신)
│   ├── scanner.py       # 4단계 신호 생성 → PO 파일 (docs/scanner.md)
│   ├── trader.py        # 실시간 매매 08:00–20:00 (docs/trader.md)
│   ├── collector.py     # KRX OHLCV + 수급 수집
│   ├── _sim_core.py     # 백테스트 (numba JIT)
│   ├── simulator.py     # 백테스트 래퍼 (dict 기반)
│   ├── kis_client.py    # KIS API 래퍼
│   ├── ws_monitor.py    # WebSocket 실시간 체결가
│   ├── log_analyzer.py  # 일일 리포트 → 텔레그램
│   ├── sector.py        # 업종 매핑
│   └── optimize/        # grid_search, signal_analysis, supply_cap_test, _cache
├── kis/                 # KIS 인증 + REST/WebSocket API
└── krx/                 # KRX 데이터 (지수, 종목, ETF OHLCV)
```

## Cron Pipeline — 일일 자동 운영

`bin/setup_cron.sh install`로 등록. 상세: `docs/cron-pipeline.md`

```
07:30 po → PO①(pre)     07:55 watchdog → trader 시작
08:00 trader → NXT 매수   09:00 KRX 개장, SL/TP 시작
10:05 po → PO②(ini)      ~10:10 trader → KRX 매수
15:05 po → PO③(fin)      15:15 trader → 청산 + 매수
15:30 NXT 에프터마켓      20:00 trader 종료
20:30 analyze → 텔레그램   토 02:00 grid_search
```

## Key Configuration (config.py)

- `Optimized` 클래스: `grid_search`가 자동 갱신 (SL/TP/보유일/점수 등)
- `SIGNAL_SCORES`: 15개 플래그 점수 (grid_search 자동 갱신)
- `SUPPLY_CAP = 3`: 수급 가산 상한 (scanner + _cache 양쪽 적용)
- `MAX_ALLOC_TIERS`: 총자산 규모별 종목당 배분 비율 상한

## Important Notes

- `domestic/codex/` 하위는 실험적 모듈 — 모든 작업에서 무시할 것
- `config.py` 수동 수정 시 다음 grid_search에서 덮어씌워짐
- `_sim_core.py`와 `grid_search` 내부 시뮬레이션은 동일 로직 병행 유지 필요
- `messageutils.send_message()`는 `future.result(timeout=10)` 대기 — CLI 종료 전 전송 보장
- `_recreate()` (collector): 트랜잭션 분리 — 묶으면 커넥션 경합 발생
- 코드 변경되면 문서도 업데이트 할 것
- 요청이 적절하지 않다고 판단하면 의견 제시할 것

## Docs

- [docs/scanner.md](docs/scanner.md) — 신호 생성 4단계 (플래그, 수급, 업종, PO)
- [docs/trader.md](docs/trader.md) — 실시간 매매 (타임라인, SL/TP, 자금 배분)
- [docs/cron-pipeline.md](docs/cron-pipeline.md) — 크론 파이프라인 + watchdog
- [docs/optimize.md](docs/optimize.md) — 그리드 서치, 신호 분석, 백테스트
- [docs/data-collection.md](docs/data-collection.md) — 데이터 수집 + KIS 인증
