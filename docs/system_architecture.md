# 자동 매매 시스템 아키텍처

> 패키지: `wye.blsh.domestic`
> 최종 갱신: 2026-03-28

---

## 1. 파일 구조

```
wye/blsh/
├── common/
│   ├── dtutils.py           # 날짜/시간 유틸리티
│   ├── fileutils.py         # 파일 I/O
│   ├── messageutils.py      # 텔레그램 알림
│   └── env.py               # DATA_DIR, KIS_ENV, TRADE_FLAG 등
├── database/
│   ├── models.py            # SQLAlchemy 모델
│   └── query.py             # DB 쿼리 (idx_clss 필터 지원)
├── kis/
│   ├── kis_auth.py          # KIS 인증 + KISWebSocket
│   └── domestic_stock/
│       ├── domestic_stock_functions.py    # KIS REST API 래퍼
│       ├── domestic_stock_functions_ws.py # KIS WebSocket 래퍼
│       └── domestic_stock_info.py        # 종목/업종 마스터
└── domestic/
    ├── __init__.py           # PO, Tick, Milestone 클래스 + PO_TYPE 상수
    ├── factor.py             # 전략 파라미터 (DAY/SWING, grid_search 자동 갱신)
    ├── kis_client.py         # KISClient (RateLimiter, SOR/KRX/NXT 라우팅)
    ├── sector.py             # 업종코드→DB 지수명 매핑 + IDX_CLSS 상수
    ├── scanner.py            # 매수 신호 스캐너
    ├── simulator.py          # 백테스트 시뮬레이터
    ├── trader.py             # 자동 매매 트레이더 (KRX+NXT 에프터마켓)
    ├── reporter.py           # 리포트 출력
    ├── collector.py          # OHLCV 데이터 수집
    └── optimize/
        ├── _cache.py         # 최적화용 벌크 데이터 캐시
        └── grid_search.py    # Grid Search 파라미터 최적화 + factor.py 자동 갱신
```

---

## 2. Trader (trader.py)

### 2.1 시간대별 동작

| 시간 | 동작 | 거래소 | 매도 방식 |
|------|------|:------:|-----------|
| **08:00** | NXT 프리마켓 → PO①(pre) 지정가 매수 | NXT | — |
| 08:00~09:00 | 체결 대기 | — | — |
| **09:00** | KRX 개장 → 실패 재주문 + 기간초과 청산 | KRX | 시장가 |
| 09:00~15:15 | SL/TP 모니터링 (10초 주기) + PO②③ | KRX | **시장가** |
| **15:15** | 만기 청산 → PO③(fin) 매수 | KRX | 시장가 |
| **15:30** | KRX 마감 → NXT 에프터마켓 전환 | — | — |
| 15:30~20:00 | SL/TP 모니터링 (10초 주기) | NXT | **지정가** |
| **20:00** | NXT 마감 → 종료 | — | — |

### 2.2 거래소 라우팅

| 메서드 | excg_id_dvsn_cd | 용도 |
|--------|:---------------:|------|
| `buy()` | 인자(NXT/KRX) | PO① NXT, 이후 KRX |
| `buy_market()` | KRX 고정 | NXT 시장가 불가 |
| `sell()` | KRX 고정 | KRX 정규장 시장가 매도 |
| `sell_nxt()` | NXT 고정 | NXT 에프터마켓 지정가 매도 |
| `cancel_order()` | **인자** | 발주 시 거래소와 일치 필요 |

### 2.3 주문 취소 + excg_cd 추적

Position/PendingOrder에 `excg_cd` 필드 추가.
매수 시 사용한 거래소 코드를 저장 → 취소 시 동일 코드 전달.

```
PO① NXT 매수 → PendingOrder(excg_cd="NXT") → cancel_order(excg="NXT")
PO② KRX 매수 → PendingOrder(excg_cd="KRX") → cancel_order(excg="KRX")
```

### 2.4 SL/TP 전략

```
KRX 정규장 (09:00~15:30):
  SL/TP 트리거 → sell() 시장가 매도 (KRX)

NXT 에프터마켓 (15:30~20:00):
  SL/TP 트리거 → sell_nxt(price=Tick.floor_tick(현재가)) 지정가 매도
```

---

## 3. 업종지수 패널티/보너스

### 3.1 idx_clss 구분

DB `idx_stk_ohlcv` 테이블에 동일 업종명(전기전자, 금속 등)이 KOSPI/KOSDAQ 양쪽에 존재.
`idx_clss` 필터 없이 조회하면 두 계열이 섞여 MA20 괴리율 오류 발생.

```
idx_clss = "02" → KOSPI 업종지수
idx_clss = "03" → KOSDAQ 업종지수
```

`sector.py`에 `IDX_CLSS_KOSPI="02"`, `IDX_CLSS_KOSDAQ="03"` 상수 정의.

### 3.2 MA20 계산 — 당일 제외

`LIMIT ma_days+1` (21행) 조회 후 `[0]`=당일(비교 대상), `[1:]`=이전 20일(MA 계산).

### 3.3 적용 흐름

```
scanner: check_index_above_ma(idx_clss=KOSPI/KOSDAQ)
         → _get_sector_gap(idx_clss=KOSPI/KOSDAQ)
         → _apply_sector_penalty: (sec_nm, idx_clss) 캐시 키

_cache:  _compute_index_env(idx_clss 필터)
         _compute_sector_gaps → (idx_nm, idx_clss, date) 3-tuple 키
         신호 수집 → sector.get_idx_clss(mkt) 파생
```

---

## 4. Grid Search 최적화

### 4.1 factor.py 자동 갱신

```bash
uv run python -m wye.blsh.domestic.optimize.grid_search           # 기본: 자동 갱신
uv run python -m wye.blsh.domestic.optimize.grid_search --no-apply # 결과만 확인
uv run python -m wye.blsh.domestic.optimize.grid_search --mode DAY # 한쪽만 (나머지 유지)
```

### 4.2 _cache.py 상수 참조

scanner.py 자체 상수(GAP_THRESHOLD, RSI_OVERSOLD 등)는 `scanner`에서 직접 import.
`factor` 모듈은 _cache.py에서 미사용 (import 제거됨).

---

## 5. CLI 사용법

```bash
uv run python -m wye.blsh                                         # 트레이더
uv run python -m wye.blsh.domestic.scanner                        # PO 발행
uv run python -m wye.blsh.domestic.optimize.grid_search --rebuild  # 최적화
```
