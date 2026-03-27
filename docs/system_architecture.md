# 자동 매매 시스템 아키텍처

> 패키지: `wye.blsh.domestic`
> 최종 갱신: 2026-03-28

---

## 1. 파일 구조

```
wye/blsh/
├── __main__.py              # CLI 진입점
├── common/
│   ├── dtutils.py           # 날짜/시간 유틸리티
│   ├── fileutils.py         # 파일 I/O
│   ├── messageutils.py      # 텔레그램 알림
│   └── env.py               # DATA_DIR, KIS_ENV, TRADE_FLAG 등
├── database/
│   ├── models.py            # SQLAlchemy 모델
│   └── query.py             # DB 쿼리
├── kis/
│   ├── kis_auth.py          # KIS 인증
│   └── domestic_stock/
│       ├── domestic_stock_functions.py   # KIS API 래퍼
│       └── domestic_stock_info.py       # 종목/업종 마스터
└── domestic/
    ├── __init__.py           # PO, Tick, Milestone 클래스 + PO_TYPE 상수
    ├── factor.py             # 전략 파라미터 (DAY/SWING 모드별)
    ├── kis_client.py         # KISClient (RateLimiter, SOR/KRX 라우팅)
    ├── sector.py             # 업종코드 → DB 지수명 매핑 테이블
    ├── scanner.py            # 매수 신호 스캐너
    ├── simulator.py          # 백테스트 시뮬레이터
    ├── trader.py             # 자동 매매 트레이더
    ├── reporter.py           # 리포트 출력
    ├── collector.py          # OHLCV 데이터 수집
    └── optimize/
        ├── _cache.py         # 최적화용 벌크 데이터 캐시
        └── grid_search.py    # Grid Search 파라미터 최적화
```

---

## 2. 핵심 모듈 (`__init__.py`)

### PO (Purchase Order) 클래스

```python
PO_TYPE_PRE = "pre"   # 전일 스캔 → 익일 08:00 매수
PO_TYPE_INI = "ini"   # 장초 스캔 → ~10:10 매수
PO_TYPE_FIN = "fin"   # 오후 스캔 → 15:15 청산 후 매수
```

PO 파일 경로: `~/.blsh/data/po/po-{entry_date}-{po_type}.json`
처리 후: `~/.blsh/data/po/done/` 이동

### Tick 클래스

KRX 호가 단위 보정 (`floor_tick`, `ceil_tick`)

### Milestone 클래스

시간 상수: `NXT_OPEN_TIME(08:00)`, `KRX_OPEN_TIME(09:00)`,
`KRX_EARLY_TIME(10:10)`, `LIQUIDATE_TIME(15:15)`, `KRX_CLOSE_TIME(15:30)`

---

## 3. Scanner (scanner.py)

### 3.1 전체 흐름

```
scan(base_date)
  ├── [0단계] 시장 지수 환경 체크 (MA20 대비 -5% 이하 → 스캔 스킵)
  ├── [1단계] KOSPI/KOSDAQ OHLCV → 15개 기술적 지표 평가 → 점수 산출
  ├── [2단계] 수급 보강 (외국인/기관 순매수 전환·연속·당일)
  └── 결과 DataFrame 반환

find_candidates(base_date)
  ├── scan() 호출
  ├── 업종지수 패널티/보너스 적용
  ├── INVEST_MIN_SCORE 이상 + MOM/MIX/REV + P_OV 제외
  ├── 모드별 max_hold_days 부여, po_type 자동 판단
  └── entry_date / expiry_date 계산

issue_po(base_date)
  ├── find_candidates() → PO json 생성
  └── DB(TradeCandidates) 저장
```

### 3.2 15개 기술적 지표

| # | 플래그 | 점수 | 성격 | 설명 |
|---|--------|:----:|:----:|------|
| 1 | MGC | +2 | 모멘텀 | MACD 골든크로스 |
| 2 | MPGC | +1 | 중립 | MACD 예상 골든크로스 (히스토그램 수렴) |
| 3 | RBO | +2 | 전환 | RSI 30 상향 돌파 |
| 4 | ROV | +1 | 전환 | RSI 과매도 (<30) |
| 5 | BBL | +1 | 전환 | 볼린저 하단 반등 |
| 6 | BBM | +1 | 중립 | 볼린저 중간선 상향 돌파 |
| 7 | VS | +1 | 모멘텀 | 거래량 급증(20일 평균 2배) + 양봉 |
| 8 | MAA | +1 | 모멘텀 | 이동평균 정배열 전환 (5>20>60) |
| 9 | SGC | +1 | 중립 | 스토캐스틱 과매도 교차 |
| 10 | W52 | +2 | 모멘텀 | 52주 신고가 돌파 + 20일 평균 거래량×1.5배 |
| 11 | PB | +2 | 모멘텀 | 눌림목 패턴 (5MA 이탈 후 복귀) |
| 12 | HMR | +1 | 전환 | 망치형 캔들 (opn=None 시 스킵) |
| 13 | LB | +2 | 모멘텀 | 장대 양봉 (c0>o0 + ATR×1.5, opn=None 시 스킵) |
| 14 | MS | +2 | 전환 | 모닝스타 3일 반전 (opn 3일 이상 필요) |
| 15 | OBV | +1 | 모멘텀 | OBV 3일 연속 상승 |

### 3.3 모드 분류

| 모드 | 조건 | 점수 계산 |
|------|------|-----------|
| **MOM** | 모멘텀 ≥2개, > 전환 | mom + neu |
| **REV** | 전환 ≥2개, > 모멘텀 | rev + neu |
| **MIX** | 둘 다 > 0 | max(mom, rev) + neu |
| WEAK | 나머지 | 투자 대상 제외 |

### 3.4 수급 보강 (2단계, score≥2인 종목만)

| 플래그 | 점수 | 조건 |
|--------|:----:|------|
| F_TRN | +3 | 외국인 순매수 전환 (2일+ 매도→오늘 매수) |
| I_TRN | +3 | 기관 순매수 전환 |
| F_C3 | +2 | 외국인 3일+ 연속 순매수 |
| I_C3 | +2 | 기관 3일+ 연속 순매수 |
| F_1 | +1 | 외국인 오늘만 순매수 |
| I_1 | +1 | 기관 오늘만 순매수 |
| FI | +1 | 외국인+기관 동시 |
| P_OV | -1 | 개인만 대량 순매수 → **투자 대상 제외** |

### 3.5 업종지수 패널티/보너스

| 조건 | 점수 조정 |
|------|:---------:|
| 업종지수 MA20 대비 < **-5%** | **-2점** |
| 업종지수 MA20 대비 ≥ **0%** | **+1점** |
| -5% ~ 0% 사이 | 변동 없음 |

업종 매핑 (`sector.py` + `_load_ticker_sector_map`):
- KOSPI: 중분류(13개) → 대분류(11개) → "코스피" fallback
- KOSDAQ: 중분류(13개) → 대분류(7개) → "코스닥" fallback
- 캐시: `~/.blsh/data/cache/sector_map.json` (base_date 기준, 빈 결과 시 미저장)
- KOSPI/KOSDAQ 독립 try-except (한쪽 실패해도 다른 쪽 보존)

---

## 4. Trader (trader.py)

### 4.1 시간대별 동작

| 시간 | 동작 | 비고 |
|------|------|------|
| **08:00** | NXT 프리마켓 → PO①(pre) NXT 지정가 매수 | 가용현금의 30% |
| 08:00~09:00 | 체결 대기 | NXT 비대상 종목은 실패 수집 |
| **09:00** | KRX 정규장 → 프리마켓 실패 재주문 + 기간초과 청산 | 1회 |
| 09:00~10:10 | PO②(ini) 감시 (15%) + SL/TP 모니터링 | 30초 주기 |
| 10:10~15:15 | SL/TP 모니터링 | 10초 주기 |
| **15:15** | 만기 청산 → PO③(fin) 매수 | 청산 후 현금의 55%×90% |
| **15:30** | 장 마감 | 미체결 전량 취소 |

### 4.2 SL/TP 전략 (분할 익절)

```
SL  = buy_price - ATR × ATR_SL_MULT          (손절선)
TP1 = buy_price + ATR × TP1_MULT             (1차 익절)
TP2 = buy_price + ATR × ATR_TP_MULT          (2차 익절)

TP1 도달 시:
  → qty × TP1_RATIO 만큼 매도
  → SL → buy_price (본전 보장)
  → 잔량은 TP2까지 트레일링

TP2 도달 시:
  → 잔량 전량 매도

트레일링 SL:
  → 현재가 기준: trail_sl = current - ATR × ATR_SL_MULT
  → trail_sl > 현재 SL이고 trail_sl < current일 때만 상향
```

**scanner의 stop_loss/take_profit은 참조용(종가 기준 추정치). trader에서 실제 매수가 기준으로 SL/TP를 완전히 재계산함.**

### 4.3 NXT/SOR 거래소 라우팅 (kis_client.py)

| 메서드 | excg_id_dvsn_cd | 이유 |
|--------|:---------------:|------|
| `buy()` 지정가 매수 | 인자로 지정 (NXT/KRX) | PO① NXT, 이후 KRX |
| `buy_market()` 시장가 매수 | KRX | NXT 시장가 불가 |
| `sell()` 시장가 매도 | KRX | NXT 시장가 불가 |
| `cancel_order()` 취소 | KRX | |

---

## 5. 전략 파라미터 (factor.py)

### 5.1 현재 파라미터 (2026-03-28)

| 파라미터 | DAY | SWING | 설명 |
|---------|:---:|:-----:|------|
| INVEST_MIN_SCORE | 14 | 9 | 투자 대상 최소 점수 |
| ATR_SL_MULT | 1.5 | 2.5 | 손절선: buy - ATR × N |
| ATR_TP_MULT | 3.0 | 3.0 | 2차 익절: buy + ATR × N |
| TP1_MULT | 1.0 | 1.5 | 1차 익절: buy + ATR × N |
| TP1_RATIO | 0.3 | 0.3 | 1차 익절 매도 비율 |
| GAP_DOWN_LIMIT | 0 | 0.05 | 갭하락 필터 |
| MAX_HOLD_DAYS (REV) | 1 | 10 | 추세전환 최대 보유일 |
| MAX_HOLD_DAYS_MIX | 1 | 5 | 혼합 최대 보유일 |
| MAX_HOLD_DAYS_MOM | 0 | 3 | 모멘텀 최대 보유일 |
| SECTOR_PENALTY_THRESHOLD | -0.05 | -0.05 | 업종 약세 기준 |
| SECTOR_PENALTY_PTS | -2 | -2 | 업종 약세 감점 |
| SECTOR_BONUS_PTS | +1 | +1 | 업종 강세 가점 |

### 5.2 환경변수

```bash
TRADE_FLAG=SWING  # 기본값. DAY로 변경 시 데이트레이딩 파라미터 적용
KIS_ENV=demo      # 기본값. real로 변경 시 실전투자
```

---

## 6. 시뮬레이터 (simulator.py)

### 6.1 trader와의 일치

- `factor.TP1_MULT` / `factor.TP1_RATIO` 참조 (하드코딩 없음)
- `SELL_COST_RATE`는 `trader.py`에서 import

### 6.2 보수적 트레일링 SL

일봉에서 당일 high/low 선후를 알 수 없으므로, **전일까지의 high만으로 SL 갱신**:

```python
prev_high = 매수일 high
for d in 보유기간:
    if d != 매수일:
        trail_sl = Tick.floor_tick(prev_high - ATR × SL_MULT)
        if trail_sl > sl: sl = trail_sl
    # 손절/TP 체크
    prev_high = max(prev_high, 당일 high)  # 봉 종료 후 갱신
```

---

## 7. 업종 매핑 (sector.py)

| 시장 | 분류 | 매핑 dict | 예시 |
|------|------|-----------|------|
| KOSPI | 중분류 | `KOSPI_MID_TO_IDX` (13개) | 5→"음식료·담배", 13→"전기전자" |
| KOSPI | 대분류 | `KOSPI_BIG_TO_IDX` (11개) | 18→"건설", 21→"금융" |
| KOSDAQ | 중분류 | `KOSDAQ_MID_TO_IDX` (13개) | 1024→"제약", 1028→"전기전자" |
| KOSDAQ | 대분류 | `KOSDAQ_BIG_TO_IDX` (7개) | 1006→"일반서비스", 1009→"제조" |

---

## 8. 최적화 (optimize/)

### 8.1 실행

```bash
uv run python -m wye.blsh.domestic.optimize.grid_search
uv run python -m wye.blsh.domestic.optimize.grid_search --mode DAY
uv run python -m wye.blsh.domestic.optimize.grid_search --no-sector
uv run python -m wye.blsh.domestic.optimize.grid_search --rebuild --years 3
```

### 8.2 캐시 (~110MB pickle)

Phase 1: OHLCV 벌크 로드 → 벡터화 신호 계산 → 업종 매핑 → 수급 보강
Phase 2: 캐시 로드 ~5초 → 조합당 ~0.3ms

### 8.3 최적화 지표

```
metric = total_ret × min(1.0, trades / 100)
```
- 거래 30건 미만 → 패널티 (-9999)

---

## 9. CLI 사용법

```bash
# 트레이더
uv run python -m wye.blsh

# PO 파일 발행
uv run python -m wye.blsh.domestic.scanner

# 최적화
uv run python -m wye.blsh.domestic.optimize.grid_search
```

---

## 10. DB 스키마 (trade_history)

```sql
CREATE TABLE trade_history (
    id          SERIAL PRIMARY KEY,
    side        VARCHAR(4) NOT NULL,
    ticker      VARCHAR(20) NOT NULL,
    name        VARCHAR(100),
    qty         INTEGER,
    price       NUMERIC,
    reason      VARCHAR(200),
    po_type     VARCHAR(10),
    traded_at   TIMESTAMP DEFAULT now()
);
```
