# 자동 매매 시스템 아키텍처

> 패키지: `wye.blsh.domestic`
> 최종 갱신: 2026-03-22
> 백테스트 기간: 최근 2년 (2024-03 ~ 2026-03)

---

## 1. 파일 구조

```
wye/blsh/
├── __main__.py              # CLI 진입점
├── common/
│   ├── dtutils.py           # 날짜/시간 유틸리티
│   ├── fileutils.py         # 파일 I/O
│   └── env.py               # DATA_DIR 등 환경 설정
├── database/
│   ├── models.py            # SQLAlchemy 모델 (TradeHistory.po_type 포함)
│   └── query.py             # DB 쿼리 (save_trade_history po_type 지원)
├── kis/
│   └── domestic_stock/
│       ├── domestic_stock_functions.py   # KIS API 래퍼
│       └── domestic_stock_info.py       # 종목/업종 마스터 다운로드
└── domestic/
    ├── _factor.py            # 전략 파라미터 (DAY/SWING 모드별)
    ├── _kis.py               # API 클래스 (SOR/KRX 라우팅)
    ├── _po.py                # PO 파일 관리 (pre/regular/final)
    ├── _tick.py              # KRX 호가 단위 보정
    ├── _report.py            # 리포트 출력
    ├── scanner.py            # 매수 신호 스캐너
    ├── simulator.py          # 백테스트 시뮬레이터
    ├── trader_v2.py          # 자동 매매 트레이더
    ├── collector.py          # OHLCV 데이터 수집
    └── optimize/
        ├── _cache.py         # 최적화용 벌크 데이터 캐시
        └── grid_search.py    # Grid Search 파라미터 최적화
```

---

## 2. Scanner (scanner.py)

### 2.1 전체 흐름

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
  ├── 모드별 max_hold_days 부여
  └── entry_date / expiry_date 계산
```

### 2.2 15개 기술적 지표

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
| 12 | HMR | +1 | 전환 | 망치형 캔들 |
| 13 | LB | +2 | 모멘텀 | 장대 양봉 (ATR×1.5 초과) |
| 14 | MS | +2 | 전환 | 모닝스타 (3일 반전 패턴) |
| 15 | OBV | +1 | 모멘텀 | OBV 3일 연속 상승 |

### 2.3 모드 분류

| 모드 | 조건 | 점수 계산 |
|------|------|-----------|
| **MOM** | 모멘텀 ≥2개, > 전환 | mom + neu |
| **REV** | 전환 ≥2개, > 모멘텀 | rev + neu |
| **MIX** | 둘 다 > 0 | max(mom, rev) + neu |
| WEAK | 나머지 | 투자 대상 제외 |

### 2.4 수급 보강 (2단계, score≥2인 종목만)

| 플래그 | 점수 | 조건 |
|--------|:----:|------|
| F_TRN | +3 | 외국인 순매수 전환 (N일 매도→오늘 매수) |
| I_TRN | +3 | 기관 순매수 전환 |
| F_C3 | +2 | 외국인 3일+ 연속 순매수 |
| I_C3 | +2 | 기관 3일+ 연속 순매수 |
| F_1 | +1 | 외국인 오늘만 순매수 |
| I_1 | +1 | 기관 오늘만 순매수 |
| FI | +1 | 외국인+기관 동시 |
| P_OV | -1 | 개인만 대량 순매수 → **투자 대상 제외** |

### 2.5 업종지수 패널티/보너스

시장 전체 지수(코스피/코스닥)와 별개로, **종목이 속한 업종지수**의 MA20 괴리율에 따라 점수를 조정한다.

| 조건 | 점수 조정 | 예시 |
|------|:---------:|------|
| 업종지수 MA20 대비 < **-5%** | **-2점** | 전기전자 업종 급락 시 삼성전자 점수 하향 |
| 업종지수 MA20 대비 ≥ **0%** | **+1점** | 제약 업종 강세 시 한미약품 점수 상향 |
| -5% ~ 0% 사이 | 변동 없음 | |

**업종 매핑 방식:**

- KOSPI: KIS 마스터의 `지수업종중분류` → DB `idx_stk_ohlcv` 지수명 (13개 업종)
- KOSPI 중분류 0인 종목: `지수업종대분류` fallback (11개 업종)
- KOSDAQ: "코스닥" 전체 지수로 fallback (세부 업종지수 데이터 부족)
- 매핑 결과는 `~/.blsh/data/cache/sector_map.json`에 당일 1회 캐시

**시장 전체 스킵 (재앙 수준):**

- 코스피/코스닥 지수가 MA20 대비 **-5% 이하**일 때만 해당 시장 전체 스캔 스킵
- 기존 -3%에서 -5%로 완화 (업종 단위 패널티가 대체)

### 2.6 투자 대상 선별 기준

```
buy_score ≥ INVEST_MIN_SCORE (DAY: 12, SWING: 11)
AND mode ∈ {MOM, MIX, REV}
AND P_OV 플래그 없음
```

---

## 3. Trader v2 (trader_v2.py)

### 3.1 시간대별 동작

| 시간 | 동작 | 비고 |
|------|------|------|
| **08:00** | NXT 프리마켓 개장 → PO① 전일스캔 SOR 매수 | 가용현금의 30% |
| 08:00~09:00 | 체결 대기, SL/TP 매도 안 함 | NXT 비대상 종목은 실패 수집 |
| **09:00** | KRX 정규장 → 프리마켓 실패 종목 재주문 | SOR→KRX 라우팅 |
| 09:00 | 기간초과 포지션 청산 (1회) | overdue_done 플래그 |
| 09:00~15:15 | PO② 오전스캔 감시 (15%) + SL/TP 모니터링 | 매 10초 현재가, 30초 po/체결 |
| **15:15** | 만기 청산 → PO③ 오후스캔 매수 | 청산 후 현금의 55%×90% |
| **15:30** | 장 마감 | 미체결 전량 취소 |

### 3.2 SL/TP 전략

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
  → 전일 high 기준으로만 갱신 (보수적, 일봉 한계 감안)
  → trail_sl = prev_high - ATR × ATR_SL_MULT
  → trail_sl > 현재 SL 일 때만 상향
```

### 3.3 NXT/SOR 거래소 라우팅

| 메서드 | excg_id_dvsn_cd | 이유 |
|--------|:---------------:|------|
| `buy()` 지정가 매수 | **SOR** | KRX/NXT 중 유리한 쪽 자동 라우팅 |
| `buy_market()` 시장가 매수 | KRX | NXT 시장가 불가 |
| `sell()` 시장가 매도 | KRX | NXT 시장가 불가 |
| `cancel_order()` 취소 | KRX | 매도가 KRX이므로 |

**프리마켓 실패 처리:**
- 08:00 SOR 매수 시 NXT 비대상 종목 → `_submit_buy_orders` 실패 dict 반환
- 09:00 KRX 개장 후 1회 재주문 (`retry_orders` + `retry_done` 플래그)

### 3.4 매입단가 보정

```python
# 갭 하락 시 entry_price보다 낮게 체결될 수 있음
buy_price = avg_prices.get(ticker) or po.entry_price
# → SL/TP를 실제 매입단가 기준으로 재계산
```

### 3.5 3단계 PO 운영

| PO | 파일명 | 시점 | 비율 | CLI |
|:--:|--------|------|:----:|-----|
| ① | `po_YYYYMMDD_pre.json` | 전일 18시 스캔 → 익일 08:00 매수 | 30% | `po pre` |
| ② | `po_YYYYMMDD_regular.json` | 당일 10시 스캔 → 감지 즉시 매수 | 15% | `po` |
| ③ | `po_YYYYMMDD_final.json` | 당일 15시 스캔 → 15:15 청산 후 매수 | 55% | `po final` |

**po_type 자동 판단 (`make_po_file`):**
- `entry_date > today` 또는 `entry_date == today && ctime < 08:00` → **pre**
- `entry_date == today && ctime >= 14:00` → **final**
- 나머지 → **regular**

### 3.6 po_type DB 추적

매수/매도 이력에 `po_type` 기록 → PO별 수익률 분석 가능:

```
_submit_buy_orders(po_type="pre"/"morning"/"final")
  → PendingOrder.po_type
    → Position.po_type
      → _save_history("buy", ..., po_type)   → trade_history
      → _sell_or_log → _save_history("sell", ..., po_type)  → trade_history
```

```sql
-- PO별 수익률 집계
SELECT b.po_type, COUNT(*), AVG((s.price - b.price) / b.price * 100)
FROM trade_history b
JOIN trade_history s ON s.ticker = b.ticker AND s.side = 'sell'
WHERE b.side = 'buy'
GROUP BY b.po_type;
```

---

## 4. 전략 파라미터 (_factor.py)

### 4.1 최적 파라미터 (2년 백테스트 기준)

| 파라미터 | DAY | SWING | 설명 |
|---------|:---:|:-----:|------|
| INVEST_MIN_SCORE | 12 | 11 | 투자 대상 최소 점수 |
| ATR_SL_MULT | 2.5 | 2.0 | 손절선: buy - ATR × N |
| ATR_TP_MULT | 2.0 | 2.0 | 2차 익절: buy + ATR × N |
| **TP1_MULT** | **0.7** | **1.0** | 1차 익절: buy + ATR × N |
| **TP1_RATIO** | **0.7** | **0.5** | 1차 익절 매도 비율 |
| **GAP_DOWN_LIMIT** | **0.03** | **0.03** | 갭하락 3% 이상 매수 스킵 |
| MAX_HOLD_DAYS (REV) | **1** | 7 | 추세전환 최대 보유일 |
| MAX_HOLD_DAYS_MIX | **1** | 3 | 혼합 최대 보유일 |
| MAX_HOLD_DAYS_MOM | 0 | 1 | 모멘텀 최대 보유일 |
| SECTOR_PENALTY_THRESHOLD | -0.05 | -0.05 | 업종 약세 기준 |
| SECTOR_PENALTY_PTS | -2 | -2 | 업종 약세 감점 |
| SECTOR_BONUS_PTS | +1 | +1 | 업종 강세 가점 |

> **DAY 모드 핵심 변경**: max_hold=0(당일청산) → max_hold=1(오버나이트 허용)으로 변경하여 TP 도달률 대폭 개선

### 4.2 환경변수

```bash
TRADE_FLAG=SWING  # 기본값. DAY로 변경 시 데이트레이딩 파라미터 적용
KIS_ENV=demo      # 기본값. real로 변경 시 실전투자
```

---

## 5. 최적화 (optimize/)

### 5.1 실행 방법

```bash
# 전체 (DAY + SWING)
uv run python -m wye.blsh.domestic.optimize.grid_search

# DAY만
uv run python -m wye.blsh.domestic.optimize.grid_search --mode DAY

# 업종 패널티 OFF (기존 방식과 비교)
uv run python -m wye.blsh.domestic.optimize.grid_search --no-sector

# 캐시 강제 재빌드
uv run python -m wye.blsh.domestic.optimize.grid_search --rebuild

# 3년 데이터
uv run python -m wye.blsh.domestic.optimize.grid_search --years 3
```

### 5.2 캐시 구조

```
Phase 1 (1회, ~3-5분):
  ① 영업일 로드
  ② OHLCV 벌크 로드 (KOSPI+KOSDAQ ~3000종목)
  ③ 지수 환경 (시장+업종)
  ④ 업종지수 MA20 괴리율 계산
  ⑤ 종목→업종 매핑 (KIS 마스터)
  ⑥ 거래대금 필터 (20일 평균 10억 이상)
  ⑦ 벡터화 신호 계산 (15개 지표)
  ⑧ 수급 보강
  → pickle 캐시 (~110MB)

Phase 2 (캐시 로드 후):
  → 캐시 로드 ~5초
  → DAY 조합당 ~0.3ms
  → SWING 조합당 ~0.3ms
```

### 5.3 Grid Search 파라미터

| 파라미터 | DAY 탐색 범위 | SWING 탐색 범위 |
|---------|-------------|----------------|
| invest_min_score | 10~14 (5) | 9~13 (5) |
| atr_sl_mult | 1.5~3.0 (4) | 1.5~3.0 (4) |
| atr_tp_mult | 1.5~3.0 (4) | 1.5~3.0 (4) |
| max_hold_days_rev | 0~2 (3) | 3,5,7,10 (4) |
| max_hold_days_mix | 0~2 (3) | 2,3,5 (3) |
| max_hold_days_mom | 0~1 (2) | 1,2,3 (3) |
| tp1_mult | 0.5~1.5 (4) | 0.7~1.5 (3) |
| tp1_ratio | 0.3~1.0 (4) | 0.3~0.7 (3) |
| gap_down_limit | 0~5% (3) | 0~5% (3) |
| sector_penalty_threshold | -3%,-5% (2) | -3%,-5% (2) |
| sector_penalty_pts | 0,-2 (2) | 0,-2 (2) |
| sector_bonus_pts | 0,+1 (2) | 0,+1 (2) |

### 5.4 최적화 지표

```
metric = total_ret × min(1.0, trades / 100)
```
- 거래 30건 미만 → 패널티 (-9999)
- 총수익률이 높되 거래 수도 충분한 조합 우선

---

## 6. 시뮬레이터 (simulator.py)

### 6.1 보수적 트레일링 SL

일봉에서 당일 high/low 선후를 알 수 없으므로, **전일까지의 high만으로 SL 갱신**:

```python
prev_high = 매수일 high
for d in 보유기간:
    if d != 매수일:
        trail_sl = floor_tick(prev_high - ATR × SL_MULT)
        if trail_sl > sl: sl = trail_sl
    # 손절/TP 체크
    prev_high = max(prev_high, 당일 high)  # 봉 종료 후 갱신
```

### 6.2 승률 계산 (_report.py)

```python
wins = df_ok[df_ok["result_type"].str.startswith("익절")]  # "익절" + "익절(전량)"
```

---

## 7. CLI 사용법

```bash
# ── 트레이더 실행
uv run python -m wye.blsh

# ── PO 파일 발행
uv run python -m wye.blsh po              # 자동 판단
uv run python -m wye.blsh po pre          # 전일 스캔 (다음 영업일용)
uv run python -m wye.blsh po final        # 오후 스캔 (청산 후 매수)

# ── 최적화
uv run python -m wye.blsh.domestic.optimize.grid_search
uv run python -m wye.blsh.domestic.optimize.grid_search --mode DAY
uv run python -m wye.blsh.domestic.optimize.grid_search --no-sector
uv run python -m wye.blsh.domestic.optimize.grid_search --rebuild --years 3
```

### crontab 예시

```bash
# 전일 스캔 → pre po
30 18 * * 1-5  cd ~/workspace/blsh && uv run python -m wye.blsh po pre

# 오전 스캔 → regular po
00 10 * * 1-5  cd ~/workspace/blsh && uv run python -m wye.blsh po

# 오후 스캔 → final po
00 15 * * 1-5  cd ~/workspace/blsh && uv run python -m wye.blsh po final

# trader 실행 (08:00 전 시작 → 내부에서 08:00 대기)
50 07 * * 1-5  cd ~/workspace/blsh && uv run python -m wye.blsh
```

---

## 8. DB 스키마 (trade_history)

```sql
CREATE TABLE trade_history (
    id          SERIAL PRIMARY KEY,
    side        VARCHAR(4) NOT NULL,          -- buy / sell
    ticker      VARCHAR(20) NOT NULL,
    name        VARCHAR(100),
    qty         INTEGER,
    price       NUMERIC,
    reason      VARCHAR(200),                 -- 손절/1차익절/만기청산 등
    po_type     VARCHAR(10),                  -- pre / morning / final
    traded_at   TIMESTAMP DEFAULT now()
);

-- 마이그레이션 (기존 테이블에 컬럼 추가)
ALTER TABLE trade_history ADD COLUMN IF NOT EXISTS po_type VARCHAR(10);
```
