# Scanner — 신호 생성 4단계

> `src/wye/blsh/domestic/scanner.py`

## 0단계: 시장 필터

- 최근 20일 평균 거래대금 >= 10억 (`TRDVAL_MIN`)
- KOSPI/KOSDAQ만 대상

## 1단계: 기술 점수 (OHLCV -> 15개 플래그)

| 플래그 | 이름 | 카테고리 | 현재 점수 | 조건 |
|--------|------|----------|-----------|------|
| MGC | MACD Golden Cross | 모멘텀 | 1 | MACD > Signal (전일 <) |
| MPGC | MACD Predicted GC | 중립 | 1 | 히스토그램 상승, 양쪽 <0, 근접 |
| RBO | RSI Breakout 30 | 반전 | 3 | RSI가 30 상향 돌파 |
| ROV | RSI Oversold | 반전 | 2 | RSI < 30 |
| BBL | Bollinger Bounce | 반전 | 2 | 전일 저가 < 하단밴드, 오늘 종가 > 하단 |
| BBM | BB Middle Cross | 중립 | 2 | 종가가 중간밴드 상향 돌파 |
| VS | Volume Spike | 모멘텀 | 2 | 거래량 > 20일 평균 x 2 + 양봉 |
| MAA | MA Alignment | 모멘텀 | 0 | 5MA > 20MA > 60MA (당일 신규) |
| SGC | Stoch Golden Cross | 중립 | 2 | K > D, 양쪽 <50, K 교차 |
| W52 | 52-week High | 모멘텀 | 3 | 52주 신고가 + 거래량 > 1.5x 평균 |
| PB | Pullback | 모멘텀 | 0 | MA20 상승 중 5MA까지 눌림 후 반등 |
| HMR | Hammer | 반전 | 2 | 아래꼬리 >50%, 윗꼬리 <10%, 몸통 <30% |
| LB | Large Bar | 모멘텀 | 0 | 양봉, 크기 > ATR x 1.5 |
| MS | Morning Star | 반전 | 2 | 3봉 패턴 (대음봉, 도지, 대양봉) |
| OBV | OBV Uptrend | 중립 | 2 | 3일 연속 OBV 증가 |

점수는 `config.SIGNAL_SCORES`에 정의. `grid_search`가 자동 갱신.

### 신호 모드 분류

- **MOM** (추세추종): 모멘텀 플래그 >= 2개 & 반전보다 많을 때. 점수 = 모멘텀 + 중립
- **REV** (반전): 반전 플래그 >= 2개 & 모멘텀보다 많을 때. 점수 = 반전 + 중립
- **MIX** (혼합): 양쪽 모두 있을 때. 점수 = max(모멘텀, 반전) + 중립
- **WEAK**: 그 외. PO 대상에서 제외

### 진입/손절/익절가

```
entry_price = ceil_tick(close + 0.5 x ATR)
SL          = floor_tick(close - ATR_SL_MULT x ATR)
TP1         = ceil_tick(close + TP1_MULT x ATR)
TP2         = ceil_tick(close + ATR_TP_MULT x ATR)
```

## 2단계: 수급 보정 (기술 점수 >= 2인 종목만)

DB(`isu_ksp_info`/`isu_ksd_info`) 또는 KIS API fallback으로 5일 투자자별 매매동향 조회.

| 플래그 | 조건 | 점수 |
|--------|------|------|
| F_TRN / I_TRN | N일 매도/0 -> 오늘 매수 전환 | +3 |
| F_C3 / I_C3 | 3일 이상 연속 매수 | +2 |
| F_1 / I_1 | 오늘만 매수 | +1 |
| FI | 외국인+기관 동반 매수 | +1 |
| P_OV | 개인만 대량 매수 (기관/외국인 없음) | -1 |

수급 가산 상한 = **+3** (`SUPPLY_CAP = 3`, 백테스트 검증)

## 3단계: 업종 환경

업종 지수 MA20 대비 괴리율(gap)로 보정:
- `gap < SECTOR_PENALTY_THRESHOLD` -> `+SECTOR_PENALTY_PTS` (패널티)
- `gap >= SECTOR_BONUS_THRESHOLD` -> `+SECTOR_BONUS_PTS` (보너스)

미매핑 종목은 KOSPI/KOSDAQ 지수를 fallback으로 사용.

DB `idx_stk_ohlcv` 테이블에서 `idx_clss` 필터 필수:
- `idx_clss = "02"` -> KOSPI 업종지수
- `idx_clss = "03"` -> KOSDAQ 업종지수

## 4단계: PO 파일 생성

최종 점수 >= `INVEST_MIN_SCORE`, 모드 in {MOM, MIX, REV}, P_OV 미포함 종목을 JSON 저장.

| 파일 | 스캔 시점 | 매수 시점 | 배분 |
|------|----------|----------|------|
| `po-{date}-pre.json` | 전일 확정 일봉 | 08:00 NXT 지정가 | 30% |
| `po-{date}-ini.json` | 장초반 (~10:05) | ~10:10 KRX 지정가 | 15% |
| `po-{date}-fin.json` | 청산 후 (~15:05) | 15:15 KRX/NXT 지정가 | 55% x 90% |
