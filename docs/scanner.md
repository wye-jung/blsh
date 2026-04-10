# Scanner — 신호 생성 4단계

> `src/wye/blsh/domestic/scanner.py`

## 0단계: 시장 필터

- 최근 20일 평균 거래대금 >= 10억 (`TRDVAL_MIN`)
- KOSPI/KOSDAQ 대상 (ETF는 `SCAN_ETF=true` 환경변수로 활성화)

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
| BE | Bullish Engulfing | 반전 | 2 | 전일 음봉을 오늘 양봉이 완전히 감싸는 형태 |
| OBV | OBV Uptrend | 중립 | 2 | 3일 연속 OBV 증가 |

점수는 `config.SIGNAL_SCORES`에 정의. `grid_search`가 자동 갱신.

### 신호 모드 분류

- **MOM** (추세추종): 모멘텀 플래그 >= 2개 & 반전보다 많을 때. 점수 = 모멘텀 + 중립
- **REV** (반전): 반전 플래그 >= 2개 & 모멘텀보다 많을 때. 점수 = 반전 + 중립
- **MIX** (혼합): 양쪽 모두 있을 때. 점수 = max(모멘텀, 반전) + 중립 → **PO 대상에서 제외** (아래 참고)
- **WEAK**: 그 외. PO 대상에서 제외

> **MIX 모드 제외 (2026-04-04)**
>
> 2년 백테스트 분석 결과 MIX 모드가 전체 성과를 끌어내리는 것으로 확인:
> - MIX: 51건, 승률 37.3%, avg +0.76% (전체 avg +2.23% 대비 현저히 낮음)
> - MIX+MGC 조합: 9건, avg -4.48% (독성 조합)
> - 같은 MGC가 REV에서는 12건, 승률 91.7%, avg +7.05% (모드에 따라 극단적 차이)
>
> 전환+모멘텀 혼합 신호는 방향이 불확실하여 수익성이 낮음.
> MIX 제거 시: 승률 +0.5%p, avg_ret +0.10%p 개선, 총수익 2% 미만 손실.
>
> `find_candidates()`에서만 제외하고 `grid_search` 백테스트에는 MIX 포함 유지.
> MIX 성과가 개선되면 `scanner.py` 1줄 복원으로 재활성화 가능.
> 진단 도구: `uv run python -m wye.blsh.domestic.optimize.diag_market`

### 진입/손절/익절가

```
entry_price = ceil_tick(close + 0.5 x ATR)
SL          = floor_tick(close - ATR_SL_MULT x ATR)
TP1         = ceil_tick(close + TP1_MULT x ATR)
TP2         = ceil_tick(close + ATR_TP_MULT x ATR)
```

## 2단계: 수급 보정 (기술 점수 >= ENRICH_SCORE인 종목만)

DB(`isu_ksp_info`/`isu_ksd_info`) 또는 KIS API fallback으로 5일 투자자별 매매동향 조회.

| 플래그 | 조건 | 점수 |
|--------|------|------|
| F_TRN / I_TRN | N일 매도/0 -> 오늘 매수 전환 | +3 |
| F_C3 / I_C3 | 3일 이상 연속 매수 | +2 |
| F_1 / I_1 | 오늘만 매수 | +1 |
| FI | 외국인+기관 동반 매수 | +1 |
| P_OV | 개인만 대량 매수 (기관/외국인 없음) | -1 |

수급 가산 상한 = **+3** (`SUPPLY_CAP = 3`, 백테스트 검증)

## 3단계: PO 파일 생성

최종 점수 >= `INVEST_MIN_SCORE`, 모드 in {MOM, REV}, P_OV 미포함 종목을 JSON 저장.

| 파일 | 스캔 시점 | 매수 시점 | 배분 |
|------|----------|----------|------|
| `po-{date}-pre.json` | 전일 확정 일봉 | 08:00 NXT 지정가 | 30% |
| `po-{date}-ini.json` | 장중 (~11:30) | ~11:35 KRX 지정가 | 15% |
| `po-{date}-fin.json` | 청산 후 (~15:05) | 15:15 KRX/NXT 지정가 | 55% x 90% |
