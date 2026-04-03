# Optimize — 파라미터 최적화

> `src/wye/blsh/domestic/optimize/`

## grid_search.py

2단계 그리드 서치로 `config.py`의 `Optimized` 클래스와 `SIGNAL_SCORES`를 자동 갱신.

### Stage 1A: 고점수 신호 최적화

- 대상: MGC, W52, PB, LB, MS, RBO
- 각 0~3점 범위, 4^6 = 4,096 조합

### Stage 1B: 저점수 신호 최적화

- 대상: MPGC, ROV, BBL, BBM, VS, MAA, SGC, HMR, OBV
- 각 0~2점 범위, 3^9 = 19,683 조합

### Stage 2: 포지션/리스크 최적화

- invest_min_score, atr_sl_mult, atr_tp_mult, tp1_mult, tp1_ratio
- max_hold_days (REV/MIX/MOM)
- sector_penalty/bonus (threshold, pts)

### 최적화 지표

```
metric = avg_ret x sqrt(trades)
```
신호 품질(평균 수익률)을 우선하면서 거래 수가 적으면 sqrt로 자연스럽게 불이익.
30건 미만은 통계 무의미로 제외 (-9999).

### GRID 범위

| 파라미터 | 탐색 범위 |
|---------|----------|
| invest_min_score | 9, 10, 11, 12, 13 |
| atr_sl_mult | 1.0 ~ 4.0 (0.5 step) |
| atr_tp_mult | 1.5, 2.0, 2.5, 3.0, 4.0, 5.0 |
| max_hold_days_rev | 3, 5, 7, 10, 15, 20 |
| max_hold_days_mix | 2, 3, 5, 7, 10 |
| max_hold_days_mom | 1, 2, 3 |
| tp1_mult | 0.7, 1.0, 1.5 |
| tp1_ratio | 0.3, 0.5, 0.7, 1.0 |

최적 파라미터가 GRID 경계값에 도달하면 `[BOUNDARY]` 경고 출력.

### 사용법

```bash
uv run python -m wye.blsh.domestic.optimize.grid_search                 # 기본 (최근 2년)
uv run python -m wye.blsh.domestic.optimize.grid_search --years 3       # 최근 3년
uv run python -m wye.blsh.domestic.optimize.grid_search --rebuild       # 캐시 강제 재빌드
uv run python -m wye.blsh.domestic.optimize.grid_search --alternating   # Stage1->2 교대 수행
```

결과는 `config.py`의 `Optimized` 클래스와 `SIGNAL_SCORES`에 자동 기록.

크론: 매주 토요일 02:00 `--alternating` 모드로 실행.

## Walk-Forward 검증

과적합 방지를 위한 롤링 윈도우 검증. 기존 최적화와 독립적으로 실행.

- 전체 기간 캐시 1개를 빌드, 날짜 필터로 train/val 분리
- 각 train 윈도우에서 Stage 2(매매 파라미터)만 최적화
- val 윈도우에서 backtest → train 대비 avg_ret 비율로 과적합 판정
- val_avg_ret / train_avg_ret < 50% → OVERFIT 경고

```bash
uv run python -m wye.blsh.domestic.optimize.grid_search --walkforward                 # 기본 (18개월 train + 6개월 val)
uv run python -m wye.blsh.domestic.optimize.grid_search --walkforward --train-months 12 --val-months 6
uv run python -m wye.blsh.domestic.optimize.grid_search --walkforward --step-months 6  # 6개월 간격 롤링
```

`--alternating`과 `--walkforward`는 상호 배타.

## signal_analysis.py

플래그별 성과 분석:
- Step 1: 각 플래그의 승률, 손절률, loss_bias 계산
- Step 2: 손실 편향 높은 플래그를 제거하고 백테스트 재실행 -> 개선 여부 확인

```bash
uv run python -m wye.blsh.domestic.optimize.signal_analysis
```

## supply_cap_test.py

수급 가산 상한 비교 백테스트:
- 상한 없음 / +3 / +2 등 다양한 캡으로 성과 비교
- 현재 결과: `SUPPLY_CAP = 3`이 최적

```bash
uv run python -m wye.blsh.domestic.optimize.supply_cap_test
```

## _cache.py

백테스트용 신호/OHLCV 캐시 빌더:
- 수급 점수 상한(`SUPPLY_CAP`) 적용 포함
- `--rebuild` 없이도 캐시 범위 불일치(5일 초과) 시 자동 재빌드
- 업종 gap 계산 시 `idx_clss` 필터 적용 (KOSPI="02", KOSDAQ="03")

## Simulator (_sim_core.py + simulator.py)

`trader.py`의 SL/TP 로직을 일봉으로 재현. numba JIT + numpy 배열로 고속 처리.

보수적 처리:
- 트레일링 SL: **전일** 고가 기준으로만 갱신 (당일 고->저 순서 불명)
- TP1 + 본전 SL 동일 봉: TP1 체결 후 잔량 본전 청산 (가장 불리한 시나리오)

`_sim_core.py`와 `grid_search` 내부 시뮬레이션은 동일 로직을 병행 유지해야 함.
