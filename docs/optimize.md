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
metric = total_return x min(1.0, trades / 100)
```
거래 수 < 30건이면 페널티 적용 (통계적 유의성).

### 사용법

```bash
uv run python -m wye.blsh.domestic.optimize.grid_search                 # 기본 (최근 2년)
uv run python -m wye.blsh.domestic.optimize.grid_search --years 3       # 최근 3년
uv run python -m wye.blsh.domestic.optimize.grid_search --rebuild       # 캐시 강제 재빌드
uv run python -m wye.blsh.domestic.optimize.grid_search --alternating   # Stage1->2 교대 수행
```

결과는 `config.py`의 `Optimized` 클래스와 `SIGNAL_SCORES`에 자동 기록.

크론: 매주 토요일 02:00 `--alternating` 모드로 실행.

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
