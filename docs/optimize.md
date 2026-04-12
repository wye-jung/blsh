# Optimize — 파라미터 최적화

> `src/wye/blsh/domestic/optimize/`

## grid_search.py

모드별 분리 그리드 서치로 `config.py`의 `Optimized` 클래스(`SIGNAL_SCORES_MOM` + `SIGNAL_SCORES_REV`)를 자동 갱신.

### Stage 1-MOM: MOM 점수 최적화 (mode_filter=6: MOM+MIX만)

- MOM-고: MGC, W52, PB, LB × [-1,0,1,2,3] → 5^4 = 625 조합
- MOM-저+NEU: VS[-1,0,1,2], MAA[0,1,2], OBV[0,1,2], MPGC[0,1,2], BBM[0,1,2], SGC[0,1,2] → 4 x 3^5 = 972 조합

### Stage 1-REV: REV 점수 최적화 (mode_filter=3: REV+MIX만)

- REV: MS, RBO, ROV, BBL, HMR, BE × [-1,0,1,2,3] → 5^6 = 15,625 조합
- REV-NEU: MPGC[0,1,2], BBM[0,1,2], SGC[0,1,2] → 3^3 = 27 조합

총 Stage 1: 17,249 조합. mode_filter로 각 모드의 독립적 최적값을 탐색.

### Stage 2: 포지션/리스크 최적화

- invest_min_score, atr_sl_mult, atr_tp_mult, tp1_mult, tp1_ratio
- max_hold_days (REV/MIX/MOM)

### 최적화 지표

```
metric = (weighted_avg_ret / weighted_std) x sqrt(min(trades, MAX_TRADES))
```
시간 가중 Sharpe 지표. half-life=120 거래일로 최근 거래에 높은 가중치 부여.
30건 미만은 통계 무의미로 제외 (-9999). std=0이면 `weighted_avg_ret x sqrt(min(trades, MAX_TRADES))` fallback.

**`MAX_TRADES = 2000`**: 거래 수 보상 cap. 2년 484일 기준 일 4건 수준에서 포화시켜, 옵티마이저가 플래그 가중치를 인플레이션시켜 거래 수를 늘리는 방향으로 치우치지 않도록 방지. Sharpe(avg/std)는 그대로 최적화되므로 거래 품질 최적화는 손상 없음.

### GRID 범위

| 파라미터 | 탐색 범위 |
|---------|----------|
| invest_min_score | 9, 10, 11, 12, 13 |
| atr_sl_mult | 1.0 ~ 4.0 (0.5 step) |
| atr_tp_mult | 1.5, 2.0, 2.5, 3.0, 4.0, 5.0 |
| max_hold_days_rev | 3, 5, 7, 10, 15, 20 |
| max_hold_days_mix | 2, 3, 5, 7, 10 |
| max_hold_days_mom | 1, 2, 3 |
| tp1_mult | 0.7, 1.0, 1.5, 2.0, 2.5 |
| tp1_ratio | 0.3, 0.5, 0.7, 1.0 |
| index_drop_limit | 0.03, 0.05, 0.10, 1.0 (1.0 = 비활성) |
| atr_cap | 0.03, 0.05, 0.08, 0.50 (0.50 = 비활성) |

최적 파라미터가 GRID 경계값에 도달하면 `[BOUNDARY]` 경고 출력.

R/R 최소 비율 제약: `_dedup_combos()`에서 R/R < 0.5인 조합을 필터링 (리스크 대비 보상이 너무 낮은 파라미터 제거).

### 사용법

```bash
uv run python -m wye.blsh.domestic.optimize.grid_search                 # 기본 (최근 2년)
uv run python -m wye.blsh.domestic.optimize.grid_search --years 3       # 최근 3년
uv run python -m wye.blsh.domestic.optimize.grid_search --rebuild       # 캐시 강제 재빌드
uv run python -m wye.blsh.domestic.optimize.grid_search --alternating   # Stage1->2 교대 수행
```

결과는 `config.py`의 `Optimized` 클래스(`SIGNAL_SCORES` 포함)에 자동 기록.

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
uv run python -m wye.blsh.domestic.optimize.grid_search --walkforward --detail         # val 윈도우별 모드/시장/플래그 상세 분석
```

`--detail`: 각 validation 윈도우에서 모드별(MOM/REV), 시장별(KOSPI/KOSDAQ), 플래그별 성과를 상세 출력.

`--alternating`과 `--walkforward`는 상호 배타.

### 주간 크론 운영 흐름

매주 토요일 02:00에 순차 실행:
1. `--alternating` → config.py 파라미터 갱신 (~35분)
2. `--walkforward --years 3` → 갱신된 파라미터 OOS 검증 + 텔레그램 리포트 (~60분)

### 과적합 경고 수신 시 운영자 대응 절차

텔레그램에 `🚨 과적합 의심: W4, W5` 등의 경고가 오면:

1. **즉시 확인**: 어떤 윈도우(최근 vs 과거)에서 경고가 발생했는지 확인
   - 최근 윈도우(W4-5)만 경고 → 시장 체제 변화 가능성
   - 전체 윈도우 경고 → 전략 구조적 문제

2. **상세 진단 실행**:
   ```bash
   # 경고 윈도우 기간의 모드/플래그별 성과 분석
   uv run python -m wye.blsh.domestic.optimize.diag_market --start 20250701 --end 20260401
   
   # WF 상세 분석 (윈도우별 모드/시장/플래그 분포)
   uv run python -m wye.blsh.domestic.optimize.grid_search --walkforward --years 3 --detail
   ```

3. **판단 기준에 따라 조치**:
   - **평균 비율 80%+ & 최근 1개 윈도우만 경고** → 정상 운영 유지. 다음 주 결과 모니터링
   - **평균 비율 50~80% & 최근 2개 윈도우 경고** → 매매 규모 축소 검토 (PRE/INI/FIN_CASH_RATIO 하향)
   - **평균 비율 50% 미만 또는 최근 Val이 마이너스** → 매매 일시 중단, 전략 재검토
   - **config.py 롤백이 필요하다고 판단되면**: `git log --oneline -10` → 이전 커밋의 config.py 복원

4. **롤백 방법** (필요 시):
   ```bash
   # 이전 config.py 복원
   git checkout HEAD~1 -- src/wye/blsh/domestic/config.py
   # 또는 특정 커밋의 config.py 복원
   git checkout <commit-hash> -- src/wye/blsh/domestic/config.py
   ```

5. **기록**: 경고 내용과 조치 사항을 기록하여 패턴 축적

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

## Simulator (_sim_core.py + simulator.py)

`trader.py`의 SL/TP 로직을 일봉으로 재현. numba JIT + numpy 배열로 고속 처리.

보수적 처리:
- 트레일링 SL: **전일** 고가 기준으로만 갱신 (당일 고->저 순서 불명)
- TP1 + 본전 SL 동일 봉: TP1 체결 후 잔량 본전 청산 (가장 불리한 시나리오)

`_sim_core.py`와 `grid_search` 내부 시뮬레이션은 동일 로직을 병행 유지해야 함.

### backtest_with_trades()

개별 TradeRecord 리스트를 반환하는 진단 함수. 각 거래의 진입/청산 정보를 상세히 확인할 때 사용.

## diag_market.py

시장별(KOSPI/KOSDAQ) + 모드별(MOM/MIX/REV) 백테스트 성과 비교 진단 도구.

```bash
uv run python -m wye.blsh.domestic.optimize.diag_market
uv run python -m wye.blsh.domestic.optimize.diag_market --start 20250101 --end 20251231  # 기간 필터
```

## 백테스트 기간별 성과 비교 (2026-04-04)

MIX 모드 제거 + SIGNAL_SCORES 재최적화 후, `--alternating --rebuild`로 실행.

| | 6개월 | 1년 | 2년 | 3년 |
|---|---|---|---|---|
| 거래 | 174건 | 413건 | 845건 | 1,449건 |
| 승률 | 61.5% | 64.4% | 51.6% | 60.7% |
| 평균수익 | +4.09% | +3.83% | +2.53% | +1.68% |
| 총수익 | +711% | +1,581% | +2,135% | +2,430% |
| ATR_SL_MULT | 3.5 | 3.5 | 3.5 | 4.0 |
| MAX_HOLD_DAYS | 15 | 15 | 10 | 20 |

관찰:
- 최근일수록 평균수익이 높음 (최근 시장 환경이 전략에 유리)
- SL 3.5가 최근 6개월~2년에서 일관적, 3년에서만 4.0으로 넓어짐
- 3년은 초기 저조 기간을 보상하기 위해 보수적 파라미터(넓은 SL, 긴 보유)로 수렴
- 6개월/1년은 거래 수가 적어 과적합 위험, 3년은 과거 시장 특성이 희석

**결론: 2년이 최근 시장 특성 반영과 데이터 충분성의 균형점. 기본값으로 채택.**

## Known limitation: 캐시 entry_price / flat_supply_bonus pre-bake

`_cache.py`는 신호 빌드 시점의 모듈 상수 `ATR_CAP` / `SUPPLY_CAP`으로 `entry_price`([L749](../src/wye/blsh/domestic/optimize/_cache.py#L749))와 `flat_supply_bonus`([L518](../src/wye/blsh/domestic/optimize/_cache.py#L518))를 pre-compute해 저장한다.

이 때문에 `grid_search`가 `params.atr_cap` 또는 장래 `supply_cap`을 탐색할 때 SL/TP는 후보 값으로 재계산되지만, **entry_price 게이트**(시가 > entry_price면 스킵)는 캐시 빌드 당시 값으로 고정. 탐색 공간과 캐시가 어긋나는 만큼 최적화 결과가 편향된다.

**임시 조치**: 매주 토요일 cron에 `--rebuild` 강제 (`bin/setup_cron.sh`). 매 실행마다 현재 config.py 기준으로 캐시를 새로 빌드하므로, 탐색이 캐시 빌드 값 근처에서만 유효해도 최종 수렴값은 일관됨. 현재 최적값과 캐시 빌드 값이 일치하므로 실전 괴리는 없음.

**근본 수정 (deferred)**:
- `_cache.py`가 `raw_atr`, `close_val`, `raw_supply_bonus`만 저장
- `entry_price` / `effective_supply_bonus` 계산을 `_sim_core` / `grid_search` 런타임으로 이동
- 각 후보 `params.atr_cap` / `params.supply_cap`로 게이트 재평가
- 회귀 검증: 기존 WF OOS 평균 112%가 유지되는지 확인. 수치가 크게 흔들리면 편향이 실제로 컸다는 뜻 → 실전 파라미터 재검토 필요

**착수 조건**: 실전 운영 1~2주간 일일 리포트에 이상 없고, 지표가 백테스트 수준과 크게 괴리되지 않음을 확인한 후.
