# 패턴 보너스 선별 로직

## 개요

과거 1년간 **"3영업일 후 수익 ≥5%"** 인 종목의 기술 플래그 조합을 모드별로 채굴하여,
스캐너 실행 시 동일한 조합이 발견되면 가산점을 부여하는 보조 선별 메커니즘.

기존 점수 체계(기술 점수 → 수급 보정 → 업종 패널티) 에 **마지막 단계**로 추가되어,
역사적으로 수익이 검증된 플래그 조합을 가진 종목이 진입 최소 점수(`INVEST_MIN_SCORE`)를 더 쉽게 통과하도록 돕는다.

---

## 로직 상세

### 1단계 — 패턴 채굴 (offline, `pattern_mine.py`)

```
for scan_date in OptCache.scan_dates:
    date_3d = forward_dates[scan_date][3]   ← scan_date 기준 3영업일 후

    for 각 신호:
        수익률 = ohlcv[ticker][date_3d]["close"] / entry_price - 1
        tech_flags = flags - 수급플래그           ← 기술 플래그만 추출
        total[mode][tech_flags] += 1
        if 수익률 ≥ threshold:
            wins[mode][tech_flags] += 1

패턴 확정: wins ≥ min_count 인 조합만 유지
정렬: count 내림차순 → win_rate 내림차순
```

**forward_dates 인덱싱 주의:**
- `forward_dates[scan_date][0]` = scan_date 당일
- `forward_dates[scan_date][1]` = 1영업일 후 (entry_date)
- `forward_dates[scan_date][3]` = 3영업일 후 (수익 측정 시점)

**entry_price:** `Tick.ceil_tick(close × 1 + 0.5 × ATR)` — 다음날 예상 매수가.
수익률 기준점이 scan_date 종가가 아닌 entry_price이므로 갭 상승 비용이 이미 반영됨.

**수급 플래그 제거:** 채굴 시 `F_TRN, I_TRN, F_C3, I_C3, F_1, I_1, FI, P_OV` 를 제외하고 기술 플래그만 패턴에 저장한다. 런타임 매칭 시에는 제거 불필요 — subset 체크가 올바르게 동작함.

**WEAK 모드 제외:** `mode == "WEAK"` 신호는 채굴 및 매칭에서 제외.

---

### 2단계 — 런타임 적용 (`scanner.py`, `_cache.py`)

```
스캔 결과에서 (mode, buy_flags) 조회
→ 해당 mode의 저장된 패턴 목록 순회
→ pattern ⊆ buy_flags 인 패턴 수 카운트 (subset 매칭)
→ bonus = min(매칭 패턴 수, PATTERN_BONUS_MAX=2)
→ buy_score += bonus
```

**적용 시점 (scanner.py):** `_apply_sector_penalty()` 이후, `INVEST_MIN_SCORE` 필터 이전.

**적용 시점 (_cache.py):** P_OV 패널티 이후, ATR 계산 이전 — `grid_search` 시뮬레이션에도 반영.

**패턴 파일 없을 경우:** 경고 로그 1회 출력, `pat_bonus=0` 으로 정상 동작 (graceful degradation).

---

## 관련 파일

| 파일 | 역할 |
|---|---|
| `src/wye/blsh/domestic/optimize/pattern_mine.py` | 패턴 채굴 모듈 (CLI) |
| `src/wye/blsh/domestic/scanner.py` | `_load_patterns()`, `_apply_pattern_bonus()` |
| `src/wye/blsh/domestic/optimize/_cache.py` | `_get_cache_patterns()`, 캐시 빌드 루프 |
| `src/wye/blsh/domestic/config.py` | `PATTERN_BONUS_MAX = 2` |
| `~/.blsh/data/flag_patterns.json` | 채굴 결과 저장 (런타임 로드) |

---

## 명령 사용 순서

### 최초 설정 (패턴 파일 최초 생성)

```bash
# 1. 패턴 채굴 (OptCache 없으면 자동 빌드 ~4분, 있으면 ~1초)
uv run python -m wye.blsh.domestic.optimize.pattern_mine --years 1 --min-count 5

# 2. (선택) grid_search 에도 패턴 보너스 반영하려면 캐시 재빌드
uv run python -m wye.blsh.domestic.optimize.grid_search --rebuild

# 3. scanner는 다음 실행부터 자동 로드
uv run python -m wye.blsh
```

### 정기 갱신 (월 1회 권장)

```bash
# 최신 데이터로 패턴 재채굴 (캐시가 최신이면 ~1초)
uv run python -m wye.blsh.domestic.optimize.pattern_mine --years 1 --min-count 5

# 이후 grid_search 재실행으로 최적 파라미터도 함께 갱신
uv run python -m wye.blsh.domestic.optimize.grid_search
```

### 주요 옵션

```bash
uv run python -m wye.blsh.domestic.optimize.pattern_mine \
  --years 1          # 분석 기간 (기본 1년)
  --min-count 5      # 패턴 최소 승자 수 (기본 5, 낮출수록 노이즈 증가)
  --threshold 0.05   # 수익 기준점 (기본 5%)
  --train-end 20251231  # 룩어헤드 방지: 이 날짜까지만 채굴
  --rebuild          # OptCache 강제 재빌드
  --out /path/to/custom.json  # 출력 파일 지정
```

---

## 실행 결과 (2025-03-31 ~ 2026-03-31, min_count=5)

**총 143개 패턴** — MOM 114개, MIX 20개, REV 9개

### MOM 모드 상위 패턴 (승자 수 기준)

| 패턴 | 승/전 | 승률 | 평균점 | 비고 |
|---|---|---|---|---|
| OBV+PB | 1111/9812 | 11.3% | 4.7 | 빈도 최다 |
| OBV+VS | 405/2885 | 14.0% | 3.5 | |
| OBV+PB+VS | 258/2001 | 12.9% | 5.5 | |
| OBV+VS+W52 | 201/1117 | 18.0% | 6.6 | |
| OBV+PB+VS+W52 | 174/947 | 18.4% | 8.5 | |
| LB+OBV+VS+W52 | 76/367 | 20.7% | 8.9 | |
| **LB+OBV+PB+VS+W52** | **70/303** | **23.1%** | **10.9** | 승률 최고 (5개 조합) |
| LB+VS+W52 | 30/132 | 22.7% | 7.9 | |

### MIX 모드 상위 패턴

| 패턴 | 승/전 | 승률 | 비고 |
|---|---|---|---|
| HMR+OBV | 92/899 | 10.2% | 빈도 최다 |
| BBL+OBV | 67/937 | 7.1% | |
| OBV+ROV | 28/203 | 13.8% | |
| **RBO+SGC+VS** | **6/23** | **26.1%** | 승률 최고 |
| BBL+BBM+SGC+VS | 6/24 | 25.0% | |
| BBM+MS+PB | 5/25 | 20.0% | |

### REV 모드 상위 패턴

| 패턴 | 승/전 | 승률 | 비고 |
|---|---|---|---|
| BBL+RBO+SGC | 31/419 | 7.4% | 빈도 최다 |
| BBL+ROV | 30/594 | 5.1% | |
| **BBL+RBO+SGC+VS** | **5/26** | **19.2%** | 승률 최고 |
| BBL+HMR+SGC | 9/65 | 13.9% | |

---

## 패턴 해석

**OBV+PB 조합이 MOM에서 압도적으로 많은 이유:**
- PB(눌림목)는 MA20 상승 중 조정 후 복귀 패턴 → OBV(거래량 추세 상승)와 결합 시 추세 재개 신호
- 발생 빈도가 높아 모수가 크고 절대 승자 수도 가장 많음

**W52(52주 신고가) 조합의 높은 승률:**
- 신고가 돌파 자체가 강한 모멘텀 신호이며, OBV·LB·VS와 결합 시 20% 이상 승률
- `LB+OBV+PB+VS+W52`: 5개 플래그 동시 발생 → 신호 강도 최상, 발생 빈도는 낮음

**REV 모드의 전반적으로 낮은 승률 (3~19%):**
- 반전 신호는 본질적으로 낮은 적중률 — 손절 위주의 리스크 관리가 중요
- `BBL+RBO+SGC+VS` (19.2%), `BBL+HMR+SGC` (13.9%) 정도만 참고 수준으로 유효

**평균점 1.0 패턴 주의:**
- MIX의 `HMR+OBV`, `BBL+OBV` 등은 평균 점수가 1.0 → 수급 보강 없이는 `INVEST_MIN_SCORE(9)` 를 달성 불가
- 실제로 이 종목들은 패턴 보너스 +2 를 받아도 점수 부족으로 대부분 필터링됨
- 이 패턴들이 채굴된 이유: 채굴은 **INVEST_MIN_SCORE 미만 신호도 포함**하기 때문

---

## 유의사항

### ⚠️ 룩어헤드 바이어스 (가장 중요)

현재 `flag_patterns.json` 은 **`--train-end` 없이 생성**되었으므로, 채굴 기간과 grid_search 백테스트 기간이 동일하다.

이 상태에서 grid_search 를 실행하면 **미래 데이터로 학습된 패턴을 과거 시뮬레이션에 적용**하게 되어 성과가 부풀려진다.

**현실적 운용 방안:**

```bash
# 실전 운용 시 패턴은 3~6개월 이전 데이터까지만 채굴
uv run python -m wye.blsh.domestic.optimize.pattern_mine \
  --years 1 \
  --train-end 20251231   # 3개월 전까지만 채굴
```

현재 파일(룩어헤드 포함)은 **scanner 실시간 선별 가산점 참고용으로만** 사용하고,
grid_search 재실행 전에는 `--train-end` 를 지정하여 재채굴할 것을 권장.

### 패턴 수 해석

- **min_count=5** 는 관대한 기준 — 1년(약 250거래일)에 5번 등장한 것
- 승자 수 기준이므로, 같은 5건이라도 total=10 (승률50%)과 total=500 (승률1%)은 의미가 다름
- `count/total` 과 `win_rate` 를 함께 보고, 모수(total)가 너무 작은 패턴(< 20건)은 과잉 적합 의심

### 패턴 보너스의 한계

- 최대 +2점 (PATTERN_BONUS_MAX) → INVEST_MIN_SCORE=9 기준으로 7점 종목도 후보로 올라올 수 있음
- **패턴 보너스는 "발견 확률"을 높이는 것이지 수익을 보장하지 않음**
- 실제 승률은 10~23% 수준 — SL/TP 리스크 관리가 여전히 핵심

### 갱신 주기

| 이벤트 | 권장 조치 |
|---|---|
| 월 1회 정기 | `pattern_mine.py` 재실행 (최신 패턴 반영) |
| 시장 환경 급변 (급락/급등 장세) | 즉시 재채굴 — 기존 패턴이 다른 환경에서 학습됐을 수 있음 |
| grid_search 파라미터 변경 후 | OptCache `--rebuild` 후 재채굴 |
| `INVEST_MIN_SCORE` 변경 후 | 재채굴 불필요 — 채굴은 모든 점수 신호 대상 |

### 롤백

이 기능 도입 전 상태로 되돌리려면:

```bash
git reset --hard ef2cc9b
```

또는 패턴 파일만 삭제하면 패턴 보너스 비활성 (코드 변경 없이 비활성화):

```bash
rm ~/.blsh/data/flag_patterns.json
```
