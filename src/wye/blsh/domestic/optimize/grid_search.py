"""
Grid Search 최적화
──────────────────────────────────
데이트레이딩(DAY) / 스윙트레이딩(SWING) 최적 파라미터 탐색

실행:
    uv run python -m wye.blsh.domestic.optimize.grid_search
    uv run python -m wye.blsh.domestic.optimize.grid_search --mode DAY
    uv run python -m wye.blsh.domestic.optimize.grid_search --mode SWING
    uv run python -m wye.blsh.domestic.optimize.grid_search --years 2
    uv run python -m wye.blsh.domestic.optimize.grid_search --rebuild
"""

import argparse
import logging
import multiprocessing as mp
import os
import time
from dataclasses import dataclass
from itertools import product
from pathlib import Path

from wye.blsh.common import dtutils
from wye.blsh.domestic.optimize._cache import build_or_load, OptCache, CACHE_DIR

log = logging.getLogger(__name__)

# fork 방식으로 캐시를 자식 프로세스에 공유 (복사 없이 CoW)
_WORKER_CACHE: OptCache | None = None


def _backtest_worker(args: tuple) -> tuple["Params", "Stats"]:
    keys, combo = args
    p = Params(**dict(zip(keys, combo)))
    return p, backtest(_WORKER_CACHE, p)

SELL_COST_RATE = 0.002


# ─────────────────────────────────────────
# 파라미터 + 결과
# ─────────────────────────────────────────
@dataclass(frozen=True)
class Params:
    invest_min_score: int
    atr_sl_mult: float
    atr_tp_mult: float
    max_hold_days_rev: int
    max_hold_days_mix: int
    max_hold_days_mom: int
    tp1_mult: float                  # 1차 익절 ATR 배수 (e.g. 0.7, 1.0, 1.5)
    tp1_ratio: float                 # 1차 익절 매도 비율 (e.g. 0.3, 0.5, 0.7)
    gap_down_limit: float            # 갭하락 한계 (e.g. 0.03 = entry 대비 3% 이상 하락 시 스킵)
    sector_penalty_threshold: float  # 업종지수 MA20 괴리율 패널티 임계값 (e.g. -0.03)
    sector_penalty_pts: int          # 임계값 이하 시 점수 패널티 (e.g. -2)
    sector_bonus_pts: int            # 업종지수 MA20 이상일 때 보너스 (e.g. +1)

    def label(self) -> str:
        parts = []
        if self.sector_penalty_pts != 0:
            parts.append(f"pen={self.sector_penalty_threshold:.0%}/{self.sector_penalty_pts:+d}")
        if self.sector_bonus_pts != 0:
            parts.append(f"bon=+0%/{self.sector_bonus_pts:+d}")
        sec = ' '.join(parts) if parts else "sec=off"
        gap = f"gap={self.gap_down_limit:.0%}" if self.gap_down_limit > 0 else ""
        return (
            f"score≥{self.invest_min_score} "
            f"SL={self.atr_sl_mult:.1f} TP1={self.tp1_mult:.1f}({self.tp1_ratio:.0%}) "
            f"TP2={self.atr_tp_mult:.1f} "
            f"REV={self.max_hold_days_rev}d MIX={self.max_hold_days_mix}d "
            f"MOM={self.max_hold_days_mom}d {gap} {sec}".rstrip()
        )


@dataclass
class Stats:
    trades: int = 0
    wins: int = 0
    losses: int = 0
    holds: int = 0
    total_ret: float = 0.0

    @property
    def win_rate(self) -> float:
        return self.wins / self.trades * 100 if self.trades else 0

    @property
    def avg_ret(self) -> float:
        return self.total_ret / self.trades if self.trades else 0

    @property
    def metric(self) -> float:
        """최적화 지표: 총수익 × min(1, trades/100). 거래 30건 미만 패널티."""
        if self.trades < 30:
            return -9999
        return self.total_ret * min(1.0, self.trades / 100)


# ─────────────────────────────────────────
# 시뮬레이션 (1건)
# ─────────────────────────────────────────
def _simulate_one(
    sig: dict,
    entry_date: str,
    ohlcv_idx: dict,
    params: Params,
    hold_dates: list[str],
) -> tuple[str, float] | None:
    """1개 후보 시뮬레이션. (result_type, ret_pct) 반환, 스킵이면 None."""
    ticker = sig["ticker"]
    atr = sig["atr"]

    t1 = ohlcv_idx.get((ticker, entry_date))
    if t1 is None:
        return None

    # 갭 상승 → 매수 불가
    if t1["open"] > sig["entry_price"]:
        return None

    # 갭 하락 필터: entry 대비 gap_down_limit 이상 하락 시 스킵
    buy = t1["open"]
    if params.gap_down_limit > 0:
        gap_floor = sig["entry_price"] * (1 - params.gap_down_limit)
        if buy < gap_floor:
            return None

    sl = buy - params.atr_sl_mult * atr
    tp1 = buy + params.tp1_mult * atr
    tp2 = buy + params.atr_tp_mult * atr

    mode = sig["mode"]
    if mode == "MOM":
        max_d = params.max_hold_days_mom
    elif mode == "MIX":
        max_d = params.max_hold_days_mix
    else:
        max_d = params.max_hold_days_rev

    # 보유 기간 날짜
    dates = [d for d in hold_dates if d >= entry_date][: max_d + 1]
    if not dates:
        return None

    remaining = 1.0
    pnl = 0.0
    t1_done = False
    result_type = None
    prev_high = t1["high"]
    last_close = t1["close"]

    for d in dates:
        ohv = ohlcv_idx.get((ticker, d))
        if ohv is None:
            continue
        last_close = ohv["close"]

        # 트레일링 SL (전일 high 기준 — 보수적)
        if d != dates[0]:
            trail = prev_high - params.atr_sl_mult * atr
            if trail > sl and trail < prev_high:
                sl = trail

        # 손절
        if ohv["low"] <= sl:
            pnl += (sl - buy) * remaining - sl * remaining * SELL_COST_RATE
            result_type = "손절"
            remaining = 0
            break

        # TP1 (분할 매도)
        if not t1_done and ohv["high"] >= tp1:
            sell_r = params.tp1_ratio
            pnl += (tp1 - buy) * sell_r - tp1 * sell_r * SELL_COST_RATE
            remaining -= sell_r
            t1_done = True
            if buy > sl:
                sl = buy

        prev_high = max(prev_high, ohv["high"])

        # TP2 (잔량)
        if ohv["high"] >= tp2 and remaining > 0:
            pnl += (tp2 - buy) * remaining - tp2 * remaining * SELL_COST_RATE
            result_type = "익절"
            remaining = 0
            break

    # 미확정 → 종가 청산
    if result_type is None:
        pnl += (last_close - buy) * remaining - last_close * remaining * SELL_COST_RATE
        result_type = "데이청산" if max_d == 0 else "미확정"

    ret = (pnl / buy) * 100 if buy else 0
    return result_type, ret


# ─────────────────────────────────────────
# 전체 기간 백테스트
# ─────────────────────────────────────────
def backtest(cache: OptCache, params: Params) -> Stats:
    """캐시 데이터로 전체 기간 백테스트."""
    st = Stats()

    for base_date in cache.scan_dates:
        entry_date = cache.next_biz.get(base_date)
        if not entry_date:
            continue

        sigs = cache.signals.get(base_date, [])
        hold_dates = cache.forward_dates.get(entry_date, [entry_date])

        for sig in sigs:
            # 업종 패널티/보너스 적용
            effective_score = sig["score"]
            sec_gap = sig.get("sector_gap", 0.0)
            if params.sector_penalty_pts != 0 and sec_gap < params.sector_penalty_threshold:
                effective_score += params.sector_penalty_pts
            elif params.sector_bonus_pts != 0 and sec_gap >= 0:
                effective_score += params.sector_bonus_pts

            if effective_score < params.invest_min_score:
                continue
            if sig["mode"] not in ("MOM", "MIX", "REV"):
                continue
            if "P_OV" in sig["flags"]:
                continue

            res = _simulate_one(sig, entry_date, cache.ohlcv_idx, params, hold_dates)
            if res is None:
                continue

            result_type, ret_pct = res
            st.trades += 1
            st.total_ret += ret_pct
            if result_type.startswith("익절"):
                st.wins += 1
            elif result_type == "손절":
                st.losses += 1
            else:
                st.holds += 1

    return st


# ─────────────────────────────────────────
# 그리드 정의
# ─────────────────────────────────────────
# DAY/초단기 모드: max_hold 0~2일 탐색 (0=당일청산, 1~2=오버나이트)
DAY_GRID = {
    "invest_min_score": [10, 11, 12, 13, 14],
    "atr_sl_mult": [1.5, 2.0, 2.5, 3.0],
    "atr_tp_mult": [1.5, 2.0, 2.5, 3.0],
    "max_hold_days_rev": [0, 1, 2],
    "max_hold_days_mix": [0, 1, 2],
    "max_hold_days_mom": [0, 1],
    "tp1_mult": [0.5, 0.7, 1.0, 1.5],
    "tp1_ratio": [0.3, 0.5, 0.7, 1.0],  # 1.0 = TP1에서 전량 청산
    "gap_down_limit": [0.0, 0.03, 0.05],  # 0 = 필터 없음
    "sector_penalty_threshold": [-0.03, -0.05],
    "sector_penalty_pts": [0, -2],
    "sector_bonus_pts": [0, 1],
}  # 552,960조합 (~3분)  --no-sector 시 69,120

SWING_GRID = {
    "invest_min_score": [9, 10, 11, 12, 13],
    "atr_sl_mult": [1.5, 2.0, 2.5, 3.0],
    "atr_tp_mult": [1.5, 2.0, 2.5, 3.0],
    "max_hold_days_rev": [3, 5, 7, 10],
    "max_hold_days_mix": [2, 3, 5],
    "max_hold_days_mom": [1, 2, 3],
    "tp1_mult": [0.7, 1.0, 1.5],
    "tp1_ratio": [0.3, 0.5, 0.7],
    "gap_down_limit": [0.0, 0.03, 0.05],
    "sector_penalty_threshold": [-0.03, -0.05],
    "sector_penalty_pts": [0, -2],
    "sector_bonus_pts": [0, 1],
}  # 5×4×4×4×3×3×3×3×3×2×2×2 = 622,080 → --no-sector 시 77,760


# ─────────────────────────────────────────
# 리포트
# ─────────────────────────────────────────
def _report(trade_mode: str, ranked: list[tuple[Params, Stats]], elapsed: float):
    log.info("")
    log.info("=" * 100)
    log.info(f"  {trade_mode} 최적화 결과  (Top 15)   [{elapsed:.0f}초]")
    log.info("=" * 100)
    log.info(
        f"  {'#':>3s}  {'거래':>6s}  {'승률':>6s}  {'평균수익':>8s}  {'총수익':>10s}  │ 파라미터"
    )
    log.info("-" * 100)

    for rank, (p, s) in enumerate(ranked[:15], 1):
        log.info(
            f"  {rank:3d}  {s.trades:>5d}건  {s.win_rate:>5.1f}%  "
            f"{s.avg_ret:>+7.2f}%  {s.total_ret:>+9.1f}%  │ {p.label()}"
        )

    log.info("-" * 100)
    if ranked:
        best_p, best_s = ranked[0]
        log.info(f"\n  ★ {trade_mode} 최적 파라미터:")
        log.info(f"    INVEST_MIN_SCORE = {best_p.invest_min_score}")
        log.info(f"    ATR_SL_MULT      = {best_p.atr_sl_mult}")
        log.info(f"    TP1_MULT         = {best_p.tp1_mult}  (매도비율 {best_p.tp1_ratio:.0%})")
        log.info(f"    ATR_TP_MULT      = {best_p.atr_tp_mult}")
        log.info(f"    GAP_DOWN_LIMIT   = {best_p.gap_down_limit:.0%}{'  (OFF)' if best_p.gap_down_limit == 0 else ''}")
        if trade_mode == "SWING":
            log.info(f"    MAX_HOLD_DAYS    = {best_p.max_hold_days_rev}")
            log.info(f"    MAX_HOLD_DAYS_MIX= {best_p.max_hold_days_mix}")
            log.info(f"    MAX_HOLD_DAYS_MOM= {best_p.max_hold_days_mom}")
        sec_parts = []
        if best_p.sector_penalty_pts != 0:
            sec_parts.append(f"패널티: 업종MA20괴리<{best_p.sector_penalty_threshold:.0%} → {best_p.sector_penalty_pts:+d}점")
        if best_p.sector_bonus_pts != 0:
            sec_parts.append(f"보너스: 업종MA20≥0% → {best_p.sector_bonus_pts:+d}점")
        log.info(f"    SECTOR_ADJUST    = {', '.join(sec_parts) if sec_parts else 'off'}")
        log.info(
            f"    → {best_s.trades}건  승률 {best_s.win_rate:.1f}%  "
            f"평균 {best_s.avg_ret:+.2f}%  총 {best_s.total_ret:+.1f}%"
        )
    log.info("=" * 100)


# ─────────────────────────────────────────
# factor.py 자동 갱신
# ─────────────────────────────────────────
_FACTOR_PATH = Path(__file__).resolve().parent.parent / "factor.py"


def _params_to_dict(p: Params) -> dict:
    return {
        "INVEST_MIN_SCORE": p.invest_min_score,
        "ATR_SL_MULT": p.atr_sl_mult,
        "ATR_TP_MULT": p.atr_tp_mult,
        "TP1_MULT": p.tp1_mult,
        "TP1_RATIO": p.tp1_ratio,
        "GAP_DOWN_LIMIT": p.gap_down_limit,
        "MAX_HOLD_DAYS": p.max_hold_days_rev,
        "MAX_HOLD_DAYS_MIX": p.max_hold_days_mix,
        "MAX_HOLD_DAYS_MOM": p.max_hold_days_mom,
        "SECTOR_PENALTY_THRESHOLD": p.sector_penalty_threshold,
        "SECTOR_PENALTY_PTS": p.sector_penalty_pts,
        "SECTOR_BONUS_PTS": p.sector_bonus_pts,
    }


def _fmt_val(key: str, val) -> str:
    """factor.py dict 값 포매팅."""
    if isinstance(val, float):
        if val == int(val) and key != "SECTOR_PENALTY_THRESHOLD":
            return str(int(val)) if val == 0 else str(val)
        return str(val)
    return str(val)


def _update_factor_file(best: dict[str, tuple[Params, Stats]], years: int):
    """DAY/SWING 최적 파라미터로 factor.py 재생성."""
    day_d = _params_to_dict(best["DAY"][0]) if "DAY" in best else None
    swing_d = _params_to_dict(best["SWING"][0]) if "SWING" in best else None

    # 기존 factor.py에서 변경되지 않는 모드는 유지
    if not day_d or not swing_d:
        try:
            from wye.blsh.domestic.factor import _DAY, _SWING
            if not day_d:
                day_d = dict(_DAY)
            if not swing_d:
                swing_d = dict(_SWING)
        except ImportError:
            log.warning("factor.py import 실패 — 두 모드 모두 최적화해야 합니다.")
            return

    today = dtutils.today()

    def fmt_pct(v):
        """0.05 → '5%', 0 → '0'"""
        if v == 0:
            return "0"
        return f"{abs(v)*100:.0f}%"

    def fmt_penalty(d):
        t = d["SECTOR_PENALTY_THRESHOLD"]
        p = d["SECTOR_PENALTY_PTS"]
        return f"-{fmt_pct(t)}/{p:+d}"

    # docstring 파라미터 테이블
    rows = [
        ("INVEST_MIN_SCORE", day_d, swing_d),
        ("ATR_SL_MULT", day_d, swing_d),
        ("ATR_TP_MULT", day_d, swing_d),
        ("TP1_MULT", day_d, swing_d),
        ("TP1_RATIO", day_d, swing_d),
        ("GAP_DOWN_LIMIT", day_d, swing_d),
    ]
    doc_lines = []
    for key, dd, sd in rows:
        doc_lines.append(f"{key:<20s} {_fmt_val(key, dd[key]):<8s}{_fmt_val(key, sd[key])}")
    doc_lines.append(f"{'MAX_HOLD_DAYS(REV)':<20s} {day_d['MAX_HOLD_DAYS']:<8}{swing_d['MAX_HOLD_DAYS']}")
    doc_lines.append(f"{'MAX_HOLD_DAYS_MIX':<20s} {day_d['MAX_HOLD_DAYS_MIX']:<8}{swing_d['MAX_HOLD_DAYS_MIX']}")
    doc_lines.append(f"{'MAX_HOLD_DAYS_MOM':<20s} {day_d['MAX_HOLD_DAYS_MOM']:<8}{swing_d['MAX_HOLD_DAYS_MOM']}")
    doc_lines.append(f"{'SECTOR_PENALTY':<20s} {fmt_penalty(day_d):<8s}{fmt_penalty(swing_d)}")
    doc_lines.append(f"{'SECTOR_BONUS':<20s} +{day_d['SECTOR_BONUS_PTS']:<7}+{swing_d['SECTOR_BONUS_PTS']}")
    doc_table = "\n".join(doc_lines)

    def dict_block(name: str, d: dict) -> str:
        items = []
        comments = {
            "TP1_MULT": "1차 익절: buy + ATR × TP1_MULT",
            "TP1_RATIO": "1차 익절 매도 비율 (1.0 = 전량)",
            "SECTOR_PENALTY_THRESHOLD": "업종지수 MA20 대비 해당값 이하",
            "SECTOR_BONUS_PTS": "업종지수 MA20 이상일 때",
        }
        for k, v in d.items():
            val_str = _fmt_val(k, v)
            comment = comments.get(k, "")
            line = f'    "{k}": {val_str},'
            if comment:
                line = f"{line}  # {comment}"
            items.append(line)
        return f"{name} = {{\n" + "\n".join(items) + "\n}"

    content = f'''"""\n최적 파라미터 ({today} 기준, 최근 {years}년 백테스트)

파라미터              DAY     SWING
──────────────────────────────────────
{doc_table}

실행 후 grid_search 최적값으로 자동 갱신:
  uv run python -m wye.blsh.domestic.optimize.grid_search
"""\nfrom wye.blsh.common.env import TRADE_FLAG

# ─────────────────────────────────────────
# 모드별 factor (grid_search 최적화 결과 반영)
# ─────────────────────────────────────────
{dict_block("_DAY", day_d)}

{dict_block("_SWING", swing_d)}

# ─────────────────────────────────────────
# 활성 factor 적용
# ─────────────────────────────────────────
_active = _DAY if TRADE_FLAG == "DAY" else _SWING

INVEST_MIN_SCORE = _active["INVEST_MIN_SCORE"]
ATR_SL_MULT = _active["ATR_SL_MULT"]
ATR_TP_MULT = _active["ATR_TP_MULT"]
TP1_MULT = _active["TP1_MULT"]
TP1_RATIO = _active["TP1_RATIO"]
GAP_DOWN_LIMIT = _active["GAP_DOWN_LIMIT"]
MAX_HOLD_DAYS = _active["MAX_HOLD_DAYS"]
MAX_HOLD_DAYS_MIX = _active["MAX_HOLD_DAYS_MIX"]
MAX_HOLD_DAYS_MOM = _active["MAX_HOLD_DAYS_MOM"]
SECTOR_PENALTY_THRESHOLD = _active["SECTOR_PENALTY_THRESHOLD"]
SECTOR_PENALTY_PTS = _active["SECTOR_PENALTY_PTS"]
SECTOR_BONUS_PTS = _active["SECTOR_BONUS_PTS"]
'''

    _FACTOR_PATH.write_text(content, encoding="utf-8")
    log.info(f"\n  💾 factor.py 자동 갱신: {_FACTOR_PATH}")
    for mode_name in ["DAY", "SWING"]:
        if mode_name in best:
            p, s = best[mode_name]
            log.info(
                f"    {mode_name}: {s.trades}건  승률 {s.win_rate:.1f}%  "
                f"평균 {s.avg_ret:+.2f}%  총 {s.total_ret:+.1f}%"
            )


# ─────────────────────────────────────────
# 메인
# ─────────────────────────────────────────
def run(mode: str = "BOTH", years: int = 2, rebuild: bool = False, sector: bool = True,
       apply: bool = True, workers: int = 0):
    global _WORKER_CACHE

    end_date = dtutils.today()
    start_date = dtutils.add_days(end_date, -years * 365)

    log.info(f"최적화 기간: {start_date} ~ {end_date} ({years}년)")

    # 캐시 빌드/로드
    if rebuild:
        # 기존 캐시 삭제
        for p in CACHE_DIR.glob("opt_cache*.pkl"):
            p.unlink()
            log.info(f"캐시 삭제: {p}")

    cache = build_or_load(start_date, end_date)

    # fork 전에 캐시를 전역 변수로 설정 (CoW — 자식 프로세스에 복사 없이 공유)
    _WORKER_CACHE = cache

    n_workers = workers if workers > 0 else os.cpu_count()
    log.info(f"병렬 처리: {n_workers}코어")

    best_results: dict[str, tuple[Params, Stats]] = {}

    for trade_mode, grid in [("DAY", DAY_GRID), ("SWING", SWING_GRID)]:
        if mode != "BOTH" and mode != trade_mode:
            continue

        keys = list(grid.keys())
        if not sector:
            grid = {
                **grid,
                "sector_penalty_threshold": [-0.03],
                "sector_penalty_pts": [0],
                "sector_bonus_pts": [0],
            }
        combos = list(product(*[grid[k] for k in keys]))

        sector_label = '' if sector else ' (업종패널티 OFF)'
        log.info(f"\n{'─' * 70}")
        log.info(f"  {trade_mode} 모드: {len(combos):,}개 조합 백테스트{sector_label}")
        log.info(f"{'─' * 70}")

        results: list[tuple[Params, Stats]] = []
        t0 = time.time()
        # chunksize: 너무 크면 결과 큐 누적 → OOM. 워커당 32개로 제한.
        chunk = max(10, min(200, len(combos) // (n_workers * 32)))

        with mp.Pool(processes=n_workers) as pool:
            for i, (p, s) in enumerate(
                pool.imap_unordered(_backtest_worker, ((keys, c) for c in combos), chunksize=chunk)
            ):
                results.append((p, s))
                n = i + 1
                if n % 5000 == 0 or n == len(combos):
                    elapsed = time.time() - t0
                    log.info(
                        f"  {n:>6d}/{len(combos)}  ({elapsed:.0f}초, "
                        f"{n / elapsed:.0f} combo/s)"
                    )

        # metric 기준 정렬
        results.sort(key=lambda x: x[1].metric, reverse=True)
        _report(trade_mode, results, time.time() - t0)

        if results and results[0][1].metric > -9999:
            best_results[trade_mode] = results[0]

    # factor.py 자동 갱신
    if apply and best_results:
        _update_factor_file(best_results, years)
    elif not apply and best_results:
        log.info("\n  ⚠️  --no-apply: factor.py 갱신 생략")


# ─────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="Factor 최적화 Grid Search")
    parser.add_argument("--mode", default="BOTH", choices=["DAY", "SWING", "BOTH"])
    parser.add_argument("--years", type=int, default=2, help="백테스트 기간 (년)")
    parser.add_argument("--rebuild", action="store_true", help="캐시 강제 재빌드")
    parser.add_argument("--no-sector", action="store_true", help="업종지수 패널티 비활성화 (기존 방식)")
    parser.add_argument("--no-apply", action="store_true", help="factor.py 자동 갱신 생략")
    parser.add_argument("--workers", type=int, default=0, help="병렬 프로세스 수 (0=자동)")
    args = parser.parse_args()

    run(
        mode=args.mode,
        years=args.years,
        rebuild=args.rebuild,
        sector=not args.no_sector,
        apply=not args.no_apply,
        workers=args.workers,
    )
