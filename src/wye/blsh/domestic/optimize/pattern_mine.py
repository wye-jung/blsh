"""
3영업일 +5% 수익 기반 buy_flags 패턴 채굴
──────────────────────────────────────────
과거 스캔 데이터에서 3영업일 후 수익률 ≥ threshold 인 종목의
기술 플래그 조합(frozenset)을 모드별로 집계하여 패턴 파일로 저장.

저장 경로: ~/.blsh/data/flag_patterns.json

실행:
    uv run python -m wye.blsh.domestic.optimize.pattern_mine
    uv run python -m wye.blsh.domestic.optimize.pattern_mine --years 1 --min-count 5
    uv run python -m wye.blsh.domestic.optimize.pattern_mine --train-end 20251231
"""

from __future__ import annotations

import argparse
import json
import logging
from collections import defaultdict
from pathlib import Path

from wye.blsh.common import dtutils
from wye.blsh.common.env import DATA_DIR
from wye.blsh.domestic.optimize._cache import OptCache, build_or_load

log = logging.getLogger(__name__)

# scanner 수급 플래그 — 기술 플래그만 패턴에 사용
_SUPPLY_FLAGS = frozenset({"F_TRN", "I_TRN", "F_C3", "I_C3", "F_1", "I_1", "FI", "P_OV"})

PATTERN_FILE = DATA_DIR / "flag_patterns.json"


def mine_patterns(
    cache: OptCache,
    threshold: float = 0.05,
    min_count: int = 5,
    train_end: str | None = None,
) -> dict[str, list[dict]]:
    """
    OptCache에서 3영업일 후 수익 ≥ threshold 인 종목의 기술 플래그 조합을 채굴.

    Args:
        cache: 사전 빌드된 OptCache
        threshold: 수익률 기준 (기본 5%)
        min_count: 패턴 최소 등장 횟수 (승자 기준)
        train_end: 이 날짜 이후 scan_date는 제외 (룩어헤드 방지, YYYYMMDD)

    Returns:
        {mode: [{flags, count, total, win_rate, avg_score, min_score}]} (count 내림차순)
    """
    # {mode: {frozenset(flags): {"wins": int, "total": int, "score_sum": float, "score_min": float}}}
    buckets: dict[str, dict[frozenset, dict]] = defaultdict(lambda: defaultdict(
        lambda: {"wins": 0, "total": 0, "score_sum": 0.0, "score_min": float("inf")}
    ))

    skipped_no_fwd = 0
    skipped_no_price = 0
    total_processed = 0

    for scan_date in cache.scan_dates:
        if train_end and scan_date > train_end:
            continue

        fwd = cache.forward_dates.get(scan_date, [])
        # forward_dates[scan_date][0] == scan_date 자체
        # [3] == 3번째 이후 영업일 == entry(+1) 기준 +3영업일
        if len(fwd) < 4:
            skipped_no_fwd += len(cache.signals.get(scan_date, []))
            continue

        date_3d = fwd[3]

        for sig in cache.signals.get(scan_date, []):
            mode = sig.get("mode", "")
            if mode == "WEAK":
                continue

            ticker = sig["ticker"]
            entry_price = sig.get("entry_price", 0)
            if entry_price <= 0:
                continue

            fwd_ohlcv = cache.ohlcv_idx.get((ticker, date_3d))
            if fwd_ohlcv is None:
                skipped_no_price += 1
                continue

            close_3d = fwd_ohlcv.get("close", 0)
            if close_3d <= 0:
                skipped_no_price += 1
                continue

            ret = close_3d / entry_price - 1
            score = sig.get("score", sig.get("tech_score", 0))

            # 수급 플래그 제거 후 기술 플래그만 사용
            raw_flags = frozenset(sig["flags"].split(",")) - _SUPPLY_FLAGS
            if not raw_flags:
                continue

            total_processed += 1
            b = buckets[mode][raw_flags]
            b["total"] += 1
            b["score_sum"] += score
            b["score_min"] = min(b["score_min"], score)
            if ret >= threshold:
                b["wins"] += 1

    log.info(
        f"채굴 완료: {total_processed:,}건 처리  "
        f"| 선도가 없음 {skipped_no_fwd:,}건  "
        f"| 3일후 가격 없음 {skipped_no_price:,}건"
    )

    # 집계 → 필터링 → 정렬
    patterns: dict[str, list[dict]] = {}
    for mode, flag_counts in sorted(buckets.items()):
        mode_patterns = []
        for flags, b in flag_counts.items():
            wins = b["wins"]
            total = b["total"]
            if wins < min_count:
                continue
            avg_score = b["score_sum"] / total if total else 0
            mode_patterns.append({
                "flags": sorted(flags),
                "count": wins,
                "total": total,
                "win_rate": round(wins / total, 4) if total else 0,
                "avg_score": round(avg_score, 2),
                "min_score": int(b["score_min"]) if b["score_min"] != float("inf") else 0,
            })
        # 승자 수 내림차순 → 승률 내림차순
        mode_patterns.sort(key=lambda x: (-x["count"], -x["win_rate"]))
        patterns[mode] = mode_patterns
        log.info(
            f"  {mode}: {len(mode_patterns)}개 패턴 "
            f"(min_count={min_count}, threshold={threshold:.0%})"
        )

    return patterns


def print_patterns(patterns: dict[str, list[dict]]) -> None:
    for mode, mode_patterns in patterns.items():
        print(f"\n{'='*60}")
        print(f"  MODE: {mode}  ({len(mode_patterns)}개 패턴)")
        print(f"{'='*60}")
        print(f"  {'flags':<35} {'승/전':<8} {'승률':>6}  {'평균점':>6}  {'최소점':>6}")
        print(f"  {'-'*70}")
        for p in mode_patterns[:30]:  # 상위 30개만 출력
            flags_str = "+".join(p["flags"])
            print(
                f"  {flags_str:<35} {p['count']}/{p['total']:<6} "
                f"{p['win_rate']:>6.1%}  {p['avg_score']:>6.1f}  {p['min_score']:>6}"
            )


def run(
    years: int = 1,
    threshold: float = 0.05,
    min_count: int = 5,
    train_end: str | None = None,
    rebuild: bool = False,
    out: Path = PATTERN_FILE,
) -> None:
    end_date = dtutils.today()
    start_date = dtutils.add_days(end_date, -years * 365)

    log.info(f"OptCache 로드: {start_date} ~ {end_date}")
    if rebuild:
        from wye.blsh.domestic.optimize._cache import _build
        cache = _build(start_date, end_date, tag="")
    else:
        cache = build_or_load(start_date, end_date)

    log.info(
        f"패턴 채굴 시작: threshold={threshold:.0%}, min_count={min_count}, "
        f"train_end={train_end or '제한없음'}"
    )
    patterns = mine_patterns(cache, threshold=threshold, min_count=min_count, train_end=train_end)

    print_patterns(patterns)

    result = {
        "generated": end_date,
        "train_end": train_end or end_date,
        "start_date": start_date,
        "end_date": end_date,
        "threshold": threshold,
        "min_count": min_count,
        "patterns": patterns,
    }

    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    total = sum(len(v) for v in patterns.values())
    log.info(f"패턴 저장 완료: {out}  ({total}개 패턴)")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="3영업일 수익 기반 flag 패턴 채굴")
    parser.add_argument("--years", type=int, default=1, help="분석 기간 (년, 기본 1)")
    parser.add_argument(
        "--train-end",
        dest="train_end",
        default=None,
        help="패턴 채굴 종료 날짜 YYYYMMDD (룩어헤드 방지, 기본: 제한 없음)",
    )
    parser.add_argument("--threshold", type=float, default=0.05, help="수익 기준점 (기본 0.05)")
    parser.add_argument("--min-count", type=int, dest="min_count", default=5, help="패턴 최소 승자 수 (기본 5)")
    parser.add_argument("--rebuild", action="store_true", help="캐시 강제 재빌드")
    parser.add_argument("--out", type=Path, default=PATTERN_FILE, help="출력 파일 경로")
    args = parser.parse_args()

    run(
        years=args.years,
        threshold=args.threshold,
        min_count=args.min_count,
        train_end=args.train_end,
        rebuild=args.rebuild,
        out=args.out,
    )
