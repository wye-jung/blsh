"""업종지수 데이터 이상값 원인 진단"""
import logging
from wye.blsh.database.query import select_all
from wye.blsh.common import dtutils

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

base_date = dtutils.get_latest_biz_date()
log.info(f"기준일: {base_date}")

# 이상 업종 목록
problem_sectors = ["전기전자", "금속", "기계·장비", "섬유·의류", "오락·문화", "화학", "건설"]

for idx_nm in problem_sectors:
    log.info(f"\n{'=' * 70}")
    log.info(f"  [{idx_nm}] 최근 25일 데이터")
    log.info("=" * 70)

    rows = select_all(
        "SELECT trd_dd, clsprc_idx FROM idx_stk_ohlcv "
        "WHERE idx_nm = :nm AND trd_dd <= :bd ORDER BY trd_dd DESC LIMIT 25",
        nm=idx_nm, bd=base_date,
    )
    if not rows:
        log.info("  데이터 없음!")
        continue

    # 중복 날짜 체크
    dates = [r["trd_dd"] for r in rows]
    prices = [float(r["clsprc_idx"]) for r in rows]
    dup_dates = [d for d in dates if dates.count(d) > 1]

    for i, r in enumerate(rows):
        dup_mark = " ⚠️ 중복!" if dates.count(r["trd_dd"]) > 1 else ""
        jump_mark = ""
        if i > 0:
            prev = prices[i - 1]
            curr = prices[i]
            if prev > 0:
                change = (prev - curr) / curr * 100
                if abs(change) > 50:
                    jump_mark = f" 🔴 급변 {change:+.0f}%"
                elif abs(change) > 20:
                    jump_mark = f" 🟡 {change:+.0f}%"
        log.info(f"  {r['trd_dd']}  {float(r['clsprc_idx']):>12,.2f}{dup_mark}{jump_mark}")

    if dup_dates:
        log.info(f"  ⚠️ 중복 날짜: {set(dup_dates)}")

    # 같은 idx_nm으로 여러 종류 데이터가 있는지 확인
    count = select_all(
        "SELECT COUNT(*) AS c, MIN(clsprc_idx) AS min_v, MAX(clsprc_idx) AS max_v "
        "FROM idx_stk_ohlcv WHERE idx_nm = :nm",
        nm=idx_nm,
    )
    if count:
        r = count[0]
        log.info(f"  전체: {r['c']}건  min={float(r['min_v']):,.2f}  max={float(r['max_v']):,.2f}  비율={float(r['max_v'])/max(float(r['min_v']),0.01):,.1f}x")

# 정상 업종도 비교용
log.info(f"\n{'=' * 70}")
log.info(f"  [코스피] 정상 참조 — 최근 5일")
log.info("=" * 70)
for r in select_all(
    "SELECT trd_dd, clsprc_idx FROM idx_stk_ohlcv "
    "WHERE idx_nm = '코스피' AND trd_dd <= :bd ORDER BY trd_dd DESC LIMIT 5",
    bd=base_date,
):
    log.info(f"  {r['trd_dd']}  {float(r['clsprc_idx']):>12,.2f}")
