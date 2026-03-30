"""업종 패널티/보너스 정상 작동 E2E 진단 (idx_clss 반영)"""
import logging
from wye.blsh.database.query import select_all, get_index_clsprc
from wye.blsh.domestic import sector

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

# ═══ [1] DB idx_nm vs sector.py ═══
log.info("=" * 70)
log.info("  [1] DB idx_nm vs sector.py 값 비교")
log.info("=" * 70)
db_names = {r["idx_nm"] for r in select_all("SELECT DISTINCT idx_nm FROM idx_stk_ohlcv")}
sector_names = (
    set(sector.KOSPI_MID_TO_IDX.values()) | set(sector.KOSPI_BIG_TO_IDX.values())
    | set(sector.KOSDAQ_MID_TO_IDX.values()) | set(sector.KOSDAQ_BIG_TO_IDX.values())
)
sector_names.add("코스닥")
sector_names.add("코스피")
matched = sector_names & db_names
missing = sector_names - db_names
log.info(f"  DB: {len(db_names)}개  sector.py: {len(sector_names)}개  매칭: {len(matched)}개")
if missing:
    for n in sorted(missing):
        log.info(f"  ❌ DB에 없음: '{n}'")
else:
    log.info("  ✅ 모든 지수명이 DB에 존재")

# ═══ [2] idx_clss 필터 적용 괴리율 테스트 ═══
log.info(f"\n{'=' * 70}")
log.info("  [2] idx_clss 필터 적용 괴리율 테스트")
log.info("=" * 70)

from wye.blsh.common import dtutils
base_date = dtutils.get_latest_biz_date()
log.info(f"  기준일: {base_date}")

# 이전에 이상값이었던 업종들
test_cases = [
    ("코스피", sector.IDX_CLSS_KOSPI),
    ("코스닥", sector.IDX_CLSS_KOSDAQ),
    ("전기전자", sector.IDX_CLSS_KOSPI),
    ("전기전자", sector.IDX_CLSS_KOSDAQ),
    ("금속", sector.IDX_CLSS_KOSPI),
    ("금속", sector.IDX_CLSS_KOSDAQ),
    ("화학", sector.IDX_CLSS_KOSPI),
    ("화학", sector.IDX_CLSS_KOSDAQ),
    ("기계·장비", sector.IDX_CLSS_KOSPI),
    ("기계·장비", sector.IDX_CLSS_KOSDAQ),
    ("섬유·의류", sector.IDX_CLSS_KOSPI),
    ("섬유·의류", sector.IDX_CLSS_KOSDAQ),
    ("건설", sector.IDX_CLSS_KOSPI),
    ("건설", sector.IDX_CLSS_KOSDAQ),
    ("제약", sector.IDX_CLSS_KOSPI),
    ("제약", sector.IDX_CLSS_KOSDAQ),
]

clss_label = {sector.IDX_CLSS_KOSPI: "KOSPI", sector.IDX_CLSS_KOSDAQ: "KOSDAQ"}

for idx_nm, clss in test_cases:
    rows = get_index_clsprc(idx_nm, base_date, 20, idx_clss=clss)
    label = clss_label[clss]
    if rows:
        prices = [float(r["clsprc_idx"]) for r in rows]
        cur = prices[0]
        ma = sum(prices) / len(prices)
        gap = (cur - ma) / ma
        ok = abs(gap) < 0.20  # 20% 이상이면 의심
        mark = "✅" if ok else "⚠️"
        log.info(f"  {mark} {idx_nm:12s} [{label:6s}]  {len(rows):2d}건  현재가={cur:>10,.2f}  MA20={ma:>10,.2f}  괴리율={gap:+.2%}")
    else:
        log.info(f"  ➖ {idx_nm:12s} [{label:6s}]  데이터 없음 (해당 시장에 미존재)")

# ═══ [3] E2E: 종목→업종→괴리율→패널티 (idx_clss 적용) ═══
log.info(f"\n{'=' * 70}")
log.info("  [3] E2E 패널티/보너스 (idx_clss 적용)")
log.info("=" * 70)

from wye.blsh.domestic.scanner import _load_ticker_sector_map, _get_sector_gap
from wye.blsh.domestic import config as factor

sector_map = _load_ticker_sector_map(base_date)
log.info(f"  종목→업종 매핑: {len(sector_map)}종목")

# KOSPI 업종별
kospi_sectors = {}
kosdaq_sectors = {}
for ticker, sec_nm in sector_map.items():
    if sec_nm in set(sector.KOSPI_MID_TO_IDX.values()) | set(sector.KOSPI_BIG_TO_IDX.values()) | {"코스피"}:
        kospi_sectors[sec_nm] = kospi_sectors.get(sec_nm, 0) + 1
    else:
        kosdaq_sectors[sec_nm] = kosdaq_sectors.get(sec_nm, 0) + 1

log.info(f"\n  KOSPI 업종별 (idx_clss={sector.IDX_CLSS_KOSPI}):")
for sec_nm, cnt in sorted(kospi_sectors.items(), key=lambda x: -x[1]):
    gap = _get_sector_gap(sec_nm, base_date, idx_clss=sector.IDX_CLSS_KOSPI)
    if gap < factor.SECTOR_PENALTY_THRESHOLD:
        adj = f"패널티 {factor.SECTOR_PENALTY_PTS:+d}"
    elif gap >= 0:
        adj = f"보너스 {factor.SECTOR_BONUS_PTS:+d}"
    else:
        adj = "변동없음"
    log.info(f"    {sec_nm:20s}  {cnt:>4}종목  괴리율={gap:+.2%}  → {adj}")

log.info(f"\n  KOSDAQ 업종별 (idx_clss={sector.IDX_CLSS_KOSDAQ}):")
for sec_nm, cnt in sorted(kosdaq_sectors.items(), key=lambda x: -x[1]):
    gap = _get_sector_gap(sec_nm, base_date, idx_clss=sector.IDX_CLSS_KOSDAQ)
    if gap < factor.SECTOR_PENALTY_THRESHOLD:
        adj = f"패널티 {factor.SECTOR_PENALTY_PTS:+d}"
    elif gap >= 0:
        adj = f"보너스 {factor.SECTOR_BONUS_PTS:+d}"
    else:
        adj = "변동없음"
    log.info(f"    {sec_nm:20s}  {cnt:>4}종목  괴리율={gap:+.2%}  → {adj}")

log.info(f"\n{'=' * 70}")
log.info("  진단 완료")
log.info("=" * 70)
