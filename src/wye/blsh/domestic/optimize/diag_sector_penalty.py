"""업종 패널티/보너스 정상 작동 E2E 진단"""
import logging
from wye.blsh.database.query import select_all
from wye.blsh.domestic import sector

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

# ═══ [1] DB idx_nm vs sector.py 값 비교 ═══
log.info("=" * 70)
log.info("  [1] DB idx_nm vs sector.py 값 비교")
log.info("=" * 70)

# DB에서 모든 idx_nm 가져오기
db_names = {r["idx_nm"] for r in select_all("SELECT DISTINCT idx_nm FROM idx_stk_ohlcv")}

# sector.py의 모든 지수명
sector_names = (
    set(sector.KOSPI_MID_TO_IDX.values())
    | set(sector.KOSPI_BIG_TO_IDX.values())
    | set(sector.KOSDAQ_MID_TO_IDX.values())
    | set(sector.KOSDAQ_BIG_TO_IDX.values())
)
sector_names.add("코스닥")
sector_names.add("코스피")

log.info(f"  DB idx_nm 총 {len(db_names)}개")
log.info(f"  sector.py 지수명 총 {len(sector_names)}개")

# 매칭 확인
matched = sector_names & db_names
missing_in_db = sector_names - db_names
extra_in_db = db_names - sector_names

log.info(f"\n  ✅ 매칭: {len(matched)}개")
for n in sorted(matched):
    log.info(f"    ✅ {n}")

if missing_in_db:
    log.info(f"\n  ❌ sector.py에 있지만 DB에 없음: {len(missing_in_db)}개")
    for n in sorted(missing_in_db):
        # 유니코드 코드포인트 표시
        codepoints = ' '.join(f'U+{ord(c):04X}' for c in n)
        log.info(f"    ❌ '{n}'  ({codepoints})")
        # DB에서 비슷한 이름 검색
        for db_n in sorted(db_names):
            # 가운뎃점/공백 등을 제거하고 비교
            clean_sector = n.replace('\u00b7', '').replace('\u318d', '').replace(' ', '')
            clean_db = db_n.replace('\u00b7', '').replace('\u318d', '').replace(' ', '')
            if clean_sector == clean_db and n != db_n:
                db_cp = ' '.join(f'U+{ord(c):04X}' for c in db_n)
                log.info(f"       → DB 유사 후보: '{db_n}'  ({db_cp})")

if not missing_in_db:
    log.info(f"\n  ✅ sector.py 모든 지수명이 DB에 존재합니다!")

# ═══ [2] 가운뎃점 유니코드 비교 ═══
log.info(f"\n{'=' * 70}")
log.info("  [2] 가운뎃점 유니코드 분석")
log.info("=" * 70)

# sector.py에서 사용하는 가운뎃점
sector_dots = set()
for name in sector_names:
    for c in name:
        if c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789':
            if ord(c) > 127 and c not in '가나다라마바사아자차카타파하':
                if not ('\uAC00' <= c <= '\uD7A3'):  # 한글 완성형 제외
                    sector_dots.add(c)

log.info(f"  sector.py 특수문자: {sector_dots}")
for c in sector_dots:
    log.info(f"    '{c}' = U+{ord(c):04X} ({repr(c)})")

# DB에서 사용하는 가운뎃점
db_dots = set()
for name in db_names:
    for c in name:
        if ord(c) > 127 and not ('\uAC00' <= c <= '\uD7A3') and c != ' ':
            db_dots.add(c)

log.info(f"  DB 특수문자: {db_dots}")
for c in db_dots:
    log.info(f"    '{c}' = U+{ord(c):04X} ({repr(c)})")

# ═══ [3] 실제 업종지수 MA20 괴리율 조회 테스트 ═══
log.info(f"\n{'=' * 70}")
log.info("  [3] 업종지수 MA20 괴리율 조회 테스트")
log.info("=" * 70)

from wye.blsh.common import dtutils
base_date = dtutils.get_latest_biz_date()
log.info(f"  기준일: {base_date}")

test_names = ["코스피", "코스닥", "전기전자", "제약", "화학"]
# 가운뎃점 포함 이름도 테스트
dot_names = [n for n in sector_names if '\u00b7' in n][:3]
test_names.extend(dot_names)

for idx_nm in test_names:
    rows = select_all(
        "SELECT clsprc_idx FROM idx_stk_ohlcv "
        "WHERE idx_nm = :nm AND trd_dd <= :bd ORDER BY trd_dd DESC LIMIT 21",
        nm=idx_nm, bd=base_date,
    )
    if rows:
        prices = [float(r["clsprc_idx"]) for r in rows]
        cur = prices[0]
        ma = sum(prices) / len(prices)
        gap = (cur - ma) / ma
        log.info(f"  ✅ '{idx_nm}': {len(rows)}건  현재가={cur:.2f}  MA20={ma:.2f}  괴리율={gap:+.2%}")
    else:
        log.info(f"  ❌ '{idx_nm}': 조회 결과 0건!")

# ═══ [4] 종목→업종→괴리율→패널티 E2E 테스트 ═══
log.info(f"\n{'=' * 70}")
log.info("  [4] 종목→업종→괴리율→패널티 E2E 테스트")
log.info("=" * 70)

from wye.blsh.domestic.scanner import _load_ticker_sector_map, _get_sector_gap
from wye.blsh.domestic import factor

sector_map = _load_ticker_sector_map(base_date)
log.info(f"  종목→업종 매핑: {len(sector_map)}종목")

# KOSPI/KOSDAQ 분포
kospi_sectors = {}
kosdaq_sectors = {}
for ticker, sec_nm in sector_map.items():
    if sec_nm in set(sector.KOSPI_MID_TO_IDX.values()) | set(sector.KOSPI_BIG_TO_IDX.values()) | {"코스피"}:
        kospi_sectors[sec_nm] = kospi_sectors.get(sec_nm, 0) + 1
    else:
        kosdaq_sectors[sec_nm] = kosdaq_sectors.get(sec_nm, 0) + 1

log.info(f"\n  KOSPI 업종별 종목수:")
for sec_nm, cnt in sorted(kospi_sectors.items(), key=lambda x: -x[1]):
    gap = _get_sector_gap(sec_nm, base_date)
    if gap < factor.SECTOR_PENALTY_THRESHOLD:
        adj = f"패널티 {factor.SECTOR_PENALTY_PTS:+d}"
    elif gap >= 0:
        adj = f"보너스 {factor.SECTOR_BONUS_PTS:+d}"
    else:
        adj = "변동없음"
    log.info(f"    {sec_nm:20s}  {cnt:>4}종목  괴리율={gap:+.2%}  → {adj}")

log.info(f"\n  KOSDAQ 업종별 종목수:")
for sec_nm, cnt in sorted(kosdaq_sectors.items(), key=lambda x: -x[1]):
    gap = _get_sector_gap(sec_nm, base_date)
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
