"""idx_clss 값 확인 + 중복 업종 식별"""
import logging
from wye.blsh.database.query import select_all

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

# 1. idx_clss 고유값
log.info("=== [1] idx_clss 고유값 ===")
rows = select_all("SELECT DISTINCT idx_clss FROM idx_stk_ohlcv ORDER BY idx_clss")
for r in rows:
    log.info(f"  '{r['idx_clss']}'")

# 2. 중복 idx_nm (여러 idx_clss에 존재)
log.info("\n=== [2] 여러 idx_clss에 존재하는 idx_nm ===")
rows = select_all("""
    SELECT idx_nm, COUNT(DISTINCT idx_clss) AS cnt, 
           STRING_AGG(DISTINCT idx_clss, ', ' ORDER BY idx_clss) AS classes
    FROM idx_stk_ohlcv
    GROUP BY idx_nm
    HAVING COUNT(DISTINCT idx_clss) > 1
    ORDER BY cnt DESC, idx_nm
""")
log.info(f"  중복 idx_nm: {len(rows)}개")
for r in rows:
    log.info(f"  {r['idx_nm']:20s}  → {r['classes']}")

# 3. "전기전자" 최근 3일 — idx_clss별
log.info("\n=== [3] '전기전자' 최근 3일 — idx_clss별 ===")
rows = select_all("""
    SELECT trd_dd, idx_clss, clsprc_idx 
    FROM idx_stk_ohlcv 
    WHERE idx_nm = '전기전자' 
    ORDER BY trd_dd DESC, idx_clss 
    LIMIT 10
""")
for r in rows:
    log.info(f"  {r['trd_dd']}  {r['idx_clss']:10s}  {float(r['clsprc_idx']):>12,.2f}")

# 4. "코스피", "코스닥" — idx_clss 확인
log.info("\n=== [4] 코스피/코스닥 idx_clss ===")
for nm in ["코스피", "코스닥"]:
    rows = select_all("""
        SELECT DISTINCT idx_clss FROM idx_stk_ohlcv WHERE idx_nm = :nm
    """, nm=nm)
    classes = [r["idx_clss"] for r in rows]
    log.info(f"  '{nm}' → {classes}")
