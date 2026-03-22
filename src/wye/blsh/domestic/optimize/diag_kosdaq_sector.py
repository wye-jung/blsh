"""코스닥 업종코드 → DB 지수명 매핑 진단"""
import logging
from wye.blsh.database.query import select_all
from wye.blsh.kis.domestic_stock.domestic_stock_info import get_kosdaq_info, get_sector_info

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

# 1. 업종코드 마스터에서 코스닥(1000번대) 업종 확인
log.info("=== [1] 업종코드 마스터 (1000번대 = 코스닥) ===")
sector_df = get_sector_info()
kd_sectors = sector_df[sector_df["업종코드"].astype(str).str.startswith("1")]
log.info(f"  코스닥 업종: {len(kd_sectors)}개")
for _, row in kd_sectors.iterrows():
    log.info(f"  {row['업종코드']:6s}  {row['업종명']}")

# 2. DB 코스닥 관련 지수 목록
log.info("\n=== [2] DB idx_stk_ohlcv 코스닥 지수 ===")
idx_names = select_all("SELECT DISTINCT idx_nm FROM idx_stk_ohlcv WHERE idx_nm LIKE '%코스닥%' ORDER BY idx_nm")
for r in idx_names:
    log.info(f"  {r['idx_nm']}")

# 3. KOSDAQ 종목 업종코드 분포
log.info("\n=== [3] KOSDAQ 종목 업종코드 분포 ===")
kd = get_kosdaq_info()
log.info(f"  총 {len(kd)}종목")

# 대분류
big_col = "지수업종 대분류 코드"
big_dist = kd[big_col].value_counts().sort_index()
log.info(f"\n  대분류 분포:")
for code, cnt in big_dist.items():
    name = sector_df[sector_df["업종코드"] == str(code)]["업종명"].values
    nm = name[0] if len(name) > 0 else "?"
    log.info(f"    {code:>6}  {nm:20s}  {cnt:>4}종목")

# 중분류
mid_col = "지수 업종 중분류 코드"
mid_dist = kd[mid_col].value_counts().sort_index()
log.info(f"\n  중분류 분포:")
for code, cnt in mid_dist.items():
    name = sector_df[sector_df["업종코드"] == str(code)]["업종명"].values
    nm = name[0] if len(name) > 0 else "?"
    log.info(f"    {code:>6}  {nm:20s}  {cnt:>4}종목")

# 4. 코스닥 150 업종지수에 데이터가 충분한지 확인
log.info("\n=== [4] 코스닥 150 업종지수 데이터 양 ===")
kd150_indices = [r['idx_nm'] for r in idx_names if '150' in r['idx_nm'] and '거버넌스' not in r['idx_nm']]
for nm in kd150_indices:
    cnt = select_all(f"SELECT COUNT(*) AS c FROM idx_stk_ohlcv WHERE idx_nm = :nm", nm=nm)
    log.info(f"  {nm:40s}  {cnt[0]['c']:>5}건")
