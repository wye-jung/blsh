"""업종지수 데이터 수집 + 종목→업종 매핑 진단"""
import logging
from wye.blsh.database.query import select_all
from wye.blsh.kis.domestic_stock.domestic_stock_info import (
    get_sector_info, get_kospi_info, get_kosdaq_info,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

# 1. DB에 어떤 지수가 있는지 확인
log.info("=== [1] idx_stk_ohlcv 지수 목록 ===")
idx_names = select_all("SELECT DISTINCT idx_nm FROM idx_stk_ohlcv ORDER BY idx_nm")
for r in idx_names:
    log.info(f"  {r['idx_nm']}")
log.info(f"  총 {len(idx_names)}개 지수")

# 2. 업종코드 마스터
log.info("\n=== [2] KIS 업종코드 마스터 ===")
sector_df = get_sector_info()
log.info(f"  {len(sector_df)}개 업종")
log.info(sector_df.head(30).to_string())

# 3. KOSPI 종목별 업종코드
log.info("\n=== [3] KOSPI 종목 업종 매핑 ===")
kospi_df = get_kospi_info()
log.info(f"  컬럼: {list(kospi_df.columns)}")
log.info(f"  종목수: {len(kospi_df)}")
# 업종 관련 컬럼 확인
sector_cols = [c for c in kospi_df.columns if '업종' in c]
log.info(f"  업종 관련 컬럼: {sector_cols}")
sample = kospi_df[['단축코드', '한글명'] + sector_cols].head(10)
log.info(f"\n{sample.to_string()}")

# 대분류 고유값
for col in sector_cols:
    uniq = kospi_df[col].dropna().unique()
    log.info(f"\n  {col} 고유값 ({len(uniq)}개): {sorted(uniq)[:20]}")

# 4. KOSDAQ 종목별 업종코드
log.info("\n=== [4] KOSDAQ 종목 업종 매핑 ===")
kosdaq_df = get_kosdaq_info()
sector_cols_kd = [c for c in kosdaq_df.columns if '업종' in c]
log.info(f"  업종 관련 컬럼: {sector_cols_kd}")
sample_kd = kosdaq_df[['단축코드', '한글종목명'] + sector_cols_kd].head(10)
log.info(f"\n{sample_kd.to_string()}")

for col in sector_cols_kd:
    uniq = kosdaq_df[col].dropna().unique()
    log.info(f"\n  {col} 고유값 ({len(uniq)}개): {sorted(uniq)[:20]}")

# 5. 업종코드 → 지수명 매핑 시도
log.info("\n=== [5] 업종코드 → DB 지수명 매핑 ===")
idx_nm_set = {r['idx_nm'] for r in idx_names}
sector_map = dict(zip(sector_df['업종코드'], sector_df['업종명']))
for code, name in list(sector_map.items())[:30]:
    # DB에 '코스피 {name}' 또는 '{name}' 형태로 있는지 확인
    matched = [n for n in idx_nm_set if name in n]
    if matched:
        log.info(f"  {code} {name:20s} → {matched}")
