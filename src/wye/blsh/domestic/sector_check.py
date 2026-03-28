"""
업종지수 매핑 일관성 확인 (주 1회 cron)

DB의 idx_stk_ohlcv 지수명과 sector.py 매핑을 비교하여
차이 발생 시 텔레그램으로 알림.

실행:
    uv run python -m wye.blsh.domestic.sector_check
"""

import logging
from sqlalchemy import text
from wye.blsh.database import engine
from wye.blsh.domestic import sector
from wye.blsh.common.messageutils import send_message

log = logging.getLogger(__name__)

# sector.py에 정의된 모든 업종지수명
_MAPPED_NAMES = (
    set(sector.KOSPI_MID_TO_IDX.values())
    | set(sector.KOSPI_BIG_TO_IDX.values())
    | set(sector.KOSDAQ_MID_TO_IDX.values())
    | set(sector.KOSDAQ_BIG_TO_IDX.values())
)

# 업종 외 집계 지수 접두어 — 이들은 매핑 대상 아님
_SKIP_PREFIXES = (
    "코스피 2", "코스피 1", "코스피 5",
    "코스피 대", "코스피 중", "코스피 소",
    "코스피 (", "코스피200",
    "코스닥 1", "코스닥 대", "코스닥 중", "코스닥 소",
    "코스닥 (", "코스닥 글", "코스닥 벤", "코스닥 우",
    "코스닥 중견", "코스닥 기술",
    "코스피", "코스닥",  # 종합지수 자체
)


def _is_aggregate(name: str) -> bool:
    """집계/규모 지수 여부 (업종 섹터 아님)."""
    return any(name.startswith(p) for p in _SKIP_PREFIXES)


def check() -> bool:
    """업종지수 매핑 확인. 차이 없으면 True, 차이 있으면 False."""
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT DISTINCT idx_nm FROM idx_stk_ohlcv "
            "WHERE idx_clss IN ('02', '03') ORDER BY idx_nm"
        ))
        db_names = {r[0] for r in rows}

    # 집계 지수 제외 후 비교 대상만 추출
    sector_only = {n for n in db_names if not _is_aggregate(n)}

    missing_in_sector = sector_only - _MAPPED_NAMES   # DB에 있지만 sector.py에 없음
    missing_in_db = _MAPPED_NAMES - db_names           # sector.py에 있지만 DB에 없음

    if not missing_in_sector and not missing_in_db:
        log.info("[sector_check] 업종지수 매핑 일치 ✓")
        return True

    lines = ["[sector_check] 업종지수 매핑 불일치 감지"]
    if missing_in_sector:
        lines.append(f"\nDB에 있으나 sector.py 미등록 ({len(missing_in_sector)}건):")
        for n in sorted(missing_in_sector):
            lines.append(f"  + {n}")
    if missing_in_db:
        lines.append(f"\nsector.py에 있으나 DB 미존재 ({len(missing_in_db)}건):")
        for n in sorted(missing_in_db):
            lines.append(f"  - {n}")
    lines.append("\nsector.py 수동 업데이트 필요")

    msg = "\n".join(lines)
    log.warning(msg)
    send_message(msg)
    return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    check()
