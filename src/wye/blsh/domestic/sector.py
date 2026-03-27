"""
업종코드 → DB idx_stk_ohlcv 지수명 매핑
"""
from typing import Final

# idx_stk_ohlcv.idx_clss 값 (collector.py Idx 클래스 참조)
IDX_CLSS_KOSPI: Final = "02"
IDX_CLSS_KOSDAQ: Final = "03"
KOSPI_MID_TO_IDX = {
    5: "음식료\u00b7담배",
    6: "섬유\u00b7의류",
    7: "종이\u00b7목재",
    8: "화학",
    9: "제약",
    10: "비금속",
    11: "금속",
    12: "기계\u00b7장비",
    13: "전기전자",
    14: "의료\u00b7정밀기기",
    15: "운송장비\u00b7부품",
    24: "증권",
    25: "보험",
}
KOSPI_BIG_TO_IDX = {
    16: "유통",
    17: "전기\u00b7가스",
    18: "건설",
    19: "운송\u00b7창고",
    20: "통신",
    21: "금융",
    26: "일반서비스",
    27: "제조",
    28: "부동산",
    29: "IT 서비스",
    30: "오락\u00b7문화",
}
# KOSDAQ 지수업종중분류 (더 세분화)
KOSDAQ_MID_TO_IDX = {
    1019: "음식료\u00b7담배",
    1020: "섬유\u00b7의류",
    1021: "종이\u00b7목재",
    1022: "출판\u00b7매체복제",
    1023: "화학",
    1024: "제약",
    1025: "비금속",
    1026: "금속",
    1027: "기계\u00b7장비",
    1028: "전기전자",
    1029: "의료\u00b7정밀기기",
    1030: "운송장비\u00b7부품",
    1031: "기타제조",
}
# KOSDAQ 지수업종대분류 (중분류 0인 종목용 fallback)
KOSDAQ_BIG_TO_IDX = {
    1006: "일반서비스",
    1009: "제조",
    1010: "건설",
    1011: "유통",
    1013: "운송\u00b7창고",
    1014: "금융",
    1015: "오락\u00b7문화",
}


def get_idx_clss(market: str) -> str:
    """시장 문자열 → idx_clss 변환. 'KOSPI'→'02', 'KOSDAQ'→'03'."""
    return IDX_CLSS_KOSPI if market == "KOSPI" else IDX_CLSS_KOSDAQ
