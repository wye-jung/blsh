class Optimized:
    # ── 백테스트 결과 (grid_search 자동 갱신) ──
    # 수행일시: 2026-04-12 13:50 (2분)
    # 기간: 20240412 ~ 20260412
    # 성과: 3860건  승률 48.5%  평균 +1.81% (std 7.30%)  총 +6973.4%
    # ──────────────────────────────────────────
    INVEST_MIN_SCORE: int = 9
    ATR_SL_MULT: float = 4.0
    ATR_TP_MULT: float = 1.5
    TP1_MULT: float = 2.5  # 1차 익절: buy + ATR × TP1_MULT
    TP1_RATIO: float = 1.0  # 1차 익절 매도 비율 (1.0 = 전량)
    MAX_HOLD_DAYS: int = 10
    MAX_HOLD_DAYS_MIX: int = 3
    MAX_HOLD_DAYS_MOM: int = 3
    INDEX_DROP_LIMIT: float = 1.0
    ATR_CAP: float = 0.05
    SIGNAL_SCORES_MOM = {
        "MGC": 0,
        "W52": 1,
        "PB": 0,
        "LB": -1,
        "VS": 2,
        "MAA": 0,
        "OBV": 2,
        "MPGC": 1,
        "BBM": 2,
        "SGC": 2,
    }
    SIGNAL_SCORES_REV = {
        "MS": 3,
        "RBO": 3,
        "ROV": -1,
        "BBL": 3,
        "HMR": 2,
        "BE": 0,
        "MPGC": 2,
        "BBM": 0,
        "SGC": 2,
    }


# for scan
MACD_SHORT: int = 12
MACD_LONG: int = 26
MACD_SIGNAL: int = 9
RSI_PERIOD: int = 14
RSI_OVERSOLD: int = 30
BB_PERIOD: int = 20
BB_STD: float = 2.0
STOCH_K: int = 14
STOCH_D: int = 3
STOCH_SMOOTH: int = 3
MA_PERIODS: list[int] = [5, 20, 60]  # 120은 미사용이므로 제거
ATR_PERIOD: int = 14
GAP_THRESHOLD: float = 0.02
W52_VOL_MULT: float = 1.5  # 52주 신고가 거래량 조건: 20일 평균의 N배
LOOKBACK_DAYS: int = 365  # 52주(252거래일) 신고가 계산을 위해 365일 이상 필요
MIN_SCORE: int = 1  # 저장 최소 점수
SUPPLY_CAP: int = 3  # 수급 가산 상한 (백테스트 검증, 2026-03-29)
TRDVAL_MIN: int = 1_000_000_000  # 최근 20일 평균 거래대금 최소값 (10억)
TRDVAL_DAYS: int = 20
INDEX_MA_DAYS: int = 20  # 지수 환경 체크 이동평균 기간
INDEX_DROP_LIMIT: float = Optimized.INDEX_DROP_LIMIT
ATR_CAP: float = Optimized.ATR_CAP
INVEST_MIN_SCORE: int = (
    Optimized.INVEST_MIN_SCORE
)  # 투자 적격 최소 점수 (백테스트 검증)
ENRICH_SCORE: int = (
    INVEST_MIN_SCORE - SUPPLY_CAP
)  # 수급 MAX 가산해도 통과 못할 종목 제외

# 매수부적합 필터: True인 항목이 활성화된 종목은 스캔에서 제외
# 추후 변경 시 값만 True/False로 토글
# 매수부적합 필터 값:
#   True  — 플래그 활성 시 탈락
#   False — 무시
#   int   — 시장경고 등 등급 코드: 해당 값 이상이면 탈락
DISQUALIFY_FLAGS: dict[str, bool | int] = {
    "거래정지": True,
    "정리매매": True,
    "관리종목": True,
    "시장경고": 2,  # 1=투자주의, 2=투자경고, 3=투자위험 (2 이상 탈락)
    "불성실공시": True,
    "단기과열": True,
    "이상급등": True,
    "SPAC": True,
    "투자주의환기": True,  # KOSDAQ only
    "공매도과열": False,
    "경고예고": False,
    "우회상장": False,
}

SIGNAL_SCORES_MOM = Optimized.SIGNAL_SCORES_MOM
SIGNAL_SCORES_REV = Optimized.SIGNAL_SCORES_REV

SUPPLY_SCORES = {
    "TRN": 3,
    "C3": 2,
    "1": 1,
}

# for trade
ATR_SL_MULT: float = Optimized.ATR_SL_MULT
ATR_TP_MULT: float = Optimized.ATR_TP_MULT
TP1_MULT: float = Optimized.TP1_MULT  # 1차 익절: buy + ATR × TP1_MULT
TP1_RATIO: float = Optimized.TP1_RATIO  # 1차 익절 매도 비율 (1.0 = 전량)
MAX_HOLD_DAYS: int = Optimized.MAX_HOLD_DAYS
MAX_HOLD_DAYS_MIX: int = Optimized.MAX_HOLD_DAYS_MIX
MAX_HOLD_DAYS_MOM: int = Optimized.MAX_HOLD_DAYS_MOM
CASH_USAGE: float = 0.9  # 가용 현금의 90% 사용
PRE_CASH_RATIO: float = (
    0.30  # PO① 전일 스캔: 가용 현금의 30% (확정 일봉, 갭 리스크 있음)
)
INI_CASH_RATIO: float = (
    0.15  # PO② 오전 스캔: 가용 현금의 15% (장중 미확정 데이터 → 탐색적)
)
FIN_CASH_RATIO: float = (
    0.55  # PO③ 오후 스캔: 청산 후 현금의 55% (확정에 가까운 데이터 → 주력)
)
MIN_ALLOC: int = 10_000  # 종목당 최소 배분액 (1만원)
MAX_ALLOC_TIERS: list[tuple[int, float]] = [  # (총자산 상한, 배분 비율)
    (100_000_000, 0.15),  # ~1억: 15%
    (500_000_000, 0.10),  # 1~5억: 10%
    (1_000_000_000, 0.07),  # 5~10억: 7%
    (5_000_000_000, 0.05),  # 10~50억: 5%
]
MAX_ALLOC_RATIO_DEFAULT: float = 0.03  # 50억~: 3%
SELL_COST_RATE: float = 0.002  # 증권거래세 + 수수료 합산 (약 0.2%)
