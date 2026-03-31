class Optimized:
    INVEST_MIN_SCORE: int = 9
    SECTOR_PENALTY_THRESHOLD: float = -0.03  # 업종지수 MA20 대비 해당값 이하
    SECTOR_PENALTY_PTS: int = 0
    SECTOR_BONUS_PTS: int = 1  # 업종지수 MA20 이상일 때
    ATR_SL_MULT: float = 3.0
    ATR_TP_MULT: float = 1.5
    TP1_MULT: float = 1.5  # 1차 익절: buy + ATR × TP1_MULT
    TP1_RATIO: float = 1.0  # 1차 익절 매도 비율 (1.0 = 전량)
    MAX_HOLD_DAYS: int = 3
    MAX_HOLD_DAYS_MIX: int = 5
    MAX_HOLD_DAYS_MOM: int = 3


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
ENRICH_SCORE: int = 2  # 수급 보강 최소 점수
SUPPLY_CAP: int = 3  # 수급 가산 상한 (백테스트 검증, 2026-03-29)
TRDVAL_MIN: int = 1_000_000_000  # 최근 20일 평균 거래대금 최소값 (10억)
TRDVAL_DAYS: int = 20
INDEX_MA_DAYS: int = 20  # 지수 환경 체크 이동평균 기간
INDEX_DROP_LIMIT: float = (
    0.05  # MA 대비 괴리율 -5% 이하일 때만 시장 전체 스캔 스킵 (재앙 수준)
)
INVEST_MIN_SCORE: int = (
    Optimized.INVEST_MIN_SCORE
)  # 투자 적격 최소 점수 (백테스트 검증)
SECTOR_PENALTY_THRESHOLD: float = (
    Optimized.SECTOR_PENALTY_THRESHOLD
)  # 업종지수 MA20 대비 해당값 이하
SECTOR_PENALTY_PTS: int = Optimized.SECTOR_PENALTY_PTS
SECTOR_BONUS_PTS: int = Optimized.SECTOR_BONUS_PTS  # 업종지수 MA20 이상일 때

SIGNAL_SCORES = {
    "MGC": 2,
    "MPGC": 1,
    "RBO": 2,
    "ROV": 1,
    "BBL": 1,
    "BBM": 1,
    "VS": 1,
    "MAA": 0,
    "SGC": 1,
    "W52": 3,
    "PB": 2,
    "HMR": 1,
    "LB": 2,
    "MS": 2,
    "OBV": 1,
}

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
SELL_COST_RATE: float = 0.002  # 증권거래세 + 수수료 합산 (약 0.2%)
