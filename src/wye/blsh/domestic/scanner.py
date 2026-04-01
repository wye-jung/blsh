"""
매수 신호 스캐너
─────────────────────────────────────────────────────
대상: KOSPI(isu_ksp_ohlcv) / KOSDAQ(isu_ksd_ohlcv)

[0단계] 종목 필터 (scan_market SQL)
  - 최근 20일 평균 거래대금(acc_trdval) 10억 이상
  - 지수 환경 체크: KOSPI/KOSDAQ MA20 아래이면 해당 시장 스킵 (config.INDEX_DROP_LIMIT 이하만)

[1단계] DB 기반 OHLCV 지표 스캔 (15개 플래그)
  ┌──────────────────────────────────────────────────────┬──────┬────────┬──────┐
  │ MACD 골든크로스                                       │  MGC │  모멘텀│  +2  │
  │ MACD 예상 골든크로스 (히스토그램 상승 + 직전 음수)    │  MPGC│  중립  │  +1  │
  │ RSI 30 상향 돌파                                      │  RBO │  전환  │  +2  │
  │ RSI 과매도 상태 (30 미만)                             │  ROV │  전환  │  +1  │
  │ 볼린저 하단 반등 (전일 하단 이탈 → 당일 회복)         │  BBL │  전환  │  +1  │
  │ 볼린저 중간선 상향 돌파                               │  BBM │  중립  │  +1  │
  │ 거래량 급증 + 양봉 (20일 평균의 2배 이상)             │  VS  │  모멘텀│  +1  │
  │ 이동평균 정배열 전환 (5>20>60, 전일 미성립)           │  MAA │  모멘텀│  +1  │
  │ 스토캐스틱 과매도 골든크로스 (K>D, 50 미만)           │  SGC │  중립  │  +1  │
  │ 52주 신고가 돌파 (20일 평균 거래량의 1.5배 이상)      │  W52 │  모멘텀│  +2  │
  │ 눌림목 패턴 (MA20 상승 중, 5MA 이탈 후 복귀)          │  PB  │  모멘텀│  +2  │
  │ 망치형 캔들 (하단 꼬리 50%↑, 상단 꼬리 10%↓)        │  HMR │  전환  │  +1  │
  │ 장대 양봉 (양봉 크기 > ATR × 1.5)                    │  LB  │  모멘텀│  +2  │
  │ 모닝스타 (3일 반전 패턴)                              │  MS  │  전환  │  +2  │
  │ OBV 상승 추세 (3일 연속)                              │  OBV │  모멘텀│  +1  │
  └──────────────────────────────────────────────────────┴──────┴────────┴──────┘

  RBO / ROV는 elif 관계 (RSI 30 상향 돌파 시 ROV 미적용).

  점수 산출 (분리 트랙):
    - 모멘텀 점수(mom), 전환 점수(rev), 중립 점수(neu) 별도 집계
    - MOM: mom_cnt ≥ 2 and mom_cnt > rev_cnt → mom + neu
    - REV: rev_cnt ≥ 2 and rev_cnt > mom_cnt → rev + neu
    - MIX: mom_cnt > 0 and rev_cnt > 0 → max(mom, rev) + neu
    - WEAK: 그 외 (둘 다 약함) → mom + rev + neu

  → mode 컬럼: MOM(모멘텀) / REV(추세전환) / MIX(혼합) / WEAK

[2단계] 수급 보강 (1단계 점수 2점 이상 종목만)
  isu_ksp_info / isu_ksd_info 최근 5일 수급 추이 판별.
  DB 미보유 종목은 KIS API(investor_trade_by_stock_daily) fallback.

  ┌──────────────────────────────────────────┬──────┬──────┐
  │ 외국인 순매수 전환 (N일 매도→오늘 매수)  │ F_TRN│  +3  │
  │ 기관   순매수 전환 (N일 매도→오늘 매수)  │ I_TRN│  +3  │
  │ 외국인 3일 이상 연속 순매수              │ F_C3 │  +2  │
  │ 기관   3일 이상 연속 순매수              │ I_C3 │  +2  │
  │ 외국인 오늘만 순매수                     │ F_1  │  +1  │
  │ 기관   오늘만 순매수                     │ I_1  │  +1  │
  │ 외국인+기관 동시 해당 (위 조건 중 하나씩) │ FI   │  +1  │
  │ 개인만 대량 순매수 (외인·기관 없을 때)   │ P_OV │  -1  │
  └──────────────────────────────────────────┴──────┴──────┘

  수급 가산 상한: SUPPLY_CAP = +3 (기술 점수 대비 초과분 제거, 백테스트 검증).
  P_OV 종목은 [4단계]에서 PO 후보 제외.

[3단계] 업종 환경 조정
  업종지수 MA20 대비 괴리율(gap)로 패널티/보너스 적용.
  미매핑 종목: KOSPI → "코스피" / KOSDAQ → "코스닥" 전체 지수로 대체.

  gap < SECTOR_PENALTY_THRESHOLD (-5%) → SECTOR_PENALTY_PTS (-2점)
  gap ≥ 0%                             → SECTOR_BONUS_PTS   (현재 0점)

[4단계] PO 후보 선별 및 파일 생성
  조건: buy_score ≥ INVEST_MIN_SCORE, mode ∈ {MOM, MIX, REV}, P_OV 없음
  entry_price = ceil_tick(close + 0.5 × ATR)

  po-{date}-pre.json — 전일 스캔 (NXT 08:00 매수, 30%)
  po-{date}-ini.json — 오전 스캔 (KRX ~10:10 매수, 15%)
  po-{date}-fin.json — 청산 후 스캔 (NXT 15:30 매수, 55%, max_hold_days +1)
"""

import logging
from logging.handlers import TimedRotatingFileHandler
import numpy as np
import pandas as pd
from wye.blsh.database import query, ModelManager
from wye.blsh.domestic import reporter, Tick, Milestone
from wye.blsh.domestic import sector
from wye.blsh.domestic import PO_TYPE_PRE, PO_TYPE_INI, PO_TYPE_FIN, PO
from wye.blsh.domestic.config import (
    MACD_SHORT,
    MACD_LONG,
    MACD_SIGNAL,
    RSI_PERIOD,
    RSI_OVERSOLD,
    BB_PERIOD,
    BB_STD,
    STOCH_K,
    STOCH_D,
    STOCH_SMOOTH,
    MA_PERIODS,
    ATR_PERIOD,
    GAP_THRESHOLD,
    W52_VOL_MULT,
    LOOKBACK_DAYS,
    MIN_SCORE,
    ENRICH_SCORE,
    SUPPLY_CAP,
    TRDVAL_MIN,
    TRDVAL_DAYS,
    INDEX_MA_DAYS,
    INDEX_DROP_LIMIT,
    INVEST_MIN_SCORE,
    SECTOR_PENALTY_THRESHOLD,
    SECTOR_PENALTY_PTS,
    SECTOR_BONUS_PTS,
    ATR_SL_MULT,
    ATR_TP_MULT,
    MAX_HOLD_DAYS,
    MAX_HOLD_DAYS_MIX,
    MAX_HOLD_DAYS_MOM,
    SIGNAL_SCORES,
    SUPPLY_SCORES,
)
from wye.blsh.database.models import TradeCandidates
from wye.blsh.common import dtutils
from wye.blsh.common.env import CACHE_DIR, LOG_DIR

log = logging.getLogger(__name__)
_fh = TimedRotatingFileHandler(
    LOG_DIR / "scanner.log",
    when="midnight",
    backupCount=30,
    encoding="utf-8",
)
_fh.suffix = "%Y-%m-%d"
_fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
log.addHandler(_fh)


# ─────────────────────────────────────────
# 신호 분류 맵 (flag → 성격)
# ─────────────────────────────────────────
_REVERSAL_FLAGS = {"ROV", "RBO", "BBL", "HMR", "MS"}
_MOMENTUM_FLAGS = {"MGC", "MAA", "W52", "PB", "LB", "VS", "OBV"}
# NEUTRAL: MPGC, BBM, SGC (위 두 집합에 속하지 않는 모든 flag)


# ─────────────────────────────────────────
# 지표 계산
# ─────────────────────────────────────────
def calc_macd(c):
    es = c.ewm(span=MACD_SHORT, adjust=False).mean()
    el = c.ewm(span=MACD_LONG, adjust=False).mean()
    m = es - el
    s = m.ewm(span=MACD_SIGNAL, adjust=False).mean()
    return m, s, m - s


def calc_rsi(c, p=RSI_PERIOD):
    d = c.diff()
    g = d.clip(lower=0).ewm(alpha=1 / p, adjust=False).mean()
    l = (-d.clip(upper=0)).ewm(alpha=1 / p, adjust=False).mean()
    return 100 - 100 / (1 + g / l.replace(0, np.nan))


def calc_bb(c, p=BB_PERIOD, k=BB_STD):
    m = c.rolling(p).mean()
    s = c.rolling(p).std()
    return m + k * s, m, m - k * s


def calc_atr(h, l, c, p=ATR_PERIOD):
    pc = c.shift(1)
    tr = pd.concat([h - l, (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
    return tr.ewm(span=p, adjust=False).mean()


def calc_stoch(h, l, c, k=STOCH_K, d=STOCH_D, sm=STOCH_SMOOTH):
    lo = l.rolling(k).min()
    hi = h.rolling(k).max()
    rk = 100 * (c - lo) / (hi - lo).replace(0, np.nan)
    pk = rk.rolling(sm).mean()
    return pk, pk.rolling(d).mean()


def calc_obv(c, v):
    sign = np.sign(c.diff()).fillna(0)
    return (sign * v).cumsum()


def _signal_score(signal: str) -> tuple[str, int]:
    return signal, SIGNAL_SCORES.get(signal, 0)


def _supply_score(supply: str) -> tuple[str, int]:
    return supply, SUPPLY_SCORES.get(supply, 0)


# ─────────────────────────────────────────
# 매수 신호 평가
# ─────────────────────────────────────────
def evaluate_buy(close, high, low, volume, opn=None):
    min_len = MACD_LONG + MACD_SIGNAL + 5
    if len(close) < min_len:
        return 0, [], {}

    macd, sig, hist = calc_macd(close)
    rsi = calc_rsi(close)
    bbu, bbm, bbl = calc_bb(close)
    atr = calc_atr(high, low, close)
    sk, sd = calc_stoch(high, low, close)
    mas = {p: close.rolling(p).mean() for p in MA_PERIODS}
    obv = calc_obv(close, volume) if volume is not None else None

    c0, c1 = close.iloc[-1], close.iloc[-2]
    h0, h1 = high.iloc[-1], high.iloc[-2]
    l0, l1 = low.iloc[-1], low.iloc[-2]
    has_opn = opn is not None and len(opn) >= 3
    o0 = opn.iloc[-1] if has_opn else None
    m0, m1 = macd.iloc[-1], macd.iloc[-2]
    s0, s1 = sig.iloc[-1], sig.iloc[-2]
    r0, r1 = rsi.iloc[-1], rsi.iloc[-2]
    bbu0 = bbu.iloc[-1]
    bbm0, bbm1 = bbm.iloc[-1], bbm.iloc[-2]
    bbl0, bbl1 = bbl.iloc[-1], bbl.iloc[-2]
    sk0, sk1 = sk.iloc[-1], sk.iloc[-2]
    sd0, sd1 = sd.iloc[-1], sd.iloc[-2]
    atr0 = atr.iloc[-1]
    ma5 = mas[5]
    ma20 = mas[20]
    ma60 = mas[60]

    # 분리 트랙 점수 집계: (flag, points) 쌍으로 수집 후 분류
    signals: list[tuple[str, int]] = []

    # 1. MACD 골든크로스 (+2) → MGC (모멘텀)
    if m0 > s0 and m1 < s1:
        signals.append(_signal_score("MGC"))
    # 2. MACD 예상 골든크로스 (+1) → MPGC (중립)
    elif (
        m0 < s0
        and len(hist) >= 3
        and hist.iloc[-3] < hist.iloc[-2] < hist.iloc[-1] < 0
        and abs(s0) > 0
        and (s0 - m0) / abs(s0) <= GAP_THRESHOLD
    ):
        signals.append(_signal_score("MPGC"))

    # 3. RSI 30 상향 돌파 (+2) → RBO (전환)
    if r0 > RSI_OVERSOLD and r1 <= RSI_OVERSOLD:
        signals.append(_signal_score("RBO"))
    # 4. RSI 과매도 (+1) → ROV (전환)
    elif r0 < RSI_OVERSOLD:
        signals.append(_signal_score("ROV"))

    # 5. 볼린저 하단 반등 (+1) → BBL (전환)
    if l1 < bbl1 and c0 > bbl0:
        signals.append(_signal_score("BBL"))

    # 6. 볼린저 중간선 상향 돌파 (+1) → BBM (중립)
    if c0 > bbm0 and c1 <= bbm1:
        signals.append(_signal_score("BBM"))

    # 7. 거래량 급증 + 양봉 (+1) → VS (모멘텀)
    if volume is not None and len(volume) >= 20:
        vol_avg = volume.iloc[-20:-1].mean()
        if volume.iloc[-1] > vol_avg * 2 and c0 > c1:
            signals.append(_signal_score("VS"))

    # 8. 이동평균 정배열 전환 (+1) → MAA (모멘텀)
    if ma5.iloc[-1] > ma20.iloc[-1] > ma60.iloc[-1] and not (
        ma5.iloc[-2] > ma20.iloc[-2] > ma60.iloc[-2]
    ):
        signals.append(_signal_score("MAA"))

    # 9. 스토캐스틱 과매도 교차 (+1) → SGC (중립)
    if sk0 > sd0 and sk1 < sd1 and sk0 < 50:
        signals.append(_signal_score("SGC"))

    # 10. 52주 신고가 돌파 (+2) → W52 (모멘텀)
    if len(close) >= 252 and volume is not None and len(volume) >= 21:
        w52_high = high.iloc[-252:-1].max()
        vol_20_avg = volume.iloc[-21:-1].mean()
        if h0 > w52_high and volume.iloc[-1] > vol_20_avg * W52_VOL_MULT:
            signals.append(_signal_score("W52"))

    # 11. 눌림목 패턴 (+2) → PB (모멘텀)
    if (
        ma20.iloc[-1] > ma20.iloc[-5]
        and (c1 < ma5.iloc[-2] or l1 < ma5.iloc[-2])
        and c0 > ma5.iloc[-1]
        and c0 > ma20.iloc[-1]
    ):
        signals.append(_signal_score("PB"))

    # 12. 망치형 캔들 (+1) → HMR (전환)
    if o0 is not None:
        body = abs(c0 - o0)
        candle_range = h0 - l0
        if candle_range > 0:
            lower_wick = min(c0, o0) - l0
            upper_wick = h0 - max(c0, o0)
            if (
                lower_wick > candle_range * 0.5
                and upper_wick < candle_range * 0.1
                and body < candle_range * 0.3
            ):
                signals.append(_signal_score("HMR"))

    # 13. 장대 양봉 (+2) → LB (모멘텀)
    if o0 is not None and c0 > o0 and (c0 - o0) > atr0 * 1.5:
        signals.append(_signal_score("LB"))

    # 14. 모닝스타 (+2) → MS (전환)
    if has_opn and len(close) >= 3:
        c_2, c_1, c_0 = close.iloc[-3], close.iloc[-2], close.iloc[-1]
        o_2, o_1, o_0 = opn.iloc[-3], opn.iloc[-2], opn.iloc[-1]
        body_d1 = o_2 - c_2
        body_d2 = abs(c_1 - o_1)
        body_d3 = c_0 - o_0
        if (
            body_d1 > atr0 * 0.7
            and body_d2 < atr0 * 0.3
            and body_d3 > atr0 * 0.7
            and c_0 > (o_2 + c_2) / 2
        ):
            signals.append(_signal_score("MS"))

    # 15. OBV 상승 추세 (+1) → OBV (모멘텀)
    if obv is not None and len(obv) >= 3:
        if obv.iloc[-3] < obv.iloc[-2] < obv.iloc[-1]:
            signals.append(_signal_score("OBV"))

    # ── 분리 트랙 점수 집계
    flags = [f for f, _ in signals]
    flag_set = set(flags)
    mom_score = sum(pts for f, pts in signals if f in _MOMENTUM_FLAGS)
    rev_score = sum(pts for f, pts in signals if f in _REVERSAL_FLAGS)
    neu_score = sum(
        pts for f, pts in signals if f not in (_MOMENTUM_FLAGS | _REVERSAL_FLAGS)
    )

    rev_cnt = len(flag_set & _REVERSAL_FLAGS)
    mom_cnt = len(flag_set & _MOMENTUM_FLAGS)

    # ── mode 분류
    if mom_cnt >= 2 and mom_cnt > rev_cnt:
        mode = "MOM"
    elif rev_cnt >= 2 and rev_cnt > mom_cnt:
        mode = "REV"
    elif mom_cnt > 0 and rev_cnt > 0:
        mode = "MIX"
    else:
        mode = "WEAK"

    # ── 최종 점수: MIX일 때 약한 쪽 제거
    if mode == "MOM":
        score = mom_score + neu_score
    elif mode == "REV":
        score = rev_score + neu_score
    elif mode == "MIX":
        score = max(mom_score, rev_score) + neu_score
    else:
        # WEAK: 둘 다 약하므로 전부 합산 (기존과 동일)
        score = mom_score + rev_score + neu_score

    # ── 매수가 / 손절 / 익절 (호가 단위 보정)
    entry_price = Tick.ceil_tick(c0 + 0.5 * atr0)
    stop_loss = Tick.floor_tick(c0 - ATR_SL_MULT * atr0)
    take_profit = Tick.ceil_tick(c0 + ATR_TP_MULT * atr0)

    indicators = {
        "mode": mode,
        "close": round(float(c0), 2),
        "atr": round(float(atr0), 4),
        "rsi": round(float(r0), 2),
        "macd": round(float(m0), 4),
        "macd_signal": round(float(s0), 4),
        "macd_hist": round(float(hist.iloc[-1]), 4),
        "bb_upper": round(float(bbu0), 2),
        "bb_middle": round(float(bbm0), 2),
        "bb_lower": round(float(bbl0), 2),
        "stoch_k": round(float(sk0), 2),
        "stoch_d": round(float(sd0), 2),
        "entry_price": entry_price,
        "stop_loss": stop_loss,
        "take_profit": take_profit,
    }
    return score, flags, indicators


# ─────────────────────────────────────────
# 공통: DataFrame → 신호 평가
# ─────────────────────────────────────────
def scan_dataframe(
    ticker,
    name,
    market,
    df,
    base_date,
    close_col,
    high_col,
    low_col,
    vol_col=None,
    open_col=None,
):
    if df is None:
        return None

    df = df.sort_index().apply(pd.to_numeric, errors="coerce")
    df = df[df.index <= base_date]

    if len(df) < LOOKBACK_DAYS // 3:
        return None

    close = df[close_col].dropna()
    high = df[high_col].dropna()
    low = df[low_col].dropna()
    vol = df[vol_col].dropna() if vol_col and vol_col in df.columns else None
    opn = df[open_col].dropna() if open_col and open_col in df.columns else None

    idx = close.index.intersection(high.index).intersection(low.index)
    if vol is not None:
        idx = idx.intersection(vol.index)
    if opn is not None:
        idx = idx.intersection(opn.index)
    close, high, low = close[idx], high[idx], low[idx]
    if vol is not None:
        vol = vol[idx]
    if opn is not None:
        opn = opn[idx]

    score, flags, ind = evaluate_buy(close, high, low, vol, opn)
    if score < MIN_SCORE:
        return None

    icon = "🔴" if score >= 5 else "🟡" if score >= 3 else "🔵"
    log.debug(
        f"  {icon} [{score:2d}pt] {ticker:10s} {name[:18]:18s} ({market}) {flags}"
    )

    return {
        "base_date": base_date,
        "entry_date": None,
        "ticker": ticker,
        "name": name,
        "market": market,
        "buy_score": score,
        "_tech_score": score,  # 수급 가산 전 기술 점수 (캡 계산용)
        "buy_flags": ",".join(flags),
        "foreign_netbuy": None,
        "inst_netbuy": None,
        "indi_netbuy": None,
        **ind,
    }


# ─────────────────────────────────────────
# 시장별 스캔
# ─────────────────────────────────────────
def scan_market(
    table,
    market,
    start,
    base_date,
    name_map,
    close_col="tdd_clsprc",
    high_col="tdd_hgprc",
    low_col="tdd_lwprc",
    vol_col="acc_trdvol",
    open_col="tdd_opnprc",
):
    log.debug(f"[{market}] {table} 스캔 시작  (기준일: {base_date})")

    df_all = pd.DataFrame(
        query.get_ohlcv(
            table,
            close_col,
            high_col,
            low_col,
            vol_col,
            {
                "start": start,
                "base_date": base_date,
                "filter_start": dtutils.add_days(base_date, TRDVAL_DAYS * -2),
                "min_val": TRDVAL_MIN,
            },
            open_col=open_col,
        )
    )

    results = []
    for ticker, group in df_all.groupby("isu_srt_cd"):
        name = name_map.get(ticker, ticker)
        row = scan_dataframe(
            ticker,
            name,
            market,
            group.set_index("trd_dd"),
            base_date,
            close_col,
            high_col,
            low_col,
            vol_col,
            open_col,
        )
        if row is not None:
            results.append(row)

    log.debug(f"[{market}] 신호 종목: {len(results)}건 (거래대금 필터 후)")
    return results


# ─────────────────────────────────────────
# [2단계] DB 수급 보강 + KIS API fallback
# ─────────────────────────────────────────
def fetch_investor_daily(ticker, base_date, n_days=5):
    """종목별 투자자매매동향(일별). 반환: (frgn_list, orgn_list) 오래된→최신."""
    from wye.blsh.kis import kis_auth as ka
    from wye.blsh.kis.domestic_stock import domestic_stock_functions as ds

    try:
        if not ka.getTREnv():
            ka.auth()
        result = ds.investor_trade_by_stock_daily(
            fid_cond_mrkt_div_code="J",
            fid_input_iscd=ticker,
            fid_input_date_1=base_date,
            fid_org_adj_prc="",
            fid_etc_cls_code="",
            tr_cont="",
            depth=0,
            max_depth=1,
        )

        if result is None:
            return [], []

        if hasattr(result, "iloc"):
            df = result.head(n_days).iloc[::-1].reset_index(drop=True)
            frgn = df["frgn_ntby_qty"].astype(float).astype(int).tolist()
            orgn = df["orgn_ntby_qty"].astype(float).astype(int).tolist()
            return frgn, orgn

        if isinstance(result, tuple) and len(result) >= 2:
            df = result[1].head(n_days).iloc[::-1].reset_index(drop=True)
            frgn = df["frgn_ntby_qty"].astype(float).astype(int).tolist()
            orgn = df["orgn_ntby_qty"].astype(float).astype(int).tolist()
            return frgn, orgn

    except Exception as e:
        log.debug(f"  investor_daily 오류 ({ticker}): {e}")
    return [], []


def classify_supply(qty_list):
    """
    수급 흐름 분류 → (flag_suffix, score)
      TRN (+3): 직전 N-1일 전부 순매도/0 → 오늘 순매수 (진정한 전환)
      C3  (+2): 3일 이상 연속 순매수
      1   (+1): 오늘만 순매수
      None ( 0): 해당 없음
    """
    if not qty_list or len(qty_list) < 2:
        return None, 0
    today = qty_list[-1]
    history = qty_list[:-1]
    if today <= 0:
        return None, 0
    # TRN: 최소 2일 이상 매도/0 후 전환이어야 의미 있음
    if len(history) >= 2 and all(q <= 0 for q in history):
        return _supply_score("TRN")
    consec = 1
    for q in reversed(history):
        if q > 0:
            consec += 1
        else:
            break
    if consec >= 3:
        return _supply_score("C3")
    return _supply_score("1")


def enrich_with_db(results: list, base_date: str) -> list:
    """[2단계] 수급 판별 후 점수 보강. DB 미보유 종목은 KIS API fallback."""
    candidates = [
        r
        for r in results
        if r["buy_score"] >= ENRICH_SCORE and r["market"] in ("KOSPI", "KOSDAQ")
    ]
    if not candidates:
        return results

    log.debug(f"[수급 보강] 대상 {len(candidates)}종목  (기준일: {base_date})")

    kospi_ticks = [r["ticker"] for r in candidates if r["market"] == "KOSPI"]
    kosdaq_ticks = [r["ticker"] for r in candidates if r["market"] == "KOSDAQ"]

    def fetch_supply_from_db(table, tickers):
        if not tickers:
            return {}
        try:
            df = pd.DataFrame(query.get_netbid_trdvol(table, tickers, base_date))
        except Exception as e:
            log.warning(f"  DB 수급 조회 오류 ({table}): {e}")
            return {}
        result = {}
        for ticker, grp in df.groupby("isu_srt_cd"):
            recent = (
                grp.sort_values("trd_dd", ascending=False).head(5).sort_values("trd_dd")
            )
            result[ticker] = {
                "frgn": recent["frgn_qty"].fillna(0).tolist(),
                "inst": recent["inst_qty"].fillna(0).tolist(),
                "today_frgn": recent["frgn_qty"].iloc[-1] if len(recent) else 0,
                "today_inst": recent["inst_qty"].iloc[-1] if len(recent) else 0,
                "today_indi": recent["indi_qty"].iloc[-1] if len(recent) else 0,
            }
        return result

    supply_db = {
        **fetch_supply_from_db("isu_ksp_info", kospi_ticks),
        **fetch_supply_from_db("isu_ksd_info", kosdaq_ticks),
    }

    missing = [r for r in candidates if r["ticker"] not in supply_db]
    supply_api = {}
    if missing:
        log.debug(f"  DB 미보유 {len(missing)}종목 → KIS API fallback")
        try:
            for row in missing:
                fl, ol = fetch_investor_daily(row["ticker"], base_date, n_days=5)
                if fl or ol:
                    supply_api[row["ticker"]] = {
                        "frgn": fl,
                        "inst": ol,
                        "today_frgn": fl[-1] if fl else 0,
                        "today_inst": ol[-1] if ol else 0,
                        "today_indi": None,
                    }
        except Exception as e:
            log.warning(f"  KIS API fallback 오류: {e}")

    supply_all = {**supply_db, **supply_api}
    ticker_to_idx = {r["ticker"]: i for i, r in enumerate(results)}

    for row in candidates:
        t = row["ticker"]
        idx = ticker_to_idx[t]
        sup = supply_all.get(t)
        if not sup:
            continue

        f_sig, f_sc = classify_supply(sup["frgn"])
        o_sig, o_sc = classify_supply(sup["inst"])

        results[idx]["foreign_netbuy"] = sup["today_frgn"]
        results[idx]["inst_netbuy"] = sup["today_inst"]
        results[idx]["indi_netbuy"] = sup.get("today_indi")

        if f_sc > 0:
            results[idx]["buy_score"] += f_sc
            results[idx]["buy_flags"] += f",F_{f_sig}"
            icon = "🔥" if f_sig == "TRN" else ("💰💰" if f_sig == "C3" else "💰")
            log.debug(
                f"  {icon} 외국인 {f_sig}({f_sc:+d}): {t} {row['name']}  {sup['frgn']}"
            )

        if o_sc > 0:
            results[idx]["buy_score"] += o_sc
            results[idx]["buy_flags"] += f",I_{o_sig}"
            icon = "🔥" if o_sig == "TRN" else ("🏦🏦" if o_sig == "C3" else "🏦")
            log.debug(
                f"  {icon} 기관   {o_sig}({o_sc:+d}): {t} {row['name']}  {sup['inst']}"
            )

        if f_sc > 0 and o_sc > 0:
            results[idx]["buy_score"] += 1
            results[idx]["buy_flags"] += ",FI"
            log.debug(f"  ⭐ 외국인+기관 동시: {t} {row['name']}")

        # 수급 가산 상한: 기술 점수 보호 (백테스트 검증, 2026-03-29)
        tech_score = results[idx]["_tech_score"]
        supply_bonus = results[idx]["buy_score"] - tech_score
        if supply_bonus > SUPPLY_CAP:
            results[idx]["buy_score"] = tech_score + SUPPLY_CAP

        indi = sup.get("today_indi") or 0
        frgn = sup["today_frgn"] or 0
        inst = sup["today_inst"] or 0
        if indi > 0 and frgn <= 0 and inst <= 0 and indi > abs(frgn) + abs(inst):
            results[idx]["buy_score"] -= 1
            results[idx]["buy_flags"] += ",P_OV"
            log.debug(
                f"  ⚠️  개인 과매수 패널티(-1): {t} {row['name']}  개인={indi:+.0f}"
            )

    return results


def check_index_above_ma(
    idx_nm, base_date, ma_days=20, drop_limit=INDEX_DROP_LIMIT, idx_clss=None
):
    """지수 환경 체크. MA 대비 -drop_limit 이하일 때만 스캔 스킵."""
    try:
        df = pd.DataFrame(
            query.get_index_clsprc(idx_nm, base_date, ma_days, idx_clss=idx_clss)
        )
        if len(df) < ma_days:
            return True
        prices = df["clsprc_idx"].astype(float)
        cur = float(prices.iloc[0])  # DESC 정렬: [0]=최신
        ma = prices.iloc[
            1:
        ].mean()  # 당일 제외 최근 N일 평균 (ma_days+1개 요청, [0]=당일 제외)
        gap_pct = (cur - ma) / ma
        skip = gap_pct < -drop_limit
        if skip:
            status = f"스킵 🚫 (MA 대비 {gap_pct:.1%})"
        elif gap_pct < 0:
            status = f"허용 ⚠️  (MA 대비 {gap_pct:.1%}, 임계 -{drop_limit:.0%} 미만)"
        else:
            status = f"위 ✅ (MA 대비 {gap_pct:+.1%})"
        log.debug(
            f"[지수 환경] {idx_nm}  현재가={cur:.2f}  {ma_days}MA={ma:.2f}  → {status}"
        )
        return not skip
    except Exception as e:
        log.warning(f"지수 환경 체크 실패 ({idx_nm}): {e}")
        return True


# ─────────────────────────────────────────
# 스캔 및 대상 선별
# ─────────────────────────────────────────
def scan(base_date=None, report: bool = False) -> pd.DataFrame:
    if base_date is None:
        base_date = dtutils.get_latest_biz_date()

    if not query.has_ohlcv_data(base_date):
        log.warning(f"{base_date} - ohlcv 데이터가 없습니다")
        return pd.DataFrame()

    start = dtutils.add_days(base_date, LOOKBACK_DAYS * -1)
    name_map = query.get_ticker_name_map()

    results = []

    if check_index_above_ma(
        "코스피", base_date, INDEX_MA_DAYS, idx_clss=sector.IDX_CLSS_KOSPI
    ):
        results += scan_market("isu_ksp_ohlcv", "KOSPI", start, base_date, name_map)
    else:
        log.warning("[KOSPI] 지수 20MA 아래 → 스캔 스킵")

    if check_index_above_ma(
        "코스닥", base_date, INDEX_MA_DAYS, idx_clss=sector.IDX_CLSS_KOSDAQ
    ):
        results += scan_market("isu_ksd_ohlcv", "KOSDAQ", start, base_date, name_map)
    else:
        log.warning("[KOSDAQ] 지수 20MA 아래 → 스캔 스킵")

    results = enrich_with_db(results, base_date)

    df = pd.DataFrame(results)

    if not df.empty:
        for col in ("foreign_netbuy", "inst_netbuy", "indi_netbuy"):
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if report:
        reporter.print_general_summary(df)

    return df


# ─────────────────────────────────────────
# 업종지수 패널티/보너스
# ─────────────────────────────────────────
_SECTOR_MAP_FILE = CACHE_DIR / "sector_map.json"


def _load_ticker_sector_map(base_date: str = "") -> dict[str, str]:
    """KOSPI 종목코드 → 업종지수명 매핑 (캐시 파일, base_date 기준 1회 갱신).

    Note: KIS 마스터는 항상 현재 데이터만 제공. 과거 base_date로 스캔 시
    현재 업종 매핑이 적용되는 한계가 있으나, 업종 변경은 매우 드뭄.
    """
    import json

    cache_date = base_date or dtutils.today()
    if _SECTOR_MAP_FILE.exists():
        try:
            data = json.loads(_SECTOR_MAP_FILE.read_text())
            if data.get("_date") == cache_date:
                return data.get("map", {})
        except Exception:
            pass

    log.debug("[업종매핑] KIS 마스터 다운로드…")
    result: dict[str, str] = {}

    # KOSPI
    try:
        from wye.blsh.kis.domestic_stock.domestic_stock_info import get_kospi_info

        kp = get_kospi_info()
        for _, row in kp.iterrows():
            ticker = str(row["단축코드"]).strip()
            mid = int(row.get("지수업종중분류", 0) or 0)
            big = int(row.get("지수업종대분류", 0) or 0)
            idx_nm = sector.KOSPI_MID_TO_IDX.get(mid) or sector.KOSPI_BIG_TO_IDX.get(
                big
            )
            if idx_nm:
                result[ticker] = idx_nm
    except Exception as e:
        log.warning(f"  KOSPI 마스터 로드 실패: {e}")

    # KOSDAQ
    try:
        from wye.blsh.kis.domestic_stock.domestic_stock_info import get_kosdaq_info

        kd = get_kosdaq_info()
        for _, row in kd.iterrows():
            ticker = str(row["단축코드"]).strip()
            mid = int(row.get("지수 업종 중분류 코드", 0) or 0)
            big = int(row.get("지수업종 대분류 코드", 0) or 0)
            idx_nm = sector.KOSDAQ_MID_TO_IDX.get(mid) or sector.KOSDAQ_BIG_TO_IDX.get(
                big
            )
            if idx_nm:
                result[ticker] = idx_nm
    except Exception as e:
        log.warning(f"  KOSDAQ 마스터 로드 실패: {e}")

    # 캐시 저장 (빈 결과면 저장 스킵)
    if result:
        _SECTOR_MAP_FILE.parent.mkdir(parents=True, exist_ok=True)
        _SECTOR_MAP_FILE.write_text(
            json.dumps({"_date": cache_date, "map": result}, ensure_ascii=False)
        )
        log.debug(f"  KOSPI+KOSDAQ 업종매핑: {len(result)}종목 캐시 저장")
    else:
        log.warning("  업종매핑 0건 → 캐시 미저장")

    return result


def _get_sector_gap(
    idx_nm: str, base_date: str, ma_days: int = 20, idx_clss: str = None
) -> float:
    """업종지수의 MA20 괴리율. 데이터 없으면 0.0 (중립)."""
    rows = query.get_index_clsprc(idx_nm, base_date, ma_days, idx_clss=idx_clss)
    if not rows or len(rows) < ma_days:
        return 0.0
    prices = [float(r["clsprc_idx"]) for r in rows]
    cur = prices[0]  # 최신 (당일)
    prev = prices[1:]  # 당일 제외
    ma = sum(prev) / len(prev)
    return (cur - ma) / ma if ma else 0.0


def _apply_sector_penalty(df: pd.DataFrame, base_date: str) -> pd.DataFrame:
    """업종지수 환경에 따라 buy_score에 패널티/보너스 적용."""
    if SECTOR_PENALTY_PTS == 0 and SECTOR_BONUS_PTS == 0:
        return df

    sector_map = _load_ticker_sector_map(base_date)
    gap_cache: dict[tuple[str, str], float] = {}  # (sec_nm, idx_clss) → gap

    def get_gap(ticker: str, market: str) -> float:
        # KOSPI 미매핑 → "코스피" 전체 지수, KOSDAQ → "코스닥" 전체 지수
        fallback = "코스피" if market == "KOSPI" else "코스닥"
        sec_nm = sector_map.get(ticker, fallback)
        if not sec_nm:
            return 0.0
        idx_clss = sector.get_idx_clss(market)
        key = (sec_nm, idx_clss)
        if key not in gap_cache:
            gap_cache[key] = _get_sector_gap(sec_nm, base_date, idx_clss=idx_clss)
        return gap_cache[key]

    adjustments = []
    for _, row in df.iterrows():
        gap = get_gap(row["ticker"], row["market"])
        adj = 0
        if SECTOR_PENALTY_PTS != 0 and gap < SECTOR_PENALTY_THRESHOLD:
            adj = SECTOR_PENALTY_PTS
        elif SECTOR_BONUS_PTS != 0 and gap >= 0:
            adj = SECTOR_BONUS_PTS
        adjustments.append(adj)

    df = df.copy()
    df["buy_score"] = df["buy_score"] + adjustments
    applied = sum(1 for a in adjustments if a != 0)
    if applied:
        log.debug(f"[업종패널티] {applied}종목 점수 조정 ({len(gap_cache)}업종 조회)")
    return df


def find_candidates(base_date=None, report: bool = False) -> pd.DataFrame:
    if base_date is None:
        base_date = dtutils.get_latest_biz_date()

    sdf = scan(base_date, report)
    if sdf.empty:
        return sdf

    # 업종 패널티/보너스 적용
    sdf = _apply_sector_penalty(sdf, base_date)

    cand_mask = (
        (sdf["buy_score"] >= INVEST_MIN_SCORE)
        & (sdf["mode"].isin(["MIX", "MOM", "REV"]))
        & (~sdf["buy_flags"].str.contains("P_OV", na=False))
    )
    df = sdf[cand_mask].copy()
    if report:
        reporter.print_invest_report(df)

    if df.empty:
        return df

    df["atr_sl_mult"] = ATR_SL_MULT
    df["atr_tp_mult"] = ATR_TP_MULT
    conditions = [
        df["mode"] == "MIX",
        df["mode"] == "MOM",
        df["mode"] == "REV",
    ]
    days = [MAX_HOLD_DAYS_MIX, MAX_HOLD_DAYS_MOM, MAX_HOLD_DAYS]
    df["max_hold_days"] = np.select(conditions, days, default=MAX_HOLD_DAYS)

    today = dtutils.today()
    ctime = dtutils.ctime()
    if base_date == today and ctime < dtutils.add_time(
        Milestone.LIQUIDATE_TIME, minutes=-3
    ):
        entry_date = today
        if ctime < Milestone.NXT_OPEN_TIME:
            po_type = PO_TYPE_PRE
        elif ctime < dtutils.add_time(Milestone.KRX_EARLY_TIME, minutes=-3):
            po_type = PO_TYPE_INI
        elif ctime > dtutils.add_time(Milestone.LIQUIDATE_TIME, hours=-1):
            df["max_hold_days"] = df["max_hold_days"] + 1
            po_type = PO_TYPE_FIN
        else:
            po_type = ""
    else:
        entry_date = dtutils.next_biz_date(base_date)
        po_type = PO_TYPE_PRE

    expiry_cache: dict[tuple, str | None] = {}

    def _get_expiry(ed, mhd):
        key = (ed, int(mhd))
        if key not in expiry_cache:
            expiry_cache[key] = dtutils.add_biz_days(str(ed), int(mhd))
        return expiry_cache[key]

    df["po_type"] = po_type
    df["entry_date"] = entry_date
    df["expiry_date"] = df.apply(
        lambda r: _get_expiry(entry_date, r["max_hold_days"]), axis=1
    )
    return df


def issue_po(base_date=None):
    df = find_candidates(base_date, True)
    if not df.empty:
        df = df[
            [
                "base_date",
                "ticker",
                "name",
                "market",
                "buy_score",
                "mode",
                "entry_price",
                "atr",
                "atr_sl_mult",
                "atr_tp_mult",
                "max_hold_days",
                "po_type",
                "entry_date",
                "expiry_date",
            ]
        ]
        po_type = df.iloc[0]["po_type"]
        entry_date = df.iloc[0]["entry_date"]

        if po_type and entry_date:
            po = PO(po_type, entry_date)
            po.create(df.set_index("ticker").to_dict("index"))

            model_manager = ModelManager(TradeCandidates)
            model_manager.delete(entry_date=entry_date, po_type=po_type)
            model_manager.create(df)


if __name__ == "__main__":
    # log.setLevel(logging.DEBUG)
    find_candidates(report=True)
