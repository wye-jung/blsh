"""
매수 신호 스캐너 v11
─────────────────────────────────────────────────────
대상: KOSPI(isu_ksp_ohlcv) / KOSDAQ(isu_ksd_ohlcv)

사용법:
  python scanner.py                  # 최근 영업일 기준
  python scanner.py --date 20260312  # 날짜 지정

⚠️  장 마감 후(16:00 이후) 실행 권장
    장중 실행 시 DB 수급 데이터(잠정치)와 KIS API 데이터 신뢰도 저하

[0단계] 종목 필터 (scan_market SQL)
  - 최근 20일 평균 거래대금(acc_trdval) 10억 이상
  - 지수 환경 체크: KOSPI/KOSDAQ 20MA 아래이면 해당 시장 스킵

[1단계] DB 기반 OHLCV 지표 스캔                              flag   성격
  ┌─────────────────────────────────────────┬──────┬────────┬──────┐
  │ MACD 골든크로스                          │  +2  │ MGC    │  모멘텀│
  │ MACD 예상 골든크로스                     │  +1  │ MPGC   │  중립  │
  │ RSI 30 상향 돌파                         │  +2  │ RBO    │  전환  │
  │ RSI 과매도 (< 30)                        │  +1  │ ROV    │  전환  │
  │ 볼린저 하단 반등                         │  +1  │ BBL    │  전환  │
  │ 볼린저 중간선 상향 돌파                  │  +1  │ BBM    │  중립  │
  │ 거래량 급증 + 양봉 (2배)                 │  +1  │ VS     │  모멘텀│
  │ 이동평균 정배열 전환 (5>20>60)           │  +1  │ MAA    │  모멘텀│
  │ 스토캐스틱 과매도 교차                   │  +1  │ SGC    │  중립  │
  │ 52주 신고가 돌파 (20일 최대 거래량 돌파) │  +2  │ W52    │  모멘텀│
  │ 눌림목 패턴 (5MA 종가/저가 이탈 후 복귀) │  +2  │ PB     │  모멘텀│
  │ 망치형 캔들                              │  +1  │ HMR    │  전환  │
  │ 장대 양봉                                │  +2  │ LB     │  모멘텀│
  │ 모닝스타 (3일 반전 패턴)                 │  +2  │ MS     │  전환  │
  │ OBV 상승 추세 (3일 연속)                 │  +1  │ OBV    │  모멘텀│
  └─────────────────────────────────────────┴──────┴────────┴──────┘

  → mode 컬럼: MOM(모멘텀) / REV(추세전환) / MIX(혼합) / WEAK

[2단계] DB 수급 보강 (1단계 점수 2점 이상 종목만)
  isu_ksp_info / isu_ksd_info 최근 5일 수급 추이 판별
  DB 미보유 종목은 KIS API(investor_trade_by_stock_daily) fallback

  ┌──────────────────────────────────────────┬──────┬──────┐
  │ 외국인 순매수 전환 (N일 매도→오늘 매수)  │  +3  │ F_TRN│
  │ 기관   순매수 전환 (N일 매도→오늘 매수)  │  +3  │ I_TRN│
  │ 외국인 3일 이상 연속 순매수              │  +2  │ F_C3 │
  │ 기관   3일 이상 연속 순매수              │  +2  │ I_C3 │
  │ 외국인 오늘만 순매수                     │  +1  │ F_1  │
  │ 기관   오늘만 순매수                     │  +1  │ I_1  │
  │ 외국인+기관 동시 해당                    │  +1  │ FI   │
  │ 개인만 대량 순매수 (외인·기관 없을 때)   │  -1  │ P_OV │
  └──────────────────────────────────────────┴──────┴──────┘

출력: stock_signals 테이블 저장
─────────────────────────────────────────────────────
"""

import argparse
import logging
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from blsh.database import query

from blsh.wye.domestic import collector, reporter

# ─────────────────────────────────────────
# 설정
# ─────────────────────────────────────────
MACD_SHORT = 12
MACD_LONG = 26
MACD_SIGNAL = 9
RSI_PERIOD = 14
RSI_OVERSOLD = 30
BB_PERIOD = 20
BB_STD = 2.0
STOCH_K = 14
STOCH_D = 3
STOCH_SMOOTH = 3
MA_PERIODS = [5, 20, 60, 120]
ATR_PERIOD = 14
ATR_SL_MULT = 1.5  # 손절: 종가 - 1.5×ATR
ATR_TP_MULT = 3.0  # 익절: 종가 + 3.0×ATR
GAP_THRESHOLD = 0.02
LOOKBACK_DAYS = 365  # 52주(252거래일) 신고가 계산을 위해 365일 이상 필요
MIN_SCORE = 1  # 저장 최소 점수
ENRICH_SCORE = 2  # 수급 보강 최소 점수

# 0단계 필터
TRDVAL_MIN = 1_000_000_000  # 최근 20일 평균 거래대금 최소값 (10억)
TRDVAL_DAYS = 20
INDEX_MA_DAYS = 20  # 지수 환경 체크 이동평균 기간

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


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


# ─────────────────────────────────────────
# 매수 신호 평가
# ─────────────────────────────────────────
def evaluate_buy(close, high, low, volume):
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

    score = 0
    flags = []

    # 1. MACD 골든크로스 (+2)                                    → MGC
    if m0 > s0 and m1 < s1:
        score += 2
        flags.append("MGC")
    # 2. MACD 예상 골든크로스 (+1)                               → MPGC
    elif (
        m0 < s0
        and len(hist) >= 3
        and hist.iloc[-3] < hist.iloc[-2] < hist.iloc[-1] < 0
        and abs(s0) > 0
        and (s0 - m0) / abs(s0) <= GAP_THRESHOLD
    ):
        score += 1
        flags.append("MPGC")

    # 3. RSI 30 상향 돌파 (+2)                                   → RBO
    if r0 > RSI_OVERSOLD and r1 <= RSI_OVERSOLD:
        score += 2
        flags.append("RBO")
    # 4. RSI 과매도 (+1)                                         → ROV
    elif r0 < RSI_OVERSOLD:
        score += 1
        flags.append("ROV")

    # 5. 볼린저 하단 반등 (+1)                                   → BBL
    if l1 < bbl1 and c0 > bbl0:
        score += 1
        flags.append("BBL")

    # 6. 볼린저 중간선 상향 돌파 (+1)                            → BBM
    if c0 > bbm0 and c1 <= bbm1:
        score += 1
        flags.append("BBM")

    # 7. 거래량 급증 + 양봉 (2배) (+1)                           → VS
    if volume is not None and len(volume) >= 20:
        vol_avg = volume.iloc[-20:-1].mean()
        if volume.iloc[-1] > vol_avg * 2 and c0 > c1:
            score += 1
            flags.append("VS")

    # 8. 이동평균 정배열 전환 (5>20>60) (+1)                     → MAA
    if ma5.iloc[-1] > ma20.iloc[-1] > ma60.iloc[-1] and not (
        ma5.iloc[-2] > ma20.iloc[-2] > ma60.iloc[-2]
    ):
        score += 1
        flags.append("MAA")

    # 9. 스토캐스틱 과매도 교차 (+1)                             → SGC
    if sk0 > sd0 and sk1 < sd1 and sk0 < 50:
        score += 1
        flags.append("SGC")

    # 10. 52주 신고가 돌파 (+2) - 최근 20일 최대 거래량 돌파 시만  → W52
    if len(close) >= 252 and volume is not None and len(volume) >= 21:
        w52_high = high.iloc[-252:-1].max()
        vol_20_max = volume.iloc[-21:-1].max()
        if h0 > w52_high and volume.iloc[-1] > vol_20_max:
            score += 2
            flags.append("W52")

    # 11. 눌림목 패턴 (+2)                                       → PB
    # 20MA 상승 중 + 전일 종가 또는 저가가 5MA 아래(꼬리 눌림 포함)
    # + 오늘 종가 5MA 위 복귀 + 20MA 위 유지
    if (
        ma20.iloc[-1] > ma20.iloc[-5]
        and (c1 < ma5.iloc[-2] or l1 < ma5.iloc[-2])
        and c0 > ma5.iloc[-1]
        and c0 > ma20.iloc[-1]
    ):
        score += 2
        flags.append("PB")

    # 12. 망치형 캔들 (+1)                                       → HMR
    body = abs(c0 - c1)
    candle_range = h0 - l0
    if candle_range > 0:
        lower_wick = min(c0, c1) - l0
        upper_wick = h0 - max(c0, c1)
        if (
            lower_wick > candle_range * 0.5
            and upper_wick < candle_range * 0.1
            and body < candle_range * 0.3
        ):
            score += 1
            flags.append("HMR")

    # 13. 장대 양봉 (+2)                                         → LB
    body_size = c0 - c1
    if body_size > atr0 * 1.5:
        score += 2
        flags.append("LB")

    # 14. 모닝스타 (3일 반전 패턴) (+2)                         → MS
    if len(close) >= 3:
        c_2, c_1, c_0 = close.iloc[-3], close.iloc[-2], close.iloc[-1]
        o_2 = close.shift(1).iloc[-3]
        body_d1 = o_2 - c_2
        body_d3 = c_0 - close.shift(1).iloc[-1]
        body_d2 = abs(c_1 - close.shift(1).iloc[-2])
        if (
            body_d1 > atr0 * 0.7
            and body_d2 < atr0 * 0.3
            and body_d3 > atr0 * 0.7
            and c_0 > (o_2 + c_2) / 2
        ):
            score += 2
            flags.append("MS")

    # 15. OBV 상승 추세 (3일 연속) (+1)                         → OBV
    if obv is not None and len(obv) >= 3:
        if obv.iloc[-3] < obv.iloc[-2] < obv.iloc[-1]:
            score += 1
            flags.append("OBV")

    # ── 신호 성격 분류 (MOM/REV/MIX/WEAK)
    REVERSAL_FLAGS = {"ROV", "RBO", "BBL", "HMR", "MS"}
    MOMENTUM_FLAGS = {"MGC", "MAA", "W52", "PB", "LB", "VS", "OBV"}
    flag_set = set(flags)
    rev_cnt = len(flag_set & REVERSAL_FLAGS)
    mom_cnt = len(flag_set & MOMENTUM_FLAGS)
    if mom_cnt >= 2 and mom_cnt > rev_cnt:
        mode = "MOM"
    elif rev_cnt >= 2 and rev_cnt > mom_cnt:
        mode = "REV"
    elif mom_cnt > 0 and rev_cnt > 0:
        mode = "MIX"
    else:
        mode = "WEAK"

    # ── 매수가 / 손절 / 익절
    entry_price = round(c0 + 0.5 * atr0, 2)
    stop_loss = round(c0 - ATR_SL_MULT * atr0, 2)
    take_profit = round(c0 + ATR_TP_MULT * atr0, 2)

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
    ticker, name, market, df, base_date, close_col, high_col, low_col, vol_col=None
):
    if df is None:
        return None

    df = df.sort_index().apply(pd.to_numeric, errors="coerce")

    # base_date 이하 데이터만 사용 (과거 날짜 지정 시 미래 데이터 차단)
    df = df[df.index <= base_date]

    if len(df) < LOOKBACK_DAYS // 3:
        return None

    close = df[close_col].dropna()
    high = df[high_col].dropna()
    low = df[low_col].dropna()
    vol = df[vol_col].dropna() if vol_col and vol_col in df.columns else None

    idx = close.index.intersection(high.index).intersection(low.index)
    close, high, low = close[idx], high[idx], low[idx]
    if vol is not None:
        vol = vol[idx]

    score, flags, ind = evaluate_buy(close, high, low, vol)
    if score < MIN_SCORE:
        return None

    icon = "🔴" if score >= 5 else "🟡" if score >= 3 else "🔵"
    log.info(f"  {icon} [{score:2d}pt] {ticker:10s} {name[:18]:18s} ({market}) {flags}")

    return {
        "base_date": base_date,
        "target_date": None,  # main()에서 채움
        "ticker": ticker,
        "name": name,
        "market": market,
        "buy_score": score,
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
):
    log.info(f"[{market}] {table} 스캔 시작  (기준일: {base_date})")

    # 0단계: 최근 TRDVAL_DAYS일 평균 거래대금 TRDVAL_MIN 이상 종목만 로드
    df_all = query.get_ohlcv(
        table,
        close_col,
        high_col,
        low_col,
        vol_col,
        {
            "start": start,
            "base_date": base_date,
            "filter_start": (
                datetime.strptime(base_date, "%Y%m%d") - timedelta(days=TRDVAL_DAYS * 2)
            ).strftime("%Y%m%d"),
            "min_val": TRDVAL_MIN,
        },
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
        )
        if row:
            results.append(row)

    log.info(f"[{market}] 신호 종목: {len(results)}건 (거래대금 필터 후)")
    return results


# ─────────────────────────────────────────
# [2단계] DB 수급 보강 + KIS API fallback
# ─────────────────────────────────────────
def fetch_investor_daily(ticker, base_date, n_days=5):
    """
    blsh.kis - 종목별 투자자매매동향(일별)
    base_date 기준 최근 n_days 거래일의 외국인/기관 순매수량 반환.

    ds.investor_trade_by_stock_daily 파라미터:
      fid_cond_mrkt_div_code : J(KRX)
      fid_input_iscd         : 종목코드 6자리
      fid_input_date_1       : 기준일자 (YYYYMMDD)
      fid_org_adj_prc        : 공란
      fid_etc_cls_code       : 공란
      max_depth              : 1 (1페이지 = 최근 20~30일치, n_days 이상 충분)

    반환값 (output2 DataFrame):
      frgn_ntby_qty : 외국인 순매수량 (최신순)
      orgn_ntby_qty : 기관 순매수량 (최신순)

    반환: (frgn_list, orgn_list) - 오래된→최신 순, 각 n_days개
    """
    from blsh.kis import kis_auth as ka
    from blsh.kis.domestic_stock import domestic_stock_functions as ds

    try:
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

        # output2: DataFrame (최신순 정렬) 또는 None
        if result is None:
            return [], []

        # DataFrame인 경우
        if hasattr(result, "iloc"):
            df = result.head(n_days).iloc[::-1].reset_index(drop=True)  # 오래된→최신
            frgn = df["frgn_ntby_qty"].astype(float).astype(int).tolist()
            orgn = df["orgn_ntby_qty"].astype(float).astype(int).tolist()
            return frgn, orgn

        # output2가 (df1, df2) 튜플로 반환되는 경우
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
      TRN (+3): 직전 N-1일 순매도 → 오늘 순매수
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
    prev = history[-1] if history else 0
    if prev <= 0:
        return "TRN", 3
    consec = 1
    for q in reversed(history):
        if q > 0:
            consec += 1
        else:
            break
    if consec >= 3:
        return "C3", 2
    return "1", 1


def enrich_with_db(results: list, base_date: str) -> list:
    """
    [2단계] isu_ksp_info / isu_ksd_info 에서 base_date 기준
    최근 5거래일 수급 판별 후 점수 보강.
    DB 미보유 종목은 KIS API fallback.
    """
    candidates = [
        r
        for r in results
        if r["buy_score"] >= ENRICH_SCORE and r["market"] in ("KOSPI", "KOSDAQ")
    ]
    if not candidates:
        return results

    log.info(f"[수급 보강] 대상 {len(candidates)}종목  (기준일: {base_date})")

    kospi_tickers = [r["ticker"] for r in candidates if r["market"] == "KOSPI"]
    kosdaq_tickers = [r["ticker"] for r in candidates if r["market"] == "KOSDAQ"]

    def fetch_supply_from_db(table, tickers):
        if not tickers:
            return {}

        try:
            df = query.get_netbid_trdvol(table, tickers, base_date)
        except Exception as e:
            log.warning(f"  DB 수급 조회 오류 ({table}): {e}")
            return {}
        result = {}
        for ticker, grp in df.groupby("isu_srt_cd"):
            recent = grp.head(5).sort_values("trd_dd")
            result[ticker] = {
                "frgn": recent["frgn_qty"].fillna(0).tolist(),
                "inst": recent["inst_qty"].fillna(0).tolist(),
                "today_frgn": recent["frgn_qty"].iloc[-1] if len(recent) else 0,
                "today_inst": recent["inst_qty"].iloc[-1] if len(recent) else 0,
                "today_indi": recent["indi_qty"].iloc[-1] if len(recent) else 0,
            }
        return result

    supply_db = {
        **fetch_supply_from_db("isu_ksp_info", kospi_tickers),
        **fetch_supply_from_db("isu_ksd_info", kosdaq_tickers),
    }

    # KIS API fallback
    missing = [r for r in candidates if r["ticker"] not in supply_db]
    supply_api = {}
    if missing:
        log.info(f"  DB 미보유 {len(missing)}종목 → KIS API fallback")
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
            log.info(
                f"  {icon} 외국인 {f_sig}({f_sc:+d}): {t} {row['name']}  {sup['frgn']}"
            )

        if o_sc > 0:
            results[idx]["buy_score"] += o_sc
            results[idx]["buy_flags"] += f",I_{o_sig}"
            icon = "🔥" if o_sig == "TRN" else ("🏦🏦" if o_sig == "C3" else "🏦")
            log.info(
                f"  {icon} 기관   {o_sig}({o_sc:+d}): {t} {row['name']}  {sup['inst']}"
            )

        if f_sc > 0 and o_sc > 0:
            results[idx]["buy_score"] += 1
            results[idx]["buy_flags"] += ",FI"
            log.info(f"  ⭐ 외국인+기관 동시: {t} {row['name']}")

        indi = sup.get("today_indi") or 0
        frgn = sup["today_frgn"] or 0
        inst = sup["today_inst"] or 0
        if indi > 0 and frgn <= 0 and inst <= 0 and indi > abs(frgn) + abs(inst):
            results[idx]["buy_score"] -= 1
            results[idx]["buy_flags"] += ",P_OV"
            log.info(
                f"  ⚠️  개인 과매수 패널티(-1): {t} {row['name']}  개인={indi:+.0f}"
            )

    return results


def check_index_above_ma(idx_nm, base_date, ma_days=20):
    """
    idx_stk_ohlcv에서 base_date 기준 지수가 MA 위에 있는지 확인.
    True = 정상 (매수 환경), False = 하락장 (스캔 스킵)
    """
    try:
        df = query.get_index_clsprc(idx_nm, base_date, ma_days)
        if len(df) < ma_days:
            return True
        prices = df["clsprc_idx"].astype(float).iloc[::-1]
        ma = prices.mean()
        above = float(prices.iloc[-1]) >= ma
        status = "위 ✅" if above else "아래 ⚠️"
        log.info(
            f"[지수 환경] {idx_nm}  현재가={prices.iloc[-1]:.2f}  "
            f"{ma_days}MA={ma:.2f}  → {status}"
        )
        return above
    except Exception as e:
        log.warning(f"지수 환경 체크 실패 ({idx_nm}): {e}")
        return True


def get_next_biz_date(base_date: str) -> str:
    """
    base_date 다음 영업일 반환.
    """
    # ── 1순위: ohlcv 테이블
    result = query.find_next_biz_date_from_ohlcv(base_date)
    if result:
        log.info(f"다음 영업일: {result}  [isu_ksp_ohlcv]")
        return result

    # ── 2순위: krx_holiday 테이블 (캐시)
    result = collector.collect_holiday_if_not_exists(base_date)
    if result:
        log.info(f"다음 영업일: {result}  [krx_holiday 테이블]")
        return result

    raise RuntimeError(f"다음 영업일 조회 실패: base_date={base_date}")


# ─────────────────────────────────────────
# 메인
# ─────────────────────────────────────────
def parse_args():
    parser = argparse.ArgumentParser(description="매수 신호 스캐너")
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        metavar="YYYYMMDD",
        help="스캔 기준일 (예: 20260312). 미지정 시 DB 최근 영업일 사용.",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    # ── 기준일 결정
    if args.date:
        base_date = args.date
        log.info(f"기준일 (지정): {base_date}")
    else:
        base_date = query.get_latest_biz_date()
        log.info(f"기준일 (최근 영업일): {base_date}")

    # ── 다음 영업일 (매수 목표일)
    target_date = get_next_biz_date(base_date)
    today_str = datetime.today().strftime("%Y%m%d")
    is_today = base_date == today_str

    if target_date:
        src = "캘린더(공휴일 미반영)" if is_today else "DB"
        log.info(f"매수 목표일: {target_date}  [{src}]")

    # ── 장중 실행 경고 (오늘 날짜 기준 스캔일 때만)
    if is_today:
        now = datetime.now()
        if now.weekday() < 5 and now.hour < 16:
            log.warning(
                "⚠️  장중 실행 감지 (%s). 수급 데이터가 잠정치일 수 있습니다.",
                now.strftime("%H:%M"),
            )

    start = (
        datetime.strptime(base_date, "%Y%m%d") - timedelta(days=LOOKBACK_DAYS)
    ).strftime("%Y%m%d")
    name_map = query.get_ticker_name_map()

    # ── 1단계: OHLCV 기술지표 스캔 (0단계 필터 포함)
    results = []

    if check_index_above_ma("코스피", base_date, INDEX_MA_DAYS):
        results += scan_market("isu_ksp_ohlcv", "KOSPI", start, base_date, name_map)
    else:
        log.warning("[KOSPI] 지수 20MA 아래 → 스캔 스킵")

    if check_index_above_ma("코스닥", base_date, INDEX_MA_DAYS):
        results += scan_market("isu_ksd_ohlcv", "KOSDAQ", start, base_date, name_map)
    else:
        log.warning("[KOSDAQ] 지수 20MA 아래 → 스캔 스킵")

    # ── target_date 채우기
    for r in results:
        r["target_date"] = target_date

    # ── 2단계: DB 수급 보강
    results = enrich_with_db(results, base_date)

    query.save_signal(results)
    reporter.print_general_summary(results)
    reporter.print_invest_report(results, base_date)
    reporter.print_return_report(results, base_date, target_date)
    log.info(
        f"전체 완료: 총 {len(results)}건  기준일={base_date}  목표일={target_date}"
    )


if __name__ == "__main__":
    main()
