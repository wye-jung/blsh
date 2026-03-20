"""
매수 신호 스캐너 v12
─────────────────────────────────────────────────────
대상: KOSPI(isu_ksp_ohlcv) / KOSDAQ(isu_ksd_ohlcv)

[0단계] 종목 필터 (scan_market SQL)
  - 최근 20일 평균 거래대금(acc_trdval) 10억 이상
  - 지수 환경 체크: KOSPI/KOSDAQ 20MA 아래이면 해당 시장 스킵

[1단계] DB 기반 OHLCV 지표 스캔                              flag   성격    점수
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

  점수 산출 (분리 트랙):
    - 모멘텀 점수(mom), 전환 점수(rev), 중립 점수(neu) 별도 집계
    - MOM/REV: 해당 트랙 점수 + neu
    - MIX: max(mom, rev) + neu  (약한 쪽 증거는 flag에만 보존, 점수 불포함)
    - WEAK: mom + rev + neu (둘 다 약하므로 합산)

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

import logging

import numpy as np
import pandas as pd
from blsh.database import query, ModelManager

from blsh.wye.domestic import _report as rep
from blsh.wye.domestic import _factor as fac
from blsh.wye.domestic._tick import floor_tick as _floor_tick, ceil_tick as _ceil_tick
from blsh.database.models import TradeCandidates
from blsh.common import dtutils

log = logging.getLogger(__name__)

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
    es = c.ewm(span=fac.MACD_SHORT, adjust=False).mean()
    el = c.ewm(span=fac.MACD_LONG, adjust=False).mean()
    m = es - el
    s = m.ewm(span=fac.MACD_SIGNAL, adjust=False).mean()
    return m, s, m - s


def calc_rsi(c, p=fac.RSI_PERIOD):
    d = c.diff()
    g = d.clip(lower=0).ewm(alpha=1 / p, adjust=False).mean()
    l = (-d.clip(upper=0)).ewm(alpha=1 / p, adjust=False).mean()
    return 100 - 100 / (1 + g / l.replace(0, np.nan))


def calc_bb(c, p=fac.BB_PERIOD, k=fac.BB_STD):
    m = c.rolling(p).mean()
    s = c.rolling(p).std()
    return m + k * s, m, m - k * s


def calc_atr(h, l, c, p=fac.ATR_PERIOD):
    pc = c.shift(1)
    tr = pd.concat([h - l, (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
    return tr.ewm(span=p, adjust=False).mean()


def calc_stoch(h, l, c, k=fac.STOCH_K, d=fac.STOCH_D, sm=fac.STOCH_SMOOTH):
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
def evaluate_buy(close, high, low, volume, opn=None):
    min_len = fac.MACD_LONG + fac.MACD_SIGNAL + 5
    if len(close) < min_len:
        return 0, [], {}

    macd, sig, hist = calc_macd(close)
    rsi = calc_rsi(close)
    bbu, bbm, bbl = calc_bb(close)
    atr = calc_atr(high, low, close)
    sk, sd = calc_stoch(high, low, close)
    mas = {p: close.rolling(p).mean() for p in fac.MA_PERIODS}
    obv = calc_obv(close, volume) if volume is not None else None

    c0, c1 = close.iloc[-1], close.iloc[-2]
    h0, h1 = high.iloc[-1], high.iloc[-2]
    l0, l1 = low.iloc[-1], low.iloc[-2]
    o0 = opn.iloc[-1] if opn is not None else c1
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
        signals.append(("MGC", 2))
    # 2. MACD 예상 골든크로스 (+1) → MPGC (중립)
    elif (
        m0 < s0
        and len(hist) >= 3
        and hist.iloc[-3] < hist.iloc[-2] < hist.iloc[-1] < 0
        and abs(s0) > 0
        and (s0 - m0) / abs(s0) <= fac.GAP_THRESHOLD
    ):
        signals.append(("MPGC", 1))

    # 3. RSI 30 상향 돌파 (+2) → RBO (전환)
    if r0 > fac.RSI_OVERSOLD and r1 <= fac.RSI_OVERSOLD:
        signals.append(("RBO", 2))
    # 4. RSI 과매도 (+1) → ROV (전환)
    elif r0 < fac.RSI_OVERSOLD:
        signals.append(("ROV", 1))

    # 5. 볼린저 하단 반등 (+1) → BBL (전환)
    if l1 < bbl1 and c0 > bbl0:
        signals.append(("BBL", 1))

    # 6. 볼린저 중간선 상향 돌파 (+1) → BBM (중립)
    if c0 > bbm0 and c1 <= bbm1:
        signals.append(("BBM", 1))

    # 7. 거래량 급증 + 양봉 (+1) → VS (모멘텀)
    if volume is not None and len(volume) >= 20:
        vol_avg = volume.iloc[-20:-1].mean()
        if volume.iloc[-1] > vol_avg * 2 and c0 > c1:
            signals.append(("VS", 1))

    # 8. 이동평균 정배열 전환 (+1) → MAA (모멘텀)
    if ma5.iloc[-1] > ma20.iloc[-1] > ma60.iloc[-1] and not (
        ma5.iloc[-2] > ma20.iloc[-2] > ma60.iloc[-2]
    ):
        signals.append(("MAA", 1))

    # 9. 스토캐스틱 과매도 교차 (+1) → SGC (중립)
    if sk0 > sd0 and sk1 < sd1 and sk0 < 50:
        signals.append(("SGC", 1))

    # 10. 52주 신고가 돌파 (+2) → W52 (모멘텀)
    # TODO: 거래량 조건을 "20일 평균의 N배"로 완화 검토
    if len(close) >= 252 and volume is not None and len(volume) >= 21:
        w52_high = high.iloc[-252:-1].max()
        vol_20_max = volume.iloc[-21:-1].max()
        if h0 > w52_high and volume.iloc[-1] > vol_20_max:
            signals.append(("W52", 2))

    # 11. 눌림목 패턴 (+2) → PB (모멘텀)
    if (
        ma20.iloc[-1] > ma20.iloc[-5]
        and (c1 < ma5.iloc[-2] or l1 < ma5.iloc[-2])
        and c0 > ma5.iloc[-1]
        and c0 > ma20.iloc[-1]
    ):
        signals.append(("PB", 2))

    # 12. 망치형 캔들 (+1) → HMR (전환)
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
            signals.append(("HMR", 1))

    # 13. 장대 양봉 (+2) → LB (모멘텀)
    body_size = c0 - o0
    if body_size > atr0 * 1.5:
        signals.append(("LB", 2))

    # 14. 모닝스타 (+2) → MS (전환)
    if opn is not None and len(close) >= 3:
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
            signals.append(("MS", 2))

    # 15. OBV 상승 추세 (+1) → OBV (모멘텀)
    if obv is not None and len(obv) >= 3:
        if obv.iloc[-3] < obv.iloc[-2] < obv.iloc[-1]:
            signals.append(("OBV", 1))

    # ── 분리 트랙 점수 집계
    flags = [f for f, _ in signals]
    flag_set = set(flags)
    mom_score = sum(pts for f, pts in signals if f in _MOMENTUM_FLAGS)
    rev_score = sum(pts for f, pts in signals if f in _REVERSAL_FLAGS)
    neu_score = sum(pts for f, pts in signals if f not in _MOMENTUM_FLAGS | _REVERSAL_FLAGS)

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
    entry_price = _ceil_tick(c0 + 0.5 * atr0)
    stop_loss = _floor_tick(c0 - fac.ATR_SL_MULT * atr0)
    take_profit = _ceil_tick(c0 + fac.ATR_TP_MULT * atr0)

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

    if len(df) < fac.LOOKBACK_DAYS // 3:
        return None

    close = df[close_col].dropna()
    high = df[high_col].dropna()
    low = df[low_col].dropna()
    vol = df[vol_col].dropna() if vol_col and vol_col in df.columns else None
    opn = df[open_col].dropna() if open_col and open_col in df.columns else None

    idx = close.index.intersection(high.index).intersection(low.index)
    close, high, low = close[idx], high[idx], low[idx]
    if vol is not None:
        vol = vol[idx]
    if opn is not None:
        opn = opn[idx]

    score, flags, ind = evaluate_buy(close, high, low, vol, opn)
    if score < fac.MIN_SCORE:
        return None

    icon = "🔴" if score >= 5 else "🟡" if score >= 3 else "🔵"
    log.info(f"  {icon} [{score:2d}pt] {ticker:10s} {name[:18]:18s} ({market}) {flags}")

    return {
        "base_date": base_date,
        "entry_date": None,
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
    open_col="tdd_opnprc",
):
    log.info(f"[{market}] {table} 스캔 시작  (기준일: {base_date})")

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
                "filter_start": dtutils.add_days(base_date, fac.TRDVAL_DAYS * -2),
                "min_val": fac.TRDVAL_MIN,
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

    log.info(f"[{market}] 신호 종목: {len(results)}건 (거래대금 필터 후)")
    return results


# ─────────────────────────────────────────
# [2단계] DB 수급 보강 + KIS API fallback
# ─────────────────────────────────────────
def fetch_investor_daily(ticker, base_date, n_days=5):
    """종목별 투자자매매동향(일별). 반환: (frgn_list, orgn_list) 오래된→최신."""
    from blsh.kis import kis_auth as ka
    from blsh.kis.domestic_stock import domestic_stock_functions as ds

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
    if all(q <= 0 for q in history):
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
    """[2단계] 수급 판별 후 점수 보강. DB 미보유 종목은 KIS API fallback."""
    candidates = [
        r
        for r in results
        if r["buy_score"] >= fac.ENRICH_SCORE and r["market"] in ("KOSPI", "KOSDAQ")
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
            df = pd.DataFrame(query.get_netbid_trdvol(table, tickers, base_date))
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


def check_index_above_ma(
    idx_nm, base_date, ma_days=20, drop_limit=fac.INDEX_DROP_LIMIT
):
    """지수 환경 체크. MA 대비 -drop_limit 이하일 때만 스캔 스킵."""
    try:
        df = pd.DataFrame(query.get_index_clsprc(idx_nm, base_date, ma_days))
        if len(df) < ma_days:
            return True
        prices = df["clsprc_idx"].astype(float).iloc[::-1]
        cur = float(prices.iloc[-1])
        ma = prices.mean()
        gap_pct = (cur - ma) / ma
        skip = gap_pct < -drop_limit
        if skip:
            status = f"스킵 🚫 (MA 대비 {gap_pct:.1%})"
        elif gap_pct < 0:
            status = f"허용 ⚠️  (MA 대비 {gap_pct:.1%}, 임계 -{drop_limit:.0%} 미만)"
        else:
            status = f"위 ✅ (MA 대비 {gap_pct:+.1%})"
        log.info(
            f"[지수 환경] {idx_nm}  현재가={cur:.2f}  {ma_days}MA={ma:.2f}  → {status}"
        )
        return not skip
    except Exception as e:
        log.warning(f"지수 환경 체크 실패 ({idx_nm}): {e}")
        return True


# ─────────────────────────────────────────
# 스캔 및 대상 선별
# ─────────────────────────────────────────
def scan(base_date=dtutils.today(), report: bool = False) -> pd.DataFrame:
    if not query.has_ohlcv_data(base_date):
        log.warning(f"{base_date} - ohlcv 데이터가 없습니다")
        return pd.DataFrame()

    start = dtutils.add_days(base_date, fac.LOOKBACK_DAYS * -1)
    name_map = query.get_ticker_name_map()

    results = []

    if check_index_above_ma("코스피", base_date, fac.INDEX_MA_DAYS):
        results += scan_market("isu_ksp_ohlcv", "KOSPI", start, base_date, name_map)
    else:
        log.warning("[KOSPI] 지수 20MA 아래 → 스캔 스킵")

    if check_index_above_ma("코스닥", base_date, fac.INDEX_MA_DAYS):
        results += scan_market("isu_ksd_ohlcv", "KOSDAQ", start, base_date, name_map)
    else:
        log.warning("[KOSDAQ] 지수 20MA 아래 → 스캔 스킵")

    results = enrich_with_db(results, base_date)

    df = pd.DataFrame(results)

    if not df.empty:
        for col in ("foreign_netbuy", "inst_netbuy", "indi_netbuy"):
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if report:
        rep.print_general_summary(df)

    return df


def find_candidates(base_date=dtutils.today(), report: bool = False) -> pd.DataFrame:
    sdf = scan(base_date, report)
    if sdf.empty:
        return sdf

    cand_mask = (
        (sdf["buy_score"] >= fac.INVEST_MIN_SCORE)
        & (sdf["mode"].isin(["MIX", "MOM", "REV"]))
        & (~sdf["buy_flags"].str.contains("P_OV", na=False))
    )
    df = sdf[cand_mask].copy()
    if report:
        rep.print_invest_report(df)

    if df.empty:
        return df

    df = df[
        [
            "base_date",
            "ticker",
            "name",
            "market",
            "buy_score",
            "mode",
            "entry_price",
            "stop_loss",
            "take_profit",
            "atr",
        ]
    ]
    df["atr_sl_mult"] = fac.ATR_SL_MULT
    df["atr_tp_mult"] = fac.ATR_TP_MULT
    conditions = [
        df["mode"] == "MIX",
        df["mode"] == "MOM",
        df["mode"] == "REV",
    ]
    days = [fac.MAX_HOLD_DAYS_MIX, fac.MAX_HOLD_DAYS_MOM, fac.MAX_HOLD_DAYS]
    df["max_hold_days"] = np.select(conditions, days, default=fac.MAX_HOLD_DAYS)

    ctime = dtutils.ctime()
    if base_date == dtutils.today() and ctime < "151500":
        entry_date = base_date
        if ctime > "130000":
            df["max_hold_days"] = df["max_hold_days"] + 1
    else:
        entry_date = query.find_next_biz_date(base_date)
    df["entry_date"] = entry_date

    expiry_cache: dict[tuple, str | None] = {}

    def _get_expiry(ed, mhd):
        key = (ed, int(mhd))
        if key not in expiry_cache:
            expiry_cache[key] = dtutils.add_biz_days(str(ed), int(mhd))
        return expiry_cache[key]

    df["expiry_date"] = df.apply(
        lambda r: _get_expiry(r["entry_date"], r["max_hold_days"]), axis=1
    )
    return df


def save_candidates(base_date=dtutils.today(), report=True) -> None:
    df = find_candidates(base_date, True)
    if not df.empty:
        entry_date = df.iloc[0]["entry_date"]
        modelManager = ModelManager(TradeCandidates)
        modelManager.delete(base_date=base_date, entry_date=entry_date)
        modelManager.create(df)


if __name__ == "__main__":
    save_candidates()
