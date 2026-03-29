"""
독립형 투자대상 스캐너

기존 scanner.py 의 후보선정 로직은 사용하지 않고, 일별 OHLCV 기반으로
추세/돌파/눌림목/변동성 확장 패턴을 독립적으로 평가해 투자대상을 리포팅한다.

핵심 개념
  - 시장 레짐: KOSPI/KOSDAQ 지수의 20/60일 위치와 기울기 평가
  - 종목 유니버스: 최근 평균 거래대금 기준 유동성 필터
  - 종목 선정: 추세 정렬, 박스 상단 근접/돌파, MA20 눌림목 회복,
               변동성 수축 후 확장(ignition) 패턴을 점수화
  - 전략 분기: TRADE_FLAG=DAY / SWING 에 따라 임계값과 리스크 파라미터 분리

주문 파일 생성이나 DB 저장은 수행하지 않는다.
"""

from __future__ import annotations

import logging
import sys
from dataclasses import dataclass
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

import numpy as np
import pandas as pd

from wye.blsh.common import dtutils
from wye.blsh.common.env import DATA_DIR, LOG_DIR, TRADE_FLAG
from wye.blsh.database import query
from wye.blsh.domestic import (
    Milestone,
    PO_TYPE_FIN,
    PO_TYPE_INI,
    PO_TYPE_PRE,
    Tick,
    sector,
)
from wye.blsh.domestic.codex import factor

log = logging.getLogger(__name__)
_fh = TimedRotatingFileHandler(
    LOG_DIR / "scanner_codex.log",
    when="midnight",
    backupCount=30,
    encoding="utf-8",
)
_fh.suffix = "%Y-%m-%d"
_fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
log.addHandler(_fh)

LOOKBACK_DAYS = 320
MIN_HISTORY = 90
UNIVERSE_FILTER_DAYS = 20
MARKET_MA_DAYS = 60
REPORT_DIR = DATA_DIR / "reports"
COMPAT_COLUMNS = [
    "base_date",
    "entry_date",
    "ticker",
    "name",
    "market",
    "buy_score",
    "buy_flags",
    "foreign_netbuy",
    "inst_netbuy",
    "indi_netbuy",
    "mode",
    "close",
    "atr",
    "rsi",
    "macd",
    "macd_signal",
    "macd_hist",
    "bb_upper",
    "bb_middle",
    "bb_lower",
    "stoch_k",
    "stoch_d",
    "entry_price",
    "stop_loss",
    "take_profit",
    "atr_sl_mult",
    "atr_tp_mult",
    "max_hold_days",
    "po_type",
    "expiry_date",
]


@dataclass(frozen=True)
class StrategyConfig:
    label: str
    avg_trdval_min: int
    score_threshold: int
    recent_high_days: int
    pullback_margin: float
    entry_atr_mult: float
    stop_atr_mult: float
    target_atr_mult: float
    tp1_mult: float
    tp1_ratio: float
    max_hold_days_rev: int
    max_hold_days_mix: int
    max_hold_days_mom: int
    max_candidates_per_market: int


_DAY_F = factor_codex.DAY_FACTORS
_SWING_F = factor_codex.SWING_FACTORS

CONFIGS = {
    "DAY": StrategyConfig(
        label="DAY",
        avg_trdval_min=_DAY_F["AVG_TRDVAL_MIN"],
        score_threshold=_DAY_F["INVEST_MIN_SCORE"],
        recent_high_days=_DAY_F["RECENT_HIGH_DAYS"],
        pullback_margin=_DAY_F["PULLBACK_MARGIN"],
        entry_atr_mult=_DAY_F["ENTRY_ATR_MULT"],
        stop_atr_mult=_DAY_F["ATR_SL_MULT"],
        target_atr_mult=_DAY_F["ATR_TP_MULT"],
        tp1_mult=_DAY_F["TP1_MULT"],
        tp1_ratio=_DAY_F["TP1_RATIO"],
        max_hold_days_rev=_DAY_F["MAX_HOLD_DAYS"],
        max_hold_days_mix=_DAY_F["MAX_HOLD_DAYS_MIX"],
        max_hold_days_mom=_DAY_F["MAX_HOLD_DAYS_MOM"],
        max_candidates_per_market=_DAY_F["MAX_CANDIDATES_PER_MARKET"],
    ),
    "SWING": StrategyConfig(
        label="SWING",
        avg_trdval_min=_SWING_F["AVG_TRDVAL_MIN"],
        score_threshold=_SWING_F["INVEST_MIN_SCORE"],
        recent_high_days=_SWING_F["RECENT_HIGH_DAYS"],
        pullback_margin=_SWING_F["PULLBACK_MARGIN"],
        entry_atr_mult=_SWING_F["ENTRY_ATR_MULT"],
        stop_atr_mult=_SWING_F["ATR_SL_MULT"],
        target_atr_mult=_SWING_F["ATR_TP_MULT"],
        tp1_mult=_SWING_F["TP1_MULT"],
        tp1_ratio=_SWING_F["TP1_RATIO"],
        max_hold_days_rev=_SWING_F["MAX_HOLD_DAYS"],
        max_hold_days_mix=_SWING_F["MAX_HOLD_DAYS_MIX"],
        max_hold_days_mom=_SWING_F["MAX_HOLD_DAYS_MOM"],
        max_candidates_per_market=_SWING_F["MAX_CANDIDATES_PER_MARKET"],
    ),
}
CFG = CONFIGS.get(TRADE_FLAG, CONFIGS["SWING"])


def calc_atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14):
    prev_close = close.shift(1)
    tr = pd.concat(
        [high - low, (high - prev_close).abs(), (low - prev_close).abs()],
        axis=1,
    ).max(axis=1)
    return tr.ewm(span=period, adjust=False).mean()


def _safe_mean(values: pd.Series, default: float = 0.0) -> float:
    value = pd.to_numeric(values, errors="coerce").dropna().mean()
    if pd.isna(value):
        return default
    return float(value)


def _safe_pct(cur: float, prev: float) -> float:
    if prev in (None, 0) or pd.isna(prev):
        return 0.0
    return float(cur / prev - 1.0)


def _get_market_regime(idx_nm: str, idx_clss: str, base_date: str) -> dict:
    rows = query.get_index_clsprc(idx_nm, base_date, ma_days=MARKET_MA_DAYS, idx_clss=idx_clss)
    if not rows or len(rows) < 30:
        return {
            "index": idx_nm,
            "label": "UNKNOWN",
            "bonus": 0,
            "score": 0,
            "close": np.nan,
            "ma20": np.nan,
            "ma60": np.nan,
            "ret5": 0.0,
        }

    prices = pd.Series([float(r["clsprc_idx"]) for r in rows], dtype="float64")
    close0 = float(prices.iloc[0])
    ma20 = float(prices.iloc[1:21].mean()) if len(prices) >= 21 else float(prices.mean())
    ma60 = float(prices.iloc[1:61].mean()) if len(prices) >= 61 else float(prices.mean())
    prev_ma20 = (
        float(prices.iloc[2:22].mean())
        if len(prices) >= 22
        else ma20
    )
    ret5 = _safe_pct(close0, float(prices.iloc[5])) if len(prices) > 5 else 0.0

    score = 0
    if close0 >= ma20:
        score += 2
    else:
        score -= 2
    if ma20 >= ma60:
        score += 1
    else:
        score -= 1
    if ma20 >= prev_ma20:
        score += 1
    else:
        score -= 1
    if ret5 >= 0.01:
        score += 1
    elif ret5 <= -0.03:
        score -= 1

    if score >= 3:
        label = "BULL"
        bonus = 1
    elif score >= 0:
        label = "NEUTRAL"
        bonus = 0
    else:
        label = "WEAK"
        bonus = -2

    return {
        "index": idx_nm,
        "label": label,
        "bonus": bonus,
        "score": score,
        "close": close0,
        "ma20": ma20,
        "ma60": ma60,
        "ret5": ret5,
    }


def _load_market_regimes(base_date: str) -> dict[str, dict]:
    return {
        "KOSPI": _get_market_regime("코스피", sector.IDX_CLSS_KOSPI, base_date),
        "KOSDAQ": _get_market_regime("코스닥", sector.IDX_CLSS_KOSDAQ, base_date),
    }


def _fetch_market_ohlcv(table: str, base_date: str) -> pd.DataFrame:
    start = dtutils.add_days(base_date, LOOKBACK_DAYS * -1)
    rows = query.get_ohlcv(
        table,
        "tdd_clsprc",
        "tdd_hgprc",
        "tdd_lwprc",
        "acc_trdvol",
        {
            "start": start,
            "base_date": base_date,
            "filter_start": dtutils.add_days(base_date, UNIVERSE_FILTER_DAYS * -2),
            "min_val": CFG.avg_trdval_min,
        },
        open_col="tdd_opnprc",
    )
    return pd.DataFrame(rows)


def _classify_setup(setup_scores: dict[str, int]) -> str:
    priority = {"BREAKOUT": 3, "PULLBACK": 2, "IGNITION": 1}
    return max(
        setup_scores.items(),
        key=lambda item: (item[1], priority[item[0]]),
    )[0]


def _build_trade_levels(
    close0: float,
    atr0: float,
    ma20_0: float,
    recent_high: float,
    setup: str,
) -> tuple[int, int, int]:
    if setup == "BREAKOUT":
        entry_anchor = max(close0, recent_high)
    elif setup == "PULLBACK":
        entry_anchor = max(close0, ma20_0)
    else:
        entry_anchor = close0

    entry_price = Tick.ceil_tick(entry_anchor + atr0 * CFG.entry_atr_mult)
    stop_seed = min(ma20_0, close0 - atr0 * CFG.stop_atr_mult)
    stop_loss = Tick.floor_tick(stop_seed)

    if stop_loss <= 0 or stop_loss >= entry_price:
        stop_loss = Tick.floor_tick(close0 - max(atr0, close0 * 0.03))

    target_seed = max(
        close0 + atr0 * CFG.target_atr_mult,
        entry_price + max(entry_price - stop_loss, 1) * 1.8,
    )
    take_profit = Tick.ceil_tick(target_seed)
    return entry_price, stop_loss, take_profit


def _mode_from_setup(setup: str) -> str:
    if setup == "BREAKOUT":
        return "MOM"
    if setup == "PULLBACK":
        return "REV"
    return "MIX"


def _hold_days_from_mode(mode: str) -> int:
    mapping = {
        "REV": CFG.max_hold_days_rev,
        "MIX": CFG.max_hold_days_mix,
        "MOM": CFG.max_hold_days_mom,
    }
    return mapping.get(mode, max(mapping.values()))


def _empty_candidates() -> pd.DataFrame:
    extra_cols = [
        "strategy",
        "setup",
        "market_regime",
        "rr",
        "atr_pct",
        "ret5_pct",
        "ret20_pct",
        "vol_ratio",
        "avg_trdval20",
    ]
    return pd.DataFrame(columns=COMPAT_COLUMNS + extra_cols)


def _apply_entry_schedule(df: pd.DataFrame, base_date: str) -> pd.DataFrame:
    if df.empty:
        return _empty_candidates()

    df = df.copy()
    df["atr_sl_mult"] = CFG.stop_atr_mult
    df["atr_tp_mult"] = CFG.target_atr_mult
    df["tp1_mult"] = CFG.tp1_mult
    df["tp1_ratio"] = CFG.tp1_ratio
    df["max_hold_days"] = df["mode"].apply(_hold_days_from_mode).astype(int)

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

    expiry_cache: dict[tuple[str, int], str | None] = {}

    def _get_expiry(ed: str, mhd: int):
        key = (ed, int(mhd))
        if key not in expiry_cache:
            expiry_cache[key] = dtutils.add_biz_days(ed, int(mhd))
        return expiry_cache[key]

    df["entry_date"] = entry_date
    df["po_type"] = po_type
    df["expiry_date"] = df["max_hold_days"].apply(lambda mhd: _get_expiry(entry_date, int(mhd)))
    return df


def _ensure_compat_schema(df: pd.DataFrame, base_date: str) -> pd.DataFrame:
    if df.empty:
        return _empty_candidates()

    df = df.copy()
    df["mode"] = df["setup"].apply(_mode_from_setup)
    df["foreign_netbuy"] = np.nan
    df["inst_netbuy"] = np.nan
    df["indi_netbuy"] = np.nan

    for col in ["rsi", "macd", "macd_signal", "macd_hist", "bb_upper", "bb_middle", "bb_lower", "stoch_k", "stoch_d"]:
        if col not in df.columns:
            df[col] = np.nan

    df = _apply_entry_schedule(df, base_date)

    for col in COMPAT_COLUMNS:
        if col not in df.columns:
            df[col] = np.nan

    extra_cols = [c for c in df.columns if c not in COMPAT_COLUMNS]
    return df[COMPAT_COLUMNS + extra_cols]


def _analyze_stock(
    ticker: str,
    name: str,
    market: str,
    market_regime: dict,
    df: pd.DataFrame,
    base_date: str,
) -> dict | None:
    if df.empty:
        return None

    frame = df.copy()
    frame = frame[frame["trd_dd"] <= base_date].sort_values("trd_dd").set_index("trd_dd")
    for col in [
        "tdd_opnprc",
        "tdd_hgprc",
        "tdd_lwprc",
        "tdd_clsprc",
        "acc_trdvol",
        "acc_trdval",
    ]:
        frame[col] = pd.to_numeric(frame[col], errors="coerce")
    frame = frame.dropna()
    if len(frame) < MIN_HISTORY:
        return None

    open_ = frame["tdd_opnprc"]
    high = frame["tdd_hgprc"]
    low = frame["tdd_lwprc"]
    close = frame["tdd_clsprc"]
    volume = frame["acc_trdvol"]
    turnover = frame["acc_trdval"]
    atr = calc_atr(high, low, close)
    ma5 = close.rolling(5).mean()
    ma20 = close.rolling(20).mean()
    ma60 = close.rolling(60).mean()
    range_ = high - low

    if len(close) < max(60, CFG.recent_high_days + 5):
        return None

    c0, c1 = float(close.iloc[-1]), float(close.iloc[-2])
    o0 = float(open_.iloc[-1])
    h0, l0 = float(high.iloc[-1]), float(low.iloc[-1])
    atr0 = float(atr.iloc[-1])
    ma5_0 = float(ma5.iloc[-1])
    ma20_0 = float(ma20.iloc[-1])
    ma20_5 = float(ma20.iloc[-6]) if len(ma20) >= 6 else ma20_0
    ma60_0 = float(ma60.iloc[-1])

    if any(pd.isna(v) for v in [atr0, ma5_0, ma20_0, ma60_0]) or atr0 <= 0:
        return None

    recent_high = float(high.iloc[-CFG.recent_high_days - 1:-1].max())
    recent_close_high10 = float(close.iloc[-11:-1].max())
    range20 = _safe_mean(range_.iloc[-21:-1], default=max(h0 - l0, 1.0))
    range5_prev = _safe_mean(range_.iloc[-6:-1], default=range20)
    vol20_prev = _safe_mean(volume.iloc[-21:-1], default=max(float(volume.iloc[-1]), 1.0))
    trdval20 = _safe_mean(turnover.iloc[-20:], default=0.0)
    vol_ratio = float(volume.iloc[-1]) / vol20_prev if vol20_prev > 0 else 1.0
    atr_pct = atr0 / c0 if c0 > 0 else 0.0
    ret5 = _safe_pct(c0, float(close.iloc[-6])) if len(close) >= 6 else 0.0
    ret20 = _safe_pct(c0, float(close.iloc[-21])) if len(close) >= 21 else 0.0
    gap_pct = _safe_pct(o0, c1)
    day_range = max(h0 - l0, 1.0)
    close_location = (c0 - l0) / day_range
    upper_wick_ratio = (h0 - max(c0, o0)) / day_range
    range_ratio = day_range / range20 if range20 > 0 else 1.0
    touched_ma20 = l0 <= ma20_0 * (1.0 + CFG.pullback_margin)
    contraction = range5_prev < range20 * 0.85

    score = market_regime["bonus"]
    flags: list[str] = [f"MRK_{market_regime['label']}"]
    setup_scores = {"BREAKOUT": 0, "PULLBACK": 0, "IGNITION": 0}

    if c0 > ma20_0 > ma60_0:
        score += 3
        flags.append("UP20_60")
        setup_scores["BREAKOUT"] += 1
        setup_scores["PULLBACK"] += 1
    elif c0 > ma20_0 and ma20_0 > ma60_0:
        score += 2
        flags.append("UP20")

    if ma20_0 > ma20_5:
        score += 1
        flags.append("MA20_UP")
    else:
        score -= 1
        flags.append("MA20_FLAT")

    if c0 > ma5_0 > ma20_0:
        score += 1
        flags.append("MA5_LEAD")

    if 0.03 <= ret20 <= 0.30:
        score += 1
        flags.append("RS20")
    elif ret20 > 0.40:
        score -= 2
        flags.append("OVERHEAT")

    if 0.015 <= atr_pct <= 0.08:
        score += 1
        flags.append("ATR_OK")
    elif atr_pct > 0.10:
        score -= 1
        flags.append("ATR_HI")

    if close_location >= 0.75:
        score += 1
        flags.append("CLOSE_HI")
    elif close_location < 0.50:
        score -= 1
        flags.append("CLOSE_WEAK")

    if vol_ratio >= 1.40:
        score += 2
        flags.append("VOL_SURGE")
    elif vol_ratio >= 1.10:
        score += 1
        flags.append("VOL_OK")

    if c0 > recent_high and close_location >= 0.65:
        score += 3
        flags.append("BRK")
        setup_scores["BREAKOUT"] += 3
    elif c0 >= recent_high * 0.985 and c0 > ma20_0:
        score += 2
        flags.append("NEAR_BRK")
        setup_scores["BREAKOUT"] += 2

    if touched_ma20 and c0 > ma20_0 and c0 > o0 and c0 > c1:
        score += 2
        flags.append("PB20")
        setup_scores["PULLBACK"] += 3

    if contraction and c0 > recent_close_high10 and close_location >= 0.65:
        score += 2
        flags.append("IGN")
        setup_scores["IGNITION"] += 3

    if upper_wick_ratio > 0.35 and close_location < 0.60:
        score -= 2
        flags.append("UPPER_WICK")

    if c0 < ma20_0 or ma20_0 < ma60_0:
        score -= 3
        flags.append("WEAK_TREND")

    if c0 > ma20_0 * 1.12:
        score -= 2
        flags.append("EXT")

    if CFG.label == "DAY":
        if vol_ratio >= 1.60 and range_ratio >= 1.10 and close_location >= 0.75:
            score += 2
            flags.append("DAY_GO")
            setup_scores["BREAKOUT"] += 1
            setup_scores["IGNITION"] += 1
        if gap_pct >= 0.05:
            score -= 2
            flags.append("GAP_HOT")
        if ret5 <= -0.04:
            score -= 1
            flags.append("RET5_BAD")
    else:
        if close.iloc[-10:].min() >= ma20.iloc[-10:].min() * 0.97 and c0 >= recent_close_high10:
            score += 2
            flags.append("BASE10")
            setup_scores["PULLBACK"] += 1
            setup_scores["BREAKOUT"] += 1
        if gap_pct >= 0.07:
            score -= 1
            flags.append("GAP_RISK")

    if max(setup_scores.values()) < 2:
        return None
    if score < CFG.score_threshold:
        return None

    setup = _classify_setup(setup_scores)
    entry_price, stop_loss, take_profit = _build_trade_levels(
        c0,
        atr0,
        ma20_0,
        recent_high,
        setup,
    )
    risk = entry_price - stop_loss
    rr = (take_profit - entry_price) / risk if risk > 0 else np.nan

    return {
        "base_date": base_date,
        "ticker": ticker,
        "name": name,
        "market": market,
        "strategy": CFG.label,
        "setup": setup,
        "market_regime": market_regime["label"],
        "buy_score": int(score),
        "buy_flags": ",".join(flags),
        "close": int(round(c0)),
        "entry_price": int(entry_price),
        "stop_loss": int(stop_loss),
        "take_profit": int(take_profit),
        "rr": round(float(rr), 2) if pd.notna(rr) else np.nan,
        "atr": round(atr0, 2),
        "atr_pct": round(atr_pct * 100, 2),
        "ret5_pct": round(ret5 * 100, 2),
        "ret20_pct": round(ret20 * 100, 2),
        "vol_ratio": round(vol_ratio, 2),
        "avg_trdval20": int(round(trdval20)),
    }


def _scan_market(
    table: str,
    market: str,
    base_date: str,
    name_map: dict[str, str],
    market_regime: dict,
) -> list[dict]:
    df_all = _fetch_market_ohlcv(table, base_date)
    if df_all.empty:
        log.warning("[%s] OHLCV 데이터 없음", market)
        return []

    results: list[dict] = []
    for ticker, group in df_all.groupby("isu_srt_cd"):
        row = _analyze_stock(
            ticker=ticker,
            name=name_map.get(ticker, ticker),
            market=market,
            market_regime=market_regime,
            df=group,
            base_date=base_date,
        )
        if row is not None:
            results.append(row)

    if not results:
        return []

    frame = pd.DataFrame(results).sort_values(
        ["buy_score", "rr", "avg_trdval20"],
        ascending=[False, False, False],
    )
    limited = frame.head(CFG.max_candidates_per_market)
    log.info(
        "[%s] 시장레짐=%s, 후보=%s건, 상위=%s건 채택",
        market,
        market_regime["label"],
        len(frame),
        len(limited),
    )
    return limited.to_dict("records")


def find_candidates(base_date: str | None = None, report: bool = False, save_report: bool = False) -> pd.DataFrame:
    if base_date is None:
        base_date = dtutils.get_latest_biz_date()

    if not query.has_ohlcv_data(base_date):
        log.warning("%s - ohlcv 데이터가 없습니다", base_date)
        return _empty_candidates()

    name_map = query.get_ticker_name_map()
    market_regimes = _load_market_regimes(base_date)
    results: list[dict] = []
    results.extend(
        _scan_market(
            "isu_ksp_ohlcv",
            "KOSPI",
            base_date,
            name_map,
            market_regimes["KOSPI"],
        )
    )
    results.extend(
        _scan_market(
            "isu_ksd_ohlcv",
            "KOSDAQ",
            base_date,
            name_map,
            market_regimes["KOSDAQ"],
        )
    )

    df = pd.DataFrame(results)
    if df.empty:
        df = _empty_candidates()
        if report:
            print_report(df, base_date, market_regimes)
        return df

    df = df.sort_values(
        ["buy_score", "rr", "avg_trdval20"],
        ascending=[False, False, False],
    ).reset_index(drop=True)
    df = _ensure_compat_schema(df, base_date)

    if report:
        print_report(df, base_date, market_regimes)
    if save_report:
        save_candidates(df, base_date)
    return df


def print_report(df: pd.DataFrame, base_date: str, market_regimes: dict[str, dict]):
    print()
    print("=" * 120)
    print(f"  Codex Scanner Report  |  strategy={CFG.label}  base_date={base_date}")
    print("=" * 120)
    for market in ("KOSPI", "KOSDAQ"):
        rg = market_regimes[market]
        print(
            f"  {market:<6s}  regime={rg['label']:<7s}"
            f"  score={rg['score']:+d}"
            f"  close={rg['close']:.2f}"
            f"  ma20={rg['ma20']:.2f}"
            f"  ma60={rg['ma60']:.2f}"
            f"  ret5={rg['ret5']:+.2%}"
        )
    print("-" * 120)

    if df.empty:
        print("  투자 대상 없음")
        print("=" * 120)
        return

    entry_date = df.iloc[0]["entry_date"]
    print(f"  entry_date={entry_date}  candidates={len(df)}")
    summary = (
        df.groupby(["market", "setup"])
        .agg(
            종목수=("ticker", "count"),
            평균점수=("buy_score", "mean"),
            평균RR=("rr", "mean"),
            평균거래대금20=("avg_trdval20", "mean"),
        )
        .round(2)
        .reset_index()
    )
    print(summary.to_string(index=False))
    print("-" * 120)

    for market in ("KOSPI", "KOSDAQ"):
        subset = df[df["market"] == market]
        if subset.empty:
            continue
        print(f"  [{market}]")
        for _, row in subset.iterrows():
            print(
                f"  [{row['buy_score']:2d}pt/{row['mode']:<3s}/{row['setup']:<8s}]"
                f" {row['ticker']} {row['name'][:14]:<14s}"
                f" 종가 {row['close']:>8,}"
                f" 진입≤{row['entry_price']:>8,}"
                f" 손절 {row['stop_loss']:>8,}"
                f" 익절 {row['take_profit']:>8,}"
                f" RR {row['rr'] if pd.notna(row['rr']) else 'N/A':>4}"
                f" 거래대금20 {row['avg_trdval20']:>12,}"
                f" vol {row['vol_ratio']:>4.2f}"
                f" r20 {row['ret20_pct']:>+6.2f}%"
                f" flags: {row['buy_flags']}"
            )
        print()

    print("=" * 120)


def save_candidates(df: pd.DataFrame, base_date: str) -> Path:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    path = REPORT_DIR / f"scanner_codex_{CFG.label.lower()}_{base_date}.csv"
    df.to_csv(path, index=False, encoding="utf-8-sig")
    log.info("리포트 저장: %s", path)
    return path


def main(argv: list[str] | None = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    base_date = argv[0] if argv else None
    save_report = "--save" in argv
    if base_date == "--save":
        base_date = None

    df = find_candidates(base_date=base_date, report=True, save_report=save_report)
    return 0 if df is not None else 1


if __name__ == "__main__":
    raise SystemExit(main())
