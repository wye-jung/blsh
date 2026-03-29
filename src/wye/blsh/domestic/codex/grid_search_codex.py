"""
Independent optimizer for the codex scanner strategy.

This module keeps all optimization logic under the codex package and does not
depend on the legacy optimize package. It bulk-loads OHLCV/index data once,
replays the codex selection rules over a configurable date range, simulates
trades with codex-specific risk parameters, and can write the best factors back
to factor_codex.py.
"""

from __future__ import annotations

import argparse
import logging
import math
import re
import time
from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from wye.blsh.common import dtutils
from wye.blsh.database import select_all
from wye.blsh.database import query
from wye.blsh.domestic import Tick, sector
from wye.blsh.domestic.codex import factor_codex
from wye.blsh.domestic.codex.scanner_codex import LOOKBACK_DAYS, MIN_HISTORY, calc_atr

log = logging.getLogger(__name__)

_FACTOR_PATH = Path(__file__).resolve().parent / "factor_codex.py"
_TABLES = {
    "KOSPI": "isu_ksp_ohlcv",
    "KOSDAQ": "isu_ksd_ohlcv",
}

DAY_SPACE = {
    "invest_min_score": [8, 9, 10, 11, 12],
    "avg_trdval_min": [1_000_000_000, 2_000_000_000, 3_000_000_000, 5_000_000_000],
    "recent_high_days": [15, 20, 25, 30],
    "pullback_margin": [0.010, 0.015, 0.020, 0.025],
    "entry_atr_mult": [0.00, 0.05, 0.10, 0.15, 0.20],
    "atr_sl_mult": [1.2, 1.4, 1.6, 1.8, 2.0],
    "atr_tp_mult": [1.8, 2.2, 2.6, 3.0, 3.4],
    "tp1_mult": [0.6, 0.8, 1.0, 1.2],
    "tp1_ratio": [0.3, 0.5, 0.7, 1.0],
    "max_hold_days": [0, 1, 2],
    "max_hold_days_mix": [1, 2, 3],
    "max_hold_days_mom": [1, 2, 3],
    "max_candidates_per_market": [5, 8, 10, 12],
}

SWING_SPACE = {
    "invest_min_score": [7, 8, 9, 10, 11],
    "avg_trdval_min": [700_000_000, 1_000_000_000, 1_500_000_000, 2_000_000_000],
    "recent_high_days": [40, 55, 70, 90],
    "pullback_margin": [0.015, 0.020, 0.025, 0.030],
    "entry_atr_mult": [0.10, 0.20, 0.30, 0.40],
    "atr_sl_mult": [1.8, 2.2, 2.6, 3.0],
    "atr_tp_mult": [2.6, 3.2, 3.8, 4.4],
    "tp1_mult": [1.0, 1.5, 2.0],
    "tp1_ratio": [0.2, 0.3, 0.5],
    "max_hold_days": [5, 7, 10, 15],
    "max_hold_days_mix": [3, 5, 7, 10],
    "max_hold_days_mom": [2, 3, 5, 7],
    "max_candidates_per_market": [8, 10, 12, 15],
}


@dataclass(frozen=True)
class Params:
    mode: str
    invest_min_score: int
    avg_trdval_min: int
    recent_high_days: int
    pullback_margin: float
    entry_atr_mult: float
    atr_sl_mult: float
    atr_tp_mult: float
    tp1_mult: float
    tp1_ratio: float
    max_hold_days: int
    max_hold_days_mix: int
    max_hold_days_mom: int
    max_candidates_per_market: int

    def as_factor_dict(self) -> dict[str, int | float]:
        return {
            "INVEST_MIN_SCORE": self.invest_min_score,
            "AVG_TRDVAL_MIN": self.avg_trdval_min,
            "RECENT_HIGH_DAYS": self.recent_high_days,
            "PULLBACK_MARGIN": self.pullback_margin,
            "ENTRY_ATR_MULT": self.entry_atr_mult,
            "ATR_SL_MULT": self.atr_sl_mult,
            "ATR_TP_MULT": self.atr_tp_mult,
            "TP1_MULT": self.tp1_mult,
            "TP1_RATIO": self.tp1_ratio,
            "MAX_HOLD_DAYS": self.max_hold_days,
            "MAX_HOLD_DAYS_MIX": self.max_hold_days_mix,
            "MAX_HOLD_DAYS_MOM": self.max_hold_days_mom,
            "MAX_CANDIDATES_PER_MARKET": self.max_candidates_per_market,
        }

    def label(self) -> str:
        return (
            f"score>={self.invest_min_score} "
            f"val>={self.avg_trdval_min/1e9:.1f}B "
            f"high={self.recent_high_days} "
            f"pb={self.pullback_margin:.3f} "
            f"entry={self.entry_atr_mult:.2f} "
            f"sl={self.atr_sl_mult:.1f} tp={self.atr_tp_mult:.1f} "
            f"tp1={self.tp1_mult:.1f}({self.tp1_ratio:.0%}) "
            f"hold={self.max_hold_days}/{self.max_hold_days_mix}/{self.max_hold_days_mom} "
            f"cap={self.max_candidates_per_market}"
        )


@dataclass
class Stats:
    trades: int = 0
    wins: int = 0
    losses: int = 0
    holds: int = 0
    total_ret: float = 0.0

    @property
    def win_rate(self) -> float:
        return self.wins / self.trades * 100 if self.trades else 0.0

    @property
    def avg_ret(self) -> float:
        return self.total_ret / self.trades if self.trades else 0.0

    @property
    def metric(self) -> float:
        if self.trades < 20:
            return -99999.0
        return self.total_ret * min(1.0, self.trades / 80)


@dataclass
class MarketData:
    features: pd.DataFrame
    price_by_ticker: dict[str, pd.DataFrame]
    next_biz: dict[str, str]
    biz_index: dict[str, int]
    biz_dates: list[str]
    scan_dates: list[str]


def _default_params(mode: str) -> Params:
    factors = factor_codex.DAY_FACTORS if mode == "DAY" else factor_codex.SWING_FACTORS
    return Params(
        mode=mode,
        invest_min_score=int(factors["INVEST_MIN_SCORE"]),
        avg_trdval_min=int(factors["AVG_TRDVAL_MIN"]),
        recent_high_days=int(factors["RECENT_HIGH_DAYS"]),
        pullback_margin=float(factors["PULLBACK_MARGIN"]),
        entry_atr_mult=float(factors["ENTRY_ATR_MULT"]),
        atr_sl_mult=float(factors["ATR_SL_MULT"]),
        atr_tp_mult=float(factors["ATR_TP_MULT"]),
        tp1_mult=float(factors["TP1_MULT"]),
        tp1_ratio=float(factors["TP1_RATIO"]),
        max_hold_days=int(factors["MAX_HOLD_DAYS"]),
        max_hold_days_mix=int(factors["MAX_HOLD_DAYS_MIX"]),
        max_hold_days_mom=int(factors["MAX_HOLD_DAYS_MOM"]),
        max_candidates_per_market=int(factors["MAX_CANDIDATES_PER_MARKET"]),
    )


def _load_ohlcv(start_date: str, end_date: str) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for market, table in _TABLES.items():
        rows = select_all(
            f"""
            SELECT isu_srt_cd AS ticker,
                   trd_dd,
                   tdd_opnprc AS open,
                   tdd_hgprc AS high,
                   tdd_lwprc AS low,
                   tdd_clsprc AS close,
                   acc_trdvol AS volume,
                   acc_trdval AS trdval
            FROM {table}
            WHERE trd_dd >= :start_date
              AND trd_dd <= :end_date
            ORDER BY isu_srt_cd, trd_dd
            """,
            start_date=start_date,
            end_date=end_date,
        )
        df = pd.DataFrame(rows)
        if df.empty:
            continue
        df["market"] = market
        frames.append(df)

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    numeric_cols = ["open", "high", "low", "close", "volume", "trdval"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna().sort_values(["ticker", "trd_dd"]).reset_index(drop=True)
    return df


def _load_market_regimes(start_date: str, end_date: str) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for idx_nm, idx_clss, market in [
        ("코스피", sector.IDX_CLSS_KOSPI, "KOSPI"),
        ("코스닥", sector.IDX_CLSS_KOSDAQ, "KOSDAQ"),
    ]:
        rows = select_all(
            """
            SELECT trd_dd, clsprc_idx
            FROM idx_stk_ohlcv
            WHERE idx_nm = :idx_nm
              AND idx_clss = :idx_clss
              AND trd_dd >= :start_date
              AND trd_dd <= :end_date
            ORDER BY trd_dd
            """,
            idx_nm=idx_nm,
            idx_clss=idx_clss,
            start_date=start_date,
            end_date=end_date,
        )
        frame = pd.DataFrame(rows)
        if frame.empty:
            continue
        frame["clsprc_idx"] = pd.to_numeric(frame["clsprc_idx"], errors="coerce")
        frame["market"] = market
        close = frame["clsprc_idx"]
        ma20 = close.shift(1).rolling(20).mean()
        ma60 = close.shift(1).rolling(60).mean()
        prev_ma20 = close.shift(2).rolling(20).mean()
        ret5 = close / close.shift(5) - 1

        score = np.zeros(len(frame), dtype=np.int16)
        score += np.where(close >= ma20, 2, -2)
        score += np.where(ma20 >= ma60, 1, -1)
        score += np.where(ma20 >= prev_ma20, 1, -1)
        score += np.where(ret5 >= 0.01, 1, np.where(ret5 <= -0.03, -1, 0))

        label = np.where(score >= 3, "BULL", np.where(score >= 0, "NEUTRAL", "WEAK"))
        bonus = np.where(score >= 3, 1, np.where(score >= 0, 0, -2))

        frame = frame.assign(
            market_bonus=bonus,
            market_label=label,
            market_score=score,
        )
        frames.append(frame[["trd_dd", "market", "market_bonus", "market_label", "market_score"]])

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _recent_windows() -> list[int]:
    values = set(DAY_SPACE["recent_high_days"]) | set(SWING_SPACE["recent_high_days"])
    values.add(int(factor_codex.DAY_FACTORS["RECENT_HIGH_DAYS"]))
    values.add(int(factor_codex.SWING_FACTORS["RECENT_HIGH_DAYS"]))
    return sorted(values)


def _build_features(
    ohlcv: pd.DataFrame,
    regime_df: pd.DataFrame,
    name_map: dict[str, str],
    scan_start: str,
) -> pd.DataFrame:
    windows = _recent_windows()
    frames: list[pd.DataFrame] = []
    grouped = ohlcv.groupby("ticker", sort=False)
    total = grouped.ngroups

    for idx, (ticker, group) in enumerate(grouped, 1):
        market = group["market"].iat[0]
        name = name_map.get(ticker, ticker)
        g = group.sort_values("trd_dd").reset_index(drop=True)
        close = g["close"]
        high = g["high"]
        low = g["low"]
        open_ = g["open"]
        volume = g["volume"]
        trdval = g["trdval"]

        atr = calc_atr(high, low, close)
        ma5 = close.rolling(5).mean()
        ma20 = close.rolling(20).mean()
        ma60 = close.rolling(60).mean()
        day_range = (high - low).clip(lower=1.0)
        range_ = high - low

        frame = pd.DataFrame(
            {
                "base_date": g["trd_dd"],
                "ticker": ticker,
                "name": name,
                "market": market,
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
                "prev_close": close.shift(1),
                "atr": atr,
                "ma5": ma5,
                "ma20": ma20,
                "ma20_5": ma20.shift(5),
                "ma60": ma60,
                "recent_close_high10": close.shift(1).rolling(10).max(),
                "close_min10": close.rolling(10).min(),
                "ma20_min10": ma20.rolling(10).min(),
                "range20": range_.shift(1).rolling(20).mean(),
                "range5_prev": range_.shift(1).rolling(5).mean(),
                "vol20_prev": volume.shift(1).rolling(20).mean(),
                "avg_trdval20": trdval.rolling(20).mean(),
                "atr_pct": atr / close,
                "ret5": close / close.shift(5) - 1,
                "ret20": close / close.shift(20) - 1,
                "gap_pct": open_ / close.shift(1) - 1,
                "close_location": (close - low) / day_range,
                "upper_wick_ratio": (high - np.maximum(close, open_)) / day_range,
                "range_ratio": day_range / range_.shift(1).rolling(20).mean(),
                "bar_count": np.arange(1, len(g) + 1, dtype=np.int32),
            }
        )
        for window in windows:
            frame[f"recent_high_{window}"] = high.shift(1).rolling(window).max()

        frames.append(frame[frame["base_date"] >= scan_start])
        if idx % 300 == 0 or idx == total:
            log.info("feature prep: %s/%s tickers", idx, total)

    features = pd.concat(frames, ignore_index=True)
    features = features.merge(
        regime_df,
        left_on=["base_date", "market"],
        right_on=["trd_dd", "market"],
        how="left",
    ).drop(columns=["trd_dd"])
    features["market_bonus"] = features["market_bonus"].fillna(0).astype(int)
    features["market_score"] = features["market_score"].fillna(0).astype(int)
    features["market_label"] = features["market_label"].fillna("UNKNOWN")
    return features


def _build_price_map(ohlcv: pd.DataFrame) -> dict[str, pd.DataFrame]:
    price_map: dict[str, pd.DataFrame] = {}
    for ticker, group in ohlcv.groupby("ticker", sort=False):
        price_map[ticker] = (
            group[["trd_dd", "open", "high", "low", "close"]]
            .drop_duplicates(subset="trd_dd")
            .set_index("trd_dd")
            .sort_index()
        )
    return price_map


def _load_market_data(years: int) -> MarketData:
    end_date = query.get_max_ohlcv_date()
    scan_start = dtutils.add_days(end_date, -years * 365)
    load_start = dtutils.add_days(scan_start, -LOOKBACK_DAYS)

    log.info("loading OHLCV: %s ~ %s", load_start, end_date)
    ohlcv = _load_ohlcv(load_start, end_date)
    if ohlcv.empty:
        raise RuntimeError("OHLCV data not found.")

    log.info("loading market regimes")
    regime_df = _load_market_regimes(load_start, end_date)
    name_map = query.get_ticker_name_map()

    log.info("building feature store")
    features = _build_features(ohlcv, regime_df, name_map, scan_start)

    biz_dates = [row["d"] for row in query.get_biz_dates(scan_start, end_date)]
    biz_dates = [d for d in biz_dates if d >= scan_start and d <= end_date]
    biz_dates.sort()
    biz_index = {d: i for i, d in enumerate(biz_dates)}
    next_biz = {biz_dates[i]: biz_dates[i + 1] for i in range(len(biz_dates) - 1)}

    return MarketData(
        features=features,
        price_by_ticker=_build_price_map(ohlcv),
        next_biz=next_biz,
        biz_index=biz_index,
        biz_dates=biz_dates,
        scan_dates=biz_dates[:-1],
    )


def _series_to_int(values: np.ndarray) -> np.ndarray:
    return np.nan_to_num(values, nan=0.0).astype(np.int16)


def _tick_levels(rows: pd.DataFrame, params: Params, recent_high: pd.Series) -> pd.DataFrame:
    entries: list[int] = []
    stops: list[int] = []
    targets: list[int] = []
    rrs: list[float] = []

    for row, recent in zip(rows.itertuples(index=False), recent_high):
        if row.setup == "BREAKOUT":
            entry_anchor = max(row.close, recent)
        elif row.setup == "PULLBACK":
            entry_anchor = max(row.close, row.ma20)
        else:
            entry_anchor = row.close

        entry_price = Tick.ceil_tick(entry_anchor + row.atr * params.entry_atr_mult)
        stop_seed = min(row.ma20, row.close - row.atr * params.atr_sl_mult)
        stop_loss = Tick.floor_tick(stop_seed)
        if stop_loss <= 0 or stop_loss >= entry_price:
            stop_loss = Tick.floor_tick(row.close - max(row.atr, row.close * 0.03))

        target_seed = max(
            row.close + row.atr * params.atr_tp_mult,
            entry_price + max(entry_price - stop_loss, 1) * 1.8,
        )
        take_profit = Tick.ceil_tick(target_seed)
        risk = entry_price - stop_loss
        rr = (take_profit - entry_price) / risk if risk > 0 else np.nan

        entries.append(int(entry_price))
        stops.append(int(stop_loss))
        targets.append(int(take_profit))
        rrs.append(float(rr) if not math.isnan(rr) else np.nan)

    return pd.DataFrame(
        {
            "entry_price": entries,
            "stop_loss": stops,
            "take_profit": targets,
            "rr": rrs,
        }
    )


def _build_candidates(data: MarketData, params: Params) -> pd.DataFrame:
    df = data.features
    recent_col = f"recent_high_{params.recent_high_days}"
    recent_high = df[recent_col].to_numpy()
    close = df["close"].to_numpy()
    open_ = df["open"].to_numpy()
    low = df["low"].to_numpy()
    prev_close = df["prev_close"].to_numpy()
    ma5 = df["ma5"].to_numpy()
    ma20 = df["ma20"].to_numpy()
    ma20_5 = df["ma20_5"].to_numpy()
    ma60 = df["ma60"].to_numpy()
    atr = df["atr"].to_numpy()
    atr_pct = df["atr_pct"].to_numpy()
    ret5 = df["ret5"].to_numpy()
    ret20 = df["ret20"].to_numpy()
    vol_ratio = np.divide(
        df["volume"].to_numpy(),
        df["vol20_prev"].to_numpy(),
        out=np.ones(len(df), dtype=float),
        where=df["vol20_prev"].to_numpy() > 0,
    )
    close_location = df["close_location"].to_numpy()
    upper_wick_ratio = df["upper_wick_ratio"].to_numpy()
    range_ratio = df["range_ratio"].to_numpy()
    recent_close_high10 = df["recent_close_high10"].to_numpy()
    close_min10 = df["close_min10"].to_numpy()
    ma20_min10 = df["ma20_min10"].to_numpy()
    avg_trdval20 = df["avg_trdval20"].to_numpy()
    gap_pct = df["gap_pct"].to_numpy()
    market_bonus = df["market_bonus"].to_numpy()
    bar_count = df["bar_count"].to_numpy()

    valid = (
        (bar_count >= MIN_HISTORY)
        & np.isfinite(recent_high)
        & np.isfinite(ma20)
        & np.isfinite(ma60)
        & np.isfinite(atr)
        & (atr > 0)
        & (avg_trdval20 >= params.avg_trdval_min)
    )

    score = market_bonus.astype(np.int16).copy()
    brk = np.zeros(len(df), dtype=np.int16)
    pb = np.zeros(len(df), dtype=np.int16)
    ign = np.zeros(len(df), dtype=np.int16)

    mask = (close > ma20) & (ma20 > ma60)
    score += np.where(mask, 3, 0)
    brk += np.where(mask, 1, 0)
    pb += np.where(mask, 1, 0)

    mask2 = (~mask) & (close > ma20) & (ma20 > ma60)
    score += np.where(mask2, 2, 0)

    up_ma20 = ma20 > ma20_5
    score += np.where(up_ma20, 1, -1)

    score += np.where((close > ma5) & (ma5 > ma20), 1, 0)
    score += np.where((ret20 >= 0.03) & (ret20 <= 0.30), 1, 0)
    score += np.where(ret20 > 0.40, -2, 0)
    score += np.where((atr_pct >= 0.015) & (atr_pct <= 0.08), 1, 0)
    score += np.where(atr_pct > 0.10, -1, 0)
    score += np.where(close_location >= 0.75, 1, 0)
    score += np.where(close_location < 0.50, -1, 0)
    score += np.where(vol_ratio >= 1.40, 2, np.where(vol_ratio >= 1.10, 1, 0))

    breakout = (close > recent_high) & (close_location >= 0.65)
    near_break = (~breakout) & (close >= recent_high * 0.985) & (close > ma20)
    score += np.where(breakout, 3, 0)
    brk += np.where(breakout, 3, 0)
    score += np.where(near_break, 2, 0)
    brk += np.where(near_break, 2, 0)

    pullback = (
        (low <= ma20 * (1.0 + params.pullback_margin))
        & (close > ma20)
        & (close > open_)
        & (close > prev_close)
    )
    score += np.where(pullback, 2, 0)
    pb += np.where(pullback, 3, 0)

    contraction = df["range5_prev"].to_numpy() < df["range20"].to_numpy() * 0.85
    ignition = contraction & (close > recent_close_high10) & (close_location >= 0.65)
    score += np.where(ignition, 2, 0)
    ign += np.where(ignition, 3, 0)

    score += np.where((upper_wick_ratio > 0.35) & (close_location < 0.60), -2, 0)
    score += np.where((close < ma20) | (ma20 < ma60), -3, 0)
    score += np.where(close > ma20 * 1.12, -2, 0)

    if params.mode == "DAY":
        extra = (vol_ratio >= 1.60) & (range_ratio >= 1.10) & (close_location >= 0.75)
        score += np.where(extra, 2, 0)
        brk += np.where(extra, 1, 0)
        ign += np.where(extra, 1, 0)
        score += np.where(gap_pct >= 0.05, -2, 0)
        score += np.where(ret5 <= -0.04, -1, 0)
    else:
        base10 = (close_min10 >= ma20_min10 * 0.97) & (close >= recent_close_high10)
        score += np.where(base10, 2, 0)
        pb += np.where(base10, 1, 0)
        brk += np.where(base10, 1, 0)
        score += np.where(gap_pct >= 0.07, -1, 0)

    setup_max = np.maximum(brk, np.maximum(pb, ign))
    mask = valid & (setup_max >= 2) & (score >= params.invest_min_score)
    if not np.any(mask):
        return pd.DataFrame()

    chosen = df.loc[mask, [
        "base_date",
        "ticker",
        "name",
        "market",
        "close",
        "ma20",
        "atr",
        "avg_trdval20",
    ]].copy()
    chosen["buy_score"] = score[mask]
    chosen["vol_ratio"] = vol_ratio[mask]
    chosen["ret20_pct"] = ret20[mask] * 100
    chosen["recent_high"] = recent_high[mask]
    chosen["brk"] = brk[mask]
    chosen["pb"] = pb[mask]
    chosen["ign"] = ign[mask]

    best_break = (chosen["brk"] >= chosen["pb"]) & (chosen["brk"] >= chosen["ign"])
    best_pull = (~best_break) & (chosen["pb"] >= chosen["ign"])
    chosen["setup"] = np.where(best_break, "BREAKOUT", np.where(best_pull, "PULLBACK", "IGNITION"))
    chosen["mode"] = chosen["setup"].map(
        {"BREAKOUT": "MOM", "PULLBACK": "REV", "IGNITION": "MIX"}
    )
    chosen["atr_sl_mult"] = params.atr_sl_mult
    chosen["atr_tp_mult"] = params.atr_tp_mult
    chosen["tp1_mult"] = params.tp1_mult
    chosen["tp1_ratio"] = params.tp1_ratio
    chosen["max_hold_days"] = chosen["mode"].map(
        {
            "REV": params.max_hold_days,
            "MIX": params.max_hold_days_mix,
            "MOM": params.max_hold_days_mom,
        }
    )

    levels = _tick_levels(chosen, params, chosen["recent_high"])
    chosen = pd.concat([chosen.reset_index(drop=True), levels], axis=1)
    chosen = chosen.sort_values(
        ["base_date", "market", "buy_score", "rr", "avg_trdval20"],
        ascending=[True, True, False, False, False],
    )
    chosen = (
        chosen.groupby(["base_date", "market"], group_keys=False)
        .head(params.max_candidates_per_market)
        .reset_index(drop=True)
    )
    return chosen


def _simulate_row(row: pd.Series, data: MarketData, params: Params) -> tuple[str, float] | None:
    entry_date = data.next_biz.get(row["base_date"])
    if not entry_date:
        return None

    price_df = data.price_by_ticker.get(row["ticker"])
    if price_df is None or entry_date not in price_df.index:
        return None

    t1 = price_df.loc[entry_date]
    if float(t1["open"]) > float(row["entry_price"]):
        return None

    buy = float(t1["open"])
    atr = float(row["atr"])
    sl = Tick.floor_tick(buy - params.atr_sl_mult * atr)
    tp1 = Tick.ceil_tick(buy + params.tp1_mult * atr)
    tp2 = Tick.ceil_tick(buy + params.atr_tp_mult * atr)

    mode = row["mode"]
    if mode == "MOM":
        max_days = params.max_hold_days_mom
    elif mode == "MIX":
        max_days = params.max_hold_days_mix
    else:
        max_days = params.max_hold_days

    idx = data.biz_index.get(entry_date)
    if idx is None:
        return None
    hold_dates = data.biz_dates[idx : idx + max_days + 1]
    if not hold_dates:
        return None

    remaining = 1.0
    pnl = 0.0
    t1_done = False
    prev_high = float(t1["high"])
    last_close = float(t1["close"])
    result_type = None

    for d in hold_dates:
        if d not in price_df.index:
            continue
        ohv = price_df.loc[d]
        low = float(ohv["low"])
        high = float(ohv["high"])
        close = float(ohv["close"])
        last_close = close

        if d != hold_dates[0]:
            trail = Tick.floor_tick(prev_high - params.atr_sl_mult * atr)
            if trail > sl and trail < prev_high:
                sl = trail

        if low <= sl:
            pnl += (sl - buy) * remaining - sl * remaining * 0.002
            result_type = "SL"
            remaining = 0.0
            break

        if (not t1_done) and high >= tp1:
            sell_ratio = min(params.tp1_ratio, remaining)
            pnl += (tp1 - buy) * sell_ratio - tp1 * sell_ratio * 0.002
            remaining -= sell_ratio
            t1_done = True
            if buy > sl:
                sl = buy

        prev_high = max(prev_high, high)

        if high >= tp2 and remaining > 0:
            pnl += (tp2 - buy) * remaining - tp2 * remaining * 0.002
            result_type = "TP"
            remaining = 0.0
            break

    if result_type is None:
        pnl += (last_close - buy) * remaining - last_close * remaining * 0.002
        result_type = "DAY" if max_days == 0 else "HOLD"

    ret_pct = (pnl / buy) * 100 if buy else 0.0
    return result_type, ret_pct


def backtest(data: MarketData, params: Params) -> Stats:
    candidates = _build_candidates(data, params)
    stats = Stats()
    if candidates.empty:
        return stats

    for _, row in candidates.iterrows():
        result = _simulate_row(row, data, params)
        if result is None:
            continue
        result_type, ret_pct = result
        stats.trades += 1
        stats.total_ret += ret_pct
        if result_type == "TP":
            stats.wins += 1
        elif result_type == "SL":
            stats.losses += 1
        else:
            stats.holds += 1
    return stats


def _search_space(mode: str) -> dict[str, list]:
    return DAY_SPACE if mode == "DAY" else SWING_SPACE


def _ordered_keys(mode: str) -> list[str]:
    if mode == "DAY":
        return [
            "invest_min_score",
            "avg_trdval_min",
            "recent_high_days",
            "pullback_margin",
            "entry_atr_mult",
            "atr_sl_mult",
            "atr_tp_mult",
            "tp1_mult",
            "tp1_ratio",
            "max_hold_days",
            "max_hold_days_mix",
            "max_hold_days_mom",
            "max_candidates_per_market",
        ]
    return [
        "invest_min_score",
        "avg_trdval_min",
        "recent_high_days",
        "pullback_margin",
        "entry_atr_mult",
        "atr_sl_mult",
        "atr_tp_mult",
        "tp1_mult",
        "tp1_ratio",
        "max_hold_days",
        "max_hold_days_mix",
        "max_hold_days_mom",
        "max_candidates_per_market",
    ]


def _rank_key(stats: Stats) -> tuple[float, float, int]:
    return (stats.metric, stats.total_ret, stats.trades)


def optimize_mode(data: MarketData, mode: str, passes: int = 2) -> tuple[Params, Stats]:
    space = _search_space(mode)
    best = _default_params(mode)
    cache: dict[Params, Stats] = {}

    def evaluate(p: Params) -> Stats:
        if p not in cache:
            cache[p] = backtest(data, p)
            s = cache[p]
            log.info(
                "[%s] trades=%s win=%.1f%% avg=%+.2f%% total=%+.1f%% metric=%+.1f | %s",
                mode,
                s.trades,
                s.win_rate,
                s.avg_ret,
                s.total_ret,
                s.metric,
                p.label(),
            )
        return cache[p]

    best_stats = evaluate(best)

    for pass_no in range(1, passes + 1):
        improved = False
        log.info("[%s] pass %s/%s", mode, pass_no, passes)
        for key in _ordered_keys(mode):
            current_best = best
            current_stats = best_stats
            for value in space[key]:
                candidate = replace(best, **{key: value})
                stats = evaluate(candidate)
                if _rank_key(stats) > _rank_key(current_stats):
                    current_best = candidate
                    current_stats = stats
            if current_best != best:
                best = current_best
                best_stats = current_stats
                improved = True
                log.info(
                    "[%s] improved via %s -> total=%+.1f%% trades=%s | %s",
                    mode,
                    key,
                    best_stats.total_ret,
                    best_stats.trades,
                    best.label(),
                )
        if not improved:
            break

    return best, best_stats


def _format_factor_dict(name: str, values: dict[str, int | float], note: str) -> str:
    comments = {
        "TP1_MULT": "1st partial take-profit ATR multiple",
        "TP1_RATIO": "fraction to sell at TP1",
    }
    lines = [f"# {name[1:]} optimized: {note}", f"{name} = {{"]
    for key, value in values.items():
        rendered = repr(value) if isinstance(value, float) and not float(value).is_integer() else str(int(value) if isinstance(value, float) and float(value).is_integer() else value)
        line = f'    "{key}": {rendered},'
        if key in comments:
            line += f"  # {comments[key]}"
        lines.append(line)
    lines.append("}")
    return "\n".join(lines)


def _existing_block(name: str) -> dict[str, int | float]:
    text = _FACTOR_PATH.read_text(encoding="utf-8")
    match = re.search(rf"{name}\s*=\s*\{{(.*?)\n\}}", text, re.S)
    if not match:
        raise RuntimeError(f"cannot read {name} from factor_codex.py")
    block = match.group(1)
    result: dict[str, int | float] = {}
    for line in block.splitlines():
        line = line.split("#", 1)[0].strip().rstrip(",")
        if not line:
            continue
        key, value = line.split(":", 1)
        key = key.strip().strip('"')
        value = value.strip()
        if "." in value:
            result[key] = float(value)
        else:
            result[key] = int(value)
    return result


def _existing_note(mode: str) -> str:
    text = _FACTOR_PATH.read_text(encoding="utf-8")
    match = re.search(rf"\[{mode}\]\s+(.+)", text)
    return match.group(1).strip() if match else "kept existing values"


def apply_results(best: dict[str, tuple[Params, Stats]], years: int) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    end_date = query.get_max_ohlcv_date()
    start_date = dtutils.add_days(end_date, -years * 365)
    day_vals = best["DAY"][0].as_factor_dict() if "DAY" in best else _existing_block("_DAY")
    swing_vals = best["SWING"][0].as_factor_dict() if "SWING" in best else _existing_block("_SWING")

    day_note = (
        f"{now}  period {start_date}~{end_date}  {best['DAY'][1].trades} trades  "
        f"win {best['DAY'][1].win_rate:.1f}%  avg {best['DAY'][1].avg_ret:+.2f}%  "
        f"total {best['DAY'][1].total_ret:+.1f}%"
        if "DAY" in best
        else _existing_note("DAY")
    )
    swing_note = (
        f"{now}  period {start_date}~{end_date}  {best['SWING'][1].trades} trades  "
        f"win {best['SWING'][1].win_rate:.1f}%  avg {best['SWING'][1].avg_ret:+.2f}%  "
        f"total {best['SWING'][1].total_ret:+.1f}%"
        if "SWING" in best
        else _existing_note("SWING")
    )

    content = f'''"""
Codex strategy factors optimized by grid_search_codex.

Optimization window
  - period: {start_date} ~ {end_date}
  - updated: {now}

[DAY]   {day_note}
[SWING] {swing_note}

Run again:
  uv run python -m wye.blsh.domestic.codex.grid_search_codex
"""

from wye.blsh.common.env import TRADE_FLAG

{_format_factor_dict("_DAY", day_vals, day_note)}

{_format_factor_dict("_SWING", swing_vals, swing_note)}

DAY_FACTORS = _DAY
SWING_FACTORS = _SWING

_active = _DAY if TRADE_FLAG == "DAY" else _SWING

INVEST_MIN_SCORE = _active["INVEST_MIN_SCORE"]
AVG_TRDVAL_MIN = _active["AVG_TRDVAL_MIN"]
RECENT_HIGH_DAYS = _active["RECENT_HIGH_DAYS"]
PULLBACK_MARGIN = _active["PULLBACK_MARGIN"]
ENTRY_ATR_MULT = _active["ENTRY_ATR_MULT"]
ATR_SL_MULT = _active["ATR_SL_MULT"]
ATR_TP_MULT = _active["ATR_TP_MULT"]
TP1_MULT = _active["TP1_MULT"]
TP1_RATIO = _active["TP1_RATIO"]
MAX_HOLD_DAYS = _active["MAX_HOLD_DAYS"]
MAX_HOLD_DAYS_MIX = _active["MAX_HOLD_DAYS_MIX"]
MAX_HOLD_DAYS_MOM = _active["MAX_HOLD_DAYS_MOM"]
MAX_CANDIDATES_PER_MARKET = _active["MAX_CANDIDATES_PER_MARKET"]
'''
    _FACTOR_PATH.write_text(content, encoding="utf-8")
    log.info("updated %s", _FACTOR_PATH)


def run(mode: str = "BOTH", years: int = 2, passes: int = 2, apply: bool = True) -> dict[str, tuple[Params, Stats]]:
    started = time.time()
    data = _load_market_data(years)
    results: dict[str, tuple[Params, Stats]] = {}

    for item in ("DAY", "SWING"):
        if mode not in ("BOTH", item):
            continue
        best_params, best_stats = optimize_mode(data, item, passes=passes)
        results[item] = (best_params, best_stats)
        log.info(
            "[%s] BEST: trades=%s win=%.1f%% avg=%+.2f%% total=%+.1f%% | %s",
            item,
            best_stats.trades,
            best_stats.win_rate,
            best_stats.avg_ret,
            best_stats.total_ret,
            best_params.label(),
        )

    if apply and results:
        apply_results(results, years)

    log.info("elapsed %.1fs", time.time() - started)
    return results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Optimize codex strategy factors.")
    parser.add_argument("--mode", choices=["BOTH", "DAY", "SWING"], default="BOTH")
    parser.add_argument("--years", type=int, default=2)
    parser.add_argument("--passes", type=int, default=2)
    parser.add_argument("--no-apply", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    run(mode=args.mode, years=args.years, passes=args.passes, apply=not args.no_apply)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
