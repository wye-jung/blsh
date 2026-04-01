"""
최적화용 데이터 캐시
──────────────────────────────────
Phase 1: OHLCV + 수급 + 지수 벌크 로드 (~1분)
Phase 2: 종목별 벡터화 지표 계산 (~2분)
Phase 3: 전 영업일 신호 수집 + 수급 보강 (~1분)
Phase 4: pickle 저장 (~10초)

재실행 시 pickle 즉시 로드 (~5초)
"""

import logging
import pickle
import time
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

from wye.blsh.common import dtutils
from wye.blsh.common.env import CACHE_DIR as _BLSH_CACHE_DIR
from wye.blsh.database.query import engine, select_all
from wye.blsh.domestic import sector, Tick
from wye.blsh.domestic.scanner import (
    calc_macd,
    calc_rsi,
    calc_bb,
    calc_atr,
    calc_stoch,
    calc_obv,
    _REVERSAL_FLAGS,
    _MOMENTUM_FLAGS,
    classify_supply,
    # scanner 자체 상수 (_cache에서도 사용)
    GAP_THRESHOLD,
    RSI_OVERSOLD,
    W52_VOL_MULT,
    INDEX_DROP_LIMIT,
    LOOKBACK_DAYS,
    TRDVAL_MIN,
    MACD_LONG,
    MACD_SIGNAL,
    MIN_SCORE,
    ENRICH_SCORE,
    SUPPLY_CAP,
)


# 업종코드 → DB 지수명 매핑은 sector.py에서 참조
_KOSPI_MID_TO_IDX = sector.KOSPI_MID_TO_IDX
_KOSPI_BIG_TO_IDX = sector.KOSPI_BIG_TO_IDX
_KOSDAQ_MID_TO_IDX = sector.KOSDAQ_MID_TO_IDX
_KOSDAQ_BIG_TO_IDX = sector.KOSDAQ_BIG_TO_IDX

log = logging.getLogger(__name__)
CACHE_DIR = _BLSH_CACHE_DIR / "optimize"

# scanner.evaluate_buy 와 동일한 점수표
_SCORES = {
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
_SIGNAL_COLS = list(_SCORES.keys())
_ALL_FLAGS = _MOMENTUM_FLAGS | _REVERSAL_FLAGS  # 중립 = 전체 - 이 집합


# ─────────────────────────────────────────
# 모드/점수 분류 (scanner.py 로직 동일)
# ─────────────────────────────────────────
def _classify_mode(flags: set) -> str:
    rev_cnt = len(flags & _REVERSAL_FLAGS)
    mom_cnt = len(flags & _MOMENTUM_FLAGS)
    if mom_cnt >= 2 and mom_cnt > rev_cnt:
        return "MOM"
    if rev_cnt >= 2 and rev_cnt > mom_cnt:
        return "REV"
    if mom_cnt > 0 and rev_cnt > 0:
        return "MIX"
    return "WEAK"


def _calc_score(flags: set, mode: str) -> int:
    mom = sum(_SCORES[f] for f in flags & _MOMENTUM_FLAGS)
    rev = sum(_SCORES[f] for f in flags & _REVERSAL_FLAGS)
    neu = sum(_SCORES.get(f, 0) for f in flags - _ALL_FLAGS)
    if mode == "MOM":
        return mom + neu
    if mode == "REV":
        return rev + neu
    if mode == "MIX":
        return max(mom, rev) + neu
    return mom + rev + neu


# ─────────────────────────────────────────
# 벡터화 신호 계산 (종목 1개, 전 기간)
# ─────────────────────────────────────────
def _compute_stock_signals(df: pd.DataFrame) -> pd.DataFrame:
    """종목 OHLCV 전체에 대해 매수 신호 벡터 계산.

    scanner.evaluate_buy() 와 동일한 15개 신호를 벡터화.
    """
    c = df["close"].astype(float)
    h = df["high"].astype(float)
    lo = df["low"].astype(float)
    v = df["volume"].astype(float)
    o = df["open"].astype(float)

    macd, sig, hist = calc_macd(c)
    rsi = calc_rsi(c)
    bbu, bbm, bbl = calc_bb(c)
    atr = calc_atr(h, lo, c)
    sk, sd = calc_stoch(h, lo, c)
    ma5 = c.rolling(5).mean()
    ma20 = c.rolling(20).mean()
    ma60 = c.rolling(60).mean()
    obv = calc_obv(c, v)

    c0, c1 = c, c.shift(1)
    l0, l1 = lo, lo.shift(1)
    m0, m1 = macd, macd.shift(1)
    s0, s1 = sig, sig.shift(1)
    r0, r1 = rsi, rsi.shift(1)
    bbm0, bbm1 = bbm, bbm.shift(1)
    bbl0, bbl1 = bbl, bbl.shift(1)
    sk0, sk1 = sk, sk.shift(1)
    sd0, sd1 = sd, sd.shift(1)

    out = pd.DataFrame(index=df.index)

    # 1. MGC: MACD 골든크로스 (+2)
    out["MGC"] = (m0 > s0) & (m1 < s1)

    # 2. MPGC: MACD 예상 골든크로스 (+1)
    gap = (s0 - m0).abs() / s0.abs().replace(0, np.nan)
    out["MPGC"] = (
        ~out["MGC"].astype(bool)
        & (m0 < s0)
        & (hist.shift(2) < hist.shift(1))
        & (hist.shift(1) < hist)
        & (hist < 0)
        & (gap <= GAP_THRESHOLD)
    )

    # 3. RBO: RSI 30 상향 돌파 (+2)
    out["RBO"] = (r0 > RSI_OVERSOLD) & (r1 <= RSI_OVERSOLD)

    # 4. ROV: RSI 과매도 (+1) — RBO와 상호 배타
    out["ROV"] = ~out["RBO"].astype(bool) & (r0 < RSI_OVERSOLD)

    # 5. BBL: 볼린저 하단 반등 (+1)
    out["BBL"] = (l1 < bbl1) & (c0 > bbl)

    # 6. BBM: 볼린저 중간선 상향 돌파 (+1)
    out["BBM"] = (c0 > bbm0) & (c1 <= bbm1)

    # 7. VS: 거래량 급증 + 양봉 (+1)
    vol_avg = v.shift(1).rolling(19, min_periods=10).mean()
    out["VS"] = (v > vol_avg * 2) & (c0 > c1)

    # 8. MAA: 이동평균 정배열 전환 (+1)
    aligned = ((ma5 > ma20) & (ma20 > ma60)).astype(bool)
    out["MAA"] = aligned & ~aligned.shift(1).astype(bool)

    # 9. SGC: 스토캐스틱 과매도 교차 (+1)
    out["SGC"] = (sk0 > sd0) & (sk1 < sd1) & (sk0 < 50)

    # 10. W52: 52주 신고가 돌파 (+2)
    w52_high = h.rolling(252, min_periods=200).max().shift(1)
    vol_20_avg = v.rolling(20).mean().shift(1)
    out["W52"] = (h > w52_high) & (v > vol_20_avg * W52_VOL_MULT)

    # 11. PB: 눌림목 패턴 (+2)
    out["PB"] = (
        (ma20 > ma20.shift(5))
        & ((c1 < ma5.shift(1)) | (l1 < ma5.shift(1)))
        & (c0 > ma5)
        & (c0 > ma20)
    )

    # 12. HMR: 망치형 캔들 (+1)
    body = (c0 - o).abs()
    rng = h - lo
    lower_wick = np.minimum(c0, o) - lo
    upper_wick = h - np.maximum(c0, o)
    out["HMR"] = (
        (rng > 0)
        & (lower_wick > rng * 0.5)
        & (upper_wick < rng * 0.1)
        & (body < rng * 0.3)
    )

    # 13. LB: 장대 양봉 (+2)
    out["LB"] = (c0 - o) > atr * 1.5

    # 14. MS: 모닝스타 (+2)
    c_2, o_2 = c.shift(2), o.shift(2)
    c_1, o_1 = c.shift(1), o.shift(1)
    out["MS"] = (
        ((o_2 - c_2) > atr * 0.7)
        & ((c_1 - o_1).abs() < atr * 0.3)
        & ((c0 - o) > atr * 0.7)
        & (c0 > (o_2 + c_2) / 2)
    )

    # 15. OBV: 3일 연속 상승 (+1)
    out["OBV"] = (obv > obv.shift(1)) & (obv.shift(1) > obv.shift(2))

    # 메타 컬럼 (나중에 entry_price / SL / TP 계산용)
    out["_atr"] = atr
    out["_close"] = c

    for col in _SIGNAL_COLS:
        out[col] = out[col].fillna(False).astype(bool)

    return out


# ─────────────────────────────────────────
# 지수 환경 (시장 + 업종)
# ─────────────────────────────────────────
def _compute_index_env(start: str, end: str) -> dict[str, dict[str, bool]]:
    """날짜별 KOSPI/KOSDAQ 20MA 환경. {date: {'KOSPI': bool, 'KOSDAQ': bool}}"""
    result: dict[str, dict[str, bool]] = {}
    for idx_nm, key, clss in [
        ("코스피", "KOSPI", sector.IDX_CLSS_KOSPI),
        ("코스닥", "KOSDAQ", sector.IDX_CLSS_KOSDAQ),
    ]:
        rows = select_all(
            "SELECT trd_dd, clsprc_idx FROM idx_stk_ohlcv "
            "WHERE idx_nm = :nm AND idx_clss = :clss "
            "AND trd_dd >= :s AND trd_dd <= :e ORDER BY trd_dd",
            nm=idx_nm,
            clss=clss,
            s=start,
            e=end,
        )
        if not rows:
            continue
        df = pd.DataFrame(rows).drop_duplicates(subset="trd_dd").set_index("trd_dd")
        price = pd.to_numeric(df["clsprc_idx"], errors="coerce")
        ma20 = price.rolling(20).mean().shift(1)  # 당일 제외 MA20
        gap = (price - ma20) / ma20
        for d, v in gap.items():
            if pd.notna(v):
                result.setdefault(d, {})[key] = v >= -INDEX_DROP_LIMIT
    return result


def _compute_sector_gaps(start: str, end: str) -> dict[tuple[str, str, str], float]:
    """업종지수별 MA20 괴리율. {(idx_nm, idx_clss, date): gap_pct}

    gap_pct: (price - MA20) / MA20
    예) -0.05 = MA20 대비 -5%
    """
    # (idx_nm, idx_clss) 쌍 생성 — KOSPI/KOSDAQ 동명 업종 구분
    sector_pairs: set[tuple[str, str]] = set()
    for nm in _KOSPI_MID_TO_IDX.values():
        sector_pairs.add((nm, sector.IDX_CLSS_KOSPI))
    for nm in _KOSPI_BIG_TO_IDX.values():
        sector_pairs.add((nm, sector.IDX_CLSS_KOSPI))
    for nm in _KOSDAQ_MID_TO_IDX.values():
        sector_pairs.add((nm, sector.IDX_CLSS_KOSDAQ))
    for nm in _KOSDAQ_BIG_TO_IDX.values():
        sector_pairs.add((nm, sector.IDX_CLSS_KOSDAQ))
    sector_pairs.add(("코스피", sector.IDX_CLSS_KOSPI))
    sector_pairs.add(("코스닥", sector.IDX_CLSS_KOSDAQ))

    result: dict[tuple[str, str, str], float] = {}

    for idx_nm, clss in sector_pairs:
        rows = select_all(
            "SELECT trd_dd, clsprc_idx FROM idx_stk_ohlcv "
            "WHERE idx_nm = :nm AND idx_clss = :clss "
            "AND trd_dd >= :s AND trd_dd <= :e ORDER BY trd_dd",
            nm=idx_nm,
            clss=clss,
            s=start,
            e=end,
        )
        if not rows:
            continue
        df = pd.DataFrame(rows).drop_duplicates(subset="trd_dd").set_index("trd_dd")
        price = pd.to_numeric(df["clsprc_idx"], errors="coerce")
        ma20 = price.rolling(20).mean().shift(1)  # 당일 제외 MA20
        gap = (price - ma20) / ma20
        for d, v in gap.items():
            if pd.notna(v):
                result[(idx_nm, clss, d)] = float(v)

    log.info(f"  업종지수 환경: {len(sector_pairs)}업종, {len(result):,}건")
    return result


def _build_ticker_sector_map(ticker_market: dict[str, str]) -> dict[str, str]:
    """종목코드 → 업종지수명 매핑.

    KOSPI: 중분류 우선, 대분류 fallback, 미매핑 → "코스피"
    KOSDAQ: 중분류 우선, 대분류 fallback, 미매핑 → "코스닥"
    """
    from wye.blsh.kis.domestic_stock.domestic_stock_info import (
        get_kospi_info,
        get_kosdaq_info,
    )

    result: dict[str, str] = {}
    kospi_tickers = {t for t, m in ticker_market.items() if m == "KOSPI"}
    kosdaq_tickers = {t for t, m in ticker_market.items() if m == "KOSDAQ"}

    try:
        # KOSPI
        kp = get_kospi_info()
        for _, row in kp.iterrows():
            ticker = str(row["단축코드"]).strip()
            if ticker not in kospi_tickers:
                continue
            mid = int(row.get("지수업종중분류", 0) or 0)
            big = int(row.get("지수업종대분류", 0) or 0)
            idx_nm = _KOSPI_MID_TO_IDX.get(mid) or _KOSPI_BIG_TO_IDX.get(big)
            result[ticker] = idx_nm or "코스피"  # 미매핑 → 전체 지수
        kp_mapped = sum(1 for t in kospi_tickers if result.get(t, "코스피") != "코스피")
        log.info(
            f"  KOSPI 업종매핑: {kp_mapped}/{len(kospi_tickers)}종목 (미매핑→코스피)"
        )

        # KOSDAQ
        kd = get_kosdaq_info()
        for _, row in kd.iterrows():
            ticker = str(row["단축코드"]).strip()
            if ticker not in kosdaq_tickers:
                continue
            mid = int(row.get("지수 업종 중분류 코드", 0) or 0)
            big = int(row.get("지수업종 대분류 코드", 0) or 0)
            idx_nm = _KOSDAQ_MID_TO_IDX.get(mid) or _KOSDAQ_BIG_TO_IDX.get(big)
            result[ticker] = idx_nm or "코스닥"  # 미매핑 → 전체 지수
        kd_mapped = sum(
            1 for t in kosdaq_tickers if result.get(t, "코스닥") != "코스닥"
        )
        log.info(
            f"  KOSDAQ 업종매핑: {kd_mapped}/{len(kosdaq_tickers)}종목 (미매핑→코스닥)"
        )
    except Exception as e:
        log.warning(f"  마스터 로드 실패: {e}")
        # fallback: 미매핑 종목 전체 지수
        for t in kospi_tickers:
            result.setdefault(t, "코스피")
        for t in kosdaq_tickers:
            result.setdefault(t, "코스닥")

    return result


# ─────────────────────────────────────────
# 수급 벌크 보강
# ─────────────────────────────────────────
def _bulk_supply(start: str, end: str, scan_dates: list[str]) -> dict:
    """수급 벌크 로드 → {(ticker,date): (bonus_score, bonus_flags_set, has_pov)}"""
    scan_set = set(scan_dates)
    result: dict[tuple, tuple] = {}
    pad_start = dtutils.add_days(start, -10)

    for table in ("isu_ksp_info", "isu_ksd_info"):
        try:
            rows = select_all(
                f"SELECT isu_srt_cd AS ticker, trd_dd AS date, "
                f"frgn_netbid_trdvol AS frgn, inst_netbid_trdvol AS inst, "
                f"indi_netbid_trdvol AS indi "
                f"FROM {table} WHERE trd_dd >= :s AND trd_dd <= :e "
                f"ORDER BY isu_srt_cd, trd_dd",
                s=pad_start,
                e=end,
            )
        except Exception as e:
            log.warning(f"수급 로드 실패 ({table}): {e}")
            continue
        if not rows:
            continue

        df = pd.DataFrame(rows)
        for col in ("frgn", "inst", "indi"):
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        for ticker, grp in df.groupby("ticker"):
            grp = grp.set_index("date").sort_index()
            for date in scan_set:
                if date not in grp.index:
                    continue
                recent = grp.loc[:date].tail(5)
                if len(recent) < 2:
                    continue

                f_sig, f_sc = classify_supply(recent["frgn"].tolist())
                o_sig, o_sc = classify_supply(recent["inst"].tolist())

                bonus = 0
                flags: set[str] = set()
                has_pov = False

                if f_sc > 0:
                    bonus += f_sc
                    flags.add(f"F_{f_sig}")
                if o_sc > 0:
                    bonus += o_sc
                    flags.add(f"I_{o_sig}")
                if f_sc > 0 and o_sc > 0:
                    bonus += 1
                    flags.add("FI")

                ti, tf, to_ = (
                    recent["indi"].iloc[-1],
                    recent["frgn"].iloc[-1],
                    recent["inst"].iloc[-1],
                )
                if ti > 0 and tf <= 0 and to_ <= 0 and ti > abs(tf) + abs(to_):
                    has_pov = True  # P_OV 패널티는 signal 빌딩에서 캡 적용 후 차감

                if bonus != 0 or has_pov:
                    result[(ticker, date)] = (bonus, flags, has_pov)

    log.info(f"  수급 보강 항목: {len(result)}건")
    return result


# ═══════════════════════════════════════════
# 캐시 클래스
# ═══════════════════════════════════════════
class OptCache:
    """최적화용 사전 계산 데이터."""

    def __init__(self):
        self.scan_dates: list[str] = []
        self.next_biz: dict[str, str] = {}
        self.forward_dates: dict[str, list[str]] = {}  # date → 이후 20 영업일
        self.signals: dict[str, list[dict]] = {}
        self.ohlcv_idx: dict[tuple[str, str], dict] = {}
        self.name_map: dict[str, str] = {}
        self.ticker_market: dict[str, str] = {}
        self.ticker_sector: dict[str, str] = {}  # ticker → 업종지수명
        self.sector_gaps: dict[
            tuple[str, str, str], float
        ] = {}  # (업종지수명, idx_clss, date) → MA20 괴리율

    # ── pickle I/O
    def save(self, tag: str = ""):
        path = CACHE_DIR / f"opt_cache{'_' + tag if tag else ''}.pkl"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(pickle.dumps(self.__dict__))
        log.info(f"캐시 저장: {path} ({path.stat().st_size / 1e6:.1f}MB)")

    @classmethod
    def load(cls, tag: str = "") -> "OptCache | None":
        path = CACHE_DIR / f"opt_cache{'_' + tag if tag else ''}.pkl"
        if not path.exists():
            return None
        t0 = time.time()
        obj = cls()
        obj.__dict__.update(pickle.loads(path.read_bytes()))
        log.info(
            f"캐시 로드: {path} ({len(obj.scan_dates)}일, {time.time() - t0:.1f}초)"
        )
        return obj


# ═══════════════════════════════════════════
# 빌드
# ═══════════════════════════════════════════
def build_or_load(start_date: str, end_date: str, tag: str = "") -> OptCache:
    """캐시 존재 시 로드, 없으면 빌드.

    end_date는 달력 날짜이지만 scan_dates는 거래일만 포함하므로,
    end_date와 최신 거래일 간 최대 5일(주말/공휴일) 차이는 허용.
    """
    cached = OptCache.load(tag)
    if cached and cached.scan_dates:
        # 시작일: 캐시가 요청보다 같거나 이전
        start_ok = cached.scan_dates[0] <= start_date
        # 종료일: end_date와 최대 5일 차이 허용 (주말/공휴일 감안)
        end_gap = int(end_date) - int(cached.scan_dates[-1])
        end_ok = 0 <= end_gap <= 5
        if start_ok and end_ok:
            return cached
        log.info(
            f"캐시 범위 불일치 → 재빌드  "
            f"(캐시={cached.scan_dates[0]}~{cached.scan_dates[-1]}, "
            f"요청={start_date}~{end_date})"
        )
    return _build(start_date, end_date, tag)


def _build(start_date: str, end_date: str, tag: str) -> OptCache:
    t_total = time.time()
    cache = OptCache()

    # ── 1. 영업일
    log.info("[1/8] 영업일 로드")
    lookback_start = dtutils.add_days(start_date, -(LOOKBACK_DAYS + 30))
    all_biz = [
        r["d"]
        for r in select_all(
            "SELECT DISTINCT trd_dd AS d FROM idx_stk_ohlcv "
            "WHERE trd_dd >= :s AND trd_dd <= :e ORDER BY 1",
            s=lookback_start,
            e=end_date,
        )
    ]
    cache.scan_dates = [d for d in all_biz if start_date <= d <= end_date]
    for i, d in enumerate(all_biz):
        if i + 1 < len(all_biz):
            cache.next_biz[d] = all_biz[i + 1]
    # forward_dates: 각 날짜 이후 20 영업일
    for i, d in enumerate(all_biz):
        cache.forward_dates[d] = all_biz[i : i + 21]
    log.info(
        f"  스캔 대상 {len(cache.scan_dates)}일  ({cache.scan_dates[0]} ~ {cache.scan_dates[-1]})"
    )

    # ── 2. 종목명
    cache.name_map = {
        r["isu_srt_cd"]: r["isu_abbrv"]
        for r in select_all("SELECT isu_srt_cd, isu_abbrv FROM isu_base_info")
    }

    # ── 3. OHLCV 벌크 로드
    log.info("[2/8] OHLCV 벌크 로드")
    ohlcv_by_ticker: dict[str, pd.DataFrame] = {}
    for table, market in [("isu_ksp_ohlcv", "KOSPI"), ("isu_ksd_ohlcv", "KOSDAQ")]:
        t0 = time.time()
        rows = select_all(
            f"SELECT isu_srt_cd AS ticker, trd_dd AS date, "
            f"tdd_opnprc AS open, tdd_hgprc AS high, tdd_lwprc AS low, "
            f"tdd_clsprc AS close, acc_trdvol AS volume, acc_trdval AS trdval "
            f"FROM {table} "
            f"WHERE trd_dd >= :s AND trd_dd <= :e ORDER BY isu_srt_cd, trd_dd",
            s=lookback_start,
            e=end_date,
        )
        df = pd.DataFrame(rows)
        if df.empty:
            continue
        for col in ("open", "high", "low", "close", "volume", "trdval"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        cnt = 0
        for ticker, grp in df.groupby("ticker"):
            cache.ticker_market[ticker] = market
            ohlcv_by_ticker[ticker] = grp.set_index("date").sort_index()
            cnt += 1
        log.info(f"  {market}: {cnt}종목 ({time.time() - t0:.1f}초)")

    # OHLCV 인덱스 (시뮬레이션용)
    for ticker, df in ohlcv_by_ticker.items():
        for date, row in df.iterrows():
            if start_date <= date <= end_date or date in cache.next_biz.values():
                cache.ohlcv_idx[(ticker, date)] = {
                    "open": row["open"],
                    "high": row["high"],
                    "low": row["low"],
                    "close": row["close"],
                }

    # ── 4. 지수 환경 + 업종지수 환경
    log.info("[3/8] 지수 환경")
    idx_ok = _compute_index_env(lookback_start, end_date)

    log.info("[4/8] 업종지수 환경")
    cache.sector_gaps = _compute_sector_gaps(lookback_start, end_date)

    log.info("[5/8] 종목→업종 매핑")
    cache.ticker_sector = _build_ticker_sector_map(cache.ticker_market)

    # ── 5. 거래대금 필터
    log.info("[6/8] 거래대금 필터")
    trdval_pass: dict[str, set[str]] = {}  # {date: set(ticker)}
    for ticker, df in ohlcv_by_ticker.items():
        avg20 = df["trdval"].rolling(20, min_periods=10).mean()
        for d in cache.scan_dates:
            if (
                d in avg20.index
                and pd.notna(avg20.loc[d])
                and avg20.loc[d] >= TRDVAL_MIN
            ):
                trdval_pass.setdefault(d, set()).add(ticker)

    # ── 6. 벡터화 신호 계산
    log.info("[7/8] 벡터화 신호 계산")
    stock_sigs: dict[str, pd.DataFrame] = {}
    total = len(ohlcv_by_ticker)
    t0 = time.time()
    for i, (ticker, df) in enumerate(ohlcv_by_ticker.items()):
        if len(df) < MACD_LONG + MACD_SIGNAL + 5:
            continue
        try:
            stock_sigs[ticker] = _compute_stock_signals(df)
        except Exception as e:
            if i < 3:  # 처음 3건만 로깅
                log.warning(f"  신호 계산 실패 ({ticker}): {e}")
        if (i + 1) % 500 == 0:
            log.info(f"  {i + 1}/{total} ({time.time() - t0:.0f}초)")
    log.info(f"  완료: {len(stock_sigs)}종목 ({time.time() - t0:.0f}초)")

    # ── 7. 수급 보강
    log.info("[8/8] 수급 보강")
    supply = _bulk_supply(start_date, end_date, cache.scan_dates)

    # ── 8. 날짜별 신호 수집
    log.info("[집계] 날짜별 신호 수집")
    for date in cache.scan_dates:
        mkt_ok = idx_ok.get(date, {"KOSPI": True, "KOSDAQ": True})
        eligible = trdval_pass.get(date, set())
        day_sigs: list[dict] = []

        for ticker, sig_df in stock_sigs.items():
            if ticker not in eligible:
                continue
            mkt = cache.ticker_market.get(ticker, "")
            if not mkt_ok.get(mkt, True):
                continue
            if date not in sig_df.index:
                continue

            row = sig_df.loc[date]
            flags = {f for f in _SIGNAL_COLS if row.get(f, False)}
            if not flags:
                continue

            mode = _classify_mode(flags)
            tech_score = _calc_score(flags, mode)
            if tech_score < MIN_SCORE:
                continue

            # 수급 보강 (tech_score >= ENRICH_SCORE 인 경우만)
            raw_supply_bonus = 0  # 캡 미적용 원본 수급 점수 (supply_cap_test용)
            score = tech_score
            if tech_score >= ENRICH_SCORE:
                sup = supply.get((ticker, date))
                if sup:
                    bonus, bonus_flags, has_pov = sup
                    raw_supply_bonus = bonus
                    score += min(bonus, SUPPLY_CAP)  # scanner.py와 동일한 수급 상한
                    flags |= bonus_flags
                    if has_pov:
                        flags.add("P_OV")
                        score -= 1  # 캡 적용 후 P_OV 패널티 (scanner.py 순서 일치)

            atr_val = float(row["_atr"]) if pd.notna(row["_atr"]) else 0
            close_val = float(row["_close"]) if pd.notna(row["_close"]) else 0
            if atr_val <= 0 or close_val <= 0:
                continue

            entry_price = Tick.ceil_tick(close_val + 0.5 * atr_val)

            # 업종지수 MA20 괴리율 (없으면 0.0 = 중립)
            sec_nm = cache.ticker_sector.get(ticker, "")
            idx_clss = sector.get_idx_clss(mkt)
            sec_gap = (
                cache.sector_gaps.get((sec_nm, idx_clss, date), 0.0) if sec_nm else 0.0
            )

            day_sigs.append(
                {
                    "ticker": ticker,
                    "name": cache.name_map.get(ticker, ticker),
                    "market": mkt,
                    "score": score,
                    "tech_score": tech_score,  # 기술 점수만 (수급 제외)
                    "raw_supply_bonus": raw_supply_bonus,  # 캡 미적용 수급 점수 원본
                    "flags": ",".join(sorted(flags)),
                    "mode": mode,
                    "atr": atr_val,
                    "close": close_val,
                    "entry_price": entry_price,
                    "sector_gap": sec_gap,
                    "sector_nm": sec_nm,
                }
            )
        cache.signals[date] = day_sigs

    total_sigs = sum(len(v) for v in cache.signals.values())
    elapsed = time.time() - t_total
    log.info(
        f"캐시 빌드 완료: {elapsed:.0f}초  |  {len(cache.scan_dates)}일  "
        f"|  총 {total_sigs:,}건 신호  |  OHLCV {len(cache.ohlcv_idx):,}건"
    )
    cache.save(tag)
    return cache
