"""캐시 상태 진단"""
import logging
from wye.blsh.common import dtutils
from wye.blsh.domestic.optimize._cache import build_or_load

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

end = dtutils.today()
start = dtutils.add_days(end, -2 * 365)
cache = build_or_load(start, end)

log.info(f"\n{'='*60}")
log.info(f"scan_dates: {len(cache.scan_dates)}일  ({cache.scan_dates[:3]}...{cache.scan_dates[-3:]})")
log.info(f"next_biz:   {len(cache.next_biz)}건")
log.info(f"ohlcv_idx:  {len(cache.ohlcv_idx):,}건")
log.info(f"signals:    {sum(len(v) for v in cache.signals.values()):,}건 (전 날짜 합)")

# 날짜별 신호 건수 분포
sig_counts = [(d, len(cache.signals.get(d, []))) for d in cache.scan_dates]
non_zero = [(d, c) for d, c in sig_counts if c > 0]
log.info(f"  신호 있는 날: {len(non_zero)} / {len(sig_counts)}")
if non_zero:
    log.info(f"  예시: {non_zero[:5]}")

# 첫 날짜 샘플
if cache.scan_dates:
    d0 = cache.scan_dates[0]
    sigs = cache.signals.get(d0, [])
    entry = cache.next_biz.get(d0)
    log.info(f"\n첫 스캔일 {d0}: 신호 {len(sigs)}건, entry_date={entry}")
    if sigs:
        s = sigs[0]
        log.info(f"  샘플: {s}")
        if entry:
            ohlcv = cache.ohlcv_idx.get((s["ticker"], entry))
            log.info(f"  OHLCV({s['ticker']},{entry}): {ohlcv}")
            fwd = cache.forward_dates.get(entry, [])
            log.info(f"  forward_dates({entry}): {fwd[:5]}...")

# 신호가 0이면 원인 추적
if sum(len(v) for v in cache.signals.values()) == 0:
    log.info(f"\n{'='*60}")
    log.info("⚠️  신호가 전혀 없습니다. 원인 추적:")

    # ticker_market 확인
    log.info(f"  ticker_market: {len(cache.ticker_market)}종목")

    # 직접 1종목 신호 테스트
    from wye.blsh.domestic.optimize._cache import _compute_stock_signals, _SIGNAL_COLS
    from wye.blsh.database.query import select_all
    import pandas as pd

    lookback_start = dtutils.add_days(start, -400)
    rows = select_all(
        "SELECT isu_srt_cd AS ticker, trd_dd AS date, "
        "tdd_opnprc AS open, tdd_hgprc AS high, tdd_lwprc AS low, "
        "tdd_clsprc AS close, acc_trdvol AS volume, acc_trdval AS trdval "
        "FROM isu_ksp_ohlcv "
        "WHERE isu_srt_cd = '005930' AND trd_dd >= :s AND trd_dd <= :e "
        "ORDER BY trd_dd",
        s=lookback_start, e=end,
    )
    df = pd.DataFrame(rows)
    log.info(f"  삼성전자 OHLCV: {len(df)}행")
    if not df.empty:
        for col in ("open", "high", "low", "close", "volume", "trdval"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        log.info(f"  최근 3행:\n{df.tail(3)}")

        df_idx = df.set_index("date").sort_index()
        sig_df = _compute_stock_signals(df_idx)
        log.info(f"  신호 DataFrame: {sig_df.shape}")

        # 최근 10일 신호 확인
        recent = sig_df.tail(10)
        for d in recent.index:
            row = recent.loc[d]
            flags = {f for f in _SIGNAL_COLS if row.get(f, False)}
            if flags:
                log.info(f"  {d}: {flags}")

        # 전체 기간 신호 유무
        any_signal = sig_df[_SIGNAL_COLS].any(axis=1).sum()
        log.info(f"  삼성전자 전체 기간 신호 발생 일수: {any_signal}")

        # trdval 확인
        avg_trdval = df_idx["trdval"].rolling(20).mean()
        log.info(f"  최근 20일 평균 거래대금: {avg_trdval.iloc[-1]:,.0f}")
        log.info(f"  TRDVAL_MIN 기준: 1,000,000,000")
