"""
백테스트
"""

import logging
import pandas as pd
from blsh.database import query
from blsh.wye.domestic import reporter, _factor as fac

log = logging.getLogger(__name__)


def simulate(candidates, target_date) -> tuple:
    """
    수익률 시뮬레이트
    """
    if candidates.empty:
        log.info("[시뮬레이트] target_date 없음 (미래 날짜) → 스킵")
        return

    log.info(f"[시뮬레이트] 목표일={target_date}  최대 {fac.MAX_HOLD_DAYS}거래일 추적")

    tickers = candidates["ticker"].tolist()

    # ── target_date 이후 최대 MAX_HOLD_DAYS 거래일 날짜 목록 조회
    date_rows = pd.DataFrame(query.get_max_hold_dates(target_date, fac.MAX_HOLD_DAYS))
    if date_rows.empty:
        log.info(f"[수익률 리포트] {target_date} 이후 OHLCV 데이터 없음 → 스킵")
        return

    hold_dates = date_rows["trd_dd"].tolist()
    actual_days = len(hold_dates)
    log.info(f"  확인 기간: {hold_dates[0]} ~ {hold_dates[-1]}  ({actual_days}거래일)")

    def fetch_ohlcv_range(table):
        try:
            return pd.DataFrame(query.get_ohlcv_range(table, hold_dates, tickers))
        except Exception as e:
            log.warning(f"  OHLCV 조회 오류 ({table}): {e}")
            return pd.DataFrame()

    ohlcv_all = pd.concat(
        [fetch_ohlcv_range("isu_ksp_ohlcv"), fetch_ohlcv_range("isu_ksd_ohlcv")],
        ignore_index=True,
    )

    # ticker → {trd_dd: {open, high, low, close}} 인덱스 구성
    ohlcv_idx: dict[str, dict[str, dict]] = {}
    for _, row in ohlcv_all.iterrows():
        ohlcv_idx.setdefault(row["ticker"], {})[row["trd_dd"]] = {
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
        }

    rows_ok = []  # 매수 진입 성공
    rows_gap = []  # 갭 상승, 매수 불가
    rows_miss = []  # target_date 데이터 자체 없음

    for _, sig in candidates.iterrows():
        t = sig["ticker"]
        entry = float(sig["entry_price"])
        sl = float(sig["stop_loss"])
        tp = float(sig["take_profit"])
        days = ohlcv_idx.get(t, {})

        # target_date 데이터 없음
        t1_ohv = days.get(hold_dates[0])
        if t1_ohv is None:
            rows_miss.append(sig.to_dict())
            continue

        # 갭 상승 체크: target_date 시가 > entry_price
        if t1_ohv["open"] > entry:
            rows_gap.append(
                {**sig.to_dict(), "t_open": t1_ohv["open"], "entry_date": hold_dates[0]}
            )
            continue

        buy_price = t1_ohv["open"]
        result_type = None
        exit_price = None
        exit_date = None
        last_ohv = t1_ohv

        # 모드별 최대 보유 기간: MOM=2일, MIX=3일, REV=5일
        mode = sig.get("mode", "")
        if mode == "MOM":
            max_days = fac.MAX_HOLD_DAYS_MOM
        elif mode == "MIX":
            max_days = fac.MAX_HOLD_DAYS_MIX
        else:
            max_days = fac.MAX_HOLD_DAYS
        sig_hold_dates = hold_dates[:max_days]

        # 날짜 순서대로 손익절 확인
        for d in sig_hold_dates:
            ohv = days.get(d)
            if ohv is None:
                continue
            last_ohv = ohv

            hit_sl = ohv["low"] <= sl
            hit_tp = ohv["high"] >= tp

            if hit_sl and hit_tp:
                # 동일 캔들에서 손절/익절 동시 터치 → 시가와 가까운 쪽 우선
                if abs(buy_price - sl) <= abs(tp - buy_price):
                    result_type, exit_price = "손절", sl
                else:
                    result_type, exit_price = "익절", tp
            elif hit_sl:
                result_type, exit_price = "손절", sl
            elif hit_tp:
                result_type, exit_price = "익절", tp

            if result_type:
                exit_date = d
                break

        # 최대 보유기간 후에도 미확정 → 마지막 거래일 종가
        if result_type is None:
            result_type = f"미확정({len(sig_hold_dates)}일)"
            exit_price = last_ohv["close"]
            exit_date = sig_hold_dates[-1]

        ret_pct = (exit_price - buy_price) / buy_price * 100
        rows_ok.append(
            {
                **sig.to_dict(),
                "buy_price": buy_price,
                "entry_date": hold_dates[0],
                "exit_price": exit_price,
                "exit_date": exit_date,
                "result_type": result_type,
                "ret_pct": ret_pct,
                "t_open": t1_ohv["open"],
                "t_high": last_ohv["high"],
                "t_low": last_ohv["low"],
                "t_close": last_ohv["close"],
            }
        )

    # 시뮬레이션 리포트
    reporter.print_simul_report(
        target_date,
        actual_days,
        candidates,
        rows_ok,
        rows_gap,
        rows_miss,
    )

    return rows_ok, rows_gap, rows_miss
