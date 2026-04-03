import json
import logging
import os
import signal
import sys
from wye.blsh.common import dtutils
from wye.blsh.common.env import DATA_DIR, KIS_ENV
from wye.blsh.database import query
from wye.blsh.domestic import scanner, collector
from wye.blsh.domestic import trader

log = logging.getLogger(__name__)

PID_FILE = DATA_DIR / "trader.pid"
POSITIONS_FILE = DATA_DIR / "positions.json"


def _start():
    """PID 파일 기록 후 트레이더 실행. 종료 시 PID 파일 삭제."""
    PID_FILE.write_text(str(os.getpid()))
    try:
        trader.run()
    finally:
        PID_FILE.unlink(missing_ok=True)


def _stop():
    """PID 파일에서 프로세스 ID를 읽어 SIGINT 전송."""
    if not PID_FILE.exists():
        log.error("트레이더 PID 파일 없음 → 실행 중인 트레이더가 없습니다.")
        return
    pid = int(PID_FILE.read_text().strip())
    try:
        os.kill(pid, signal.SIGINT)
        log.info(f"트레이더(PID: {pid})에 종료 신호 전송 완료.")
    except ProcessLookupError:
        log.warning(f"트레이더(PID: {pid})가 이미 종료되었습니다.")
        PID_FILE.unlink(missing_ok=True)


def _is_running() -> tuple[bool, int | None]:
    """PID 파일로 트레이더 실행 여부 확인. (running, pid)"""
    if not PID_FILE.exists():
        return False, None
    pid = int(PID_FILE.read_text().strip())
    try:
        os.kill(pid, 0)
        return True, pid
    except (ProcessLookupError, PermissionError):
        return False, pid


def _status(sub: str | None = None):
    running, pid = _is_running()

    if sub is None:
        if running:
            print(f"[상태] 트레이더 실행 중 (PID: {pid}, KIS_ENV: {KIS_ENV})")
        elif pid:
            print(f"[상태] 트레이더 중지됨 (PID 파일 잔존: {pid}, KIS_ENV: {KIS_ENV})")
        else:
            print(f"[상태] 트레이더 미실행 (KIS_ENV: {KIS_ENV})")

    elif sub == "positions":
        if not POSITIONS_FILE.exists():
            print("[보유] positions.json 없음")
            return
        data = json.loads(POSITIONS_FILE.read_text())
        if not data:
            print("[보유] 0종목")
            return
        print(f"[보유] {len(data)}종목")
        for t, p in data.items():
            name = p.get("name", t)
            buy = p.get("buy_price", 0)
            sl = p.get("sl", 0)
            tp1 = p.get("tp1", 0)
            mode = p.get("mode", "")
            expiry = p.get("expiry_date", "")
            t1 = " T1완료" if p.get("t1_done") else ""
            print(
                f"  {t} {name:<10s}  매수={buy:>10,.0f}  SL={sl:>10,.0f}"
                f"  TP1={tp1:>10,.0f}  {mode:<3s}  만기={expiry}{t1}"
            )

    elif sub == "pendings":
        from wye.blsh.domestic.kis_client import KISClient
        kis = KISClient()
        today = dtutils.today()
        orders = kis.get_pending_orders(today)
        if not orders:
            print("[대기] 미체결 주문 없음")
            return
        print(f"[대기] {len(orders)}건 미체결")
        for o in orders:
            print(
                f"  {o['ticker']} {o['name']:<10s}  {o['side']}"
                f"  {o['qty']}주 @ {o['price']:>10,}원  odno={o['odno']}"
            )

    elif sub == "holdings":
        from wye.blsh.domestic.kis_client import KISClient
        from wye.blsh.kis.domestic_stock import domestic_stock_functions as ds
        kis = KISClient()
        kis.rate_limiter.wait()
        df1, df2 = ds.inquire_balance(
            env_dv=kis.env_dv,
            cano=kis.trenv.my_acct,
            acnt_prdt_cd=kis.trenv.my_prod,
            afhr_flpr_yn="N",
            inqr_dvsn="02",
            unpr_dvsn="01",
            fund_sttl_icld_yn="N",
            fncg_amt_auto_rdpt_yn="N",
            prcs_dvsn="01",
        )
        if df1 is None or df1.empty:
            print("[잔고] 보유종목 없음")
        else:
            rows = df1[df1["hldg_qty"].astype(int) > 0]
            print(f"[잔고] {len(rows)}종목  (KIS_ENV: {KIS_ENV})")
            for _, r in rows.iterrows():
                ticker = str(r.get("pdno", ""))
                name = str(r.get("prdt_name", ""))
                qty = int(r.get("hldg_qty", 0))
                avg_price = float(r.get("pchs_avg_pric", 0))
                evlu_rt = float(r.get("evlu_pfls_rt", 0))
                print(
                    f"  {ticker} {name:<10s}  {qty:>5}주"
                    f"  매입={avg_price:>10,.0f}  수익률={evlu_rt:>+6.2f}%"
                )
        if df2 is not None and not df2.empty:
            cash = float(df2.iloc[0].get("dnca_tot_amt", 0))
            print(f"  현금: {cash:>12,.0f}원")

    elif sub == "cash":
        from wye.blsh.domestic.kis_client import KISClient
        kis = KISClient()
        _, _, cash = kis.get_balance()
        print(f"[현금] 가용: {cash:>12,.0f}원  (KIS_ENV: {KIS_ENV})")

    else:
        print(f"[오류] 알 수 없는 서브커맨드: {sub}")
        print("  사용법: status [positions|pendings|cash]")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        trader.run()
    elif sys.argv[1] == "start":
        _start()
    elif sys.argv[1] == "stop":
        _stop()
    elif sys.argv[1] == "status":
        _status(sys.argv[2] if len(sys.argv) > 2 else None)
    elif sys.argv[1] == "po":
        collector.collect_holiday()
        today = dtutils.today()
        kh = query.get_krx_holiday(today)
        if kh is None:
            log.error(f"오늘({today}) KRX 휴장일 정보 없음 → po 생성 불가. collect_holiday() 재실행 필요.")
        elif kh["opnd_yn"] == "Y":
            collected, max_ohlcv_date = collector.collect()
            if collected:
                scanner.issue_po(max_ohlcv_date)
            else:
                log.warning(
                    f"최대 OHLCV 날짜 {max_ohlcv_date}가 오늘 {today} 또는 가장 가까운 영업일이 아닙니다."
                )

    else:
        log.warning("invalid arguments")
