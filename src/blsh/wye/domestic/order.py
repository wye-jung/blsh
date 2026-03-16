from blsh.kis import kis_auth as ka
from blsh.kis.domestic_stock import domestic_stock_functions as ds

class Order:

    __init__(self, env_dv="demo"): 
        ka.auth("prod" if env_dv == "real" else "vps")
        self.trenv = ka.getTREnv()
        self.env_dv = env_dv


def _get_price(self, ticker: str) -> float | None:
    try:
        with _api_sem:
            ka.smart_sleep()
            df = ds.inquire_price(self.env_dv, "J", ticker)
        if df is not None and not df.empty:
            return float(df.iloc[0]["stck_prpr"])
    except Exception as e:
        log.debug(f"현재가 조회 실패 ({ticker}): {e}")
    return None


def _fetch_prices(self, tickers: list[str]) -> dict[str, float]:
    """여러 종목 현재가 병렬 조회"""
    if not tickers:
        return {}
    result: dict[str, float] = {}
    with ThreadPoolExecutor(max_workers=min(len(tickers), _API_CONCURRENCY)) as ex:
        futs = {ex.submit(_get_price, self.env_dv, t): t for t in tickers}
        try:
            for fut in as_completed(futs, timeout=POLL_SEC):
                t = futs[fut]
                try:
                    p = fut.result()
                except Exception as e:
                    log.warning(f"가격 조회 스레드 오류 ({t}): {e}")
                    p = None
                if p is not None:
                    result[t] = p
        except FuturesTimeoutError:
            timed_out = [futs[f] for f in futs if not f.done()]
            log.warning(f"현재가 조회 타임아웃 ({POLL_SEC}s): {timed_out}")
    return result


def _get_balance(self, trenv) -> tuple[dict[str, int], float]:
    """보유 종목 수량 + 현금 잔고를 API 1회 호출로 반환."""
    try:
        with _api_sem:
            ka.smart_sleep()
            df1, df2 = ds.inquire_balance(
                env_dv=self.env_dv,
                cano=self.trenv.my_acct,
                acnt_prdt_cd=self.trenv.my_prod,
                afhr_flpr_yn="N",
                inqr_dvsn="02",
                unpr_dvsn="01",
                fund_sttl_icld_yn="N",
                fncg_amt_auto_rdpt_yn="N",
                prcs_dvsn="01",
            )
        holdings = (
            dict(
                zip(df1["pdno"].astype(str), df1["hldg_qty"].astype(float).astype(int))
            )
            if df1 is not None and not df1.empty
            else {}
        )
        cash = (
            float(df2.iloc[0].get("dnca_tot_amt", 0))
            if df2 is not None and not df2.empty
            else 0.0
        )
        return holdings, cash
    except Exception as e:
        log.warning(f"잔고 조회 실패: {e}")
    return {}, 0.0


def _buy(ticker: str, qty: int, entry_price: float) -> str | None:
    """지정가 매수. 성공 시 주문번호 반환."""
    try:
        with _api_sem:
            ka.smart_sleep()
            df = ds.order_cash(
                env_dv=self.env_dv,
                ord_dv="buy",
                cano=self.trenv.my_acct,
                acnt_prdt_cd=self.trenv.my_prod,
                pdno=ticker,
                ord_dvsn="00",
                ord_qty=str(qty),
                ord_unpr=str(int(entry_price)),
                excg_id_dvsn_cd="KRX",
            )
        if df is not None and not df.empty:
            odno = str(df.iloc[0].get("odno", ""))
            log.info(
                f"  📥 매수주문: {ticker}  수량={qty}  지정가={int(entry_price):,}  no={odno}"
            )
            return odno
    except Exception as e:
        log.error(f"  매수 오류 ({ticker}): {e}")
    return None


def _sell(ticker: str, qty: int, reason: str = "") -> bool:
    """시장가 매도. 성공 시 True."""
    try:
        with _api_sem:
            ka.smart_sleep()
            df = ds.order_cash(
                env_dv=self.env_dv,
                ord_dv="sell",
                cano=self.trenv.my_acct,
                acnt_prdt_cd=self.trenv.my_prod,
                pdno=ticker,
                ord_dvsn="01",
                ord_qty=str(qty),
                ord_unpr="0",
                excg_id_dvsn_cd="KRX",
                sll_type="01",
            )
        if df is not None and not df.empty:
            log.info(f"  📤 매도완료: {ticker}  수량={qty}  [{reason}]")
            return True
    except Exception as e:
        log.error(f"  매도 오류 ({ticker}): {e}")
    return False


def _cancel_order(ticker: str, odno: str, qty: int) -> bool:
    """주문 취소. 성공 시 True."""
    try:
        with _api_sem:
            ka.smart_sleep()
            ds.order_rvsecncl(
                env_dv=self.env_dv,
                cano=self.trenv.my_acct,
                acnt_prdt_cd=self.trenv.my_prod,
                krx_fwdg_ord_orgno="",
                orgn_odno=odno,
                ord_dvsn="00",
                rvse_cncl_dvsn_cd="02",
                ord_qty=str(qty),
                ord_unpr="0",
                qty_all_ord_yn="Y",
                excg_id_dvsn_cd="KRX",
            )
        log.info(f"  🚫 주문취소: {ticker}  no={odno}")
        return True
    except Exception as e:
        log.warning(f"  주문 취소 실패 ({ticker}): {e}")
        return False
