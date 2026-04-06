"""
일일 로그 분석 리포터
─────────────────────────────────────────────────────
매일 장 마감 후(크론탭 20:30) 실행하여 당일 로그를 분석하고
텔레그램으로 요약 리포트를 발송한다.

실행:
    uv run python -m wye.blsh.domestic.log_analyzer
    uv run python -m wye.blsh.domestic.log_analyzer 20260327  # 특정 날짜

분석 대상:
    1. trader.log  — 거래 성과, 시스템 건전성
    2. scanner.log — 신호 품질
    3. trade_history (DB) — 실제 체결 기반 성과
─────────────────────────────────────────────────────
"""

import logging
import re
import sys
from collections import Counter
from pathlib import Path

from wye.blsh.common import dtutils, messageutils
from wye.blsh.common.env import LOG_DIR, KIS_ENV
from wye.blsh.database import query

log = logging.getLogger(__name__)

# ─────────────────────────────────────────
# 로그 파싱
# ─────────────────────────────────────────
# 포맷: "%(asctime)s [%(name)s][%(levelname)s] %(message)s"  (wye.blsh.__init__)
_LOG_PATTERN = re.compile(
    r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})\s+\[[\w.]+\]\[(\w+)\]\s+(.*)$"
)


def _parse_log_file(path: Path, target_date: str) -> list[dict]:
    """로그 파일에서 target_date(YYYYMMDD)에 해당하는 라인만 파싱."""
    if not path.exists():
        return []

    date_prefix = f"{target_date[:4]}-{target_date[4:6]}-{target_date[6:8]}"
    lines = []

    try:
        for raw in path.read_text(encoding="utf-8").splitlines():
            m = _LOG_PATTERN.match(raw)
            if m:
                ts, level, msg = m.groups()
                if ts.startswith(date_prefix):
                    lines.append({"ts": ts, "level": level, "msg": msg})
    except Exception as e:
        log.warning(f"로그 파싱 실패 ({path}): {e}")

    return lines


# ─────────────────────────────────────────
# trader.log 분석
# ─────────────────────────────────────────
def _analyze_trader(lines: list[dict]) -> dict:
    """trader.log 라인 목록 → 분석 결과 dict."""
    result = {
        "buy_count": 0,
        "sell_count": 0,
        "sl_count": 0,
        "tp1_count": 0,
        "tp2_count": 0,
        "expire_count": 0,
        "orphan_count": 0,
        "sell_fail_count": 0,
        "buy_fail_count": 0,
        "position_count": 0,
        "daily_pnl": None,
        "winners": 0,
        "losers": 0,
        "trail_sl_count": 0,
        "warnings": 0,
        "errors": 0,
        "criticals": 0,
        "warning_msgs": [],
    }

    warning_set: set[str] = set()

    for line in lines:
        level = line["level"]
        msg = line["msg"]

        if level == "WARNING":
            result["warnings"] += 1
            key = msg[:60]
            if key not in warning_set and len(warning_set) < 10:
                warning_set.add(key)
        elif level == "ERROR":
            result["errors"] += 1
        elif level == "CRITICAL":
            result["criticals"] += 1

        if "po체결:" in msg:
            result["buy_count"] += 1

        if "매도완료:" in msg or "NXT매도:" in msg:
            result["sell_count"] += 1

        if "손절" in msg and "매도 실패" not in msg:
            result["sl_count"] += 1
        if "1차익절" in msg and "매도 실패" not in msg:
            result["tp1_count"] += 1
        if "2차익절" in msg and "매도 실패" not in msg:
            result["tp2_count"] += 1

        if "만기청산" in msg or "기간초과" in msg:
            if "실패" not in msg:
                result["expire_count"] += 1

        if "추적불가 청산:" in msg and "실패" not in msg:
            result["orphan_count"] += 1

        if "매도 실패" in msg:
            result["sell_fail_count"] += 1

        if "주문 실패:" in msg or "주문실패" in msg:
            result["buy_fail_count"] += 1

        if "[포지션 로드]" in msg:
            m2 = re.search(r"(\d+)종목", msg)
            if m2:
                result["position_count"] = int(m2.group(1))

        if "[당일 결과]" in msg:
            m_pnl = re.search(r"추정손익\s+([+\-]?[\d,]+)원", msg)
            m_win = re.search(r"수익\s+(\d+)/손실\s+(\d+)", msg)
            if m_pnl:
                result["daily_pnl"] = int(m_pnl.group(1).replace(",", ""))
            if m_win:
                result["winners"] = int(m_win.group(1))
                result["losers"] = int(m_win.group(2))

        if "트레일링 SL:" in msg and "스킵" not in msg:
            result["trail_sl_count"] += 1

    result["warning_msgs"] = sorted(warning_set)[:5]
    return result


# ─────────────────────────────────────────
# scanner.log 분석
# ─────────────────────────────────────────
def _analyze_scanner(lines: list[dict]) -> dict:
    """scanner.log 라인 목록 → 분석 결과 dict."""
    result = {
        "scan_total": 0,
        "kospi_signals": 0,
        "kosdaq_signals": 0,
        "etf_signals": 0,
        "enrich_count": 0,
        "supply_hits": Counter(),
        "kospi_skipped": False,
        "kosdaq_skipped": False,
        "sector_adj_count": 0,
        "po_created": 0,
        "realtime_verified": 0,
        "realtime_dropped": 0,
        "realtime_dropped_names": [],
        # [임시] DB vs API 수급 비교
        "supply_cmp_total": 0,
        "supply_cmp_match": 0,
        "supply_cmp_mismatch": 0,
        # [임시] ETF 수급 조회
        "etf_supply_total": 0,
        "etf_supply_has_data": 0,
        "etf_supply_empty": 0,
    }

    for line in lines:
        msg = line["msg"]

        m = re.search(r"\[(\w+)\]\s+신호 종목:\s+(\d+)건", msg)
        if m:
            market, count = m.group(1), int(m.group(2))
            if market == "KOSPI":
                result["kospi_signals"] += count
            elif market == "KOSDAQ":
                result["kosdaq_signals"] += count
            elif market == "ETF":
                result["etf_signals"] += count
            result["scan_total"] += count

        m = re.search(r"\[수급 보강\]\s+대상\s+(\d+)종목", msg)
        if m:
            result["enrich_count"] += int(m.group(1))

        m_supply = re.search(r"(외국인|기관)\s+(TRN|C3|1)\(", msg)
        if m_supply:
            who = "F" if m_supply.group(1) == "외국인" else "I"
            result["supply_hits"][f"{who}_{m_supply.group(2)}"] += 1
        if "외국인+기관 동시" in msg:
            result["supply_hits"]["FI"] += 1
        if "개인 과매수 패널티" in msg:
            result["supply_hits"]["P_OV"] += 1

        if "[KOSPI] 지수 20MA 아래" in msg:
            result["kospi_skipped"] = True
        if "[KOSDAQ] 지수 20MA 아래" in msg:
            result["kosdaq_skipped"] = True

        m = re.search(r"\[업종패널티\]\s+(\d+)종목", msg)
        if m:
            result["sector_adj_count"] += int(m.group(1))

        m = re.search(r"(\d+)\s+종목\.\s+po-.*생성", msg)
        if m:
            result["po_created"] += int(m.group(1))

        # 실시간 부적합 검증
        m = re.search(r"\[실시간 검증\]\s+(\d+)종목 중\s+(\d+)종목 부적합", msg)
        if m:
            result["realtime_verified"] = int(m.group(1))
            result["realtime_dropped"] = int(m.group(2))
        m = re.search(r"\[실시간 검증\]\s+\S+\s+(\S+)\s+→\s+(.+)", msg)
        if m:
            result["realtime_dropped_names"].append(f"{m.group(1)}({m.group(2)})")

        # [임시] DB vs API 수급 비교
        if "✅" in msg and "외인: DB=" in msg:
            result["supply_cmp_total"] += 1
            result["supply_cmp_match"] += 1
        elif "❌" in msg and "외인: DB=" in msg:
            result["supply_cmp_total"] += 1
            result["supply_cmp_mismatch"] += 1

        # [임시] ETF 수급 조회
        if "[ETF수급]" in msg:
            result["etf_supply_total"] += 1
            if "데이터=있음" in msg:
                result["etf_supply_has_data"] += 1
            elif "데이터=없음" in msg:
                result["etf_supply_empty"] += 1

    return result


# ─────────────────────────────────────────
# DB trade_history 분석
# ─────────────────────────────────────────
def _analyze_db(date_str: str) -> dict:
    """trade_history 테이블에서 당일 실적 분석."""
    result = {
        "db_buys": 0,
        "db_sells": 0,
        "tickers_traded": set(),
    }

    try:
        rows = query.get_trade_history(date_str)
        for r in rows:
            side = r.get("side", "")
            ticker = r.get("ticker", "")
            result["tickers_traded"].add(ticker)
            if side == "buy":
                result["db_buys"] += 1
            elif side == "sell":
                result["db_sells"] += 1
    except Exception as e:
        log.warning(f"trade_history 조회 실패: {e}")

    result["tickers_traded"] = len(result["tickers_traded"])
    return result


# ─────────────────────────────────────────
# 리포트 생성
# ─────────────────────────────────────────
def _build_report(date_str: str, trader: dict, scanner: dict, db: dict) -> str:
    """분석 결과 → 텔레그램 리포트 문자열."""
    d = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
    env_label = "🚨real" if KIS_ENV == "real" else "📋demo"
    parts = [f"📊 일일 리포트 ({d}) [{env_label}]", "━" * 24]

    total_closed = (
        trader["sl_count"]
        + trader["tp1_count"]
        + trader["tp2_count"]
        + trader["expire_count"]
    )
    parts.append("【거래】")
    parts.append(f"  매수 {trader['buy_count']}건 / 매도 {trader['sell_count']}건")
    if total_closed > 0:
        parts.append(
            f"  손절 {trader['sl_count']} / TP1 {trader['tp1_count']}"
            f" / TP2 {trader['tp2_count']} / 만기 {trader['expire_count']}"
        )
    if trader["daily_pnl"] is not None:
        pnl = trader["daily_pnl"]
        icon = "📈" if pnl >= 0 else "📉"
        parts.append(
            f"  {icon} 추정손익 {pnl:+,}원"
            f"  (수익 {trader['winners']} / 손실 {trader['losers']})"
        )
    if trader["trail_sl_count"]:
        parts.append(f"  트레일링 SL 갱신 {trader['trail_sl_count']}회")

    parts.append("")
    parts.append("【신호】")
    if scanner["kospi_skipped"]:
        parts.append("  KOSPI 지수 MA 아래 → 스킵")
    if scanner["kosdaq_skipped"]:
        parts.append("  KOSDAQ 지수 MA 아래 → 스킵")

    scan_detail = f"KP {scanner['kospi_signals']} / KQ {scanner['kosdaq_signals']}"
    if scanner["etf_signals"]:
        scan_detail += f" / ETF {scanner['etf_signals']}"
    parts.append(f"  스캔 {scanner['scan_total']}종목 ({scan_detail})")
    if scanner["enrich_count"]:
        parts.append(f"  수급 보강 {scanner['enrich_count']}종목")
    if scanner["supply_hits"]:
        hits = ", ".join(f"{k}:{v}" for k, v in scanner["supply_hits"].most_common(5))
        parts.append(f"  수급 플래그: {hits}")
    if scanner["sector_adj_count"]:
        parts.append(f"  업종 점수 조정 {scanner['sector_adj_count']}종목")
    if scanner["realtime_dropped"]:
        names = ", ".join(scanner["realtime_dropped_names"][:5])
        parts.append(
            f"  🚫 실시간 검증 탈락 {scanner['realtime_dropped']}종목: {names}"
        )
    if scanner["po_created"]:
        parts.append(f"  PO 생성 {scanner['po_created']}종목")

    # [임시] 수급 비교 / ETF 수급 리포트
    if scanner["supply_cmp_total"] or scanner["etf_supply_total"]:
        parts.append("")
        parts.append("【수급 검증 (임시)】")
        if scanner["supply_cmp_total"]:
            parts.append(
                f"  DB vs API: {scanner['supply_cmp_total']}종목 대조"
                f" → 일치 {scanner['supply_cmp_match']}"
                f" / 불일치 {scanner['supply_cmp_mismatch']}"
            )
        if scanner["etf_supply_total"]:
            parts.append(
                f"  ETF 수급: {scanner['etf_supply_total']}종목 조회"
                f" → 데이터 {scanner['etf_supply_has_data']}"
                f" / 빈값 {scanner['etf_supply_empty']}"
            )
            if scanner["etf_supply_total"] == scanner["etf_supply_empty"]:
                parts.append("  ⚠️ ETF 수급 전량 빈값 → 데이터 정확성 의심")

    parts.append("")
    parts.append("【건전성】")
    health_items = []
    if trader["criticals"]:
        health_items.append(f"🔴 CRITICAL {trader['criticals']}")
    if trader["errors"]:
        health_items.append(f"🟠 ERROR {trader['errors']}")
    health_items.append(f"⚠️ WARN {trader['warnings']}")
    parts.append(f"  {' / '.join(health_items)}")

    if trader["sell_fail_count"]:
        parts.append(f"  매도 실패 {trader['sell_fail_count']}건")
    if trader["buy_fail_count"]:
        parts.append(f"  매수 실패 {trader['buy_fail_count']}건")
    if trader["orphan_count"]:
        parts.append(f"  🚨 추적불가 청산 {trader['orphan_count']}건")

    if trader["warning_msgs"]:
        parts.append("  주요 경고:")
        for wm in trader["warning_msgs"][:3]:
            parts.append(f"    · {wm[:50]}")

    if db["db_buys"] or db["db_sells"]:
        parts.append("")
        parts.append("【DB 검증】")
        parts.append(
            f"  이력: 매수 {db['db_buys']}건 / 매도 {db['db_sells']}건"
            f" / {db['tickers_traded']}종목"
        )
        if trader["buy_count"] != db["db_buys"]:
            parts.append(
                f"  ⚠️ 매수 불일치: 로그 {trader['buy_count']} ≠ DB {db['db_buys']}"
            )
        if trader["sell_count"] != db["db_sells"]:
            parts.append(
                f"  ⚠️ 매도 불일치: 로그 {trader['sell_count']} ≠ DB {db['db_sells']}"
            )

    parts.append("━" * 24)
    return "\n".join(parts)


# ─────────────────────────────────────────
# 메인
# ─────────────────────────────────────────
def analyze(date_str: str | None = None):
    """일일 로그 분석 + 텔레그램 리포트 발송."""
    date_str = date_str or dtutils.today()

    date_suffix = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
    trader_log = LOG_DIR / f"trader.log.{date_suffix}"
    scanner_log = LOG_DIR / f"scanner.log.{date_suffix}"

    if date_str == dtutils.today():
        if not trader_log.exists():
            trader_log = LOG_DIR / "trader.log"
        if not scanner_log.exists():
            scanner_log = LOG_DIR / "scanner.log"

    log.info(f"[로그 분석] 날짜={date_str}")
    log.info(f"  trader: {trader_log}  (존재: {trader_log.exists()})")
    log.info(f"  scanner: {scanner_log}  (존재: {scanner_log.exists()})")

    trader_lines = _parse_log_file(trader_log, date_str)
    scanner_lines = _parse_log_file(scanner_log, date_str)

    if not trader_lines and not scanner_lines:
        log.info(f"[로그 분석] {date_str} 로그 없음 → 리포트 미발송")
        return

    trader_result = _analyze_trader(trader_lines)
    scanner_result = _analyze_scanner(scanner_lines)
    db_result = _analyze_db(date_str)

    report = _build_report(date_str, trader_result, scanner_result, db_result)

    print(report)
    messageutils.send_message(report)
    log.info("[로그 분석] 리포트 발송 완료")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )
    target = sys.argv[1] if len(sys.argv) > 1 else None
    analyze(target)
