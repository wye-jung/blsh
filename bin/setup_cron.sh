#!/bin/bash
# ─────────────────────────────────────────
# BLSH 크론탭 등록/해제 스크립트
# ─────────────────────────────────────────
# 사용법:
#   bin/setup_cron.sh install   # 크론탭 등록
#   bin/setup_cron.sh remove    # 크론탭 해제
#   bin/setup_cron.sh status    # 현재 등록 상태 확인
# ─────────────────────────────────────────

BLSH_DIR="/home/wye/workspace/blsh"
BLSH_TAG="# BLSH_AUTO"

CRON_LOG_DIR="$HOME/.blsh/logs"
CRON_INIT="mkdir -p $CRON_LOG_DIR && cd $BLSH_DIR"

# crontab 헤더에 선언할 환경변수 (인라인 VAR=val은 sh에서 단일 명령에만 적용됨)
# crontab 환경변수
# 주의: crontab은 인라인 주석(#)을 지원하지 않음 — 값의 일부로 해석됨
# BLSH_TAG는 install 함수에서 별도 주석줄로 삽입하여 remove 시 함께 제거
CRON_ENV_LINES=(
    "HOME=/home/wye"
    "PATH=/home/wye/.local/bin:/usr/local/bin:/usr/bin:/bin"
)

# 등록할 크론 작업 목록
CRON_ENTRIES=(
    # 1. 휴장일 데이터 수집 (매주 월 06:00)
    "0 6 * * 1 $CRON_INIT && uv run python -m wye.blsh holiday >> $CRON_LOG_DIR/holiday.log 2>&1 $BLSH_TAG"

    # 2. 데이터 수집 + PO 생성 — PRE (매일 월~금 07:30, 전일 스캔 → PO①)
    "30 7 * * 1-5 $CRON_INIT && uv run python -m wye.blsh po >> $CRON_LOG_DIR/po.log 2>&1 $BLSH_TAG"

    # 3. 데이터 수집 + PO 생성 — INI (매일 월~금 11:30, 장초반 스캔 → PO②)
    "30 11 * * 1-5 $CRON_INIT && uv run python -m wye.blsh po >> $CRON_LOG_DIR/po.log 2>&1 $BLSH_TAG"

    # 4. 데이터 수집 + PO 생성 — FIN (매일 월~금 15:05, 청산 후 매수 → PO③)
    "5 15 * * 1-5 $CRON_INIT && uv run python -m wye.blsh po >> $CRON_LOG_DIR/po.log 2>&1 $BLSH_TAG"

    # 5. 트레이더 실행 + 모니터링 (매일 월~금 07:55)
    "55 7 * * 1-5 $CRON_INIT && bin/watchdog.sh >> $CRON_LOG_DIR/watchdog.log 2>&1 $BLSH_TAG"

    # 6. 일일 로그 분석 리포트 (매일 월~금 20:30)
    "30 20 * * 1-5 $CRON_INIT && uv run python -m wye.blsh analyze >> $CRON_LOG_DIR/analyze.log 2>&1 $BLSH_TAG"

    # 7. Grid Search 최적화 (매주 토 02:00)
    # 캐시 강제 재빌드 하려면 --rebuild 인자 지정(+~3분). 미지정 시 캐시 범위 불일치(5일 초과)시에만 자동 재빌드.
    "0 2 * * 6 $CRON_INIT && uv run python -m wye.blsh.domestic.optimize.grid_search --alternating >> $CRON_LOG_DIR/optimize.log 2>&1 $BLSH_TAG"

    # 8. 업종지수 매핑 확인 (매주 월 06:30)
    "30 6 * * 1 $CRON_INIT && uv run python -m wye.blsh sector >> $CRON_LOG_DIR/sector.log 2>&1 $BLSH_TAG"
)

install_cron() {
    local existing
    existing=$(crontab -l 2>/dev/null | grep -v "$BLSH_TAG")

    {
        # 기존 항목 (BLSH 제외) 유지 + 빈 줄 정리
        echo "$existing" | sed '/^$/N;/^\n$/d'
        echo ""
        echo "# ═══════════════════════════════════════ $BLSH_TAG"
        echo "# BLSH 자동매매 시스템 $BLSH_TAG"
        echo "# ═══════════════════════════════════════ $BLSH_TAG"
        echo "# BLSH_ENV_BEGIN $BLSH_TAG"
        for env_line in "${CRON_ENV_LINES[@]}"; do
            echo "$env_line"
        done
        echo "# BLSH_ENV_END $BLSH_TAG"
        echo ""
        for entry in "${CRON_ENTRIES[@]}"; do
            echo "$entry"
        done
    } | crontab -

    echo "✅ 크론탭 등록 완료"
    echo ""
    echo "등록된 작업:"
    echo "  월   06:00  휴장일 데이터 수집"
    echo "  월~금 07:30  데이터 수집 + PO① (전일 스캔)"
    echo "  월~금 07:55  트레이더 실행 + watchdog"
    echo "  월~금 10:05  데이터 수집 + PO② (장초반 스캔)"
    echo "  월~금 15:05  데이터 수집 + PO③ (청산 후 스캔)"
    echo "  월~금 20:30  일일 로그 분석 리포트"
    echo "  토   02:00  Grid Search 최적화"
    echo "  월   06:30  업종지수 매핑 확인"
    echo ""
    echo "로그 위치: ~/.blsh/logs/"
}

remove_cron() {
    local remaining
    # BLSH_TAG 포함 줄 + BLSH_ENV 블록 내부(HOME=, PATH=) 제거
    remaining=$(crontab -l 2>/dev/null | sed '/BLSH_ENV_BEGIN/,/BLSH_ENV_END/d' | grep -v "$BLSH_TAG")

    echo "$remaining" | crontab -
    echo "✅ BLSH 크론탭 항목 제거 완료"
}

show_status() {
    echo "── 현재 BLSH 크론탭 ──"
    local entries
    entries=$(crontab -l 2>/dev/null | grep "$BLSH_TAG")
    if [ -z "$entries" ]; then
        echo "  (등록된 항목 없음)"
    else
        echo "$entries" | sed "s/ $BLSH_TAG//"
    fi
}

# ── 메인
case "${1:-status}" in
    install)
        mkdir -p "$CRON_LOG_DIR"
        install_cron
        ;;
    remove)
        remove_cron
        ;;
    status)
        show_status
        ;;
    *)
        echo "사용법: $0 {install|remove|status}"
        exit 1
        ;;
esac
