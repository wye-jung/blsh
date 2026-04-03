#!/bin/bash
# ─────────────────────────────────────────
# 트레이더 로그 모니터 + 프로세스 감시
# ─────────────────────────────────────────
# 사용법:
#   bin/watchdog.sh              # 트레이더 실행 + 모니터링
#   bin/watchdog.sh monitor      # 모니터링만 (트레이더는 이미 실행 중)
#   bin/watchdog.sh stop         # 트레이더 + watchdog 종료
#   bin/watchdog.sh status       # 트레이더 상태 확인
#   bin/watchdog.sh status positions/pendings/holdings/cash
#
# 크론탭 예시 (매일 07:55 시작):
#   55 7 * * 1-5 /home/wye/workspace/blsh/bin/watchdog.sh >> ~/.blsh/logs/watchdog.log 2>&1
# ─────────────────────────────────────────

cd /home/wye/workspace/blsh || { echo "[ERROR] 디렉토리 이동 실패: /home/wye/workspace/blsh"; exit 1; }

# KIS_ENV: 환경변수 우선, 없으면 .env 로드, 기본값 demo
if [ -z "$KIS_ENV" ] && [ -f "$HOME/.blsh/config/.env" ]; then
    KIS_ENV=$(grep -E '^KIS_ENV=' "$HOME/.blsh/config/.env" | cut -d= -f2 | tr -d '"' | tr -d "'" | xargs)
fi
KIS_ENV="${KIS_ENV:-demo}"

ENV_DIR="$HOME/.blsh/${KIS_ENV}"
LOG_FILE="${ENV_DIR}/logs/trader.log"
PID_FILE="${ENV_DIR}/data/trader.pid"
MONITOR_PID_FILE="${ENV_DIR}/data/monitor.pid"
WATCHDOG_PID_FILE="${ENV_DIR}/data/watchdog.pid"
CHECK_INTERVAL=60  # 프로세스 생존 확인 주기 (초)

# ── 텔레그램 알림 (env에서 토큰 로드)
send_alert() {
    local msg="$1"
    # .env에서 토큰/채팅ID 로드 (source 대신 grep — .env에 bash 비호환 구문 방지)
    if [ -f "$HOME/.blsh/config/.env" ]; then
        TELEGRAM_BOT_TOKEN=$(grep -E '^TELEGRAM_BOT_TOKEN=' "$HOME/.blsh/config/.env" | cut -d= -f2 | tr -d '"' | tr -d "'" | xargs)
        TELEGRAM_CHAT_ID=$(grep -E '^TELEGRAM_CHAT_ID=' "$HOME/.blsh/config/.env" | cut -d= -f2 | tr -d '"' | tr -d "'" | xargs)
    fi
    if [ -z "$TELEGRAM_BOT_TOKEN" ] || [ -z "$TELEGRAM_CHAT_ID" ]; then
        echo "[$(date '+%H:%M:%S')] 텔레그램 미설정 → 콘솔 출력: $msg"
        return
    fi
    curl -s -X POST "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" \
        -d chat_id="$TELEGRAM_CHAT_ID" \
        -d text="🤖 [BLSH] $msg" \
        -d parse_mode="HTML" > /dev/null 2>&1
}

# ── 로그 모니터 (백그라운드)
start_log_monitor() {
    # 기존 모니터 종료
    if [ -f "$MONITOR_PID_FILE" ]; then
        local old_pid
        old_pid=$(cat "$MONITOR_PID_FILE")
        kill "$old_pid" 2>/dev/null
        rm -f "$MONITOR_PID_FILE"
    fi

    # [FIX] 로그 파일 대기 + tail을 모두 백그라운드로 처리
    # (동기 대기 시 trader 미시작 → trader.log 미생성 → 교착 발생 방지)
    (
        while [ ! -f "$LOG_FILE" ]; do sleep 2; done
        exec tail -n 0 -F "$LOG_FILE" 2>/dev/null
    ) | while IFS= read -r line; do
        # CRITICAL 레벨 → 즉시 알림
        if echo "$line" | grep -q "\[CRITICAL\]"; then
            send_alert "🔴 CRITICAL: $line"
        # ERROR 레벨 → 알림
        elif echo "$line" | grep -q "\[ERROR\]"; then
            send_alert "🟠 ERROR: $line"
        # 특정 키워드 감지
        elif echo "$line" | grep -qE "추적불가 청산|포지션 파일 로드 실패|인증 실패|positions.json 없음"; then
            send_alert "⚠️ $line"
        fi
    done &

    echo $! > "$MONITOR_PID_FILE"
    echo "[$(date '+%H:%M:%S')] 로그 모니터 시작 (PID: $(cat "$MONITOR_PID_FILE"))"
}

# ── 프로세스 감시 루프
watch_trader() {
    local trader_pid="$1"
    echo "[$(date '+%H:%M:%S')] 트레이더 감시 시작 (PID: $trader_pid)"

    while true; do
        sleep "$CHECK_INTERVAL"

        # 트레이더 프로세스 생존 확인
        if ! kill -0 "$trader_pid" 2>/dev/null; then
            # 정상 종료(exit 0) vs 비정상 종료 구분
            wait "$trader_pid" 2>/dev/null
            local exit_code=$?

            if [ "$exit_code" -eq 0 ]; then
                echo "[$(date '+%H:%M:%S')] 트레이더 정상 종료 (exit 0)"
                send_alert "✅ 트레이더 정상 종료"
            else
                echo "[$(date '+%H:%M:%S')] 🚨 트레이더 비정상 종료 (exit $exit_code)"
                send_alert "🚨 트레이더 비정상 종료 (exit code: $exit_code)\n마지막 로그:\n$(tail -5 "$LOG_FILE")"
            fi

            # 모니터 종료
            if [ -f "$MONITOR_PID_FILE" ]; then
                kill "$(cat "$MONITOR_PID_FILE")" 2>/dev/null
                rm -f "$MONITOR_PID_FILE"
            fi
            rm -f "$PID_FILE"
            break
        fi
    done
}

# ── 메인
main() {
    local mode="${1:-full}"  # full(기본) 또는 monitor
    _WATCHDOG_MODE="$mode"

    if [ "$mode" = "status" ]; then
        if [ -n "$2" ]; then
            $HOME/.local/bin/uv run python -m wye.blsh status "$2"
        else
            $HOME/.local/bin/uv run python -m wye.blsh status
        fi
        exit 0
    fi

    if [ "$mode" = "stop" ]; then
        # watchdog 본체 종료 (SIGTERM → cleanup()이 트레이더+모니터 종료)
        if [ -f "$WATCHDOG_PID_FILE" ]; then
            local wd_pid
            wd_pid=$(cat "$WATCHDOG_PID_FILE")
            if kill -0 "$wd_pid" 2>/dev/null; then
                echo "[$(date '+%H:%M:%S')] watchdog 종료 요청 (PID: $wd_pid)"
                kill "$wd_pid" 2>/dev/null
                # 종료 대기 (최대 15초)
                for i in $(seq 1 15); do
                    kill -0 "$wd_pid" 2>/dev/null || break
                    sleep 1
                done
                if kill -0 "$wd_pid" 2>/dev/null; then
                    echo "[$(date '+%H:%M:%S')] watchdog 강제 종료"
                    kill -9 "$wd_pid" 2>/dev/null
                fi
                echo "[$(date '+%H:%M:%S')] watchdog 종료 완료"
            else
                echo "[$(date '+%H:%M:%S')] watchdog 이미 종료됨"
            fi
            rm -f "$WATCHDOG_PID_FILE"
        fi
        # watchdog이 없는 경우 직접 정리
        if [ -f "$PID_FILE" ]; then
            local trader_pid
            trader_pid=$(cat "$PID_FILE")
            if kill -0 "$trader_pid" 2>/dev/null; then
                echo "[$(date '+%H:%M:%S')] 트레이더 종료 요청 (PID: $trader_pid)"
                kill -INT "$trader_pid" 2>/dev/null
            fi
            rm -f "$PID_FILE"
        fi
        if [ -f "$MONITOR_PID_FILE" ]; then
            kill "$(cat "$MONITOR_PID_FILE")" 2>/dev/null
            rm -f "$MONITOR_PID_FILE"
        fi
        exit 0
    fi

    if [ "$mode" = "monitor" ]; then
        # 모니터링만 (트레이더는 이미 실행 중)
        start_log_monitor
        echo "[$(date '+%H:%M:%S')] 모니터 모드 — Ctrl+C로 종료"
        wait
        exit 0
    fi

    # 이미 실행 중인지 확인
    if [ -f "$PID_FILE" ]; then
        local existing_pid
        existing_pid=$(cat "$PID_FILE")
        if kill -0 "$existing_pid" 2>/dev/null; then
            echo "[$(date '+%H:%M:%S')] 트레이더 이미 실행 중 (PID: $existing_pid)"
            exit 0
        fi
        rm -f "$PID_FILE"
    fi

    # watchdog PID 저장
    echo $$ > "$WATCHDOG_PID_FILE"

    # 로그 모니터 시작
    start_log_monitor

    # 트레이더 실행 (백그라운드)
    echo "[$(date '+%H:%M:%S')] 트레이더 시작"
    $HOME/.local/bin/uv run python -m wye.blsh start &
    local trader_pid=$!

    send_alert "🟢 트레이더 시작 (PID: $trader_pid)"

    # 프로세스 감시
    watch_trader "$trader_pid"

    echo "[$(date '+%H:%M:%S')] watchdog 종료"
}

# 시그널 핸들러 (Ctrl+C 등)
_WATCHDOG_MODE=""
cleanup() {
    echo "[$(date '+%H:%M:%S')] 종료 시그널 수신"
    if [ -f "$MONITOR_PID_FILE" ]; then
        kill "$(cat "$MONITOR_PID_FILE")" 2>/dev/null
        rm -f "$MONITOR_PID_FILE"
    fi
    # monitor 모드에서는 로그 모니터만 종료, 트레이더는 유지
    if [ "$_WATCHDOG_MODE" != "monitor" ] && [ -f "$PID_FILE" ]; then
        local trader_pid
        trader_pid=$(cat "$PID_FILE")
        echo "[$(date '+%H:%M:%S')] 트레이더 종료 대기 (SIGINT → Python finally 실행)"
        kill -INT "$trader_pid" 2>/dev/null
        wait "$trader_pid" 2>/dev/null
        rm -f "$PID_FILE"
    fi
    rm -f "$WATCHDOG_PID_FILE"
    exit 0
}
# status/stop은 trap 불필요 → main 내부에서 exit 0으로 즉시 종료
case "${1:-}" in
    status|stop) main "$@" ;;
    *)
        trap cleanup SIGINT SIGTERM
        main "$@"
        ;;
esac
