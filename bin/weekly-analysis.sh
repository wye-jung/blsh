#!/bin/bash
# ─────────────────────────────────────────
# Claude Code 주간 로그 분석
# ─────────────────────────────────────────
# 사용법:
#   bin/weekly-analysis.sh              # 최근 7일 분석
#   bin/weekly-analysis.sh 14           # 최근 14일 분석
#
# 요구사항:
#   - Claude Code CLI 설치: curl -fsSL https://claude.ai/install.sh | bash
#   - ANTHROPIC_API_KEY 환경변수 또는 ~/.claude/ 인증
#
# 크론탭 예시 (매주 일 21:00):
#   0 21 * * 0 /home/wye/workspace/blsh/bin/weekly-analysis.sh >> ~/.blsh/logs/weekly-analysis.log 2>&1
# ─────────────────────────────────────────

set -e

cd /home/wye/workspace/blsh || { echo "[ERROR] 디렉토리 이동 실패"; exit 1; }

# ── 설정
DAYS="${1:-7}"
KIS_ENV="${KIS_ENV:-demo}"
LOGS_DIR="$HOME/.blsh/${KIS_ENV}/logs"
REPORTS_DIR="$HOME/.blsh/reports"
REPORT_FILE="$REPORTS_DIR/weekly-$(date +%Y%m%d).md"
TIMESTAMP="$(date '+%Y-%m-%d %H:%M:%S')"

mkdir -p "$REPORTS_DIR"

echo "[$TIMESTAMP] 주간 로그 분석 시작 (${DAYS}일, KIS_ENV=${KIS_ENV})"

# ── 로그 파일 존재 확인
if [ ! -d "$LOGS_DIR" ]; then
    echo "[ERROR] 로그 디렉토리 없음: $LOGS_DIR"
    exit 1
fi

# ── 최근 N일 로그 파일 목록 수집
TRADER_LOGS=$(find "$LOGS_DIR" -name "trader.log*" -mtime -"$DAYS" -type f | sort)
SCANNER_LOGS=$(find "$LOGS_DIR" -name "scanner.log*" -mtime -"$DAYS" -type f | sort)
OPTIMIZE_LOGS=$(find "$LOGS_DIR" -name "optimize.log*" -mtime -"$DAYS" -type f | sort)
BLSH_LOG=$(find "$LOGS_DIR" -name "blsh.log*" -mtime -"$DAYS" -type f | sort)

TRADER_COUNT=$(echo "$TRADER_LOGS" | grep -c '.' 2>/dev/null || echo 0)
SCANNER_COUNT=$(echo "$SCANNER_LOGS" | grep -c '.' 2>/dev/null || echo 0)

echo "  trader 로그: ${TRADER_COUNT}개"
echo "  scanner 로그: ${SCANNER_COUNT}개"

if [ "$TRADER_COUNT" -eq 0 ] && [ "$SCANNER_COUNT" -eq 0 ]; then
    echo "[SKIP] 분석할 로그 없음"
    exit 0
fi

# ── Claude Code 분석 프롬프트
PROMPT="당신은 한국 주식 자동매매 봇(blsh)의 운영 분석가입니다.
최근 ${DAYS}일간의 로그 파일을 분석하여 주간 리포트를 작성하세요.

분석 대상 로그 디렉토리: ${LOGS_DIR}
- trader.log*: 매매 실행 로그 (매수/매도/SL/TP/에러)
- scanner.log*: 신호 생성 로그 (스캔/수급/PO 생성)
- optimize.log*: 최적화 로그 (grid_search/walk-forward)
- blsh.log*: 일일 분석 리포트 (log_analyzer 결과)

## 리포트 구성 (한국어)

### 1. 주간 성과 요약
- 총 매수/매도 건수, 승률, 추정 손익
- 일별 거래 현황 (거래 있는 날만)
- SL/TP1/TP2/만기청산 비율

### 2. 오류 및 이상 감지
- ERROR/CRITICAL 로그 분석 (원인과 영향)
- API 에러 코드별 빈도 (EGW*, OPSQ* 등)
- 매수/매도 실패 패턴
- 추적불가 종목, 부분 체결, 유령 포지션

### 3. 신호 품질 분석
- 시장별(KOSPI/KOSDAQ) 신호 수 추이
- 수급 보강 히트율 (DB vs API fallback)
- PO 생성 건수 추이
- 실시간 검증 탈락 패턴

### 4. 시스템 건전성
- 로그 공백 (수집 누락, 프로세스 다운)
- 토큰 갱신/rate limit 이벤트
- 데이터 수집 정상 여부

### 5. 개선 제안
- 반복되는 문제 패턴에 대한 구체적 제안
- 파라미터 조정 검토 필요 여부
- 다음 주 주의 사항

리포트를 마크다운 형식으로 ${REPORT_FILE}에 저장하세요.
파일 상단에 '# 주간 분석 리포트 (${TIMESTAMP})' 제목을 포함하세요."

# ── Claude Code 실행
echo "  Claude Code 분석 실행..."
if claude -p "$PROMPT" \
    --allowedTools "Read,Bash,Glob,Grep" \
    > /dev/null 2>&1; then
    echo "  분석 완료: $REPORT_FILE"
else
    echo "[ERROR] Claude Code 분석 실패"
    exit 1
fi

# ── 리포트 파일 확인
if [ ! -f "$REPORT_FILE" ]; then
    echo "[ERROR] 리포트 파일 미생성: $REPORT_FILE"
    exit 1
fi

REPORT_SIZE=$(wc -c < "$REPORT_FILE")
echo "  리포트 크기: ${REPORT_SIZE} bytes"

# ── 텔레그램 알림 (요약 + 파일 경로)
echo "  텔레그램 알림 전송..."
uv run python -c "
import sys
sys.path.insert(0, 'src')
from wye.blsh.common.messageutils import send_message
from pathlib import Path

report = Path('$REPORT_FILE')
content = report.read_text(encoding='utf-8')

# 요약 추출: 첫 500자 또는 '### 2.' 이전까지
summary = content
for marker in ['### 2.', '### 3.', '## 2.', '## 3.']:
    idx = content.find(marker)
    if idx > 0:
        summary = content[:idx].strip()
        break
if len(summary) > 800:
    summary = summary[:800] + '...'

msg = f'''📋 주간 로그 분석 리포트
━━━━━━━━━━━━━━━━━━━━━━━━
{summary}
━━━━━━━━━━━━━━━━━━━━━━━━
전체 리포트: {report.absolute()}'''

send_message(msg)
print('알림 전송 완료')
" 2>&1 || echo "[WARN] 텔레그램 전송 실패"

# ── 이메일 발송 (Gmail SMTP)
echo "  이메일 발송..."
uv run python -c "
import sys, os, smtplib
from email.mime.text import MIMEText
from pathlib import Path

# .env에서 Gmail 설정 로드
env_path = Path.home() / '.blsh/config/.env'
env_vars = {}
if env_path.exists():
    for line in env_path.read_text().splitlines():
        if '=' in line and not line.startswith('#'):
            k, v = line.split('=', 1)
            env_vars[k.strip()] = v.strip().strip('\"').strip(\"'\")

gmail_user = env_vars.get('GMAIL_USER', '')
gmail_pass = env_vars.get('GMAIL_APP_PASSWORD', '')

if not gmail_user or not gmail_pass:
    print('[SKIP] GMAIL_USER/GMAIL_APP_PASSWORD 미설정')
    sys.exit(0)

report = Path('$REPORT_FILE')
content = report.read_text(encoding='utf-8')

msg = MIMEText(content, _charset='utf-8')
msg['Subject'] = f'[BLSH] 주간 분석 리포트 ($TIMESTAMP)'
msg['From'] = gmail_user
msg['To'] = gmail_user

try:
    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as s:
        s.login(gmail_user, gmail_pass)
        s.send_message(msg)
    print('이메일 발송 완료')
except Exception as e:
    print(f'[WARN] 이메일 발송 실패: {e}')
" 2>&1 || echo "[WARN] 이메일 발송 실패"

echo "[$TIMESTAMP] 주간 분석 완료"
