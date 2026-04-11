# Cron Pipeline — 일일 자동 운영

> `bin/setup_cron.sh`, `bin/watchdog.sh`

## 설치 / 해제

```bash
bin/setup_cron.sh install    # 크론탭 등록
bin/setup_cron.sh remove     # 크론탭 해제
bin/setup_cron.sh status     # 현재 등록 상태 확인
```

## 스케줄

모든 작업은 `CRON_INIT` (`mkdir -p $CRON_LOG_DIR && cd $BLSH_DIR`) 후 실행.

| 시각 | 빈도 | 작업 | 로그 |
|------|------|------|------|
| 06:00 | 월 | `holiday` -- KRX 휴장일 수집 (향후 30일) | holiday.log |
| 06:30 | 월 | `sector` -- 업종지수 매핑 확인 | sector.log |
| 07:30 | 월~금 | `po` -- 데이터 수집 + PO(1) (PRE: 전일 확정 일봉 스캔) | po.log |
| 07:55 | 월~금 | `watchdog.sh` -- 트레이더 시작 + 크래시 감지 | watchdog.log |
| 11:30 | 월~금 | `po` -- 데이터 수집 + PO(2) (INI: 장중 스캔) | po.log |
| 15:05 | 월~금 | `po` -- 데이터 수집 + PO(3) (FIN: 청산 후 스캔) | po.log |
| 20:30 | 월~금 | `analyze` -- 일일 로그 분석 -> 텔레그램 리포트 | analyze.log |
| 02:00 | 토 | `grid_search --alternating` + `--walkforward` -- 최적화 + OOS 검증 | optimize.log |
| 21:00 | 일 | `weekly-analysis.sh` -- Claude Code 주간 로그 분석 → 텔레그램 | weekly-analysis.log |

로그 위치: `~/.blsh/logs/`
리포트 위치: `~/.blsh/reports/weekly-YYYYMMDD.md`

## 평일 파이프라인 흐름

```
07:30  po -> PO(1)(pre) 생성
07:55  watchdog -> trader 시작
08:00  trader -> PO(1) 읽기 -> NXT 지정가 매수 (30%)
09:00  trader -> KRX 개장, SL/TP 모니터링 시작
11:30  po -> PO(2)(ini) 생성
~11:35 trader -> PO(2) 감지 -> KRX 지정가 매수 (15%), 10분 후 미체결 취소
15:05  po -> PO(3)(fin) 생성
15:15  trader -> 만기 종목 청산 -> PO(3) 읽기 -> 매수 (55% x 90%)
15:30  trader -> KRX 마감 -> NXT 에프터마켓 SL/TP
20:00  trader -> 종료, 체결가 보정, DB 저장
20:30  analyze -> 당일 매매 요약 -> 텔레그램 발송
```

## watchdog.sh

트레이더 프로세스 관리 + 로그 모니터링 + 텔레그램 알림.

```bash
bin/watchdog.sh              # 트레이더 실행 + 모니터링 (기본)
bin/watchdog.sh monitor      # 모니터링만 (트레이더 이미 실행 중)
bin/watchdog.sh stop         # 전체 종료 (트레이더 + 모니터 + watchdog)
bin/watchdog.sh status       # 트레이더 상태 확인
```

### 관리 프로세스

| 프로세스 | PID 파일 | 역할 |
|----------|----------|------|
| 트레이더 | trader.pid | Python 자동매매 |
| 로그 모니터 | monitor.pid | trader.log tail -> CRITICAL/ERROR 감지 -> 텔레그램 |
| watchdog 본체 | watchdog.pid | 트레이더 생존 확인 (60초 주기) |

### stop 순서

1. 트레이더에 SIGINT (Python finally 블록 -> position 저장)
2. 최대 15초 대기, 미종료 시 SIGKILL
3. watchdog 본체 + 자식 프로세스 종료
4. PID 파일 정리

## Log Analyzer

`trader.log`와 DB `trade_history`를 파싱하여 텔레그램으로 당일 매매 요약 발송.

```bash
uv run python -m wye.blsh analyze              # 오늘
uv run python -m wye.blsh analyze 20260401     # 특정 날짜
```

분석 항목:
- 매수/매도 건수, SL/TP1/TP2/만기 청산 건수
- 일일 손익 추정
- 신호 품질 (스캔 수, 수급 플래그 분포)
- 시스템 건강도 (CRITICAL/ERROR/WARNING 수)
- 실패 요약 (주문 실패, 유령 포지션 등)
