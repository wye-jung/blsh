# Trader — 실시간 매매

> `src/wye/blsh/domestic/trader.py`

단일 스레드, 차등 주기 루프 (TICK_SEC=10초 / SLOW_EVERY=3배).

## 타임라인

| 시각 | 동작 |
|------|------|
| 08:00 | PO(1) NXT 지정가 매수 (30%), 30초 간격 pending 체결 확인 |
| 09:00 | KRX 개장. 실패 PRE 재시도, 유령 체크, 만기 청산, SL/TP 시작 |
| ~11:35 | PO(2) 파일 감지 -> KRX 지정가 매수 (15%), 10분 후 미체결 취소 |
| 15:15 | 만기 종목 시장가 청산 -> PO(3) KRX 매수 (55% x 90%) |
| 15:30 | KRX 마감 -> FIN 미체결분 NXT 재발주 |
| 15:30-20:00 | NXT 에프터마켓 SL/TP (NXT는 지정가만) |
| 20:00 | 세션 종료. 체결가 보정, DB update, 포지션 저장 |

## SL/TP 로직

1. **손절**: `현재가 <= SL` -> 전량 시장가 매도 (KRX) / 하한가 지정가 (NXT)
2. **1차 익절**: `현재가 >= TP1` -> `TP1_RATIO` 비율 매도, SL -> 매수가(본전)
3. **트레일링 SL**: 진입 이후 최고가 기준 `high_since_entry - ATR x ATR_SL_MULT`로 상향만 (시뮬레이션과 동일)
4. **2차 익절**: `현재가 >= TP2` -> 잔량 전량 매도

### 거래소 라우팅

| 메서드 | 거래소 | 용도 |
|--------|--------|------|
| `buy()` | 인자(NXT/KRX) | PO(1) NXT, 이후 KRX |
| `sell()` | KRX 고정 | 정규장 시장가 매도 |
| `sell_nxt()` | NXT 고정 | 에프터마켓 지정가 매도 |
| `cancel_order()` | 인자 | 발주 시 거래소와 일치 필요 |

## 최대 보유일 (모드별)

| 모드 | 보유일 | 설명 |
|------|--------|------|
| REV | MAX_HOLD_DAYS (현재 10) | 반전 -- 길게 |
| MIX | MAX_HOLD_DAYS_MIX (현재 5) | 혼합 -- 중간 |
| MOM | MAX_HOLD_DAYS_MOM (현재 3) | 추세 -- 짧게 |

## 자금 배분

균등 배분 + 총자산 대비 상한:

```
alloc = min(가용금액 / 종목수, 총자산 x 배분비율)
```

총자산(현금+보유평가) 규모별 배분 비율 (`MAX_ALLOC_TIERS`):

| 총자산 | 비율 | 예: 종목당 상한 |
|--------|------|----------------|
| ~1억 | 15% | ~1,500만원 |
| 1~5억 | 10% | 1,000~5,000만원 |
| 5~10억 | 7% | 3,500~7,000만원 |
| 10~50억 | 5% | 5,000만~2.5억 |
| 50억~ | 3% | 1.5억~ |

## Position 영속화

- **파일**: `~/.blsh/{KIS_ENV}/data/positions.json` (백업: `.bak`)
- **DB**: 포지션 미저장. `trade_history` 테이블에 매수/매도 체결만 기록
- **복구**: `_restore_positions_from_db()` -- trade_history + KIS API로 재구성 (모드/SL/TP 추정)

### Position 데이터

```
ticker, name, qty, buy_price, atr
sl, tp1, tp2, mode, max_hold_days
entry_date, expiry_date, t1_done, qty_t1
realized_pnl, po_type, excg_cd, sell_fail_count
high_since_entry  # 진입 이후 최고가 (트레일링 SL 기준)
```

## 종료 처리

- `__main__.py stop` -> SIGINT -> Python `KeyboardInterrupt` -> `finally` 블록 -> `_save_positions(swing_only=True)`
- `watchdog.sh stop` -> SIGINT + 15초 대기 + SIGKILL fallback -> 모니터/watchdog 정리
- `stop` 시 watchdog가 실행 중이면 경고 메시지 출력 (트레이더만 종료됨)
