#!/bin/bash
# Grid Search 최적화

cd /home/wye/workspace/blsh

# KIS_ENV: 환경변수 우선, 없으면 .env 로드, 기본값 demo
if [ -z "$KIS_ENV" ] && [ -f "$HOME/.blsh/config/.env" ]; then
    KIS_ENV=$(grep -E '^KIS_ENV=' "$HOME/.blsh/config/.env" | cut -d= -f2 | tr -d '"' | tr -d "'" | xargs)
fi
KIS_ENV="${KIS_ENV:-demo}"
LOG_DIR="$HOME/.blsh/${KIS_ENV}/logs"
mkdir -p "$LOG_DIR"

$HOME/.local/bin/uv run python -m wye.blsh.domestic.optimize.grid_search >> "$LOG_DIR/optimize.log" 2>&1
